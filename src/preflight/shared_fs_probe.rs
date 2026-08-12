//! Active Slurm-backed shared-filesystem probe protocol.

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::process_probe;
use crate::runtime_plan::RuntimePlan;
use crate::spec::ScratchScope;

const DEFAULT_FS_PROBE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_FS_PROBE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const FS_PROBE_TIMEOUT_ENV: &str = "HPC_COMPOSE_FS_PROBE_TIMEOUT_MS";
const FS_PROBE_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const FS_PROBE_CANCEL_TIMEOUT: Duration = Duration::from_secs(10);
const FS_PROBE_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SharedFsProbeTarget {
    pub(super) label: &'static str,
    pub(super) path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SharedFsProbeOutcome {
    pub(super) available_bytes: Option<u64>,
}

#[derive(Debug)]
struct SharedFsProbeSubmitError {
    detail: String,
    submitted_job_id: Option<String>,
}

pub(super) fn fs_probe_timeout_from_env() -> Duration {
    env::var(FS_PROBE_TIMEOUT_ENV)
        .ok()
        .as_deref()
        .and_then(parse_fs_probe_timeout_ms)
        .unwrap_or(DEFAULT_FS_PROBE_TIMEOUT)
}

fn parse_fs_probe_timeout_ms(raw: &str) -> Option<Duration> {
    let millis = raw.trim().parse::<u64>().ok()?;
    validate_fs_probe_timeout(Duration::from_millis(millis))
}

fn validate_fs_probe_timeout(timeout: Duration) -> Option<Duration> {
    (!timeout.is_zero() && timeout <= MAX_FS_PROBE_TIMEOUT).then_some(timeout)
}

pub(super) fn shared_fs_probe_targets(plan: &RuntimePlan) -> Vec<SharedFsProbeTarget> {
    let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut seen = BTreeSet::new();
    let mut targets = Vec::new();

    push_shared_fs_probe_target(
        &mut targets,
        &mut seen,
        "cache directory",
        plan.cache_dir.clone(),
    );

    if let Some(raw) = plan.slurm.runtime_root.as_deref() {
        push_shared_fs_probe_target(
            &mut targets,
            &mut seen,
            "runtime root",
            crate::path_util::absolute_path(Path::new(raw), &cwd),
        );
    }

    if let Some(raw) = plan.slurm.resume_dir() {
        push_shared_fs_probe_target(
            &mut targets,
            &mut seen,
            "resume directory",
            crate::path_util::absolute_path(Path::new(raw), &cwd),
        );
    }

    if let Some(scratch) = &plan.slurm.scratch
        && scratch.scope == ScratchScope::Shared
    {
        push_shared_fs_probe_target(
            &mut targets,
            &mut seen,
            "shared scratch",
            crate::path_util::absolute_path(Path::new(&scratch.base), &cwd),
        );
    }

    targets
}

fn push_shared_fs_probe_target(
    targets: &mut Vec<SharedFsProbeTarget>,
    seen: &mut BTreeSet<PathBuf>,
    label: &'static str,
    path: PathBuf,
) {
    if seen.insert(path.clone()) {
        targets.push(SharedFsProbeTarget { label, path });
    }
}

pub(super) fn run_shared_fs_probe(
    target: &SharedFsProbeTarget,
    sbatch_bin: &str,
    scancel_bin: &str,
    timeout: Duration,
) -> std::result::Result<SharedFsProbeOutcome, String> {
    fs::create_dir_all(&target.path).map_err(|err| {
        format!(
            "failed to create probe parent directory {}: {err}",
            target.path.display()
        )
    })?;

    let probe_id = shared_fs_probe_id();
    let probe_root = target
        .path
        .join(format!(".hpc-compose-fs-probe-{probe_id}"));
    fs::create_dir_all(&probe_root).map_err(|err| {
        format!(
            "failed to create probe directory {}: {err}",
            probe_root.display()
        )
    })?;

    let login_token = format!("login:{probe_id}");
    fs::write(
        probe_root.join("login-sentinel"),
        format!("{login_token}\n"),
    )
    .map_err(|err| {
        format!(
            "failed to write login sentinel in {}: {err}",
            probe_root.display()
        )
    })?;

    let script_path = probe_root.join("probe.sbatch");
    let script = render_shared_fs_probe_script(&probe_root, &login_token);
    crate::secure_io::write(&script_path, script, true).map_err(|err| {
        format!(
            "failed to write probe script {}: {err}",
            script_path.display()
        )
    })?;

    let result_path = probe_root.join("result.env");
    let started = Instant::now();
    let Some(deadline) = started.checked_add(timeout) else {
        return Err(format!(
            "shared filesystem probe timeout is too large; probe files left at {}",
            probe_root.display()
        ));
    };
    let job_id = match submit_shared_fs_probe(sbatch_bin, &script_path, deadline, timeout) {
        Ok(job_id) => job_id,
        Err(submit_error) => {
            let mut message = submit_error.detail;
            if let Some(job_id) = submit_error.submitted_job_id {
                append_probe_cancellation(&mut message, scancel_bin, &job_id);
            }
            if let Some(probe_message) = read_failed_probe_message(&probe_root) {
                message.push_str(&format!("; probe reported: {probe_message}"));
            }
            message.push_str(&format!("; probe files left at {}", probe_root.display()));
            return Err(message);
        }
    };

    while !result_path.exists() {
        if Instant::now() >= deadline {
            let mut message = format!(
                "shared filesystem probe job {job_id} timed out after {:.1}s",
                timeout.as_secs_f64()
            );
            append_probe_cancellation(&mut message, scancel_bin, &job_id);
            message.push_str(&format!("; probe files left at {}", probe_root.display()));
            return Err(message);
        }
        thread::sleep(
            FS_PROBE_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }

    let raw = fs::read_to_string(&result_path).map_err(|err| {
        format!(
            "failed to read probe result {}: {err}; probe files left at {}",
            result_path.display(),
            probe_root.display()
        )
    })?;
    let outcome = parse_shared_fs_probe_result(&raw).map_err(|err| {
        format!(
            "probe reported: {err}; probe files left at {}",
            probe_root.display()
        )
    })?;

    let compute_sentinel = probe_root.join("compute-sentinel");
    let compute_payload = fs::read_to_string(&compute_sentinel).map_err(|err| {
        format!(
            "login node could not read compute sentinel {}: {err}; probe files left at {}",
            compute_sentinel.display(),
            probe_root.display()
        )
    })?;
    if !compute_payload.starts_with("compute:") {
        return Err(format!(
            "compute sentinel had unexpected contents in {}; probe files left at {}",
            compute_sentinel.display(),
            probe_root.display()
        ));
    }

    let _ = fs::remove_dir_all(&probe_root);
    Ok(outcome)
}

fn submit_shared_fs_probe(
    sbatch_bin: &str,
    script_path: &Path,
    deadline: Instant,
    timeout: Duration,
) -> std::result::Result<String, SharedFsProbeSubmitError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(SharedFsProbeSubmitError {
            detail: format!(
                "shared filesystem sbatch probe timed out after {:.1}s at '{sbatch_bin}'",
                timeout.as_secs_f64()
            ),
            submitted_job_id: None,
        });
    }

    // Keep acceptance separate from result polling so the scheduler job ID is
    // available for bounded cleanup instead of delegating the wait to sbatch.
    let mut command = Command::new(sbatch_bin);
    command.arg("--parsable").arg(script_path);
    match process_probe::run(
        &mut command,
        "shared filesystem sbatch probe",
        process_probe::ProbeOptions {
            timeout: remaining,
            max_output_bytes: FS_PROBE_MAX_OUTPUT_BYTES,
        },
    ) {
        Ok(output) => {
            let submitted_job_id = parse_sbatch_job_id(&output.stdout);
            if !output.status.success() {
                return Err(SharedFsProbeSubmitError {
                    detail: format!(
                        "sbatch --parsable exited with {}{}",
                        output.status,
                        format_probe_output_detail("stderr", &output.stderr)
                    ),
                    submitted_job_id,
                });
            }
            submitted_job_id.ok_or_else(|| SharedFsProbeSubmitError {
                detail: format!(
                    "sbatch --parsable did not publish a numeric job ID{}",
                    format_probe_output_detail("stdout", &output.stdout)
                ),
                submitted_job_id: None,
            })
        }
        Err(err) => {
            let submitted_job_id = parse_sbatch_job_id(err.captured_stdout());
            let mut detail = err.detail();
            let stderr = format_probe_output_detail("stderr", err.captured_stderr());
            if !stderr.is_empty() {
                detail.push_str(&stderr);
            }
            let (stdout_truncated, stderr_truncated) = err.captured_output_truncated();
            if stdout_truncated || stderr_truncated {
                detail.push_str("; captured output was truncated");
            }
            Err(SharedFsProbeSubmitError {
                detail,
                submitted_job_id,
            })
        }
    }
}

fn append_probe_cancellation(message: &mut String, scancel_bin: &str, job_id: &str) {
    let mut command = Command::new(scancel_bin);
    command.arg(job_id);
    match process_probe::run(
        &mut command,
        "shared filesystem probe cancellation",
        process_probe::ProbeOptions {
            timeout: FS_PROBE_CANCEL_TIMEOUT,
            max_output_bytes: FS_PROBE_MAX_OUTPUT_BYTES,
        },
    ) {
        Ok(output) if output.status.success() => {
            message.push_str(&format!("; canceled submitted job {job_id}"));
        }
        Ok(output) => {
            message.push_str(&format!(
                "; failed to cancel submitted job {job_id}: scancel exited with {}{}",
                output.status,
                format_probe_output_detail("stderr", &output.stderr)
            ));
        }
        Err(err) => {
            message.push_str(&format!(
                "; failed to cancel submitted job {job_id}: {}",
                err.detail()
            ));
        }
    }
}

fn format_probe_output_detail(label: &str, bytes: &[u8]) -> String {
    let detail = String::from_utf8_lossy(bytes);
    let detail = detail.trim();
    if detail.is_empty() {
        String::new()
    } else {
        format!(": {label}: {detail}")
    }
}

fn parse_sbatch_job_id(stdout: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .find_map(parse_sbatch_job_id_line)
}

fn parse_sbatch_job_id_line(line: &str) -> Option<String> {
    let candidate = line
        .trim()
        .split_once(';')
        .map_or(line.trim(), |(id, _)| id);
    (!candidate.is_empty() && candidate.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| candidate.to_string())
}

fn read_failed_probe_message(probe_root: &Path) -> Option<String> {
    let raw = fs::read_to_string(probe_root.join("result.env")).ok()?;
    raw.lines()
        .find_map(|line| line.strip_prefix("message="))
        .map(str::to_string)
}

fn shared_fs_probe_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("{}-{millis}", std::process::id())
}

fn render_shared_fs_probe_script(probe_root: &Path, login_token: &str) -> String {
    let probe_root = crate::shell_quote::quote(&probe_root.display().to_string());
    let login_token = crate::shell_quote::quote(login_token);
    format!(
        r#"#!/bin/bash
#SBATCH --job-name=hpc-compose-fs-probe
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:01:00

set -euo pipefail

PROBE_ROOT={probe_root}
LOGIN_TOKEN={login_token}
RESULT="$PROBE_ROOT/result.env"

fail() {{
  tmp_result="$RESULT.tmp.$$"
  printf 'status=error\nmessage=%s\n' "$1" > "$tmp_result"
  mv "$tmp_result" "$RESULT"
  exit 1
}}

login_sentinel="$PROBE_ROOT/login-sentinel"
[[ -f "$login_sentinel" ]] || fail "login sentinel is not visible on the compute node"
observed="$(cat "$login_sentinel")"
[[ "$observed" == "$LOGIN_TOKEN" ]] || fail "login sentinel contents changed before compute read"

compute_tmp="$PROBE_ROOT/compute-sentinel.tmp"
compute_final="$PROBE_ROOT/compute-sentinel"
printf 'compute:%s\n' "${{SLURM_JOB_ID:-unknown}}" > "$compute_tmp"
mv "$compute_tmp" "$compute_final"
[[ -f "$compute_final" ]] || fail "compute-to-login rename target was not created"

rename_tmp="$PROBE_ROOT/rename.tmp"
rename_final="$PROBE_ROOT/rename.final"
printf 'rename-ok\n' > "$rename_tmp"
mv "$rename_tmp" "$rename_final"
[[ "$(cat "$rename_final")" == "rename-ok" ]] || fail "rename atomicity check read unexpected contents"

available_kb=""
if command -v df >/dev/null 2>&1; then
  available_kb="$(df -Pk "$PROBE_ROOT" 2>/dev/null | awk 'NR==2 {{print $4}}')" || available_kb=""
fi

tmp_result="$RESULT.tmp"
printf 'status=ok\navailable_kb=%s\n' "$available_kb" > "$tmp_result"
mv "$tmp_result" "$RESULT"
"#
    )
}

fn parse_shared_fs_probe_result(raw: &str) -> std::result::Result<SharedFsProbeOutcome, String> {
    let mut status = None;
    let mut message = None;
    let mut available_kb = None;

    for line in raw.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        match key {
            "status" => status = Some(value.to_string()),
            "message" => message = Some(value.to_string()),
            "available_kb" if !value.trim().is_empty() => {
                available_kb = value.trim().parse::<u64>().ok();
            }
            _ => {}
        }
    }

    match status.as_deref() {
        Some("ok") => Ok(SharedFsProbeOutcome {
            available_bytes: available_kb.and_then(|kb| kb.checked_mul(1024)),
        }),
        Some("error") => Err(message.unwrap_or_else(|| "probe reported an unknown error".into())),
        Some(other) => Err(format!("probe reported unexpected status '{other}'")),
        None => Err("probe result did not include a status".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::spec::{ResumeConfig, ScratchConfig, SlurmConfig};

    const PROBE_TEST_TIMEOUT: Duration = Duration::from_secs(10);

    fn runtime_plan(tmpdir: &Path) -> RuntimePlan {
        RuntimePlan {
            name: "demo".into(),
            cache_dir: tmpdir.join("cache"),
            runtime: crate::spec::RuntimeConfig::default(),
            slurm: SlurmConfig::default(),
            ordered_services: Vec::new(),
        }
    }

    fn write_fake_binary(path: &Path, body: &str) {
        fs::write(path, body).expect("write fake binary");
        let mut perms = fs::metadata(path).expect("meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn write_fake_sbatch_submit(path: &Path) {
        write_fake_binary(
            path,
            r#"#!/bin/bash
set -euo pipefail
script_path=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --parsable)
      shift
      ;;
    --*)
      shift
      ;;
    *)
      script_path="$1"
      shift
      ;;
  esac
done
if [[ -z "$script_path" ]]; then
  echo "missing script path" >&2
  exit 2
fi
export SLURM_JOB_ID=999
bash "$script_path" >/dev/null 2>&1 &
echo "999;test-cluster"
"#,
        );
    }

    #[test]
    fn shared_fs_probe_script_preserves_exact_bytes_and_shell_quoting() {
        let script = render_shared_fs_probe_script(
            Path::new("/shared/user's cache/.hpc-compose-fs-probe-fixture"),
            "login:fixture's-token",
        );

        assert_eq!(
            script,
            r#"#!/bin/bash
#SBATCH --job-name=hpc-compose-fs-probe
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:01:00

set -euo pipefail

PROBE_ROOT='/shared/user'"'"'s cache/.hpc-compose-fs-probe-fixture'
LOGIN_TOKEN='login:fixture'"'"'s-token'
RESULT="$PROBE_ROOT/result.env"

fail() {
  tmp_result="$RESULT.tmp.$$"
  printf 'status=error\nmessage=%s\n' "$1" > "$tmp_result"
  mv "$tmp_result" "$RESULT"
  exit 1
}

login_sentinel="$PROBE_ROOT/login-sentinel"
[[ -f "$login_sentinel" ]] || fail "login sentinel is not visible on the compute node"
observed="$(cat "$login_sentinel")"
[[ "$observed" == "$LOGIN_TOKEN" ]] || fail "login sentinel contents changed before compute read"

compute_tmp="$PROBE_ROOT/compute-sentinel.tmp"
compute_final="$PROBE_ROOT/compute-sentinel"
printf 'compute:%s\n' "${SLURM_JOB_ID:-unknown}" > "$compute_tmp"
mv "$compute_tmp" "$compute_final"
[[ -f "$compute_final" ]] || fail "compute-to-login rename target was not created"

rename_tmp="$PROBE_ROOT/rename.tmp"
rename_final="$PROBE_ROOT/rename.final"
printf 'rename-ok\n' > "$rename_tmp"
mv "$rename_tmp" "$rename_final"
[[ "$(cat "$rename_final")" == "rename-ok" ]] || fail "rename atomicity check read unexpected contents"

available_kb=""
if command -v df >/dev/null 2>&1; then
  available_kb="$(df -Pk "$PROBE_ROOT" 2>/dev/null | awk 'NR==2 {print $4}')" || available_kb=""
fi

tmp_result="$RESULT.tmp"
printf 'status=ok\navailable_kb=%s\n' "$available_kb" > "$tmp_result"
mv "$tmp_result" "$RESULT"
"#
        );
    }

    #[test]
    fn shared_fs_probe_targets_cover_shared_paths_and_deduplicate() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut plan = runtime_plan(tmpdir.path());
        let cache = plan.cache_dir.clone();
        let scratch = tmpdir.path().join("scratch");
        plan.slurm.runtime_root = Some(cache.display().to_string());
        plan.slurm.resume = Some(ResumeConfig {
            path: cache.display().to_string(),
        });
        plan.slurm.scratch = Some(ScratchConfig {
            scope: ScratchScope::Shared,
            base: scratch.display().to_string(),
            mount: "/scratch".to_string(),
            cleanup: Default::default(),
        });

        let targets = shared_fs_probe_targets(&plan);

        assert_eq!(targets.len(), 2, "deduplicated targets: {targets:#?}");
        assert_eq!(targets[0].label, "cache directory");
        assert_eq!(targets[0].path, cache);
        assert_eq!(targets[1].label, "shared scratch");
        assert_eq!(targets[1].path, scratch);
    }

    #[test]
    fn shared_fs_probe_runner_submits_parsable_probe_and_cleans_success() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let target = SharedFsProbeTarget {
            label: "cache directory",
            path: tmpdir.path().join("cache"),
        };
        let sbatch = tmpdir.path().join("sbatch");
        write_fake_sbatch_submit(&sbatch);

        let outcome = run_shared_fs_probe(
            &target,
            sbatch.to_str().expect("path"),
            "scancel",
            PROBE_TEST_TIMEOUT,
        )
        .expect("probe should pass");

        assert!(
            outcome.available_bytes.is_some_and(|bytes| bytes > 0),
            "compute-side df should report available bytes, got {outcome:#?}"
        );
        let leftovers = fs::read_dir(&target.path)
            .expect("read target")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            leftovers
                .iter()
                .all(|name| !name.starts_with(".hpc-compose-fs-probe-")),
            "successful probe should clean its probe directory, left: {leftovers:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_fs_probe_cancels_successful_submit_when_result_never_appears() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let target = SharedFsProbeTarget {
            label: "cache directory",
            path: tmpdir.path().join("cache"),
        };
        let sbatch = tmpdir.path().join("sbatch-no-result");
        let scancel = tmpdir.path().join("scancel");
        let scancel_log = tmpdir.path().join("scancel.log");
        write_fake_binary(
            &sbatch,
            "#!/bin/bash\nprintf '5150;test-cluster\\n'\nexit 0\n",
        );
        write_fake_binary(
            &scancel,
            &format!(
                "#!/bin/bash\nprintf '%s\\n' \"$*\" > {}\n",
                crate::shell_quote::quote(&scancel_log.display().to_string())
            ),
        );

        let err = run_shared_fs_probe(
            &target,
            sbatch.to_str().expect("path"),
            scancel.to_str().expect("path"),
            PROBE_TEST_TIMEOUT,
        )
        .expect_err("a submitted probe without a result must time out");

        assert_eq!(
            fs::read_to_string(&scancel_log).expect("scancel invocation"),
            "5150\n"
        );
        let probe_root = fs::read_dir(&target.path)
            .expect("probe target")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name().is_some_and(|name| {
                    name.to_string_lossy().starts_with(".hpc-compose-fs-probe-")
                })
            })
            .expect("timed-out probe evidence directory");
        assert_eq!(
            err,
            format!(
                "shared filesystem probe job 5150 timed out after 10.0s; canceled submitted job 5150; probe files left at {}",
                probe_root.display()
            )
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_fs_probe_preserves_compute_report_and_evidence_on_failure() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let target = SharedFsProbeTarget {
            label: "cache directory",
            path: tmpdir.path().join("cache"),
        };
        let sbatch = tmpdir.path().join("sbatch-compute-fail");
        write_fake_binary(
            &sbatch,
            r#"#!/bin/bash
set -euo pipefail
script_path="${@: -1}"
rm -f "$(dirname "$script_path")/login-sentinel"
export SLURM_JOB_ID=777
bash "$script_path" >/dev/null 2>&1 &
echo "777;test-cluster"
"#,
        );

        let err = run_shared_fs_probe(
            &target,
            sbatch.to_str().expect("path"),
            "scancel",
            PROBE_TEST_TIMEOUT,
        )
        .expect_err("compute-side probe failure must be reported");

        assert!(
            err.contains("login sentinel is not visible on the compute node"),
            "missing compute diagnostic: {err}"
        );
        assert!(
            err.contains("probe files left at"),
            "missing evidence: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_fs_probe_deadline_cancels_published_job_and_kills_submit_descendants() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let target = SharedFsProbeTarget {
            label: "cache directory",
            path: tmpdir.path().join("cache"),
        };
        let sbatch = tmpdir.path().join("sbatch-hang");
        let scancel = tmpdir.path().join("scancel");
        let scancel_log = tmpdir.path().join("scancel.log");
        let descendant_heartbeat = tmpdir.path().join("descendant-heartbeat");
        write_fake_binary(
            &sbatch,
            &format!(
                "#!/bin/bash\nset -euo pipefail\n(while true; do printf x >> {heartbeat}; sleep 0.05; done) &\nwhile [[ ! -s {heartbeat} ]]; do sleep 0.01; done\necho '999;test-cluster'\nsleep 30\n",
                heartbeat = crate::shell_quote::quote(&descendant_heartbeat.display().to_string())
            ),
        );
        write_fake_binary(
            &scancel,
            &format!(
                "#!/bin/bash\nprintf '%s\\n' \"$*\" > {}\n",
                crate::shell_quote::quote(&scancel_log.display().to_string())
            ),
        );

        let started = std::time::Instant::now();
        let err = run_shared_fs_probe(
            &target,
            sbatch.to_str().expect("path"),
            scancel.to_str().expect("path"),
            PROBE_TEST_TIMEOUT,
        )
        .expect_err("hung scheduler probe must time out");
        let elapsed = started.elapsed();

        assert!(
            elapsed < PROBE_TEST_TIMEOUT + Duration::from_secs(2),
            "probe ignored its client deadline and ran for {elapsed:?}"
        );
        assert!(err.contains("timed out"), "unexpected error: {err}");
        assert_eq!(
            fs::read_to_string(&scancel_log).unwrap_or_else(|read_err| panic!(
                "scancel invocation missing ({read_err}); {err}"
            )),
            "999\n",
            "the accepted allocation must be canceled by its parsed job ID"
        );
        let heartbeat_before = fs::metadata(&descendant_heartbeat)
            .expect("descendant heartbeat")
            .len();
        std::thread::sleep(Duration::from_millis(300));
        let heartbeat_after = fs::metadata(&descendant_heartbeat)
            .expect("descendant heartbeat after cleanup")
            .len();
        assert_eq!(
            heartbeat_after, heartbeat_before,
            "the timed-out sbatch process group left a heartbeat descendant alive"
        );
        assert!(
            err.contains("probe files left at"),
            "missing evidence path: {err}"
        );
        assert!(
            fs::read_dir(&target.path)
                .expect("probe target")
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".hpc-compose-fs-probe-")),
            "timed-out probe should retain its evidence directory"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shared_fs_probe_cancels_published_job_after_submit_output_limit() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let target = SharedFsProbeTarget {
            label: "cache directory",
            path: tmpdir.path().join("cache"),
        };
        let sbatch = tmpdir.path().join("sbatch-verbose");
        let scancel = tmpdir.path().join("scancel");
        let scancel_log = tmpdir.path().join("scancel.log");
        write_fake_binary(
            &sbatch,
            "#!/bin/bash\nset -euo pipefail\nprintf '999;test-cluster\\n'\nhead -c 1048576 /dev/zero\n",
        );
        write_fake_binary(
            &scancel,
            &format!(
                "#!/bin/bash\nprintf '%s\\n' \"$*\" > {}\n",
                crate::shell_quote::quote(&scancel_log.display().to_string())
            ),
        );

        let err = run_shared_fs_probe(
            &target,
            sbatch.to_str().expect("path"),
            scancel.to_str().expect("path"),
            PROBE_TEST_TIMEOUT,
        )
        .expect_err("oversized submit output must fail safely");

        assert!(err.contains("capture limit"), "unexpected error: {err}");
        assert_eq!(
            fs::read_to_string(scancel_log).expect("scancel invocation"),
            "999\n",
            "accepted probe allocation must be canceled even when submit output is oversized"
        );
    }

    #[test]
    fn parse_shared_fs_probe_result_reports_status_and_headroom() {
        let outcome =
            parse_shared_fs_probe_result("status=ok\navailable_kb=2048\n").expect("ok result");
        assert_eq!(outcome.available_bytes, Some(2 * 1024 * 1024));

        let err = parse_shared_fs_probe_result("status=error\nmessage=not visible\n")
            .expect_err("error result");
        assert_eq!(err, "not visible");
    }

    #[test]
    fn filesystem_probe_timeout_parsers_require_bounded_nonzero_values() {
        assert_eq!(
            parse_fs_probe_timeout_ms("250"),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_fs_probe_timeout_ms("0"), None);
        assert_eq!(parse_fs_probe_timeout_ms("not-a-number"), None);
        assert_eq!(
            parse_fs_probe_timeout_ms(&(MAX_FS_PROBE_TIMEOUT.as_millis() + 1).to_string()),
            None
        );
    }

    #[test]
    fn parsable_sbatch_output_accepts_job_id_and_optional_cluster_only() {
        assert_eq!(parse_sbatch_job_id(b"12345\n"), Some("12345".into()));
        assert_eq!(parse_sbatch_job_id(b"12345;site-a\n"), Some("12345".into()));
        assert_eq!(parse_sbatch_job_id(b"Submitted batch job 12345\n"), None);
        assert_eq!(parse_sbatch_job_id(b"--other-option\n"), None);
    }
}
