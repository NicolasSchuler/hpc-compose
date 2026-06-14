mod support;

use std::fs;
use std::time::Duration;

use hpc_compose::job::{
    SubmissionBackend, build_submission_record_with_backend, load_submission_record,
    write_submission_record,
};
use serde_json::Value;
use support::*;

#[test]
fn test_command_requires_explicit_execution_mode() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &["test", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("choose --local or --submit"));
}

#[test]
fn test_submit_success_outputs_json_and_applies_time_override() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  api:
    image: {}
    command: /bin/true
  worker:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("smoke.sbatch");
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--time",
            "00:02:00",
            "--timeout",
            "30s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(true));
    assert_eq!(payload["backend"], Value::from("slurm"));
    assert_eq!(payload["job_id"], Value::from("12345"));
    assert_eq!(payload["services"].as_array().map(Vec::len), Some(2));
    let rendered = fs::read_to_string(script_out).expect("script");
    assert!(rendered.contains("#SBATCH --time=00:02:00"));
}

#[test]
fn test_submit_passes_scheduler_dependency_on_sbatch_cli() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
  after_job:
    id: "12345"
    condition: afterok
  dependency: singleton
services:
  api:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = tmpdir.path().join("sbatch-log-run");
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
script_path="${{!#}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path" >/dev/null 2>&1
echo "Submitted batch job 12345"
"#,
            sbatch_log.display(),
            tmpdir.path().display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "30s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(
        fs::read_to_string(sbatch_log)
            .expect("sbatch log")
            .contains("--dependency=afterok:12345,singleton")
    );
}

#[test]
fn test_local_success_outputs_json_and_tracks_local_backend() {
    if std::env::consts::OS != "linux" {
        return;
    }

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("local.sh");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--local",
            "--timeout",
            "10s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(true));
    assert_eq!(payload["backend"], Value::from("local"));
    assert_eq!(payload["services"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        payload["services"][0]["completed_successfully"],
        Value::from(true)
    );

    let job_id = payload["job_id"].as_str().expect("job id");
    let record = load_submission_record(&compose, Some(job_id)).expect("tracked record");
    assert_eq!(record.backend, SubmissionBackend::Local);
    assert_eq!(record.job_id, job_id);
    assert_eq!(
        load_submission_record(&compose, None)
            .expect("latest record")
            .job_id,
        job_id
    );
    assert!(script_out.exists());
}

#[test]
fn test_local_rejects_scheduler_dependency_before_launcher_render() {
    if std::env::consts::OS != "linux" {
        return;
    }

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "local-dependency.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
  dependency: singleton
services:
  app:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("local.sh");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--local",
            "--timeout",
            "10s",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--local does not support Slurm job dependencies"));
    assert!(!script_out.exists());
    assert!(load_submission_record(&compose, None).is_err());
}

#[test]
fn test_submit_fails_when_sbatch_output_has_no_trackable_job_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let sbatch = tmpdir.path().join("sbatch-no-job-id");
    write_script(
        &sbatch,
        r#"#!/bin/bash
set -euo pipefail
echo "queued without id"
"#,
    );
    let srun = write_fake_srun(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "30s",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    let combined = format!("{}{}", stdout_text(&output), stderr_text(&output));
    assert!(combined.contains("not trackable"), "{combined}");
    assert!(
        combined.contains("sbatch output did not include"),
        "{combined}"
    );
    assert!(load_submission_record(&compose, None).is_err());
}

#[test]
fn test_submit_reports_nonzero_service_exit() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: /bin/false
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "30s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(false));
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("app")
    );
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("complete successfully")
    );
}

#[test]
fn test_submit_reports_readiness_timeout() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: /bin/true
    readiness:
      type: log
      pattern: never-ready
      timeout_seconds: 1
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "30s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(false));
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("readiness")
    );
}

#[test]
fn test_submit_timeout_triggers_best_effort_cancel() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    let scancel_log = tmpdir.path().join("scancel.log");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "1s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(false));
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("timed out")
    );
    assert!(
        payload["phases"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|phase| phase["name"] == "terminal" && phase["status"] == "timeout")
    );
    assert_eq!(
        fs::read_to_string(scancel_log).expect("scancel log"),
        "12345\n"
    );
}

#[test]
fn test_submit_fails_when_ignored_service_did_not_complete() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  main:
    image: {}
    command: /bin/true
  sidecar:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: ignore
"#,
            tmpdir.path().join("cache").display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--timeout",
            "30s",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ok"], Value::from(false));
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("sidecar")
    );
    assert!(
        payload["failure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("complete successfully")
    );
}

#[test]
fn dev_rejects_missing_watch_path_before_launch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("dev.sh");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "dev",
            "--watch-paths",
            "missing-source",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("dev --watch-paths must point to an existing directory"));
    assert!(!script_out.exists());
    assert!(load_submission_record(&compose, None).is_err());
}

#[test]
fn dev_rejects_no_watchable_mounts_without_explicit_watch_path() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("dev.sh");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "dev",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("dev could not infer any watchable source directories"));
    assert!(!script_out.exists());
    assert!(load_submission_record(&compose, None).is_err());
}

#[test]
fn dev_ignores_cache_dir_volume_when_inferring_watch_targets() {
    if std::env::consts::OS != "linux" {
        return;
    }

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let src_dir = tmpdir.path().join("src");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    fs::create_dir_all(&src_dir).expect("src dir");
    fs::write(src_dir.join("main.py"), "print('ok')\n").expect("source");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "dev-cache.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: /bin/true
    volumes:
      - {}:/cache
      - ./src:/workspace
"#,
            cache_dir.display(),
            local_image.display(),
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    // `dev` keeps watching after launch; this test only needs to observe the
    // inferred watch roots before terminating the local process group.
    let output = run_cli_until_stdout_contains(
        tmpdir.path(),
        &[
            "dev",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
        "watching source directories",
        Duration::from_secs(10),
    );
    let stdout = stdout_text(&output);
    assert!(stdout.contains("watching source directories"));
    assert!(stdout.contains(&src_dir.display().to_string()));
    let watched_roots = stdout
        .lines()
        .skip_while(|line| !line.contains("watching source directories"))
        .skip(1)
        .take_while(|line| line.starts_with("  "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(watched_roots.contains(&src_dir.display().to_string()));
    assert!(!watched_roots.contains(&cache_dir.display().to_string()));
}

#[test]
fn tmux_uses_one_tail_pane_per_tracked_local_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  api:
    image: {}
    command: /bin/true
  worker:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let script_path = tmpdir.path().join("local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &plan,
        "local-test-123",
        SubmissionBackend::Local,
    )
    .expect("record");
    for log_path in record.service_logs.values() {
        fs::create_dir_all(log_path.parent().expect("log parent")).expect("log dir");
        fs::write(log_path, "ready\n").expect("log");
    }
    write_submission_record(&record).expect("write record");

    let tmux_log = tmpdir.path().join("tmux.log");
    let tmux = tmpdir.path().join("tmux");
    write_script(
        &tmux,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
case "${{1:-}}" in
  -V)
    echo "tmux 3.4"
    ;;
  has-session)
    exit 1
    ;;
  split-window)
    echo "%2"
    ;;
esac
"#,
            tmux_log.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "tmux",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "local-test-123",
            "--tmux-bin",
            tmux.to_str().expect("path"),
            "--no-attach",
            "--lines",
            "7",
        ],
    );
    assert_success(&output);
    let calls = fs::read_to_string(tmux_log).expect("tmux log");
    assert!(calls.contains("new-session"));
    assert!(calls.contains("split-window"));
    assert!(calls.contains("select-pane"));
    assert!(calls.contains("select-layout"));
    assert!(calls.contains("tail -n 7 -F"));
    assert!(calls.contains("api"));
    assert!(calls.contains("worker"));
}

#[test]
fn tmux_rejects_slurm_record_before_invoking_tmux() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit.sbatch"),
        &plan,
        "12345",
        SubmissionBackend::Slurm,
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    let tmux_log = tmpdir.path().join("tmux.log");
    let tmux = tmpdir.path().join("tmux");
    write_script(
        &tmux,
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
            tmux_log.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "tmux",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--tmux-bin",
            tmux.to_str().expect("path"),
            "--no-attach",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("tmux only supports tracked local jobs"));
    assert!(!tmux_log.exists());
}

#[test]
fn tmux_existing_session_does_not_split_duplicate_panes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  api:
    image: {}
    command: /bin/true
  worker:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("local.sh"),
        &plan,
        "local-test-existing",
        SubmissionBackend::Local,
    )
    .expect("record");
    for log_path in record.service_logs.values() {
        fs::create_dir_all(log_path.parent().expect("log parent")).expect("log dir");
        fs::write(log_path, "ready\n").expect("log");
    }
    write_submission_record(&record).expect("write record");

    let tmux_log = tmpdir.path().join("tmux-existing.log");
    let tmux = tmpdir.path().join("tmux-existing");
    write_script(
        &tmux,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
case "${{1:-}}" in
  -V)
    echo "tmux 3.4"
    ;;
  has-session)
    exit 0
    ;;
esac
"#,
            tmux_log.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "tmux",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "local-test-existing",
            "--tmux-bin",
            tmux.to_str().expect("path"),
            "--no-attach",
        ],
    );
    assert_success(&output);
    let calls = fs::read_to_string(tmux_log).expect("tmux log");
    assert!(calls.contains("-V"));
    assert!(calls.contains("has-session"));
    assert!(!calls.contains("new-session"));
    assert!(!calls.contains("split-window"));
}

#[test]
fn tmux_missing_binary_reports_actionable_error() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "x-slurm:\n  cache_dir: {}\nservices:\n  app:\n    image: {}\n    command: /bin/true\n",
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let script_path = tmpdir.path().join("local.sh");
    let record = build_submission_record_with_backend(
        &compose,
        tmpdir.path(),
        &script_path,
        &plan,
        "local-test-404",
        SubmissionBackend::Local,
    )
    .expect("record");
    for log_path in record.service_logs.values() {
        fs::create_dir_all(log_path.parent().expect("log parent")).expect("log dir");
        fs::write(log_path, "ready\n").expect("log");
    }
    write_submission_record(&record).expect("write record");

    let missing_tmux = tmpdir.path().join("missing-tmux");
    let output = run_cli(
        tmpdir.path(),
        &[
            "tmux",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "local-test-404",
            "--tmux-bin",
            missing_tmux.to_str().expect("path"),
            "--no-attach",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("failed to execute tmux binary"));
}
