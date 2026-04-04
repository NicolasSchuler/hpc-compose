use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;

use hpc_compose::planner::build_plan;
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
use hpc_compose::render::log_file_name_for_service;
use hpc_compose::spec::ComposeSpec;
use serde_json::Value;

fn bin_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_hpc-compose"))
}

fn run_cli(cwd: &Path, args: &[&str]) -> Output {
    Command::new(bin_path())
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run cli")
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("fake-enroot.sh");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name)
          name="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start)
    exit 0
    ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#,
    );
    path
}

fn write_fake_srun(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
exit 0
"#,
    );
    path
}

fn write_fake_sbatch(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
echo "Submitted batch job 12345"
"#,
    );
    path
}

fn write_fake_squeue(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("squeue");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
state="$(cat '{}' 2>/dev/null || true)"
case "$state" in
  ""|NONE)
    exit 0
    ;;
  *)
    echo "$state"
    ;;
esac
"#,
            state_file.display()
        ),
    );
    path
}

fn write_fake_sacct(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("sacct");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
state="$(cat '{}' 2>/dev/null || true)"
if [[ -n "$state" && "$state" != "NONE" ]]; then
  echo "$state"
fi
"#,
            state_file.display()
        ),
    );
    path
}

fn write_fake_watch_sbatch(
    tmpdir: &Path,
    squeue_state: &Path,
    sacct_state: &Path,
    terminal_state: &str,
    final_log_line: &str,
    gap_seconds: u64,
) -> PathBuf {
    let path = tmpdir.join(format!("sbatch-{}", terminal_state.to_lowercase()));
    let log_dir = tmpdir.join(".hpc-compose/12345/logs");
    let service_log = log_dir.join(log_file_name_for_service("app"));
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
mkdir -p '{}'
printf 'PENDING\n' > '{}'
rm -f '{}'
(
  sleep 1
  printf 'RUNNING\n' > '{}'
  printf 'booting\n' > '{}'
  sleep 1
  printf '{}\n' >> '{}'
  printf 'NONE\n' > '{}'
  sleep {}
  printf '{}\n' > '{}'
) >/dev/null 2>&1 &
echo "Submitted batch job 12345"
"#,
            log_dir.display(),
            squeue_state.display(),
            sacct_state.display(),
            squeue_state.display(),
            service_log.display(),
            final_log_line,
            service_log.display(),
            squeue_state.display(),
            gap_seconds,
            terminal_state,
            sacct_state.display()
        ),
    );
    path
}

fn write_compose(tmpdir: &Path, name: &str, body: &str) -> PathBuf {
    let path = tmpdir.join(name);
    fs::write(&path, body).expect("write compose");
    path
}

fn runtime_plan(path: &Path) -> RuntimePlan {
    let spec = ComposeSpec::load(path).expect("load spec");
    let plan = build_plan(path, spec).expect("build plan");
    build_runtime_plan(&plan)
}

fn safe_cache_dir() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
    fs::create_dir_all(&root).expect("cache root");
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(root)
        .expect("cache tempdir")
}

fn write_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir,
        "compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install --no-cache-dir click
"#,
            cache_dir.display()
        ),
    )
}

fn write_mount_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::create_dir_all(tmpdir.join("deps")).expect("deps dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir,
        "compose-mount.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install --no-cache-dir click
        mounts:
          - ./deps:/deps
"#,
            cache_dir.display()
        ),
    )
}

fn write_env_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    write_compose(
        tmpdir,
        "compose-env.yaml",
        &format!(
            r#"
name: env-demo
x-slurm:
  job_name: env-demo
  time: "00:10:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    environment:
      SECRET_TOKEN: super-secret
    command:
      - python
      - -c
      - print("hi")
"#,
            cache_dir.display()
        ),
    )
}

#[test]
fn validate_and_render_commands_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_success(&validate);
    assert!(stdout_text(&validate).contains("spec is valid"));

    let script_path = tmpdir.path().join("job.sbatch");
    let render = run_cli(
        tmpdir.path(),
        &[
            "render",
            "-f",
            compose.to_str().expect("path"),
            "--output",
            script_path.to_str().expect("path"),
        ],
    );
    assert_success(&render);
    let script = fs::read_to_string(&script_path).expect("script");
    assert!(script.contains("#SBATCH --job-name=demo"));
    assert!(script.contains("--container-image="));
}

#[test]
fn inspect_and_preflight_commands_cover_dev_workflow() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_mount_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let inspect = run_cli(
        tmpdir.path(),
        &["inspect", "-f", compose.to_str().expect("path")],
    );
    assert_success(&inspect);
    assert!(
        stdout_text(&inspect)
            .contains("rebuild on submit because x-enroot.prepare.mounts are present")
    );

    let preflight = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&preflight);
    let preflight_stderr = stderr_text(&preflight);
    assert!(preflight_stderr.contains("Summary:"));
    assert!(preflight_stderr.contains("Passed checks:"));
    assert!(preflight_stderr.contains("srun reports Pyxis container support"));
    assert!(preflight_stderr.contains("cache directory is writable"));

    let strict = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--strict",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&strict);
    assert!(stderr_text(&strict).contains("preflight reported warnings"));
}

#[test]
fn prepare_and_cache_commands_manage_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let enroot = write_fake_enroot(tmpdir.path());

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);
    assert!(stdout_text(&prepare).contains("BUILD service 'app' runtime image"));
    assert!(plan.ordered_services[0].runtime_image.exists());
    assert!(
        hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists()
    );

    let list = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "list",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&list);
    let list_stdout = stdout_text(&list);
    assert!(list_stdout.contains("prepared"));
    assert!(list_stdout.contains("base"));

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
        ],
    );
    assert_success(&inspect);
    let inspect_stdout = stdout_text(&inspect);
    assert!(inspect_stdout.contains("manifest kind: prepared"));
    assert!(inspect_stdout.contains("current reuse expectation: cache hit"));

    for artifact in [
        hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image),
        hpc_compose::cache::manifest_path_for(&hpc_compose::prepare::base_image_path(
            &plan.cache_dir,
            &plan.ordered_services[0],
        )),
    ] {
        let mut manifest: Value =
            serde_json::from_str(&fs::read_to_string(&artifact).expect("manifest")).expect("json");
        manifest["created_at"] = Value::from(1_u64);
        manifest["last_used_at"] = Value::from(1_u64);
        fs::write(
            &artifact,
            serde_json::to_vec_pretty(&manifest).expect("serialize"),
        )
        .expect("rewrite manifest");
    }

    let prune = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--age",
            "1",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&prune);
    assert!(stdout_text(&prune).contains("removed: 2"));
    assert!(!plan.ordered_services[0].runtime_image.exists());
}

#[test]
fn cache_prune_argument_validation_and_all_unused_path_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose_a = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let plan_a = runtime_plan(&compose_a);

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose_a.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let no_strategy = run_cli(tmpdir.path(), &["cache", "prune"]);
    assert_failure(&no_strategy);
    assert!(stderr_text(&no_strategy).contains("requires either --age DAYS or --all-unused"));

    let invalid_combo = run_cli(
        tmpdir.path(),
        &["cache", "prune", "--age", "1", "--all-unused"],
    );
    assert_failure(&invalid_combo);
    assert!(stderr_text(&invalid_combo).contains("only one strategy"));

    let compose_b = write_compose(
        tmpdir.path(),
        "compose-other.yaml",
        &format!(
            r#"
name: other
x-slurm:
  cache_dir: {}
services:
  redis:
    image: redis:7
"#,
            cache_dir.display()
        ),
    );

    let prune_unused = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--all-unused",
            "-f",
            compose_b.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&prune_unused);
    assert!(stdout_text(&prune_unused).contains("removed: 2"));
    assert!(!plan_a.ordered_services[0].runtime_image.exists());
}

#[test]
fn submit_command_runs_end_to_end_with_fake_tools() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_stdout = stdout_text(&submit);
    assert!(submit_stdout.contains("Submitted batch job 12345"));
    assert!(submit_stdout.contains("rendered script:"));
    assert!(submit_stdout.contains("log  service 'app':"));
    assert!(script_out.exists());
    assert!(plan.ordered_services[0].runtime_image.exists());
}

#[test]
fn submit_skip_prepare_reuses_existing_artifact() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(!stdout_text(&submit).contains("BUILD service 'app' runtime image"));
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
}

#[test]
fn submit_succeeds_when_tracking_metadata_cannot_be_written() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_root = tmpdir.path().join("readonly-compose");
    fs::create_dir_all(&compose_root).expect("compose root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&compose_root, &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let mut perms = fs::metadata(&compose_root).expect("meta").permissions();
    perms.set_mode(0o555);
    fs::set_permissions(&compose_root, perms).expect("chmod readonly");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    let mut restore = fs::metadata(&compose_root).expect("meta").permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&compose_root, restore).expect("chmod restore");

    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
    assert!(stdout_text(&submit).contains("tracking metadata could not be written"));
    assert!(
        stderr_text(&submit)
            .contains("warning: job submitted, but failed to write tracking metadata")
    );
    assert!(!compose_root.join(".hpc-compose/latest.json").exists());
}

#[test]
fn status_and_logs_commands_use_submission_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let metadata = tmpdir.path().join(".hpc-compose/latest.json");
    assert!(metadata.exists());

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "alpha\nbeta\n").expect("log");
    let batch_log = tmpdir.path().join("slurm-12345.out");
    fs::write(&batch_log, "batch-line\n").expect("batch log");

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let status_stdout = stdout_text(&status);
    assert!(status_stdout.contains("job id: 12345"));
    assert!(status_stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(status_stdout.contains("compose file:"));
    assert!(status_stdout.contains("batch log:"));
    assert!(status_stdout.contains("log  service 'app':"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_json);
    let value: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status json");
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    assert_eq!(value["scheduler"]["state"], Value::from("COMPLETED"));
    assert!(
        value["record"]["batch_log"]
            .as_str()
            .unwrap_or_default()
            .ends_with("slurm-12345.out")
    );

    let logs = run_cli(
        tmpdir.path(),
        &[
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--lines",
            "1",
        ],
    );
    assert_success(&logs);
    assert!(stdout_text(&logs).contains("[app] beta"));
}

#[test]
fn status_reports_missing_record_cleanly() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let status = run_cli(
        tmpdir.path(),
        &["status", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&status);
    assert!(stderr_text(&status).contains("no tracked submission metadata exists"));
}

#[test]
fn submit_watch_covers_completed_and_failed_states() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());

    let success_squeue_state = tmpdir.path().join("watch-success-squeue.state");
    let success_sacct_state = tmpdir.path().join("watch-success-sacct.state");
    let success_squeue = write_fake_squeue(tmpdir.path(), &success_squeue_state);
    let success_sacct = write_fake_sacct(tmpdir.path(), &success_sacct_state);
    let success_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &success_squeue_state,
        &success_sacct_state,
        "COMPLETED",
        "ready",
        2,
    );

    let success = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--watch",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            success_sbatch.to_str().expect("path"),
            "--squeue-bin",
            success_squeue.to_str().expect("path"),
            "--sacct-bin",
            success_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&success);
    let success_stdout = stdout_text(&success);
    assert!(success_stdout.contains("watching job 12345"));
    assert!(!success_stdout.contains("scheduler state: unknown (local-only)"));
    assert!(success_stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(success_stdout.contains("[app] ready"));

    let failure_squeue_state = tmpdir.path().join("watch-failure-squeue.state");
    let failure_sacct_state = tmpdir.path().join("watch-failure-sacct.state");
    let failure_squeue = write_fake_squeue(tmpdir.path(), &failure_squeue_state);
    let failure_sacct = write_fake_sacct(tmpdir.path(), &failure_sacct_state);
    let failure_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &failure_squeue_state,
        &failure_sacct_state,
        "FAILED",
        "boom",
        0,
    );

    let failure = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--watch",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            failure_sbatch.to_str().expect("path"),
            "--squeue-bin",
            failure_squeue.to_str().expect("path"),
            "--sacct-bin",
            failure_sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failure);
    assert!(stdout_text(&failure).contains("[app] boom"));
    assert!(stderr_text(&failure).contains("finished in scheduler state FAILED"));
}

#[test]
fn logs_follow_streams_appended_lines() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "start\n").expect("log");

    let mut child = Command::new(bin_path())
        .current_dir(tmpdir.path())
        .args([
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--follow",
            "--lines",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn logs");

    thread::sleep(Duration::from_millis(250));
    let mut file = OpenOptions::new()
        .append(true)
        .open(&log_path)
        .expect("open log");
    writeln!(file, "follow-line").expect("append");
    file.flush().expect("flush");
    thread::sleep(Duration::from_millis(1400));
    child.kill().expect("kill");
    let output = child.wait_with_output().expect("wait");
    assert!(String::from_utf8_lossy(&output.stdout).contains("[app] follow-line"));
}

#[test]
fn inspect_json_preflight_json_and_init_cover_new_modes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_env_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let inspect_verbose = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect_verbose);
    let inspect_verbose_stdout = stdout_text(&inspect_verbose);
    assert!(inspect_verbose_stdout.contains("environment keys: SECRET_TOKEN"));
    assert!(!inspect_verbose_stdout.contains("super-secret"));
    assert!(inspect_verbose_stdout.contains("effective srun args:"));

    let inspect_json = run_cli(
        tmpdir.path(),
        &["inspect", "-f", compose.to_str().expect("path"), "--json"],
    );
    assert_success(&inspect_json);
    assert!(stdout_text(&inspect_json).contains("super-secret"));

    let preflight_json = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&preflight_json);
    let preflight_value: Value =
        serde_json::from_str(&stdout_text(&preflight_json)).expect("preflight json");
    assert!(
        preflight_value["summary"]["passed_checks"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(preflight_value["passed_checks"].is_array());

    for template in [
        "dev-python-app",
        "app-redis-worker",
        "llm-curl-workflow",
        "llm-curl-workflow-workdir",
        "llama-app",
    ] {
        let output = tmpdir.path().join(format!("{template}.yaml"));
        let init = run_cli(
            tmpdir.path(),
            &[
                "init",
                "--template",
                template,
                "--name",
                "custom-init",
                "--cache-dir",
                "/tmp/custom-cache",
                "--output",
                output.to_str().expect("path"),
                "--force",
            ],
        );
        assert_success(&init);
        assert!(output.exists());
        let validate = run_cli(
            tmpdir.path(),
            &["validate", "-f", output.to_str().expect("path")],
        );
        assert_success(&validate);
        let rendered = fs::read_to_string(&output).expect("rendered template");
        assert!(rendered.contains("name: custom-init"));
        assert!(rendered.contains("job_name: custom-init"));
        assert!(rendered.contains("cache_dir: /tmp/custom-cache"));
        assert!(stdout_text(&init).contains("hpc-compose submit --watch -f"));
    }
}
