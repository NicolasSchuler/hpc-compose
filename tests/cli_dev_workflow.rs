mod support;

use std::fs;

use hpc_compose::job::{
    SubmissionBackend, build_submission_record_with_backend, write_submission_record,
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
