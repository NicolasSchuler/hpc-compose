mod support;

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use hpc_compose::job::{
    build_submission_record, latest_record_path_for, load_submission_record,
    write_submission_record,
};
use hpc_compose::render::log_file_name_for_service;
use serde_json::Value;
use support::*;

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
fn submit_restart_on_failure_restarts_once_and_status_reports_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    let app = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "app")
        .expect("app service");
    assert_eq!(app["failure_policy_mode"], "restart_on_failure");
    assert_eq!(app["restart_count"], 1);
    assert_eq!(app["max_restarts"], 3);
    assert_eq!(app["window_seconds"], 60);
    assert_eq!(app["max_restarts_in_window"], 3);
    assert_eq!(app["restart_failures_in_window"], 1);
    assert_eq!(app["last_exit_code"], 0);
}

#[test]
fn submit_ignore_policy_allows_job_success_with_failed_sidecar() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
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
            local_image.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    let sidecar = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "sidecar")
        .expect("sidecar service");
    assert_eq!(sidecar["failure_policy_mode"], "ignore");
    assert_eq!(sidecar["last_exit_code"], 42);
}

#[test]
fn submit_restart_on_failure_exhausted_retries_fails_job() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  flaky:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 1
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let combined = format!("{}\n{}", stdout_text(&submit), stderr_text(&submit));
    assert!(combined.contains("after 1/1 restarts"));
}

#[test]
fn submit_restart_on_failure_window_limit_blocks_crash_loop() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  loopy:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 5
        backoff_seconds: 1
        window_seconds: 60
        max_restarts_in_window: 2
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy_plan(
        tmpdir.path(),
        "hpc-compose:loopy",
        &[(41, 0), (41, 0), (41, 0)],
    );
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let combined = format!("{}\n{}", stdout_text(&submit), stderr_text(&submit));
    assert!(combined.contains("after 2/2 restart-triggering exits in 60s"));
}

#[test]
fn submit_restart_on_failure_window_ages_out_failures() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  spaced:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 5
        backoff_seconds: 1
        window_seconds: 2
        max_restarts_in_window: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy_plan(
        tmpdir.path(),
        "hpc-compose:spaced",
        &[(51, 0), (52, 2), (0, 2)],
    );
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    let spaced = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "spaced")
        .expect("spaced service");
    assert_eq!(spaced["restart_count"], 2);
    assert_eq!(spaced["max_restarts"], 5);
    assert_eq!(spaced["window_seconds"], 2);
    assert_eq!(spaced["max_restarts_in_window"], 1);
    assert_eq!(spaced["restart_failures_in_window"], 0);
    assert_eq!(spaced["last_exit_code"], 0);
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
    assert!(status_stdout.contains("Scheduler:"));
    assert!(status_stdout.contains("  state: COMPLETED (sacct)"));
    assert!(status_stdout.contains("Runtime:"));
    assert!(status_stdout.contains("  compose file:"));
    assert!(status_stdout.contains("  batch log:"));
    assert!(status_stdout.contains("  log  service 'app':"));
    assert!(!status_stdout.contains("pending reason:"));
    assert!(!status_stdout.contains("eligible time:"));
    assert!(!status_stdout.contains("start time:"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--format",
            "json",
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
    assert!(value.get("queue_diagnostics").is_none());
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
fn status_reports_pending_queue_diagnostics_in_text_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("pending-squeue.state");
    let sacct_state = tmpdir.path().join("pending-sacct.state");
    fs::write(
        &squeue_state,
        "STATE=PENDING\nREASON=Priority\nSTART=2026-04-07T12:34:56\n",
    )
    .expect("squeue state");
    fs::write(
        &sacct_state,
        "STATE=PENDING\nELIGIBLE=2026-04-07T10:00:00\nSTART=Unknown\nREASON=Priority\n",
    )
    .expect("sacct state");
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
    assert!(status_stdout.contains("  state: PENDING (squeue)"));
    assert!(status_stdout.contains("  pending reason: Priority"));
    assert!(status_stdout.contains("  eligible time: 2026-04-07T10:00:00"));
    assert!(status_stdout.contains("  start time: 2026-04-07T12:34:56"));
    assert!(status_stdout.contains("Runtime:"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_json);
    let value: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status json");
    assert_eq!(value["scheduler"]["state"], Value::from("PENDING"));
    assert_eq!(
        value["queue_diagnostics"]["pending_reason"],
        Value::from("Priority")
    );
    assert_eq!(
        value["queue_diagnostics"]["eligible_time"],
        Value::from("2026-04-07T10:00:00")
    );
    assert_eq!(
        value["queue_diagnostics"]["start_time"],
        Value::from("2026-04-07T12:34:56")
    );
    assert_eq!(value["record"]["job_id"], "12345");
}

#[test]
fn stats_command_reports_live_step_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-squeue.state");
    let sacct_state = tmpdir.path().join("stats-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.batch|1|00:00:01|1M|1M|cpu=1|cpu=00:00:01
12345.0|1|00:00:10|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
12345.extern|1|00:00:01|1M|1M|cpu=1|cpu=00:00:01
12345.1|2|00:00:20|256M|512M|cpu=2,mem=8G|cpu=00:00:20
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

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

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let stdout = stdout_text(&stats);
    assert!(stdout.contains("job id: 12345"));
    assert!(stdout.contains("stats source: sstat"));
    assert!(stdout.contains("step: 12345.0"));
    assert!(stdout.contains("step: 12345.1"));
    assert!(stdout.contains("gpu util: 65"));
    assert!(!stdout.contains("12345.batch"));
    assert!(!stdout.contains("12345.extern"));

    let stats_json = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_json);
    let value: Value = serde_json::from_str(&stdout_text(&stats_json)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["available"], Value::from(true));
    assert_eq!(value["source"], Value::from("sstat"));
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    let steps = value["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["gpu_util"], Value::from("65"));
    assert_eq!(steps[0]["gpu_mem"], Value::from("1024M"));
    assert_eq!(steps[0]["gpu_count"], Value::from("1"));
}

#[test]
fn stats_command_supports_jsonl_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-jsonl-squeue.state");
    let sacct_state = tmpdir.path().join("stats-jsonl-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("stats-jsonl.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.0|1|00:00:10|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
12345.1|2|00:00:20|256M|512M|cpu=2,mem=8G|cpu=00:00:20
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

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

    let stats_jsonl = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "jsonl",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_jsonl);

    let stdout = stdout_text(&stats_jsonl);
    let records = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("jsonl record"))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["record_type"], Value::from("summary"));
    assert_eq!(records[0]["job_id"], Value::from("12345"));
    assert_eq!(records[0]["stats_source"], Value::from("sstat"));
    assert_eq!(records[1]["record_type"], Value::from("step"));
    assert_eq!(records[1]["step"]["step_id"], Value::from("12345.0"));
    assert_eq!(records[2]["record_type"], Value::from("step"));
    assert_eq!(records[2]["step"]["step_id"], Value::from("12345.1"));
}

#[test]
fn stats_command_supports_csv_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-csv-squeue.state");
    let sacct_state = tmpdir.path().join("stats-csv-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("stats-csv.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.0|1|00:00:10|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

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

    let stats_csv = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "csv",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_csv);
    let stdout = stdout_text(&stats_csv);
    assert!(stdout.contains("job_id,scheduler_state,scheduler_source,stats_source"));
    assert!(stdout.contains("\"12345\",\"RUNNING\",\"squeue\",\"sstat\""));
}

#[test]
fn stats_command_prefers_sampler_metrics_when_present() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_metrics_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let squeue_state = tmpdir.path().join("sampler-squeue.state");
    let sacct_state = tmpdir.path().join("sampler-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sampler-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.0|1|00:00:11|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:11
",
    )
    .expect("sstat output");
    let _runtime_sstat = write_fake_sstat(tmpdir.path(), &sstat_output);
    let stats_sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let gpu_output = tmpdir.path().join("nvidia-smi-gpu.output");
    fs::write(
        &gpu_output,
        "0, GPU-aaaa, NVIDIA H100, 91, 77, 4096, 8192, 55, 220, 300\n",
    )
    .expect("gpu output");
    let gpu_processes = tmpdir.path().join("nvidia-smi-proc.output");
    fs::write(&gpu_processes, "GPU-aaaa, 4242, python, 2048\n").expect("gpu proc output");
    let _nvidia_smi = write_fake_nvidia_smi(tmpdir.path(), &gpu_output, &gpu_processes);

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

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["source"], Value::from("sampler"));
    assert_eq!(
        value["sampler"]["gpu"]["gpus"][0]["utilization_gpu"],
        Value::from("91")
    );
    assert_eq!(
        value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
    assert_eq!(value["steps"][0]["step_id"], Value::from("12345.0"));
    assert_eq!(value["steps"][0]["ave_cpu"], Value::from("00:00:11"));
    assert!(
        value["metrics_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/.hpc-compose/12345/metrics")
    );

    let stats_explicit = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_explicit);
    let explicit_value: Value =
        serde_json::from_str(&stdout_text(&stats_explicit)).expect("explicit stats json");
    assert_eq!(explicit_value["source"], Value::from("sampler"));
    assert_eq!(explicit_value["record"]["job_id"], Value::from("12345"));
    assert_eq!(
        explicit_value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
}

#[test]
fn stats_command_supports_explicit_job_id_without_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-explicit-squeue.state");
    let sacct_state = tmpdir.path().join("stats-explicit-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat-explicit.output");
    fs::write(
        &sstat_output,
        "67890.0|1|00:00:02|64M|128M|cpu=1,mem=1G|cpu=00:00:02\n",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let stats_text = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_text);
    assert_eq!(
        stdout_text(&stats_text)
            .matches("GPU accounting metrics are unavailable")
            .count(),
        1
    );

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("67890"));
    assert_eq!(value["available"], Value::from(true));
    assert!(value["record"].is_null());
}

#[test]
fn stats_command_reports_unavailable_for_pending_and_completed_jobs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sstat_output = tmpdir.path().join("sstat-empty.output");
    fs::write(&sstat_output, "").expect("empty sstat");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let pending_squeue_state = tmpdir.path().join("pending-squeue.state");
    let pending_sacct_state = tmpdir.path().join("pending-sacct.state");
    fs::write(&pending_squeue_state, "PENDING\n").expect("pending squeue");
    fs::write(&pending_sacct_state, "NONE\n").expect("pending sacct");
    let pending_squeue = write_fake_squeue(tmpdir.path(), &pending_squeue_state);
    let pending_sacct = write_fake_sacct(tmpdir.path(), &pending_sacct_state);
    let pending = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            pending_squeue.to_str().expect("path"),
            "--sacct-bin",
            pending_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&pending);
    let pending_value: Value = serde_json::from_str(&stdout_text(&pending)).expect("pending json");
    assert_eq!(pending_value["available"], Value::from(false));
    assert!(
        pending_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("not running yet")
    );

    let completed_squeue_state = tmpdir.path().join("completed-squeue.state");
    let completed_sacct_state = tmpdir.path().join("completed-sacct.state");
    fs::write(&completed_squeue_state, "NONE\n").expect("completed squeue");
    fs::write(&completed_sacct_state, "COMPLETED\n").expect("completed sacct");
    let completed_squeue = write_fake_squeue(tmpdir.path(), &completed_squeue_state);
    let completed_sacct = write_fake_sacct(tmpdir.path(), &completed_sacct_state);
    let completed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            completed_squeue.to_str().expect("path"),
            "--sacct-bin",
            completed_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&completed);
    let completed_value: Value =
        serde_json::from_str(&stdout_text(&completed)).expect("completed json");
    assert_eq!(completed_value["available"], Value::from(false));
    assert!(
        completed_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no longer running")
    );
}

#[test]
fn stats_command_surfaces_sstat_failures_and_malformed_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-fail-squeue.state");
    let sacct_state = tmpdir.path().join("stats-fail-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let failed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--sstat-bin",
            sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("sstat failed for job 42"));
    assert!(stderr_text(&failed).contains("job accounting unavailable"));

    let malformed_output = tmpdir.path().join("sstat-malformed.output");
    fs::write(&malformed_output, "12345.0|1|00:00:01\n").expect("malformed output");
    let sstat_bad = write_fake_sstat(tmpdir.path(), &malformed_output);
    let malformed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--sstat-bin",
            sstat_bad.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&malformed);
    assert!(stderr_text(&malformed).contains("malformed sstat output"));
}

#[test]
fn cancel_uses_tracked_or_explicit_job_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

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

    let cancel_latest = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_latest);
    assert!(stdout_text(&cancel_latest).contains("cancelled job: 12345"));

    let cancel_explicit = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_explicit);
    assert!(stdout_text(&cancel_explicit).contains("cancelled job: 67890"));

    let log = fs::read_to_string(scancel_log).expect("scancel log");
    assert!(log.contains("12345"));
    assert!(log.contains("67890"));
}

#[test]
fn cancel_reports_missing_record_and_scancel_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let missing = run_cli(
        tmpdir.path(),
        &["cancel", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&missing);
    assert!(stderr_text(&missing).contains("no tracked submission metadata exists"));

    let scancel_log = tmpdir.path().join("scancel-fail.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, false);
    let failed = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("scancel failed for job 42"));
    assert!(stderr_text(&failed).contains("permission denied"));
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
fn submit_watch_skips_when_job_id_is_not_trackable() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sbatch = tmpdir.path().join("sbatch-no-job-id");
    write_script(
        &sbatch,
        "#!/bin/bash\nset -euo pipefail\necho 'submitted without parsable id'\n",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--watch",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let stdout = stdout_text(&submit);
    assert!(stdout.contains("did not include a numeric Slurm job id"));
    assert!(stdout.contains("skipping watch because the submission is not trackable"));
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
fn submit_dry_run_skips_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("dry-run.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "--dry-run",
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
    assert_success(&output);
    let out = stdout_text(&output);
    assert!(out.contains("dry run: skipping sbatch submission"));
    assert!(!out.contains("Submitted batch job"));
    assert!(script_out.exists());
}

#[test]
fn submit_reports_script_write_errors_before_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let script_out = tmpdir.path().join("missing/script/out.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            write_fake_sbatch(tmpdir.path()).to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("failed to write rendered script"));
}

#[test]
fn artifacts_command_exports_collected_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

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

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    assert!(tracked_manifest.exists(), "artifact manifest should exist");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("success")
    );
    assert!(
        tracked_manifest_value["copied_relative_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item.as_str() == Some("metrics/meta.json"))
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("did not match any paths"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    let value: Value = serde_json::from_str(&stdout_text(&artifacts)).expect("artifacts json");
    assert!(
        value["export_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/results/12345")
    );
    assert!(
        value["exported_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .ends_with("/results/12345/metrics/meta.json"))
    );
    assert_eq!(
        fs::read_to_string(tmpdir.path().join("results/12345/metrics/meta.json"))
            .expect("exported"),
        fs::read_to_string(
            tmpdir
                .path()
                .join(".hpc-compose/12345/artifacts/payload/metrics/meta.json")
        )
        .expect("payload")
    );
}

#[test]
fn artifact_collection_handles_overlapping_paths_without_nested_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    let compose = write_compose(
        tmpdir.path(),
        "compose-artifacts-overlap.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  artifacts:
    collect: always
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/logs/app.log
      - /hpc-compose/job/logs
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

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

    let payload_root = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/payload/logs");
    assert!(payload_root.join("app.log").exists());
    assert!(!payload_root.join("logs/app.log").exists());

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    assert!(tmpdir.path().join("results/12345/logs/app.log").exists());
    assert!(
        !tmpdir
            .path()
            .join("results/12345/logs/logs/app.log")
            .exists()
    );
}

#[test]
fn artifact_collection_policy_skips_when_job_outcome_does_not_match() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "on_success");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun_failure(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());

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

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("failure")
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("does not match policy 'on_success'"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &["artifacts", "-f", compose.to_str().expect("path")],
    );
    assert_success(&artifacts);
    let out = stdout_text(&artifacts);
    assert!(out.contains("exported paths: 0"));
}

#[test]
fn submit_multi_node_mpi_example_pins_helper_and_tracks_allocation_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_example_compose(tmpdir.path(), "multi-node-mpi.yaml", &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun_log = tmpdir.path().join("srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch = write_fake_sbatch_runs_script_with_nodelist(
        tmpdir.path(),
        "sbatch-multi-node-mpi",
        "node01,node02",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "--no-preflight",
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

    let srun_text = fs::read_to_string(&srun_log).expect("srun log");
    assert!(srun_text.contains("--job-name=hpc-compose:bootstrap"));
    assert!(srun_text.contains("--nodes=1"));
    assert!(srun_text.contains("--ntasks=1"));
    assert!(srun_text.contains("--nodelist=node01"));
    assert!(srun_text.contains("--job-name=hpc-compose:mpi"));
    assert!(srun_text.contains("--nodes=2"));
    assert!(srun_text.contains("--ntasks-per-node=2"));
    assert!(srun_text.contains("env:node01|2|node01 node02|/hpc-compose/job/allocation/nodes.txt"));

    let state: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/state.json")).expect("state"),
    )
    .expect("state json");
    let services = state["services"].as_array().expect("services");
    let bootstrap = services
        .iter()
        .find(|service| service["service_name"] == "bootstrap")
        .expect("bootstrap state");
    let mpi = services
        .iter()
        .find(|service| service["service_name"] == "mpi")
        .expect("mpi state");
    assert_eq!(bootstrap["placement_mode"], "primary_node");
    assert_eq!(bootstrap["nodes"], 1);
    assert_eq!(bootstrap["nodelist"], "node01");
    assert_eq!(mpi["placement_mode"], "distributed");
    assert_eq!(mpi["nodes"], 2);
    assert_eq!(mpi["ntasks_per_node"], 2);
    assert_eq!(mpi["nodelist"], "node01 node02");
}

#[test]
fn inspect_and_submit_multi_node_torchrun_example_show_distributed_geometry() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_example_compose(tmpdir.path(), "multi-node-torchrun.yaml", &cache_dir);

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect);
    let inspect_text = stdout_text(&inspect);
    assert!(inspect_text.contains("allocation geometry: nodes=2"));
    assert!(inspect_text.contains("step geometry: mode=distributed nodes=2"));
    assert!(inspect_text.contains("--nodes=2"));
    assert!(inspect_text.contains("--ntasks-per-node=4"));

    let enroot = write_fake_enroot(tmpdir.path());
    let srun_log = tmpdir.path().join("torchrun-srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch = write_fake_sbatch_runs_script_with_nodelist(
        tmpdir.path(),
        "sbatch-multi-node-torchrun",
        "node01,node02",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "--no-preflight",
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

    let srun_text = fs::read_to_string(&srun_log).expect("srun log");
    assert!(srun_text.contains("--job-name=hpc-compose:trainer"));
    assert!(srun_text.contains("--nodes=2"));
    assert!(srun_text.contains("--ntasks-per-node=4"));
    assert!(!srun_text.contains("--nodelist=node01"));
    assert!(srun_text.contains("env:node01|2|node01 node02|/hpc-compose/job/allocation/nodes.txt"));
}

#[test]
fn clean_command_removes_old_job_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    // Submit a job to create tracking metadata
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
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");
    let runtime_dir = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(runtime_dir.join("logs")).expect("job runtime dir");
    fs::write(runtime_dir.join("logs/app.log"), "hello\n").expect("job log");

    // clean --all should keep the only tracked job.
    let clean_all = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean_all);
    let clean_all_payload: Value =
        serde_json::from_str(&stdout_text(&clean_all)).expect("clean all json");
    assert_eq!(clean_all_payload["removed_job_ids"], serde_json::json!([]));
    assert_eq!(
        clean_all_payload["kept_job_ids"],
        serde_json::json!(["12345"])
    );
    assert_eq!(
        clean_all_payload["latest_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(
        clean_all_payload["latest_job_id_after"],
        Value::from("12345")
    );
    assert!(runtime_dir.exists());
    assert!(tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());

    // clean --age 0 should remove the job because it is older than "now".
    let clean_age = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean_age);
    let clean_age_payload: Value =
        serde_json::from_str(&stdout_text(&clean_age)).expect("clean age json");
    assert_eq!(clean_age_payload["mode"], Value::from("age"));
    assert_eq!(clean_age_payload["dry_run"], Value::from(false));
    assert_eq!(
        clean_age_payload["removed_job_ids"],
        serde_json::json!(["12345"])
    );
    assert_eq!(clean_age_payload["kept_job_ids"], serde_json::json!([]));
    assert_eq!(
        clean_age_payload["latest_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(clean_age_payload["latest_job_id_after"], Value::Null);
    assert!(!tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());
    assert!(!runtime_dir.exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
}

#[test]
fn clean_text_reports_selected_jobs_and_kept_ids() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);

    let mut old_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-old.sbatch"),
        &plan,
        "11111",
    )
    .expect("old record");
    old_record.submitted_at = 1;
    write_submission_record(&old_record).expect("write old");

    let mut latest_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-latest.sbatch"),
        &plan,
        "22222",
    )
    .expect("latest record");
    latest_record.submitted_at = u64::MAX / 2;
    write_submission_record(&latest_record).expect("write latest");

    fs::create_dir_all(tmpdir.path().join(".hpc-compose/11111/logs")).expect("old runtime");
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/22222/logs")).expect("latest runtime");

    let clean = run_cli(
        tmpdir.path(),
        &["clean", "--age", "0", "-f", compose.to_str().expect("path")],
    );
    assert_success(&clean);
    let stdout = stdout_text(&clean);
    assert!(stdout.contains("mode: age"));
    assert!(stdout.contains("selected jobs: 1"));
    assert!(stdout.contains("selected ids: 11111"));
    assert!(stdout.contains("kept ids: 22222"));
    assert!(stdout.contains("removed 11111"));
}

#[test]
fn clean_all_preserves_latest_tracked_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let sbatch_first = tmpdir.path().join("sbatch-first");
    write_script(
        &sbatch_first,
        "#!/bin/bash\nset -euo pipefail\necho \"Submitted batch job 11111\"\n",
    );
    let first_submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch_first.to_str().expect("path"),
        ],
    );
    assert_success(&first_submit);
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/11111/logs")).expect("first job dir");

    let sbatch_second = tmpdir.path().join("sbatch-second");
    write_script(
        &sbatch_second,
        "#!/bin/bash\nset -euo pipefail\necho \"Submitted batch job 22222\"\n",
    );
    let second_submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch_second.to_str().expect("path"),
        ],
    );
    assert_success(&second_submit);
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/22222/logs")).expect("second job dir");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["11111"]));
    assert_eq!(payload["kept_job_ids"], serde_json::json!(["22222"]));
    assert_eq!(payload["latest_job_id_before"], Value::from("22222"));
    assert_eq!(payload["latest_job_id_after"], Value::from("22222"));
    assert!(!tmpdir.path().join(".hpc-compose/jobs/11111.json").exists());
    assert!(tmpdir.path().join(".hpc-compose/jobs/22222.json").exists());
    assert!(!tmpdir.path().join(".hpc-compose/11111").exists());
    assert!(tmpdir.path().join(".hpc-compose/22222").exists());

    let latest: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/latest.json")).expect("latest"),
    )
    .expect("latest json");
    assert_eq!(latest["job_id"], Value::from("22222"));
}

#[test]
fn clean_dry_run_does_not_remove_state_and_reports_json_contract() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            write_fake_sbatch(tmpdir.path()).to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");

    let runtime_dir = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(runtime_dir.join("logs")).expect("runtime dir");
    fs::write(runtime_dir.join("logs/app.log"), "hello\n").expect("runtime log");
    let record_path = tmpdir.path().join(".hpc-compose/jobs/12345.json");
    let latest_path = tmpdir.path().join(".hpc-compose/latest.json");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--dry-run",
            "--disk-usage",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(
        payload["compose_file"],
        Value::from(compose.display().to_string())
    );
    assert_eq!(payload["mode"], Value::from("age"));
    assert_eq!(payload["dry_run"], Value::from(true));
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["12345"]));
    assert_eq!(payload["kept_job_ids"], serde_json::json!([]));
    assert_eq!(payload["latest_job_id_before"], Value::from("12345"));
    assert_eq!(payload["latest_job_id_after"], Value::Null);
    assert!(payload["total_bytes_reclaimed"].as_u64().unwrap_or(0) > 0);
    let jobs = payload["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["job_id"], Value::from("12345"));
    assert_eq!(jobs[0]["selected"], Value::from(true));
    assert!(jobs[0]["bytes_reclaimed"].as_u64().unwrap_or(0) > 0);
    assert!(record_path.exists());
    assert!(runtime_dir.exists());
    assert!(latest_path.exists());
}

#[test]
fn clean_uses_recorded_submit_dir_for_runtime_cleanup() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_root = tmpdir.path().join("repo");
    let submit_root = tmpdir.path().join("submit-dir");
    fs::create_dir_all(&compose_root).expect("compose root");
    fs::create_dir_all(&submit_root).expect("submit root");

    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&compose_root, &cache_dir);
    let sbatch = write_fake_sbatch(&submit_root);

    let submit = run_cli(
        &submit_root,
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");

    let submit_runtime_dir = submit_root.join(".hpc-compose/12345");
    fs::create_dir_all(submit_runtime_dir.join("logs")).expect("runtime dir");
    fs::write(submit_runtime_dir.join("logs/app.log"), "hello\n").expect("runtime log");

    let clean = run_cli(
        &compose_root,
        &[
            "clean",
            "--age",
            "0",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["12345"]));
    assert!(!submit_runtime_dir.exists());
    assert!(!compose_root.join(".hpc-compose/jobs/12345.json").exists());
    assert!(!compose_root.join(".hpc-compose/latest.json").exists());
}

#[test]
fn clean_repairs_latest_pointer_and_removes_it_when_no_jobs_remain() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix time")
        .as_secs();

    let mut old_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-old.sbatch"),
        &plan,
        "11111",
    )
    .expect("old record");
    old_record.submitted_at = now.saturating_sub(10 * 86_400);
    write_submission_record(&old_record).expect("write old");

    let mut new_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-new.sbatch"),
        &plan,
        "22222",
    )
    .expect("new record");
    new_record.submitted_at = now.saturating_sub(1);
    write_submission_record(&new_record).expect("write new");

    fs::write(
        latest_record_path_for(&compose),
        serde_json::to_vec_pretty(&old_record).expect("stale latest"),
    )
    .expect("overwrite latest");

    let first_clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "7",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&first_clean);
    let first_payload: Value =
        serde_json::from_str(&stdout_text(&first_clean)).expect("first clean json");
    assert_eq!(
        first_payload["removed_job_ids"],
        serde_json::json!(["11111"])
    );
    assert_eq!(first_payload["latest_job_id_before"], Value::from("11111"));
    assert_eq!(first_payload["latest_job_id_after"], Value::from("22222"));
    let latest_after_first: Value = serde_json::from_str(
        &fs::read_to_string(latest_record_path_for(&compose)).expect("latest after first"),
    )
    .expect("latest json");
    assert_eq!(latest_after_first["job_id"], Value::from("22222"));

    let second_clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&second_clean);
    let second_payload: Value =
        serde_json::from_str(&stdout_text(&second_clean)).expect("second clean json");
    assert_eq!(
        second_payload["removed_job_ids"],
        serde_json::json!(["22222"])
    );
    assert_eq!(second_payload["latest_job_id_after"], Value::Null);
    assert!(!latest_record_path_for(&compose).exists());
}

#[test]
fn clean_requires_strategy_flag() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("write");

    let output = run_cli(
        tmpdir.path(),
        &["clean", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&output);
    let err = stderr_text(&output);
    assert!(
        err.contains("--age") || err.contains("--all"),
        "error should mention required flags: {err}"
    );
}
