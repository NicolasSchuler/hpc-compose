use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;

use super::logs::LogCursor;
use super::runtime_state::ServiceRuntimeStateFile;
use super::scheduler::{
    build_batch_log_status, is_terminal_state, is_transitional_local_only,
    reconcile_scheduler_status, stats_unavailable_reason, unix_timestamp_now,
};
use super::stats::SlurmSampleRow;
use super::*;
use crate::planner::{ExecutionSpec, ImageSource, ServicePlacement};
use crate::prepare::RuntimeService;
use crate::spec::{ServiceFailurePolicy, ServiceSlurmConfig, SlurmConfig};

fn runtime_plan(tmpdir: &Path) -> RuntimePlan {
    RuntimePlan {
        name: "demo".into(),
        cache_dir: tmpdir.join("cache"),
        slurm: SlurmConfig::default(),
        ordered_services: vec![
            RuntimeService {
                name: "api".into(),
                runtime_image: tmpdir.join("api.sqsh"),
                execution: ExecutionSpec::Shell("echo api".into()),
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::Remote("docker://redis:7".into()),
            },
            RuntimeService {
                name: "worker".into(),
                runtime_image: tmpdir.join("worker.sqsh"),
                execution: ExecutionSpec::Shell("echo worker".into()),
                environment: Vec::new(),
                volumes: Vec::new(),
                working_dir: None,
                depends_on: Vec::new(),
                readiness: None,
                failure_policy: ServiceFailurePolicy::default(),
                placement: ServicePlacement::default(),
                slurm: ServiceSlurmConfig::default(),
                prepare: None,
                source: ImageSource::Remote("docker://python:3.11-slim".into()),
            },
        ],
    }
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

#[test]
fn submission_records_round_trip() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services: {}\n").expect("compose");
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &runtime_plan(tmpdir.path()),
        "12345",
    )
    .expect("persist");
    assert_eq!(record.job_id, "12345");
    assert!(jobs_dir_for(&compose).join("12345.json").exists());
    assert!(latest_record_path_for(&compose).exists());
    let loaded = load_submission_record(&compose, None).expect("latest");
    assert_eq!(loaded.job_id, "12345");
    assert_eq!(loaded.batch_log, tmpdir.path().join("slurm-12345.out"));
    assert_eq!(
        log_dir_for_record(&loaded),
        tmpdir.path().join(".hpc-compose/12345/logs")
    );
}

#[test]
fn defaults_and_path_helpers_cover_remaining_helpers() {
    let scheduler = SchedulerOptions::default();
    assert_eq!(scheduler.squeue_bin, "squeue");
    assert_eq!(scheduler.sacct_bin, "sacct");

    let stats = StatsOptions::default();
    assert_eq!(stats.sstat_bin, "sstat");
    assert_eq!(stats.scheduler.squeue_bin, "squeue");

    let spec_path = Path::new("compose.yaml");
    assert!(metadata_root_for(spec_path).ends_with(".hpc-compose"));
    assert!(jobs_dir_for(spec_path).ends_with(".hpc-compose/jobs"));
    assert!(latest_record_path_for(spec_path).ends_with(".hpc-compose/latest.json"));
    assert_eq!(scheduler_source_label(SchedulerSource::Squeue), "squeue");
    assert_eq!(scheduler_source_label(SchedulerSource::Sacct), "sacct");
    assert_eq!(
        scheduler_source_label(SchedulerSource::LocalOnly),
        "local-only"
    );

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.output = Some("logs/%j.out".into());
    let record = build_submission_record(
        &tmpdir.path().join("compose.yaml"),
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    assert_eq!(
        metrics_dir_for_record(&record),
        tmpdir.path().join(".hpc-compose/12345/metrics")
    );
    assert_eq!(
        artifacts_dir_for_record(&record),
        tmpdir.path().join(".hpc-compose/12345/artifacts")
    );
    assert_eq!(
        artifact_manifest_path_for_record(&record),
        tmpdir
            .path()
            .join(".hpc-compose/12345/artifacts/manifest.json")
    );
    assert_eq!(
        artifact_payload_dir_for_record(&record),
        tmpdir.path().join(".hpc-compose/12345/artifacts/payload")
    );
    assert_eq!(
        resolve_export_dir(&record.compose_file, "./results/${SLURM_JOB_ID}", "12345"),
        tmpdir.path().join("results/12345")
    );
    assert_eq!(
        resolve_export_dir(
            &record.compose_file,
            "/tmp/results/${SLURM_JOB_ID}",
            "12345"
        ),
        PathBuf::from("/tmp/results/12345")
    );

    let missing = build_batch_log_status(&tmpdir.path().join("missing.log"), unix_timestamp_now());
    assert!(!missing.present);
    let batch_log = tmpdir.path().join("slurm-12345.out");
    fs::write(&batch_log, "hello\n").expect("batch log");
    let present = build_batch_log_status(&batch_log, unix_timestamp_now());
    assert!(present.present);

    let fallback_record = SubmissionRecord {
        schema_version: 1,
        backend: SubmissionBackend::Slurm,
        kind: SubmissionKind::Main,
        job_id: "999".into(),
        submitted_at: 0,
        compose_file: tmpdir.path().join("compose.yaml"),
        submit_dir: tmpdir.path().to_path_buf(),
        script_path: tmpdir.path().join("job.sbatch"),
        cache_dir: tmpdir.path().join("cache"),
        batch_log,
        service_logs: BTreeMap::new(),
        artifact_export_dir: None,
        resume_dir: None,
        service_name: None,
        command_override: None,
        requested_walltime: None,
        config_snapshot_yaml: None,
        cached_artifacts: Vec::new(),
    };
    assert_eq!(
        log_dir_for_record(&fallback_record),
        tmpdir.path().join(".hpc-compose/999/logs")
    );
    let _ = current_user_name();
}

#[test]
fn scheduler_status_prefers_squeue_then_sacct() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    write_script(&squeue, "#!/bin/bash\necho RUNNING\n");
    write_script(&sacct, "#!/bin/bash\necho FAILED\n");
    let status = probe_scheduler_status(
        "42",
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    );
    assert_eq!(status.state, "RUNNING");
    assert_eq!(status.source, SchedulerSource::Squeue);

    write_script(&squeue, "#!/bin/bash\nexit 0\n");
    let status = probe_scheduler_status(
        "42",
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    );
    assert_eq!(status.state, "FAILED");
    assert_eq!(status.source, SchedulerSource::Sacct);
    assert!(status.failed);
}

#[test]
fn build_status_snapshot_and_log_selection_cover_additional_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");

    fs::create_dir_all(log_dir_for_record(&record)).expect("log dir");
    fs::write(&record.batch_log, "batch\n").expect("batch log");
    for path in record.service_logs.values() {
        fs::write(path, "line one\nline two\n").expect("service log");
    }
    let now = unix_timestamp_now();
    fs::write(
        tmpdir.path().join(".hpc-compose/12345/state.json"),
        format!(
            r#"{{
  "attempt": 1,
  "is_resume": true,
  "resume_dir": "/shared/runs/demo",
  "services": [
    {{
      "service_name": "api",
      "step_name": "hpc-compose:api",
      "log_path": "{}",
      "launch_index": 0,
      "launcher_pid": 4242,
      "healthy": true,
      "readiness_configured": true,
      "failure_policy_mode": "restart_on_failure",
      "restart_count": 1,
      "max_restarts": 3,
      "window_seconds": 60,
      "max_restarts_in_window": 3,
      "restart_failures_in_window": 2,
      "restart_failure_timestamps": [{}, {}],
      "last_exit_code": 0
    }},
    {{
      "service_name": "worker",
      "step_name": "hpc-compose:worker",
      "log_path": "{}",
      "launch_index": 1,
      "launcher_pid": 5252,
      "healthy": false,
      "readiness_configured": false,
      "last_exit_code": 42
    }}
  ]
}}"#,
            record.service_logs.get("api").expect("api log").display(),
            now.saturating_sub(10),
            now.saturating_sub(90),
            record
                .service_logs
                .get("worker")
                .expect("worker log")
                .display()
        ),
    )
    .expect("state");

    let parsed_state: ServiceRuntimeStateFile =
        read_json(&tmpdir.path().join(".hpc-compose/12345/state.json"))
            .expect("parse runtime state");
    assert_eq!(parsed_state.attempt, Some(1));

    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    write_script(&squeue, "#!/bin/bash\necho RUNNING\n");
    write_script(&sacct, "#!/bin/bash\nexit 0\n");

    let snapshot = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status snapshot");
    assert_eq!(snapshot.scheduler.state, "RUNNING");
    assert!(snapshot.queue_diagnostics.is_none());
    assert_eq!(snapshot.attempt, Some(1));
    assert_eq!(snapshot.is_resume, Some(true));
    assert_eq!(
        snapshot.resume_dir,
        Some(PathBuf::from("/shared/runs/demo"))
    );
    assert!(snapshot.batch_log.present);
    assert_eq!(snapshot.services.len(), 2);
    let api = snapshot
        .services
        .iter()
        .find(|service| service.service_name == "api")
        .expect("api");
    assert_eq!(
        api.failure_policy_mode.as_deref(),
        Some("restart_on_failure")
    );
    assert_eq!(api.restart_count, Some(1));
    assert_eq!(api.max_restarts, Some(3));
    assert_eq!(api.window_seconds, Some(60));
    assert_eq!(api.max_restarts_in_window, Some(3));
    assert_eq!(api.restart_failures_in_window, Some(1));
    assert_eq!(api.last_exit_code, Some(0));
    assert_eq!(api.step_name.as_deref(), Some("hpc-compose:api"));
    assert_eq!(api.launch_index, Some(0));
    assert_eq!(api.launcher_pid, Some(4242));
    assert_eq!(api.healthy, Some(true));
    assert_eq!(api.readiness_configured, Some(true));
    assert_eq!(api.status.as_deref(), Some("ready"));
    let worker = snapshot
        .services
        .iter()
        .find(|service| service.service_name == "worker")
        .expect("worker");
    assert!(worker.failure_policy_mode.is_none());
    assert!(worker.restart_count.is_none());
    assert!(worker.max_restarts.is_none());
    assert!(worker.window_seconds.is_none());
    assert!(worker.max_restarts_in_window.is_none());
    assert!(worker.restart_failures_in_window.is_none());
    assert_eq!(worker.last_exit_code, Some(42));
    assert_eq!(worker.step_name.as_deref(), Some("hpc-compose:worker"));
    assert_eq!(worker.launch_index, Some(1));
    assert_eq!(worker.launcher_pid, Some(5252));
    assert_eq!(worker.healthy, Some(false));
    assert_eq!(worker.readiness_configured, Some(false));
    assert_eq!(worker.status.as_deref(), Some("running"));

    let selected = selected_service_logs(&record, Some("api")).expect("selected");
    assert_eq!(selected.len(), 1);
    let err = selected_service_logs(&record, Some("missing")).expect_err("missing service");
    assert!(err.to_string().contains("service 'missing'"));

    print_logs(&record, Some("api"), 1, false).expect("print logs");
}

#[test]
fn status_snapshot_tolerates_missing_or_legacy_state_files() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    fs::create_dir_all(log_dir_for_record(&record)).expect("log dir");
    fs::write(&record.batch_log, "batch\n").expect("batch log");
    for path in record.service_logs.values() {
        fs::write(path, "line one\n").expect("service log");
    }
    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    write_script(&squeue, "#!/bin/bash\necho RUNNING\n");
    write_script(&sacct, "#!/bin/bash\nexit 0\n");

    let missing_state = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status missing state");
    assert!(
        missing_state
            .services
            .iter()
            .all(|service| service.failure_policy_mode.is_none()
                && service.restart_count.is_none()
                && service.max_restarts.is_none()
                && service.window_seconds.is_none()
                && service.max_restarts_in_window.is_none()
                && service.restart_failures_in_window.is_none()
                && service.last_exit_code.is_none())
    );

    fs::write(
        tmpdir.path().join(".hpc-compose/12345/state.json"),
        r#"{"services":[{"service_name":"api"}]}"#,
    )
    .expect("legacy state");
    let legacy_state = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status legacy state");
    let api = legacy_state
        .services
        .iter()
        .find(|service| service.service_name == "api")
        .expect("api");
    assert!(api.failure_policy_mode.is_none());
    assert!(api.restart_count.is_none());
    assert!(api.max_restarts.is_none());
    assert!(api.window_seconds.is_none());
    assert!(api.max_restarts_in_window.is_none());
    assert!(api.restart_failures_in_window.is_none());
    assert!(api.last_exit_code.is_none());
}

#[test]
fn build_status_snapshot_merges_queue_diagnostics_from_squeue_and_sacct() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");

    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    write_script(
        &squeue,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${*: -1}" == "%T|%r|%S" ]]; then
  echo "PENDING|Priority|2026-04-07T12:34:56"
else
  echo "PENDING"
fi
"#,
    );
    write_script(
        &sacct,
        r#"#!/bin/bash
set -euo pipefail
case "$*" in
  *"State,Eligible,Start,Reason"*)
    echo "PENDING|2026-04-07T10:00:00|Unknown|Priority"
    ;;
  *)
    echo "PENDING"
    ;;
esac
"#,
    );

    let snapshot = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status snapshot");
    assert_eq!(snapshot.scheduler.state, "PENDING");
    assert_eq!(
        snapshot.queue_diagnostics,
        Some(QueueDiagnostics {
            pending_reason: Some("Priority".into()),
            eligible_time: Some("2026-04-07T10:00:00".into()),
            start_time: Some("2026-04-07T12:34:56".into()),
        })
    );
}

#[test]
fn build_status_snapshot_reuses_one_squeue_snapshot_for_state_and_queue_details() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");

    let squeue = tmpdir.path().join("squeue");
    let squeue_calls = tmpdir.path().join("squeue.calls");
    let sacct = tmpdir.path().join("sacct");
    write_script(
        &squeue,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
count=0
if [[ -f "{calls}" ]]; then
  count="$(cat "{calls}")"
fi
count=$((count + 1))
printf '%s' "$count" > "{calls}"
if [[ "${{*: -1}}" == "%T|%r|%S" ]]; then
  if [[ "$count" -eq 1 ]]; then
    echo "PENDING|Priority|N/A"
  else
    echo "RUNNING|None|2026-04-07T12:34:56"
  fi
else
  echo "PENDING"
fi
"#,
            calls = squeue_calls.display()
        ),
    );
    write_script(&sacct, "#!/bin/bash\nexit 0\n");

    let snapshot = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status snapshot");
    assert_eq!(snapshot.scheduler.state, "PENDING");
    assert_eq!(
        snapshot.queue_diagnostics,
        Some(QueueDiagnostics {
            pending_reason: Some("Priority".into()),
            eligible_time: None,
            start_time: None,
        })
    );
    assert_eq!(
        fs::read_to_string(&squeue_calls).expect("squeue calls"),
        "1"
    );
}

#[test]
fn build_status_snapshot_waiting_for_scheduler_omits_queue_diagnostics() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");

    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    write_script(&squeue, "#!/bin/bash\nexit 0\n");
    write_script(&sacct, "#!/bin/bash\nexit 0\n");

    let snapshot = build_status_snapshot(
        &compose,
        None,
        &SchedulerOptions {
            squeue_bin: squeue.display().to_string(),
            sacct_bin: sacct.display().to_string(),
        },
    )
    .expect("status snapshot");
    assert_eq!(snapshot.scheduler.state, "WAITING_FOR_SCHEDULER");
    assert!(snapshot.queue_diagnostics.is_none());
}

#[test]
fn scheduler_status_grace_modes_cover_recent_submit_and_accounting_gap() {
    let unknown = SchedulerStatus {
        state: "unknown".into(),
        source: SchedulerSource::LocalOnly,
        terminal: false,
        failed: false,
        detail: Some("x".into()),
    };
    let recent = reconcile_scheduler_status(unknown.clone(), 100, None, 105);
    assert_eq!(recent.state, "WAITING_FOR_SCHEDULER");
    assert!(is_transitional_local_only(&recent));

    let accounting = reconcile_scheduler_status(unknown.clone(), 0, Some(200), 205);
    assert_eq!(accounting.state, "WAITING_FOR_ACCOUNTING");
    assert!(is_transitional_local_only(&accounting));

    let stale = reconcile_scheduler_status(unknown, 0, Some(0), 100);
    assert_eq!(stale.state, "unknown");
}

#[test]
fn batch_log_path_uses_default_and_slurm_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let default = batch_log_path_for(&runtime_plan(tmpdir.path()), tmpdir.path(), "77");
    assert_eq!(default, tmpdir.path().join("slurm-77.out"));

    let mut plan = runtime_plan(tmpdir.path());
    plan.name = "custom-name".into();
    plan.slurm.output = Some("logs/%x-%j.out".into());
    let custom = batch_log_path_for(&plan, tmpdir.path(), "77");
    assert_eq!(custom, tmpdir.path().join("logs/custom-name-77.out"));

    assert_eq!(
        expand_slurm_filename_pattern("logs/%u-%4j-%%-%x.out", "77", "custom-name", Some("alice")),
        "logs/alice-0077-%-custom-name.out"
    );
    assert_eq!(
        expand_slurm_filename_pattern("logs/%u-%j.out", "77", "custom-name", None),
        "logs/%u-77.out"
    );
}

#[test]
fn terminal_state_classification_keeps_requeue_states_active() {
    for state in [
        "REQUEUED",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
        "RESV_DEL_HOLD",
        "SPECIAL_EXIT",
        "STOPPED",
        "UPDATE_DB",
        "POWER_UP_NODE",
    ] {
        assert!(
            !is_terminal_state(state),
            "{state} should stay non-terminal"
        );
    }

    for state in ["COMPLETED", "FAILED", "PREEMPTED", "TIMEOUT"] {
        assert!(is_terminal_state(state), "{state} should stay terminal");
    }
}

#[test]
fn tail_and_follow_helpers_cover_missing_and_growth() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = tmpdir.path().join("service.log");
    fs::write(&log, "one\ntwo\nthree\n").expect("log");
    assert_eq!(tail_lines(&log, 2).expect("tail"), vec!["two", "three"]);
    assert!(
        tail_lines(&tmpdir.path().join("missing.log"), 10)
            .expect("missing")
            .is_empty()
    );

    let mut cursor = LogCursor {
        service_name: "svc".into(),
        path: log.clone(),
        offset: 0,
        pending: String::new(),
    };
    let lines = read_new_lines(&mut cursor).expect("initial");
    assert_eq!(lines, vec!["one", "two", "three"]);

    fs::write(&log, "one\ntwo\nthree\nfour").expect("append partial");
    let lines = read_new_lines(&mut cursor).expect("partial");
    assert!(lines.is_empty());
    fs::write(&log, "one\ntwo\nthree\nfour\n").expect("append newline");
    let lines = read_new_lines(&mut cursor).expect("newline");
    assert_eq!(lines, vec!["four"]);
}

#[test]
fn parse_sstat_output_keeps_only_numbered_steps_and_maps_gpu_fields() {
    let steps = parse_sstat_output(
            "12345",
            "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.batch|1|00:00:01|10M|10M|cpu=1,mem=10M|cpu=00:00:01
12345.0|1|00:00:03|128M|256M|cpu=1,mem=512M,gres/gpu:a100=2|cpu=00:00:03,gres/gpuutil=77,gres/gpumem=4096M
12345.extern|1|00:00:01|1M|1M|cpu=1|cpu=00:00:01
12345.1|2|00:00:05|64M|128M|cpu=2,mem=256M|cpu=00:00:05
",
        )
        .expect("steps");

    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_id, "12345.0");
    assert_eq!(steps[0].gpu_count.as_deref(), Some("2"));
    assert_eq!(steps[0].gpu_util.as_deref(), Some("77"));
    assert_eq!(steps[0].gpu_mem.as_deref(), Some("4096M"));
    assert_eq!(steps[1].step_id, "12345.1");
    assert_eq!(steps[1].gpu_util, None);
}

#[test]
fn parse_sstat_output_rejects_malformed_rows_and_tres_entries() {
    let err = parse_sstat_output("12345", "12345.0|1|00:00:01").expect_err("bad row");
    assert!(err.to_string().contains("malformed sstat output"));

    let err = parse_sstat_output(
        "12345",
        "12345.0|1|00:00:01|128M|256M|cpu=1,broken|cpu=00:00:01",
    )
    .expect_err("bad tres");
    assert!(err.to_string().contains("failed to parse AllocTRES"));
}

#[test]
fn load_sampler_snapshot_reads_latest_groups() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let metrics_dir = tmpdir.path().join("metrics");
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    fs::write(
            metrics_dir.join("meta.json"),
            r#"{
  "sampler_pid": 123,
  "interval_seconds": 5,
  "collectors": [
    {"name":"gpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:10Z"},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:10Z"}
  ]
}"#,
        )
        .expect("meta");
    fs::write(
            metrics_dir.join("gpu.jsonl"),
            concat!(
                "{\"sampled_at\":\"2026-04-05T10:00:00Z\",\"index\":\"0\",\"uuid\":\"GPU-old\",\"name\":\"Old\",\"utilization_gpu\":\"11\",\"utilization_memory\":\"22\",\"memory_used_mib\":\"10\",\"memory_total_mib\":\"20\",\"temperature_c\":\"30\",\"power_draw_w\":\"40\",\"power_limit_w\":\"50\"}\n",
                "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"index\":\"0\",\"uuid\":\"GPU-new\",\"name\":\"New\",\"utilization_gpu\":\"91\",\"utilization_memory\":\"77\",\"memory_used_mib\":\"4096\",\"memory_total_mib\":\"8192\",\"temperature_c\":\"55\",\"power_draw_w\":\"220\",\"power_limit_w\":\"300\"}\n"
            ),
        )
        .expect("gpu");
    fs::write(
            metrics_dir.join("gpu_processes.jsonl"),
            concat!(
                "{\"sampled_at\":\"2026-04-05T10:00:00Z\",\"gpu_uuid\":\"GPU-old\",\"pid\":\"1\",\"process_name\":\"old\",\"used_memory_mib\":\"10\"}\n",
                "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"gpu_uuid\":\"GPU-new\",\"pid\":\"4242\",\"process_name\":\"python\",\"used_memory_mib\":\"2048\"}\n"
            ),
        )
        .expect("gpu proc");
    fs::write(
            metrics_dir.join("slurm.jsonl"),
            concat!(
                "{\"sampled_at\":\"2026-04-05T10:00:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:01\",\"ave_rss\":\"10M\",\"max_rss\":\"10M\",\"alloc_tres\":\"cpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:01\"}\n",
                "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"step_id\":\"12345.1\",\"ntasks\":\"2\",\"ave_cpu\":\"00:00:11\",\"ave_rss\":\"512M\",\"max_rss\":\"1G\",\"alloc_tres\":\"cpu=2,gres/gpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:11,gres/gpuutil=91,gres/gpumem=4096M\"}\n"
            ),
        )
        .expect("slurm");

    let outcome = load_sampler_snapshot(&metrics_dir);
    assert!(outcome.notes.is_empty());
    let sampler = outcome.sampler.expect("sampler");
    assert_eq!(sampler.interval_seconds, 5);
    let gpu = sampler.gpu.expect("gpu");
    assert_eq!(gpu.sampled_at, "2026-04-05T10:00:10Z");
    assert_eq!(gpu.gpus.len(), 1);
    assert_eq!(gpu.gpus[0].uuid.as_deref(), Some("GPU-new"));
    assert_eq!(gpu.processes[0].pid.as_deref(), Some("4242"));
    let slurm = sampler.slurm.expect("slurm");
    assert_eq!(slurm.sampled_at, "2026-04-05T10:00:10Z");
    assert_eq!(slurm.steps.len(), 1);
    assert_eq!(slurm.steps[0].step_id, "12345.1");
    assert_eq!(slurm.steps[0].gpu_util.as_deref(), Some("91"));
}

#[test]
fn sampler_and_parser_error_paths_cover_remaining_functions() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let malformed_meta_dir = tmpdir.path().join("malformed-meta");
    fs::create_dir_all(&malformed_meta_dir).expect("dir");
    fs::write(malformed_meta_dir.join("meta.json"), "{not-json}\n").expect("meta");
    let outcome = load_sampler_snapshot(&malformed_meta_dir);
    assert!(outcome.sampler.is_none());
    assert!(
        outcome
            .notes
            .iter()
            .any(|note| note.contains("failed to parse metrics sampler metadata"))
    );

    let disabled_dir = tmpdir.path().join("disabled");
    fs::create_dir_all(&disabled_dir).expect("dir");
    fs::write(
        disabled_dir.join("meta.json"),
        r#"{
  "interval_seconds": 5,
  "collectors": [
    {"name":"gpu","enabled":false,"available":false,"note":"disabled","last_sampled_at":null},
    {"name":"slurm","enabled":false,"available":false,"note":null,"last_sampled_at":null}
  ]
}"#,
    )
    .expect("meta");
    let outcome = load_sampler_snapshot(&disabled_dir);
    let sampler = outcome.sampler.expect("sampler");
    assert!(sampler.gpu.is_none());
    assert!(sampler.slurm.is_none());
    assert!(outcome.notes.is_empty());

    let broken_collectors_dir = tmpdir.path().join("broken-collectors");
    fs::create_dir_all(&broken_collectors_dir).expect("dir");
    fs::write(
            broken_collectors_dir.join("meta.json"),
            r#"{
  "interval_seconds": 5,
  "collectors": [
    {"name":"gpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:00Z"},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:00Z"}
  ]
}"#,
        )
        .expect("meta");
    fs::write(broken_collectors_dir.join("gpu.jsonl"), "{not-json}\n").expect("gpu");
    fs::write(
            broken_collectors_dir.join("slurm.jsonl"),
            "{\"sampled_at\":\"2026-04-05T10:00:00Z\",\"step_id\":null,\"ntasks\":\"1\",\"ave_cpu\":\"\",\"ave_rss\":\"\",\"max_rss\":\"\",\"alloc_tres\":\"cpu=1,broken\",\"tres_usage_in_ave\":\"cpu=00:00:01\"}\n",
        )
        .expect("slurm");
    let outcome = load_sampler_snapshot(&broken_collectors_dir);
    assert!(
        outcome
            .notes
            .iter()
            .any(|note| note.contains("failed to parse GPU sampler data"))
    );
    assert!(
        outcome
            .notes
            .iter()
            .any(|note| note.contains("failed to parse Slurm sampler data"))
    );

    let err = step_from_slurm_sample_row(SlurmSampleRow {
        sampled_at: "2026-04-05T10:00:00Z".into(),
        step_id: None,
        ntasks: Some("1".into()),
        ave_cpu: Some("".into()),
        ave_rss: Some("".into()),
        max_rss: Some("".into()),
        alloc_tres: Some("cpu=1".into()),
        tres_usage_in_ave: Some("cpu=00:00:01".into()),
    })
    .expect_err("missing step id");
    assert!(err.to_string().contains("missing required field 'step_id'"));

    let err = step_from_slurm_sample_row(SlurmSampleRow {
        sampled_at: "2026-04-05T10:00:00Z".into(),
        step_id: Some("12345.0".into()),
        ntasks: Some("1".into()),
        ave_cpu: Some("".into()),
        ave_rss: Some("".into()),
        max_rss: Some("".into()),
        alloc_tres: Some("cpu=1,broken".into()),
        tres_usage_in_ave: Some("cpu=00:00:01".into()),
    })
    .expect_err("bad alloc tres");
    assert!(err.to_string().contains("failed to parse AllocTRES"));

    let sstat = tmpdir.path().join("sstat-fail");
    write_script(
        &sstat,
        "#!/bin/bash\nset -euo pipefail\necho nope >&2\nexit 1\n",
    );
    let err = probe_step_stats("12345", sstat.to_str().expect("path")).expect_err("sstat");
    assert!(err.to_string().contains("sstat failed for job 12345: nope"));
}

#[test]
fn build_stats_snapshot_falls_back_when_sampler_data_is_malformed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let metrics_dir = metrics_dir_for_record(&record);
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    fs::write(
            metrics_dir.join("meta.json"),
            r#"{
  "sampler_pid": 123,
  "interval_seconds": 5,
  "collectors": [
    {"name":"gpu","enabled":false,"available":false,"note":null,"last_sampled_at":null},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:10Z"}
  ]
}"#,
        )
        .expect("meta");
    fs::write(metrics_dir.join("slurm.jsonl"), "{not-json}\n").expect("bad slurm");

    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    let sstat = tmpdir.path().join("sstat");
    write_script(&squeue, "#!/bin/bash\necho RUNNING\n");
    write_script(&sacct, "#!/bin/bash\nexit 0\n");
    write_script(
        &sstat,
        "#!/bin/bash\ncat <<'EOF'\n12345.0|1|00:00:03|128M|256M|cpu=1,mem=512M|cpu=00:00:03\nEOF\n",
    );

    let snapshot = build_stats_snapshot(
        &compose,
        None,
        &StatsOptions {
            scheduler: SchedulerOptions {
                squeue_bin: squeue.display().to_string(),
                sacct_bin: sacct.display().to_string(),
            },
            sstat_bin: sstat.display().to_string(),
        },
    )
    .expect("snapshot");

    assert_eq!(snapshot.source, "sstat");
    assert_eq!(snapshot.steps.len(), 1);
    assert!(
        snapshot
            .notes
            .iter()
            .any(|note| note.contains("failed to parse Slurm sampler data"))
    );
}

#[test]
fn build_stats_snapshot_uses_tracked_sampler_for_explicit_job_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let metrics_dir = metrics_dir_for_record(&record);
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    fs::write(
            metrics_dir.join("meta.json"),
            r#"{
  "sampler_pid": 123,
  "interval_seconds": 5,
  "collectors": [
    {"name":"gpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:10Z"},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-05T10:00:10Z"}
  ]
}"#,
        )
        .expect("meta");
    fs::write(
            metrics_dir.join("gpu.jsonl"),
            "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"index\":\"0\",\"uuid\":\"GPU-new\",\"name\":\"New\",\"utilization_gpu\":\"91\",\"utilization_memory\":\"77\",\"memory_used_mib\":\"4096\",\"memory_total_mib\":\"8192\",\"temperature_c\":\"55\",\"power_draw_w\":\"220\",\"power_limit_w\":\"300\"}\n",
        )
        .expect("gpu");
    fs::write(
            metrics_dir.join("gpu_processes.jsonl"),
            "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"gpu_uuid\":\"GPU-new\",\"pid\":\"4242\",\"process_name\":\"python\",\"used_memory_mib\":\"2048\"}\n",
        )
        .expect("gpu proc");
    fs::write(
            metrics_dir.join("slurm.jsonl"),
            "{\"sampled_at\":\"2026-04-05T10:00:10Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:11\",\"ave_rss\":\"512M\",\"max_rss\":\"1G\",\"alloc_tres\":\"cpu=1,mem=4G,gres/gpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:11,gres/gpuutil=91,gres/gpumem=4096M\"}\n",
        )
        .expect("slurm");

    let squeue = tmpdir.path().join("squeue");
    let sacct = tmpdir.path().join("sacct");
    let sstat = tmpdir.path().join("sstat");
    write_script(&squeue, "#!/bin/bash\necho RUNNING\n");
    write_script(&sacct, "#!/bin/bash\nexit 0\n");
    write_script(
        &sstat,
        "#!/bin/bash\necho sstat should not run >&2\nexit 1\n",
    );

    let snapshot = build_stats_snapshot(
        &compose,
        Some("12345"),
        &StatsOptions {
            scheduler: SchedulerOptions {
                squeue_bin: squeue.display().to_string(),
                sacct_bin: sacct.display().to_string(),
            },
            sstat_bin: sstat.display().to_string(),
        },
    )
    .expect("snapshot");

    assert_eq!(snapshot.source, "sampler");
    assert_eq!(
        snapshot.record.as_ref().map(|item| item.job_id.as_str()),
        Some("12345")
    );
    assert_eq!(snapshot.metrics_dir.as_ref(), Some(&metrics_dir));
    assert_eq!(
        snapshot
            .sampler
            .as_ref()
            .and_then(|item| item.gpu.as_ref())
            .and_then(|gpu| gpu.processes.first())
            .and_then(|process| process.pid.as_deref()),
        Some("4242")
    );
    assert_eq!(snapshot.steps.len(), 1);
    assert_eq!(snapshot.steps[0].gpu_util.as_deref(), Some("91"));
}

#[test]
fn export_artifacts_copies_payloads_into_resolved_directory() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results/${SLURM_JOB_ID}".into()),
        paths: vec!["/hpc-compose/job/metrics/**".into()],
        bundles: BTreeMap::new(),
    });
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let payload_dir = artifact_payload_dir_for_record(&record);
    fs::create_dir_all(payload_dir.join("metrics")).expect("metrics dir");
    fs::write(payload_dir.join("metrics/meta.json"), "{\"ok\":true}\n").expect("meta");
    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
            matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
            copied_relative_paths: vec!["metrics/meta.json".into()],
            warnings: vec!["pattern '/hpc-compose/job/unused/*' did not match any paths".into()],
            bundles: BTreeMap::from([(
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
                    copied_relative_paths: vec!["metrics/meta.json".into()],
                    warnings: vec![
                        "pattern '/hpc-compose/job/unused/*' did not match any paths".into(),
                    ],
                },
            )]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");

    let report =
        export_artifacts(&compose, None, &ArtifactExportOptions::default()).expect("export");
    assert_eq!(report.record.job_id, "12345");
    assert_eq!(report.export_dir, tmpdir.path().join("results/12345"));
    assert_eq!(report.exported_paths.len(), 1);
    assert_eq!(
        fs::read_to_string(report.export_dir.join("metrics/meta.json")).expect("exported"),
        "{\"ok\":true}\n"
    );
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("did not match any paths"))
    );
}

#[test]
fn export_artifacts_uses_tracked_export_dir_without_reparsing_compose() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results/${SLURM_JOB_ID}".into()),
        paths: vec!["/hpc-compose/job/metrics/**".into()],
        bundles: BTreeMap::new(),
    });
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let payload_dir = artifact_payload_dir_for_record(&record);
    fs::create_dir_all(payload_dir.join("metrics")).expect("metrics dir");
    fs::write(payload_dir.join("metrics/meta.json"), "{\"ok\":true}\n").expect("meta");
    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
            matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
            copied_relative_paths: vec!["metrics/meta.json".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([(
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
                    copied_relative_paths: vec!["metrics/meta.json".into()],
                    warnings: Vec::new(),
                },
            )]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");

    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("mutate compose");

    let report =
        export_artifacts(&compose, None, &ArtifactExportOptions::default()).expect("export");
    assert_eq!(report.export_dir, tmpdir.path().join("results/12345"));
    assert_eq!(
        fs::read_to_string(report.export_dir.join("metrics/meta.json")).expect("exported"),
        "{\"ok\":true}\n"
    );
}

#[cfg(unix)]
#[test]
fn export_artifacts_preserves_symlinks() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/checkpoints/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results/${SLURM_JOB_ID}".into()),
        paths: vec!["/hpc-compose/job/checkpoints/**".into()],
        bundles: BTreeMap::new(),
    });
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let payload_dir = artifact_payload_dir_for_record(&record);
    fs::create_dir_all(payload_dir.join("checkpoints")).expect("checkpoints dir");
    fs::write(payload_dir.join("checkpoints/step-1.bin"), "weights").expect("weights");
    std::os::unix::fs::symlink("step-1.bin", payload_dir.join("checkpoints/latest"))
        .expect("symlink");
    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec!["/hpc-compose/job/checkpoints/**".into()],
            matched_source_paths: vec![
                "/hpc-compose/job/checkpoints/step-1.bin".into(),
                "/hpc-compose/job/checkpoints/latest".into(),
            ],
            copied_relative_paths: vec!["checkpoints".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([(
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/checkpoints/**".into()],
                    matched_source_paths: vec![
                        "/hpc-compose/job/checkpoints/step-1.bin".into(),
                        "/hpc-compose/job/checkpoints/latest".into(),
                    ],
                    copied_relative_paths: vec!["checkpoints".into()],
                    warnings: Vec::new(),
                },
            )]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");

    let report =
        export_artifacts(&compose, None, &ArtifactExportOptions::default()).expect("export");
    let latest = report.export_dir.join("checkpoints/latest");
    let metadata = fs::symlink_metadata(&latest).expect("latest metadata");
    assert!(metadata.file_type().is_symlink());
    assert_eq!(
        fs::read_link(&latest).expect("read link"),
        PathBuf::from("step-1.bin")
    );
}

#[test]
fn export_artifacts_requires_manifest_and_configured_block() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
    let plan = runtime_plan(tmpdir.path());
    persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");

    let err = export_artifacts(&compose, None, &ArtifactExportOptions::default())
        .expect_err("missing config");
    assert!(err.to_string().contains("tracked submission metadata"));

    let mut plan_with_artifacts = runtime_plan(tmpdir.path());
    plan_with_artifacts.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results".into()),
        paths: vec!["/hpc-compose/job/metrics/**".into()],
        bundles: BTreeMap::new(),
    });
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose with artifacts");
    persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job-with-artifacts.sbatch"),
        &plan_with_artifacts,
        "67890",
    )
    .expect("record with artifacts");

    let err = export_artifacts(&compose, Some("67890"), &ArtifactExportOptions::default())
        .expect_err("missing manifest");
    assert!(
        err.to_string()
            .contains("tracked artifact manifest does not exist")
    );
}

#[test]
fn export_artifacts_reports_manifest_mismatch_and_missing_payloads() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/metrics/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results/${SLURM_JOB_ID}".into()),
        paths: vec!["/hpc-compose/job/metrics/**".into()],
        bundles: BTreeMap::new(),
    });
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    fs::create_dir_all(artifacts_dir_for_record(&record)).expect("artifacts dir");

    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "99999".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
            matched_source_paths: vec!["/hpc-compose/job/metrics/missing.json".into()],
            copied_relative_paths: vec!["metrics/missing.json".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([(
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/metrics/missing.json".into()],
                    copied_relative_paths: vec!["metrics/missing.json".into()],
                    warnings: Vec::new(),
                },
            )]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");
    let err =
        export_artifacts(&compose, None, &ArtifactExportOptions::default()).expect_err("mismatch");
    assert!(
        err.to_string()
            .contains("artifact manifest job id 99999 does not match")
    );

    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
            matched_source_paths: vec!["/hpc-compose/job/metrics/missing.json".into()],
            copied_relative_paths: vec!["metrics/missing.json".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([(
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/metrics/missing.json".into()],
                    copied_relative_paths: vec!["metrics/missing.json".into()],
                    warnings: Vec::new(),
                },
            )]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");
    let report =
        export_artifacts(&compose, None, &ArtifactExportOptions::default()).expect("export");
    assert!(report.exported_paths.is_empty());
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("collected payload path"))
    );
}

#[test]
fn export_artifacts_supports_named_bundles_and_tarballs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(
        &compose,
        r#"
x-slurm:
  artifacts:
    export_dir: ./results/${SLURM_JOB_ID}
    paths:
      - /hpc-compose/job/metrics/**
    bundles:
      logs:
        paths:
          - /hpc-compose/job/logs/**
services:
  app:
    image: redis:7
"#,
    )
    .expect("compose");
    let mut plan = runtime_plan(tmpdir.path());
    plan.slurm.artifacts = Some(crate::spec::ArtifactsConfig {
        collect: crate::spec::ArtifactCollectPolicy::Always,
        export_dir: Some("./results/${SLURM_JOB_ID}".into()),
        paths: vec!["/hpc-compose/job/metrics/**".into()],
        bundles: BTreeMap::from([(
            "logs".into(),
            crate::spec::ArtifactBundleSpec {
                paths: vec!["/hpc-compose/job/logs/**".into()],
            },
        )]),
    });
    let record = persist_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    let payload_dir = artifact_payload_dir_for_record(&record);
    fs::create_dir_all(payload_dir.join("metrics")).expect("metrics dir");
    fs::create_dir_all(payload_dir.join("logs")).expect("logs dir");
    fs::write(payload_dir.join("metrics/meta.json"), "{\"ok\":true}\n").expect("meta");
    fs::write(payload_dir.join("logs/app.log"), "ready\n").expect("log");
    fs::write(
        artifact_manifest_path_for_record(&record),
        serde_json::to_vec_pretty(&ArtifactManifest {
            schema_version: 2,
            job_id: "12345".into(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: vec![
                "/hpc-compose/job/logs/**".into(),
                "/hpc-compose/job/metrics/**".into(),
            ],
            matched_source_paths: vec![
                "/hpc-compose/job/logs/app.log".into(),
                "/hpc-compose/job/metrics/meta.json".into(),
            ],
            copied_relative_paths: vec!["logs/app.log".into(), "metrics/meta.json".into()],
            warnings: Vec::new(),
            bundles: BTreeMap::from([
                (
                    "default".into(),
                    ArtifactBundleManifest {
                        declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                        matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
                        copied_relative_paths: vec!["metrics/meta.json".into()],
                        warnings: Vec::new(),
                    },
                ),
                (
                    "logs".into(),
                    ArtifactBundleManifest {
                        declared_source_patterns: vec!["/hpc-compose/job/logs/**".into()],
                        matched_source_paths: vec!["/hpc-compose/job/logs/app.log".into()],
                        copied_relative_paths: vec!["logs/app.log".into()],
                        warnings: Vec::new(),
                    },
                ),
            ]),
        })
        .expect("manifest"),
    )
    .expect("write manifest");

    let report = export_artifacts(
        &compose,
        None,
        &ArtifactExportOptions {
            selected_bundles: vec!["logs".into()],
            tarball: true,
        },
    )
    .expect("export");
    assert_eq!(report.selected_bundles, vec!["logs".to_string()]);
    assert!(report.export_dir.join("bundles/logs/logs/app.log").exists());
    assert!(!report.export_dir.join("metrics/meta.json").exists());
    assert_eq!(report.bundles.len(), 1);
    assert_eq!(report.bundles[0].name, "logs");
    assert!(report.bundles[0].provenance_path.exists());
    assert!(report.tarball_paths[0].exists());
    assert!(
        report.bundles[0]
            .files
            .iter()
            .any(|entry| entry.relative_path == "logs/app.log" && entry.sha256.is_some())
    );
}

#[test]
fn copy_helpers_cover_files_directories_and_symlink_overwrites() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let source_file = tmpdir.path().join("source.txt");
    let dest_file = tmpdir.path().join("dest.txt");
    fs::write(&source_file, "new").expect("source");
    fs::write(&dest_file, "old").expect("dest");
    copy_path_recursive(&source_file, &dest_file).expect("copy file");
    assert_eq!(fs::read_to_string(&dest_file).expect("read"), "new");

    let source_dir = tmpdir.path().join("source-dir");
    let nested = source_dir.join("nested");
    fs::create_dir_all(&nested).expect("dir");
    fs::write(nested.join("data.txt"), "payload").expect("payload");
    let dest_dir = tmpdir.path().join("dest-dir");
    copy_path_recursive(&source_dir, &dest_dir).expect("copy dir");
    assert_eq!(
        fs::read_to_string(dest_dir.join("nested/data.txt")).expect("read"),
        "payload"
    );

    let removable_file = tmpdir.path().join("remove-file");
    fs::write(&removable_file, "x").expect("file");
    remove_existing_destination(&removable_file).expect("remove file");
    assert!(!removable_file.exists());

    let removable_dir = tmpdir.path().join("remove-dir");
    fs::create_dir_all(&removable_dir).expect("dir");
    remove_existing_destination(&removable_dir).expect("remove dir");
    assert!(!removable_dir.exists());

    #[cfg(unix)]
    {
        let symlink_source = tmpdir.path().join("symlink-source");
        fs::write(&symlink_source, "target").expect("target");
        let source_link = tmpdir.path().join("source-link");
        std::os::unix::fs::symlink(&symlink_source, &source_link).expect("source link");

        let dest_link = tmpdir.path().join("dest-link");
        fs::write(&dest_link, "occupied").expect("occupied");
        copy_path_recursive(&source_link, &dest_link).expect("copy symlink");
        assert!(
            fs::symlink_metadata(&dest_link)
                .expect("meta")
                .file_type()
                .is_symlink()
        );
    }
}

#[test]
fn stats_unavailable_reason_covers_pending_running_and_terminal_states() {
    let pending = stats_unavailable_reason(&SchedulerStatus {
        state: "PENDING".into(),
        source: SchedulerSource::Squeue,
        terminal: false,
        failed: false,
        detail: None,
    });
    assert!(pending.contains("not running yet"));

    let running = stats_unavailable_reason(&SchedulerStatus {
        state: "RUNNING".into(),
        source: SchedulerSource::Squeue,
        terminal: false,
        failed: false,
        detail: None,
    });
    assert!(running.contains("running job"));

    let completed = stats_unavailable_reason(&SchedulerStatus {
        state: "COMPLETED".into(),
        source: SchedulerSource::Sacct,
        terminal: true,
        failed: false,
        detail: None,
    });
    assert!(completed.contains("no longer running"));
}

#[test]
fn scan_job_records_returns_all_tracked_jobs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(tmpdir.path());

    let record1 = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s1"),
        &plan,
        "111",
    )
    .expect("record");
    write_submission_record(&record1).expect("write");

    let record2 = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s2"),
        &plan,
        "222",
    )
    .expect("record");
    write_submission_record(&record2).expect("write");

    let records = scan_job_records(&compose_path).expect("scan");
    assert_eq!(records.len(), 2);
    let ids: Vec<&str> = records.iter().map(|r| r.job_id.as_str()).collect();
    assert!(ids.contains(&"111"));
    assert!(ids.contains(&"222"));
}

#[test]
fn clean_all_except_latest_preserves_latest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(tmpdir.path());

    let record1 = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s1"),
        &plan,
        "100",
    )
    .expect("record");
    write_submission_record(&record1).expect("write");

    let record2 = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s2"),
        &plan,
        "200",
    )
    .expect("record");
    write_submission_record(&record2).expect("write");

    // latest.json should point to record2 (the last written)
    let result = clean_all_except_latest(&compose_path).expect("clean");
    assert_eq!(result.removed_job_ids, vec!["100"]);

    let remaining = scan_job_records(&compose_path).expect("scan");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].job_id, "200");
}

#[test]
fn clean_by_age_removes_old_jobs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(tmpdir.path());

    let mut record = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s1"),
        &plan,
        "300",
    )
    .expect("record");
    // Set submitted_at to 10 days ago
    record.submitted_at = unix_timestamp_now().saturating_sub(10 * 86400);
    write_submission_record(&record).expect("write");

    let recent = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s2"),
        &plan,
        "400",
    )
    .expect("record");
    write_submission_record(&recent).expect("write");

    let result = clean_by_age(&compose_path, 7).expect("clean");
    assert_eq!(result.removed_job_ids, vec!["300"]);

    let remaining = scan_job_records(&compose_path).expect("scan");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].job_id, "400");
}

#[test]
fn clean_removes_job_log_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(tmpdir.path());

    let record = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s1"),
        &plan,
        "500",
    )
    .expect("record");
    write_submission_record(&record).expect("write");

    // Create the job log directory
    let job_dir = metadata_root_for(&compose_path).join("500");
    fs::create_dir_all(job_dir.join("logs")).expect("mkdir");
    fs::write(job_dir.join("logs/test.log"), "log content").expect("write log");
    assert!(job_dir.exists());

    let result = clean_all_except_latest(&compose_path).expect("clean");
    // 500 is latest, so it should NOT be removed
    assert!(result.removed_job_ids.is_empty());

    // Add another record to make 500 non-latest
    let record2 = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("s2"),
        &plan,
        "600",
    )
    .expect("record");
    write_submission_record(&record2).expect("write");

    let result = clean_all_except_latest(&compose_path).expect("clean");
    assert_eq!(result.removed_job_ids, vec!["500"]);
    assert!(!job_dir.exists());
}

#[test]
fn scan_job_inventory_recovers_latest_when_pointer_is_missing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let project_dir = tmpdir.path().join("project");
    fs::create_dir_all(&project_dir).expect("project dir");
    let compose_path = project_dir.join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(&project_dir);

    let mut older = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("older.sbatch"),
        &plan,
        "100",
    )
    .expect("older");
    older.submitted_at = 10;
    write_submission_record(&older).expect("write older");

    let mut newer = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("newer.sbatch"),
        &plan,
        "200",
    )
    .expect("newer");
    newer.submitted_at = 20;
    write_submission_record(&newer).expect("write newer");

    fs::remove_file(latest_record_path_for(&compose_path)).expect("remove latest");

    let scan = scan_job_inventory(tmpdir.path(), false).expect("scan inventory");
    assert_eq!(scan.jobs.len(), 2);
    let newer_entry = scan
        .jobs
        .iter()
        .find(|entry| entry.job_id == "200")
        .expect("newer entry");
    let older_entry = scan
        .jobs
        .iter()
        .find(|entry| entry.job_id == "100")
        .expect("older entry");
    assert!(newer_entry.is_latest);
    assert!(!older_entry.is_latest);
}

#[test]
fn scan_job_inventory_ignores_stale_latest_pointer() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let project_dir = tmpdir.path().join("project");
    fs::create_dir_all(&project_dir).expect("project dir");
    let compose_path = project_dir.join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(&project_dir);

    let mut older = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("older.sbatch"),
        &plan,
        "100",
    )
    .expect("older");
    older.submitted_at = 10;
    write_submission_record(&older).expect("write older");

    let mut newer = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("newer.sbatch"),
        &plan,
        "200",
    )
    .expect("newer");
    newer.submitted_at = 20;
    write_submission_record(&newer).expect("write newer");

    write_json(&latest_record_path_for(&compose_path), &older).expect("stale latest");

    let scan = scan_job_inventory(tmpdir.path(), false).expect("scan inventory");
    assert_eq!(scan.jobs.len(), 2);
    let newer_entry = scan
        .jobs
        .iter()
        .find(|entry| entry.job_id == "200")
        .expect("newer entry");
    let older_entry = scan
        .jobs
        .iter()
        .find(|entry| entry.job_id == "100")
        .expect("older entry");
    assert!(newer_entry.is_latest);
    assert!(!older_entry.is_latest);
}

#[test]
fn cleanup_report_dedupes_paths_and_repairs_latest_pointer() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmpdir.path().join("compose.yaml");
    fs::write(&compose_path, "").expect("write");
    let plan = runtime_plan(tmpdir.path());
    let now = unix_timestamp_now();

    let mut old_record = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("old.sbatch"),
        &plan,
        "500",
    )
    .expect("old");
    old_record.submitted_at = now.saturating_sub(10 * 86_400);
    write_submission_record(&old_record).expect("write old");

    let mut new_record = build_submission_record(
        &compose_path,
        tmpdir.path(),
        &tmpdir.path().join("new.sbatch"),
        &plan,
        "600",
    )
    .expect("new");
    new_record.submitted_at = now.saturating_sub(60);
    write_submission_record(&new_record).expect("write new");

    write_json(&latest_record_path_for(&compose_path), &old_record).expect("stale latest");

    let report = build_cleanup_report(
        &compose_path,
        CleanupMode::Age { age_days: 7 },
        false,
        false,
    )
    .expect("cleanup report");
    assert_eq!(report.removed_job_ids, vec!["500"]);
    assert_eq!(report.latest_pointer_job_id_before.as_deref(), Some("500"));
    assert_eq!(report.latest_job_id_before.as_deref(), Some("600"));
    assert_eq!(report.latest_job_id_after.as_deref(), Some("600"));
    let selected = report
        .jobs
        .iter()
        .find(|job| job.inventory.job_id == "500")
        .expect("selected job");
    assert_eq!(selected.removable_paths.len(), 2);

    run_cleanup_report(&report).expect("run cleanup");
    let latest = load_submission_record(&compose_path, None).expect("latest after cleanup");
    assert_eq!(latest.job_id, "600");

    let final_report = build_cleanup_report(
        &compose_path,
        CleanupMode::Age { age_days: 0 },
        false,
        false,
    )
    .expect("final cleanup report");
    run_cleanup_report(&final_report).expect("final cleanup");
    assert!(!latest_record_path_for(&compose_path).exists());
}

#[test]
fn local_scheduler_status_marks_dead_supervisor_as_failed() {
    let state = ServiceRuntimeStateFile {
        backend: Some(SubmissionBackend::Local),
        job_status: Some("RUNNING".into()),
        job_exit_code: None,
        supervisor_pid: Some(u32::MAX),
        attempt: None,
        is_resume: None,
        resume_dir: None,
        services: Vec::new(),
    };

    let status = scheduler::build_local_scheduler_status(Some(&state));
    assert_eq!(status.state, "FAILED");
    assert!(status.terminal);
    assert!(status.failed);
    assert!(
        status
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("exited before recording a terminal outcome")
    );
}
