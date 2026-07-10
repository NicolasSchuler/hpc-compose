use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::artifacts::artifact_manifest_path_for_record;
use super::checkpoints::collect_checkpoint_history;
use super::model::SubmissionRecord;
use super::record::state_path_for_record;
use super::runtime_state::ServiceRuntimeStateFile;
use super::scheduler::{JobState, SchedulerStatus, StatusSnapshot};

const STATUS_VERIFICATION_SCHEMA_VERSION: u32 = 1;
const SEVERITY_ERROR: &str = "error";
const SEVERITY_WARNING: &str = "warning";
const SEVERITY_INFO: &str = "info";
const STATUS_PASSED: &str = "passed";
const STATUS_WARNING: &str = "warning";
const STATUS_FAILED: &str = "failed";
const STATUS_SKIPPED: &str = "skipped";

/// Local consistency report for a tracked job status snapshot.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct StatusVerificationReport {
    pub schema_version: u32,
    pub ok: bool,
    pub errors: usize,
    pub warnings: usize,
    pub checks: Vec<StatusVerificationCheck>,
}

/// One verification check result.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct StatusVerificationCheck {
    pub id: String,
    pub severity: String,
    pub status: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

/// Builds a read-only local verification report for an already-built status snapshot.
///
/// The verifier never contacts Slurm and never mutates tracked records. It reads
/// only local tracked state and artifact/log paths that the record or snapshot
/// already names.
pub fn build_status_verification_report(
    record: &SubmissionRecord,
    snapshot: &StatusSnapshot,
) -> StatusVerificationReport {
    let runtime_state = read_latest_runtime_state(record);
    let checkpoint_history = collect_checkpoint_history(record);
    let checks = vec![
        scheduler_vs_runtime_check(snapshot, &runtime_state),
        state_json_health_check(snapshot, &runtime_state),
        checkpoint_history_check(
            snapshot,
            &checkpoint_history.degraded,
            checkpoint_history.entries.len(),
        ),
        log_presence_check(snapshot),
        artifacts_manifest_check(record, snapshot),
    ];
    StatusVerificationReport::from_checks(checks)
}

impl StatusVerificationReport {
    fn from_checks(checks: Vec<StatusVerificationCheck>) -> Self {
        let errors = checks
            .iter()
            .filter(|check| check.status == STATUS_FAILED)
            .count();
        let warnings = checks
            .iter()
            .filter(|check| check.status == STATUS_WARNING)
            .count();
        Self {
            schema_version: STATUS_VERIFICATION_SCHEMA_VERSION,
            ok: errors == 0,
            errors,
            warnings,
            checks,
        }
    }
}

#[derive(Debug)]
enum RuntimeStateRead {
    Readable {
        path: PathBuf,
        state: ServiceRuntimeStateFile,
    },
    Missing {
        path: PathBuf,
    },
    Unreadable {
        path: PathBuf,
        error: String,
    },
}

impl RuntimeStateRead {
    fn path(&self) -> &Path {
        match self {
            Self::Readable { path, .. }
            | Self::Missing { path }
            | Self::Unreadable { path, .. } => path,
        }
    }

    fn state(&self) -> Option<&ServiceRuntimeStateFile> {
        match self {
            Self::Readable { state, .. } => Some(state),
            Self::Missing { .. } | Self::Unreadable { .. } => None,
        }
    }
}

fn read_latest_runtime_state(record: &SubmissionRecord) -> RuntimeStateRead {
    let path = state_path_for_record(record);
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return RuntimeStateRead::Missing { path };
        }
        Err(err) => {
            return RuntimeStateRead::Unreadable {
                path,
                error: err.to_string(),
            };
        }
    };
    match serde_json::from_str::<ServiceRuntimeStateFile>(&raw) {
        Ok(state) => RuntimeStateRead::Readable { path, state },
        Err(err) => RuntimeStateRead::Unreadable {
            path,
            error: err.to_string(),
        },
    }
}

fn scheduler_vs_runtime_check(
    snapshot: &StatusSnapshot,
    runtime_state: &RuntimeStateRead,
) -> StatusVerificationCheck {
    let scheduler_state = JobState::parse(&snapshot.scheduler.state);
    let scheduler_terminal = snapshot.scheduler.terminal || scheduler_state.is_terminal();
    let scheduler_live = scheduler_state.is_live();
    let mut active_evidence = Vec::new();
    let mut terminal_evidence = Vec::new();

    if let Some(state) = runtime_state.state()
        && let Some(job_status) = state.job_status.as_deref()
    {
        let parsed = JobState::parse(job_status);
        if parsed.is_live() {
            active_evidence.push(format!(
                "latest state.json job_status is {}",
                parsed.as_str()
            ));
        } else if parsed.is_terminal() {
            terminal_evidence.push(format!(
                "latest state.json job_status is {}",
                parsed.as_str()
            ));
        }
    }

    for service in &snapshot.services {
        if let Some(status) = service.status.as_deref()
            && is_active_service_status(status)
        {
            active_evidence.push(format!(
                "service '{}' status is {}",
                service.service_name, status
            ));
        }
    }

    if scheduler_terminal && !active_evidence.is_empty() {
        return check(
            "scheduler-vs-runtime",
            SEVERITY_ERROR,
            STATUS_FAILED,
            "scheduler is terminal but runtime still appears active",
            Some(active_evidence.join("; ")),
            Some(
                "Inspect the latest state.json and service logs before treating the job as fully reconciled.",
            ),
        );
    }

    if scheduler_live && !terminal_evidence.is_empty() {
        return check(
            "scheduler-vs-runtime",
            SEVERITY_WARNING,
            STATUS_WARNING,
            "scheduler is live but runtime recorded a terminal job status",
            Some(terminal_evidence.join("; ")),
            Some(
                "Re-run status after scheduler accounting catches up, then inspect state.json if the mismatch remains.",
            ),
        );
    }

    if runtime_state.state().is_none()
        && snapshot
            .services
            .iter()
            .all(|service| service.status.is_none())
    {
        return check(
            "scheduler-vs-runtime",
            SEVERITY_INFO,
            STATUS_SKIPPED,
            "no readable runtime state or service status was available to compare",
            Some(format!("looked for {}", runtime_state.path().display())),
            None,
        );
    }

    check(
        "scheduler-vs-runtime",
        SEVERITY_INFO,
        STATUS_PASSED,
        "scheduler and runtime liveness agree",
        None,
        None,
    )
}

fn state_json_health_check(
    snapshot: &StatusSnapshot,
    runtime_state: &RuntimeStateRead,
) -> StatusVerificationCheck {
    match runtime_state {
        RuntimeStateRead::Readable { path, .. } => check(
            "state-json-health",
            SEVERITY_INFO,
            STATUS_PASSED,
            "latest state.json is readable",
            Some(path.display().to_string()),
            None,
        ),
        RuntimeStateRead::Missing { path }
            if is_pre_runtime_scheduler_state(&snapshot.scheduler) =>
        {
            check(
                "state-json-health",
                SEVERITY_INFO,
                STATUS_SKIPPED,
                "latest state.json is not expected before runtime starts",
                Some(path.display().to_string()),
                None,
            )
        }
        RuntimeStateRead::Missing { path } => check(
            "state-json-health",
            SEVERITY_WARNING,
            STATUS_WARNING,
            "latest state.json is absent",
            Some(path.display().to_string()),
            Some(
                "Wait for the runtime to initialize, or inspect the job root if the scheduler is already terminal.",
            ),
        ),
        RuntimeStateRead::Unreadable { path, error } => check(
            "state-json-health",
            SEVERITY_WARNING,
            STATUS_WARNING,
            "latest state.json is unreadable",
            Some(format!("{}: {error}", path.display())),
            Some(
                "Inspect or repair the local tracked state file; verification continues with the available snapshot data.",
            ),
        ),
    }
}

fn checkpoint_history_check(
    snapshot: &StatusSnapshot,
    degraded: &[String],
    entry_count: usize,
) -> StatusVerificationCheck {
    if degraded.is_empty() {
        let status = if entry_count == 0 {
            STATUS_SKIPPED
        } else {
            STATUS_PASSED
        };
        let summary = if entry_count == 0 {
            "no checkpoint history entries were available"
        } else {
            "checkpoint history has no degraded entries"
        };
        return check(
            "checkpoint-history",
            SEVERITY_INFO,
            status,
            summary,
            None,
            None,
        );
    }

    if is_pre_runtime_scheduler_state(&snapshot.scheduler)
        && degraded
            .iter()
            .all(|entry| entry.contains("could not read state at "))
    {
        return check(
            "checkpoint-history",
            SEVERITY_INFO,
            STATUS_SKIPPED,
            "checkpoint history is not expected before runtime starts",
            Some(degraded.join("; ")),
            None,
        );
    }

    check(
        "checkpoint-history",
        SEVERITY_WARNING,
        STATUS_WARNING,
        "checkpoint history is degraded",
        Some(degraded.join("; ")),
        Some("Inspect the per-attempt state.json files under the tracked job root."),
    )
}

fn log_presence_check(snapshot: &StatusSnapshot) -> StatusVerificationCheck {
    if !should_check_log_presence(&snapshot.scheduler) {
        return check(
            "log-presence",
            SEVERITY_INFO,
            STATUS_SKIPPED,
            "log presence is skipped during transitional local-only state",
            Some(format!(
                "scheduler state is {} from {:?}",
                snapshot.scheduler.state, snapshot.scheduler.source
            )),
            None,
        );
    }

    let mut missing = Vec::new();
    if !snapshot.batch_log.present {
        missing.push(format!("batch log {}", snapshot.batch_log.path.display()));
    }
    for service in &snapshot.services {
        if !service.present {
            missing.push(format!(
                "service '{}' log {}",
                service.service_name,
                service.path.display()
            ));
        }
    }

    if missing.is_empty() {
        return check(
            "log-presence",
            SEVERITY_INFO,
            STATUS_PASSED,
            "expected logs are present",
            None,
            None,
        );
    }

    check(
        "log-presence",
        SEVERITY_WARNING,
        STATUS_WARNING,
        "expected logs are missing",
        Some(missing.join("; ")),
        Some(
            "Inspect the tracked log directory and confirm the runtime wrote logs before cleanup ran.",
        ),
    )
}

fn artifacts_manifest_check(
    record: &SubmissionRecord,
    snapshot: &StatusSnapshot,
) -> StatusVerificationCheck {
    if !scheduler_is_terminal(&snapshot.scheduler) {
        return check(
            "artifacts-manifest",
            SEVERITY_INFO,
            STATUS_SKIPPED,
            "artifact manifest check waits for terminal scheduler state",
            None,
            None,
        );
    }

    if record
        .artifact_export_dir
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        return check(
            "artifacts-manifest",
            SEVERITY_INFO,
            STATUS_SKIPPED,
            "artifact export is not configured",
            None,
            None,
        );
    }

    let manifest_path = artifact_manifest_path_for_record(record);
    if manifest_path.exists() {
        return check(
            "artifacts-manifest",
            SEVERITY_INFO,
            STATUS_PASSED,
            "artifact manifest is present",
            Some(manifest_path.display().to_string()),
            None,
        );
    }

    check(
        "artifacts-manifest",
        SEVERITY_WARNING,
        STATUS_WARNING,
        "artifact export was configured but no manifest was found",
        Some(manifest_path.display().to_string()),
        Some(
            "Inspect artifact collection logs or run the artifact pull/export workflow after the job finishes.",
        ),
    )
}

fn check(
    id: &str,
    severity: &str,
    status: &str,
    summary: &str,
    detail: Option<String>,
    suggestion: Option<&str>,
) -> StatusVerificationCheck {
    StatusVerificationCheck {
        id: id.to_string(),
        severity: severity.to_string(),
        status: status.to_string(),
        summary: summary.to_string(),
        detail,
        suggestion: suggestion.map(str::to_string),
    }
}

fn is_active_service_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "running" | "ready" | "starting"
    )
}

fn scheduler_is_terminal(status: &SchedulerStatus) -> bool {
    status.terminal || JobState::parse(&status.state).is_terminal()
}

fn should_check_log_presence(status: &SchedulerStatus) -> bool {
    scheduler_is_terminal(status)
        || matches!(
            JobState::parse(&status.state),
            JobState::Running | JobState::Completing
        )
}

fn is_pre_runtime_scheduler_state(status: &SchedulerStatus) -> bool {
    matches!(
        JobState::parse(&status.state),
        JobState::WaitingForScheduler
            | JobState::WaitingForAccounting
            | JobState::WaitingForLocalRuntime
            | JobState::Pending
            | JobState::Configuring
            | JobState::Unknown
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::{
        BatchLogStatus, PsServiceRow, SUBMISSION_SCHEMA_VERSION, SchedulerSource, SchedulerStatus,
        StatusSnapshot, SubmissionBackend, SubmissionKind, SubmissionRecord,
    };
    use super::*;

    fn record_for(root: &Path, job_id: &str) -> SubmissionRecord {
        SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: job_id.to_string(),
            submitted_at: 1,
            compose_file: root.join("compose.yaml"),
            submit_dir: root.to_path_buf(),
            script_path: root.join("job.sbatch"),
            cache_dir: root.join("cache"),
            runtime_root: None,
            batch_log: root.join("logs/batch.out"),
            batch_log_managed: true,
            service_logs: BTreeMap::from([("app".to_string(), root.join("logs/app.log"))]),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
            provenance: None,
            tags: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn scheduler(state: &str, source: SchedulerSource) -> SchedulerStatus {
        let parsed = JobState::parse(state);
        SchedulerStatus {
            state: state.to_string(),
            source,
            terminal: parsed.is_terminal(),
            failed: parsed.is_terminal() && !parsed.is_success(),
            detail: None,
        }
    }

    fn snapshot_for(
        record: &SubmissionRecord,
        scheduler: SchedulerStatus,
        batch_present: bool,
        service_present: bool,
        service_status: Option<&str>,
    ) -> StatusSnapshot {
        StatusSnapshot {
            record: record.clone(),
            scheduler,
            queue_diagnostics: None,
            array: None,
            verification: None,
            log_dir: record.batch_log.parent().unwrap().to_path_buf(),
            batch_log: BatchLogStatus {
                path: record.batch_log.clone(),
                present: batch_present,
                updated_at: None,
                updated_age_seconds: None,
            },
            services: record
                .service_logs
                .iter()
                .map(|(service_name, path)| PsServiceRow {
                    service_name: service_name.clone(),
                    path: path.clone(),
                    present: service_present,
                    updated_at: None,
                    updated_age_seconds: None,
                    log_path: Some(path.clone()),
                    step_name: None,
                    launch_index: None,
                    launcher_pid: None,
                    healthy: None,
                    completed_successfully: None,
                    readiness_configured: None,
                    status: service_status.map(str::to_string),
                    failure_policy_mode: None,
                    restart_count: None,
                    max_restarts: None,
                    window_seconds: None,
                    max_restarts_in_window: None,
                    restart_failures_in_window: None,
                    last_exit_code: None,
                    started_at: None,
                    finished_at: None,
                    duration_seconds: None,
                    assertions: None,
                    placement_mode: None,
                    nodes: None,
                    ntasks: None,
                    ntasks_per_node: None,
                    nodelist: None,
                })
                .collect(),
            telemetry_coverage: Vec::new(),
            watchdog: None,
            attempt: None,
            is_resume: None,
            resume_dir: None,
        }
    }

    fn write_state(record: &SubmissionRecord, raw: &str) {
        let path = state_path_for_record(record);
        fs::create_dir_all(path.parent().expect("state parent")).expect("state dir");
        fs::write(path, raw).expect("write state");
    }

    fn write_logs(record: &SubmissionRecord) {
        fs::create_dir_all(record.batch_log.parent().expect("batch parent")).expect("logs dir");
        fs::write(&record.batch_log, "batch\n").expect("batch log");
        for path in record.service_logs.values() {
            fs::write(path, "service\n").expect("service log");
        }
    }

    fn check_by_id<'a>(
        report: &'a StatusVerificationReport,
        id: &str,
    ) -> &'a StatusVerificationCheck {
        report
            .checks
            .iter()
            .find(|check| check.id == id)
            .expect("check")
    }

    #[test]
    fn terminal_scheduler_with_active_runtime_fails() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = record_for(tmpdir.path(), "123");
        write_logs(&record);
        write_state(&record, r#"{"job_status":"RUNNING","services":[]}"#);
        let snapshot = snapshot_for(
            &record,
            scheduler("COMPLETED", SchedulerSource::Sacct),
            true,
            true,
            Some("running"),
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(!report.ok);
        assert_eq!(report.errors, 1);
        assert_eq!(
            check_by_id(&report, "scheduler-vs-runtime").status,
            STATUS_FAILED
        );
        assert!(
            check_by_id(&report, "scheduler-vs-runtime")
                .summary
                .contains("runtime still appears active")
        );
    }

    #[test]
    fn live_scheduler_with_terminal_runtime_warns() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = record_for(tmpdir.path(), "124");
        write_logs(&record);
        write_state(&record, r#"{"job_status":"COMPLETED","services":[]}"#);
        let snapshot = snapshot_for(
            &record,
            scheduler("RUNNING", SchedulerSource::Squeue),
            true,
            true,
            Some("unknown"),
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(report.ok);
        assert_eq!(report.warnings, 1);
        assert_eq!(
            check_by_id(&report, "scheduler-vs-runtime").status,
            STATUS_WARNING
        );
        assert!(
            check_by_id(&report, "scheduler-vs-runtime")
                .summary
                .contains("terminal job status")
        );
    }

    #[test]
    fn missing_state_json_warns_without_failing_report() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = record_for(tmpdir.path(), "125");
        write_logs(&record);
        let snapshot = snapshot_for(
            &record,
            scheduler("WAITING_FOR_SCHEDULER", SchedulerSource::LocalOnly),
            true,
            true,
            None,
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(report.ok);
        assert_eq!(
            check_by_id(&report, "state-json-health").status,
            STATUS_SKIPPED
        );
        assert_eq!(
            check_by_id(&report, "checkpoint-history").status,
            STATUS_SKIPPED
        );
        assert_eq!(check_by_id(&report, "log-presence").status, STATUS_SKIPPED);
    }

    #[test]
    fn pending_slurm_job_skips_pre_runtime_local_checks() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = record_for(tmpdir.path(), "128");
        let snapshot = snapshot_for(
            &record,
            scheduler("PENDING", SchedulerSource::Squeue),
            false,
            false,
            None,
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(report.ok);
        assert_eq!(
            check_by_id(&report, "state-json-health").status,
            STATUS_SKIPPED
        );
        assert_eq!(
            check_by_id(&report, "checkpoint-history").status,
            STATUS_SKIPPED
        );
        assert_eq!(check_by_id(&report, "log-presence").status, STATUS_SKIPPED);
    }

    #[test]
    fn degraded_checkpoint_history_is_warning() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = record_for(tmpdir.path(), "126");
        write_logs(&record);
        write_state(&record, "{not json");
        let snapshot = snapshot_for(
            &record,
            scheduler("COMPLETED", SchedulerSource::Sacct),
            true,
            true,
            Some("exited"),
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(report.ok);
        assert_eq!(
            check_by_id(&report, "checkpoint-history").status,
            STATUS_WARNING
        );
    }

    #[test]
    fn terminal_export_without_manifest_warns() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut record = record_for(tmpdir.path(), "127");
        record.artifact_export_dir = Some("./results".to_string());
        write_logs(&record);
        write_state(&record, r#"{"job_status":"COMPLETED","services":[]}"#);
        let snapshot = snapshot_for(
            &record,
            scheduler("COMPLETED", SchedulerSource::Sacct),
            true,
            true,
            Some("exited"),
        );

        let report = build_status_verification_report(&record, &snapshot);

        assert!(report.ok);
        assert_eq!(
            check_by_id(&report, "artifacts-manifest").status,
            STATUS_WARNING
        );
    }
}
