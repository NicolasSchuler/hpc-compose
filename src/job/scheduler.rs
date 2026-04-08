use super::runtime_state::{
    active_restart_failures_in_window, load_runtime_state, runtime_state_by_service,
};
use super::*;

/// Builds the tracked status snapshot used by `hpc-compose status`.
pub fn build_status_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
) -> Result<StatusSnapshot> {
    let record = load_submission_record(spec_path, job_id)?;
    let (raw_scheduler, queue_diagnostics) = probe_status_components(&record.job_id, options);
    let scheduler = reconcile_scheduler_status(
        raw_scheduler,
        record.submitted_at,
        None,
        unix_timestamp_now(),
    );
    let now = unix_timestamp_now();
    let batch_log = build_batch_log_status(&record.batch_log, now);
    let runtime_state = load_runtime_state(&record);
    let runtime_state_by_service = runtime_state.as_ref().map(runtime_state_by_service);
    let mut services = Vec::with_capacity(record.service_logs.len());
    for (service_name, path) in &record.service_logs {
        let log_status = build_log_status(path, now);
        let runtime_state = runtime_state_by_service
            .as_ref()
            .and_then(|state| state.get(service_name));
        services.push(ServiceLogStatus {
            service_name: service_name.clone(),
            path: path.clone(),
            present: log_status.present,
            updated_age_seconds: log_status.updated_age_seconds,
            updated_at: log_status.updated_at,
            failure_policy_mode: runtime_state.and_then(|state| state.failure_policy_mode.clone()),
            restart_count: runtime_state.and_then(|state| state.restart_count),
            max_restarts: runtime_state.and_then(|state| state.max_restarts),
            window_seconds: runtime_state.and_then(|state| state.window_seconds),
            max_restarts_in_window: runtime_state.and_then(|state| state.max_restarts_in_window),
            restart_failures_in_window: runtime_state
                .and_then(|state| active_restart_failures_in_window(state, now))
                .or_else(|| runtime_state.and_then(|state| state.restart_failures_in_window)),
            last_exit_code: runtime_state.and_then(|state| state.last_exit_code),
            placement_mode: runtime_state.and_then(|state| state.placement_mode.clone()),
            nodes: runtime_state.and_then(|state| state.nodes),
            ntasks: runtime_state.and_then(|state| state.ntasks),
            ntasks_per_node: runtime_state.and_then(|state| state.ntasks_per_node),
            nodelist: runtime_state.and_then(|state| state.nodelist.clone()),
        });
    }
    Ok(StatusSnapshot {
        log_dir: log_dir_for_record(&record),
        batch_log,
        record,
        scheduler,
        queue_diagnostics,
        services,
        attempt: runtime_state.as_ref().and_then(|state| state.attempt),
        is_resume: runtime_state.as_ref().and_then(|state| state.is_resume),
        resume_dir: runtime_state
            .as_ref()
            .and_then(|state| state.resume_dir.clone()),
    })
}

/// Probes scheduler state using `squeue` first and `sacct` as fallback.
pub fn probe_scheduler_status(job_id: &str, options: &SchedulerOptions) -> SchedulerStatus {
    probe_status_components(job_id, options).0
}

/// Returns the human-readable label for a scheduler source.
pub fn scheduler_source_label(source: SchedulerSource) -> &'static str {
    match source {
        SchedulerSource::Squeue => "squeue",
        SchedulerSource::Sacct => "sacct",
        SchedulerSource::LocalOnly => "local-only",
    }
}

fn probe_status_components(
    job_id: &str,
    options: &SchedulerOptions,
) -> (SchedulerStatus, Option<QueueDiagnostics>) {
    let squeue = probe_squeue_queue_diagnostics(job_id, &options.squeue_bin);
    let sacct = probe_sacct_queue_diagnostics(job_id, &options.sacct_bin);
    let scheduler = scheduler_status_from_probe(squeue.as_ref(), SchedulerSource::Squeue)
        .or_else(|| scheduler_status_from_probe(sacct.as_ref(), SchedulerSource::Sacct))
        .unwrap_or_else(|| SchedulerStatus {
            state: "unknown".to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "scheduler state is unavailable because squeue/sacct could not determine this job"
                    .to_string(),
            ),
        });
    let queue_diagnostics =
        build_status_queue_diagnostics(&scheduler, squeue.as_ref(), sacct.as_ref());
    (scheduler, queue_diagnostics)
}

fn build_status_queue_diagnostics(
    scheduler: &SchedulerStatus,
    squeue: Option<&QueueDiagnosticsProbe>,
    sacct: Option<&QueueDiagnosticsProbe>,
) -> Option<QueueDiagnostics> {
    let pending_reason = if scheduler.state == "PENDING" {
        squeue
            .and_then(|probe| probe.pending_reason.clone())
            .or_else(|| match sacct.and_then(|probe| probe.state.as_deref()) {
                Some("PENDING") => sacct.and_then(|probe| probe.pending_reason.clone()),
                _ => None,
            })
    } else {
        None
    };
    let diagnostics = QueueDiagnostics {
        pending_reason,
        eligible_time: sacct.and_then(|probe| probe.eligible_time.clone()),
        start_time: squeue
            .and_then(|probe| probe.start_time.clone())
            .or_else(|| sacct.and_then(|probe| probe.start_time.clone())),
    };
    (diagnostics.pending_reason.is_some()
        || diagnostics.eligible_time.is_some()
        || diagnostics.start_time.is_some())
    .then_some(diagnostics)
}

fn scheduler_status_from_probe(
    probe: Option<&QueueDiagnosticsProbe>,
    source: SchedulerSource,
) -> Option<SchedulerStatus> {
    let state = probe.and_then(|probe| probe.state.clone())?;
    Some(build_scheduler_status(state, source))
}
