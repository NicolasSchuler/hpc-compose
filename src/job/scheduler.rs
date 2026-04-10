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
    let now = unix_timestamp_now();
    let runtime_state = load_runtime_state(&record);
    let (scheduler, queue_diagnostics) = match record.backend {
        SubmissionBackend::Slurm => {
            let (raw_scheduler, queue_diagnostics) =
                probe_status_components(&record.job_id, options);
            (
                reconcile_scheduler_status(raw_scheduler, record.submitted_at, None, now),
                queue_diagnostics,
            )
        }
        SubmissionBackend::Local => (build_local_scheduler_status(runtime_state.as_ref()), None),
    };
    let batch_log = build_batch_log_status(&record.batch_log, now);
    let runtime_state_by_service = runtime_state.as_ref().map(runtime_state_by_service);
    let mut services = Vec::with_capacity(record.service_logs.len());
    for (service_name, path) in &record.service_logs {
        let log_status = build_log_status(path, now);
        let runtime_state = runtime_state_by_service
            .as_ref()
            .and_then(|state| state.get(service_name));
        let launcher_pid =
            runtime_state.and_then(|state| active_launcher_pid(state, record.backend));
        services.push(ServiceLogStatus {
            service_name: service_name.clone(),
            path: path.clone(),
            present: log_status.present,
            updated_age_seconds: log_status.updated_age_seconds,
            updated_at: log_status.updated_at,
            log_path: runtime_state
                .and_then(|state| state.log_path.clone())
                .or_else(|| Some(path.clone())),
            step_name: runtime_state.and_then(|state| state.step_name.clone()),
            launch_index: runtime_state.and_then(|state| state.launch_index),
            launcher_pid,
            healthy: runtime_state
                .and_then(|state| launcher_pid.map(|_| state.healthy.unwrap_or(false))),
            readiness_configured: runtime_state.and_then(|state| state.readiness_configured),
            status: Some(
                derive_service_status(&scheduler, runtime_state, record.backend).to_string(),
            ),
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

fn active_launcher_pid(
    state: &ServiceRuntimeStateEntry,
    backend: SubmissionBackend,
) -> Option<u32> {
    let pid = state.launcher_pid.filter(|pid| *pid > 0)?;
    match backend {
        SubmissionBackend::Slurm => Some(pid),
        SubmissionBackend::Local => pid_is_running(pid).then_some(pid),
    }
}

fn derive_service_status(
    scheduler: &SchedulerStatus,
    state: Option<&ServiceRuntimeStateEntry>,
    backend: SubmissionBackend,
) -> &'static str {
    let Some(state) = state else {
        return if scheduler.terminal {
            if scheduler.failed { "failed" } else { "exited" }
        } else if backend == SubmissionBackend::Local {
            "starting"
        } else {
            "unknown"
        };
    };

    if active_launcher_pid(state, backend).is_some() {
        if state.healthy.unwrap_or(false) {
            return "ready";
        }
        return if state.readiness_configured.unwrap_or(false) {
            "starting"
        } else {
            "running"
        };
    }

    if let Some(last_exit_code) = state.last_exit_code {
        return if last_exit_code == 0 {
            "exited"
        } else {
            "failed"
        };
    }

    if scheduler.terminal {
        if scheduler.failed { "failed" } else { "exited" }
    } else if backend == SubmissionBackend::Local {
        "starting"
    } else {
        "unknown"
    }
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

pub(crate) fn build_local_scheduler_status(
    runtime_state: Option<&ServiceRuntimeStateFile>,
) -> SchedulerStatus {
    let supervisor_pid = runtime_state
        .and_then(|state| state.supervisor_pid)
        .filter(|pid| *pid > 0);
    if let Some(pid) = supervisor_pid
        && pid_is_running(pid)
    {
        let state = runtime_state
            .and_then(|state| state.job_status.clone())
            .map(|state| normalize_scheduler_state(&state))
            .unwrap_or_else(|| "RUNNING".to_string());
        return SchedulerStatus {
            state,
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: None,
        };
    }

    if let Some(state) = runtime_state
        .and_then(|state| state.job_status.clone())
        .map(|state| normalize_scheduler_state(&state))
        && is_terminal_state(&state)
    {
        return build_scheduler_status(state, SchedulerSource::LocalOnly);
    }

    if let Some(exit_code) = runtime_state.and_then(|state| state.job_exit_code) {
        return build_scheduler_status(
            if exit_code == 0 {
                "COMPLETED"
            } else {
                "FAILED"
            }
            .to_string(),
            SchedulerSource::LocalOnly,
        );
    }

    if let Some(pid) = supervisor_pid {
        return SchedulerStatus {
            state: "FAILED".to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: true,
            failed: true,
            detail: Some(format!(
                "local supervisor pid {pid} exited before recording a terminal outcome"
            )),
        };
    }

    if runtime_state.is_some() {
        return SchedulerStatus {
            state: "WAITING_FOR_LOCAL_RUNTIME".to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "local runtime state exists but the supervisor has not reported a terminal outcome yet"
                    .to_string(),
            ),
        };
    }

    SchedulerStatus {
        state: "WAITING_FOR_LOCAL_RUNTIME".to_string(),
        source: SchedulerSource::LocalOnly,
        terminal: false,
        failed: false,
        detail: Some(
            "local runtime state has not been written yet; waiting for the launcher to initialize"
                .to_string(),
        ),
    }
}

fn pid_is_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        if pid == 0 || pid > i32::MAX as u32 {
            return false;
        }
        // Signal 0 checks for process existence without sending a real signal.
        let result = unsafe { libc::kill(pid as i32, 0) };
        if result == 0 {
            return true;
        }
        let error = std::io::Error::last_os_error();
        matches!(error.raw_os_error(), Some(libc::EPERM))
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_entry() -> ServiceRuntimeStateEntry {
        ServiceRuntimeStateEntry {
            service_name: "api".into(),
            step_name: Some("hpc-compose:api".into()),
            log_path: Some(PathBuf::from("/tmp/api.log")),
            launch_index: Some(0),
            launcher_pid: Some(std::process::id()),
            healthy: Some(false),
            readiness_configured: Some(false),
            failure_policy_mode: None,
            restart_count: Some(0),
            max_restarts: None,
            window_seconds: None,
            max_restarts_in_window: None,
            restart_failures_in_window: None,
            restart_failure_timestamps: None,
            last_exit_code: None,
            placement_mode: None,
            nodes: None,
            ntasks: None,
            ntasks_per_node: None,
            nodelist: None,
        }
    }

    #[test]
    fn scheduler_helpers_cover_probe_and_service_status_paths() {
        let pending_scheduler = build_scheduler_status("PENDING".into(), SchedulerSource::Squeue);
        let squeue = QueueDiagnosticsProbe {
            state: Some("PENDING".into()),
            pending_reason: Some("Resources".into()),
            eligible_time: None,
            start_time: Some("2026-04-10T12:00:00".into()),
        };
        let sacct = QueueDiagnosticsProbe {
            state: Some("PENDING".into()),
            pending_reason: Some("Priority".into()),
            eligible_time: Some("2026-04-10T11:55:00".into()),
            start_time: None,
        };
        let diagnostics =
            build_status_queue_diagnostics(&pending_scheduler, Some(&squeue), Some(&sacct))
                .expect("pending diagnostics");
        assert_eq!(diagnostics.pending_reason.as_deref(), Some("Resources"));
        assert_eq!(
            diagnostics.eligible_time.as_deref(),
            Some("2026-04-10T11:55:00")
        );
        assert_eq!(
            diagnostics.start_time.as_deref(),
            Some("2026-04-10T12:00:00")
        );

        let running_scheduler = build_scheduler_status("RUNNING".into(), SchedulerSource::Squeue);
        let start_only = build_status_queue_diagnostics(
            &running_scheduler,
            Some(&QueueDiagnosticsProbe {
                state: Some("RUNNING".into()),
                pending_reason: Some("ignored".into()),
                eligible_time: None,
                start_time: Some("2026-04-10T12:01:00".into()),
            }),
            None,
        )
        .expect("start-only diagnostics");
        assert!(start_only.pending_reason.is_none());
        assert_eq!(
            start_only.start_time.as_deref(),
            Some("2026-04-10T12:01:00")
        );
        assert!(build_status_queue_diagnostics(&running_scheduler, None, None).is_none());

        assert!(scheduler_status_from_probe(None, SchedulerSource::Squeue).is_none());
        let sacct_status = scheduler_status_from_probe(
            Some(&QueueDiagnosticsProbe {
                state: Some("COMPLETED".into()),
                pending_reason: None,
                eligible_time: None,
                start_time: None,
            }),
            SchedulerSource::Sacct,
        )
        .expect("scheduler status");
        assert_eq!(sacct_status.state, "COMPLETED");
        assert_eq!(scheduler_source_label(sacct_status.source), "sacct");

        let mut ready = runtime_entry();
        ready.healthy = Some(true);
        assert_eq!(
            active_launcher_pid(&ready, SubmissionBackend::Slurm),
            Some(std::process::id())
        );
        assert_eq!(
            derive_service_status(&running_scheduler, Some(&ready), SubmissionBackend::Slurm),
            "ready"
        );

        let mut starting = runtime_entry();
        starting.readiness_configured = Some(true);
        assert_eq!(
            derive_service_status(
                &running_scheduler,
                Some(&starting),
                SubmissionBackend::Slurm
            ),
            "starting"
        );

        let running = runtime_entry();
        assert_eq!(
            derive_service_status(&running_scheduler, Some(&running), SubmissionBackend::Slurm),
            "running"
        );

        let mut exited = runtime_entry();
        exited.launcher_pid = None;
        exited.last_exit_code = Some(0);
        assert_eq!(
            derive_service_status(&running_scheduler, Some(&exited), SubmissionBackend::Local),
            "exited"
        );

        let mut failed = exited.clone();
        failed.last_exit_code = Some(1);
        assert_eq!(
            derive_service_status(&running_scheduler, Some(&failed), SubmissionBackend::Local),
            "failed"
        );

        let terminal_failed = build_scheduler_status("FAILED".into(), SchedulerSource::LocalOnly);
        assert_eq!(
            derive_service_status(&terminal_failed, None, SubmissionBackend::Slurm),
            "failed"
        );
        assert_eq!(
            derive_service_status(&running_scheduler, None, SubmissionBackend::Local),
            "starting"
        );
        assert_eq!(
            derive_service_status(&running_scheduler, None, SubmissionBackend::Slurm),
            "unknown"
        );
    }

    #[test]
    fn local_scheduler_status_covers_waiting_running_exit_and_pid_helpers() {
        let current_pid = std::process::id();
        assert!(pid_is_running(current_pid));
        assert!(!pid_is_running(u32::MAX));

        let waiting = build_local_scheduler_status(None);
        assert_eq!(waiting.state, "WAITING_FOR_LOCAL_RUNTIME");
        assert!(!waiting.terminal);

        let running_state = ServiceRuntimeStateFile {
            backend: Some(SubmissionBackend::Local),
            job_status: Some("running".into()),
            job_exit_code: None,
            supervisor_pid: Some(current_pid),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            services: Vec::new(),
        };
        let running = build_local_scheduler_status(Some(&running_state));
        assert_eq!(running.state, "RUNNING");
        assert!(!running.terminal);

        let cancelled_state = ServiceRuntimeStateFile {
            backend: Some(SubmissionBackend::Local),
            job_status: Some("cancelled".into()),
            job_exit_code: None,
            supervisor_pid: None,
            attempt: None,
            is_resume: None,
            resume_dir: None,
            services: Vec::new(),
        };
        let cancelled = build_local_scheduler_status(Some(&cancelled_state));
        assert_eq!(cancelled.state, "CANCELLED");
        assert!(cancelled.terminal);

        let failed_by_exit = ServiceRuntimeStateFile {
            backend: Some(SubmissionBackend::Local),
            job_status: None,
            job_exit_code: Some(7),
            supervisor_pid: None,
            attempt: None,
            is_resume: None,
            resume_dir: None,
            services: Vec::new(),
        };
        let failed = build_local_scheduler_status(Some(&failed_by_exit));
        assert_eq!(failed.state, "FAILED");
        assert!(failed.failed);

        let pending_local = ServiceRuntimeStateFile {
            backend: Some(SubmissionBackend::Local),
            job_status: None,
            job_exit_code: None,
            supervisor_pid: None,
            attempt: None,
            is_resume: None,
            resume_dir: None,
            services: Vec::new(),
        };
        let waiting_existing = build_local_scheduler_status(Some(&pending_local));
        assert_eq!(waiting_existing.state, "WAITING_FOR_LOCAL_RUNTIME");
        assert!(
            waiting_existing
                .detail
                .as_deref()
                .unwrap_or_default()
                .contains("local runtime state exists")
        );
    }
}
