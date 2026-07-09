use super::runtime_state::{
    ServiceRuntimeAssertionState, ServiceRuntimeStateEntry, ServiceRuntimeStateFile,
    active_restart_failures_in_window, load_runtime_state, runtime_state_by_service,
};
use super::*;
use crate::process_probe::{self, ProbeError, ProbeOptions};
use crate::time_util::system_time_to_unix;
use std::error::Error;
use std::fmt;
use std::process::Output;
use std::time::Duration;

const DEFAULT_SCHEDULER_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const SCHEDULER_COMMAND_TIMEOUT_ENV: &str = "HPC_COMPOSE_SCHEDULER_COMMAND_TIMEOUT_MS";

#[derive(Debug)]
pub(crate) struct SchedulerCommandUnavailable {
    detail: String,
}

impl SchedulerCommandUnavailable {
    fn new(detail: String) -> Self {
        Self { detail }
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for SchedulerCommandUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for SchedulerCommandUnavailable {}

#[derive(Debug)]
pub(super) enum SchedulerCommandError {
    Unavailable(SchedulerCommandUnavailable),
    Io(std::io::Error),
}

impl SchedulerCommandError {
    fn unavailable_detail(self) -> Option<String> {
        match self {
            Self::Unavailable(err) => Some(err.detail().to_string()),
            Self::Io(_) => None,
        }
    }

    pub(super) fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Unavailable(err) => err.into(),
            Self::Io(err) => err.into(),
        }
    }
}

fn scheduler_command_timeout() -> Duration {
    std::env::var(SCHEDULER_COMMAND_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SCHEDULER_COMMAND_TIMEOUT)
}

pub(super) fn run_scheduler_command(
    command: &mut Command,
    command_name: &str,
    binary: &str,
) -> std::result::Result<Output, SchedulerCommandError> {
    run_scheduler_command_with_timeout(command, command_name, binary, scheduler_command_timeout())
}

fn run_scheduler_command_with_timeout(
    command: &mut Command,
    command_name: &str,
    _binary: &str,
    timeout: Duration,
) -> std::result::Result<Output, SchedulerCommandError> {
    let output = process_probe::run(
        command,
        command_name,
        ProbeOptions {
            timeout,
            ..ProbeOptions::default()
        },
    )
    .map_err(|err| match err {
        ProbeError::Unavailable { .. } | ProbeError::TimedOut { .. } => {
            SchedulerCommandError::Unavailable(SchedulerCommandUnavailable::new(err.detail()))
        }
        err @ ProbeError::OutputLimitExceeded { .. } => SchedulerCommandError::Io(
            std::io::Error::new(std::io::ErrorKind::InvalidData, err.detail()),
        ),
        ProbeError::Io(err) => SchedulerCommandError::Io(err),
    })?;
    Ok(Output {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

/// Live walltime progress derived from a tracked job record and scheduler diagnostics.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalltimeProgress {
    pub original: String,
    pub elapsed_seconds: u64,
    pub total_seconds: u64,
    pub remaining_seconds: u64,
}

/// Scheduler state as observed by the tracker.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct SchedulerStatus {
    pub state: String,
    pub source: SchedulerSource,
    pub terminal: bool,
    pub failed: bool,
    pub detail: Option<String>,
}

/// Presence and freshness information for one tracked service log.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PsServiceRow {
    pub service_name: String,
    pub path: PathBuf,
    pub present: bool,
    pub updated_at: Option<u64>,
    pub updated_age_seconds: Option<u64>,
    #[serde(default)]
    pub log_path: Option<PathBuf>,
    #[serde(default)]
    pub step_name: Option<String>,
    #[serde(default)]
    pub launch_index: Option<u32>,
    #[serde(default)]
    pub launcher_pid: Option<u32>,
    #[serde(default)]
    pub healthy: Option<bool>,
    #[serde(default)]
    pub completed_successfully: Option<bool>,
    #[serde(default)]
    pub readiness_configured: Option<bool>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub failure_policy_mode: Option<String>,
    #[serde(default)]
    pub restart_count: Option<u32>,
    #[serde(default)]
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub window_seconds: Option<u64>,
    #[serde(default)]
    pub max_restarts_in_window: Option<u32>,
    #[serde(default)]
    pub restart_failures_in_window: Option<u32>,
    #[serde(default)]
    pub last_exit_code: Option<i32>,
    #[serde(default)]
    pub started_at: Option<u64>,
    #[serde(default)]
    pub finished_at: Option<u64>,
    #[serde(default)]
    pub duration_seconds: Option<u64>,
    #[serde(default)]
    pub assertions: Option<ServiceAssertionStatus>,
    #[serde(default)]
    pub placement_mode: Option<String>,
    #[serde(default)]
    pub nodes: Option<u32>,
    #[serde(default)]
    pub ntasks: Option<u32>,
    #[serde(default)]
    pub ntasks_per_node: Option<u32>,
    #[serde(default)]
    pub nodelist: Option<String>,
}

/// Post-run assertion result for one tracked service.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ServiceAssertionStatus {
    pub configured: bool,
    pub status: Option<String>,
    pub expected_exit_code: Option<i32>,
    pub artifacts_contain: Option<String>,
    pub max_duration_seconds: Option<u64>,
    pub duration_seconds: Option<u64>,
    pub failures: Vec<String>,
}

impl From<&ServiceRuntimeAssertionState> for ServiceAssertionStatus {
    fn from(value: &ServiceRuntimeAssertionState) -> Self {
        Self {
            configured: value.configured,
            status: value.status.clone(),
            expected_exit_code: value.expected_exit_code,
            artifacts_contain: value.artifacts_contain.clone(),
            max_duration_seconds: value.max_duration_seconds,
            duration_seconds: value.duration_seconds,
            failures: value.failures.clone(),
        }
    }
}

/// Backwards-compatible alias for one tracked service row.
pub type ServiceLogStatus = PsServiceRow;

/// Presence and freshness information for the top-level batch log.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BatchLogStatus {
    pub path: PathBuf,
    pub present: bool,
    pub updated_at: Option<u64>,
    pub updated_age_seconds: Option<u64>,
}

/// Combined tracked-job status returned by the `status` command.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StatusSnapshot {
    pub record: SubmissionRecord,
    pub scheduler: SchedulerStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_diagnostics: Option<QueueDiagnostics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub array: Option<ArrayStatusSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<StatusVerificationReport>,
    pub log_dir: PathBuf,
    pub batch_log: BatchLogStatus,
    pub services: Vec<PsServiceRow>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog: Option<WatchdogSnapshot>,
    pub attempt: Option<u32>,
    pub is_resume: Option<bool>,
    pub resume_dir: Option<PathBuf>,
}

/// Slurm array task rows observed by `status --array`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ArrayStatusSnapshot {
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub parent_job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filtered_task_id: Option<u32>,
    pub tasks: Vec<ArrayTaskStatus>,
    pub state_counts: BTreeMap<String, usize>,
}

/// One Slurm array task row.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ArrayTaskStatus {
    pub task_id: Option<u32>,
    pub job_id_raw: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub source: SchedulerSource,
}

/// Optional queue-facing scheduler diagnostics returned only by `status`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct QueueDiagnostics {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct QueueDiagnosticsProbe {
    pub(super) state: Option<String>,
    pub(super) pending_reason: Option<String>,
    pub(super) eligible_time: Option<String>,
    pub(super) start_time: Option<String>,
}

/// Formats a duration for watch output using `HH:MM:SS` or `D-HH:MM:SS` when needed.
pub fn format_walltime_duration(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    if days > 0 {
        format!("{days}-{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

/// Formats one walltime progress line for watch output.
pub fn format_walltime_summary(progress: &WalltimeProgress) -> String {
    format!(
        "{} / {} remaining {}",
        format_walltime_duration(progress.elapsed_seconds),
        format_walltime_duration(progress.total_seconds),
        format_walltime_duration(progress.remaining_seconds)
    )
}

/// Returns the integer completion percentage for a walltime progress sample.
pub fn walltime_progress_percent(progress: &WalltimeProgress) -> u64 {
    if progress.total_seconds == 0 {
        return 100;
    }
    ((u128::from(progress.elapsed_seconds) * 100) / u128::from(progress.total_seconds)) as u64
}

/// Parses a scheduler timestamp like `2026-04-10T12:34:56` or `2026-04-10T12:34:56Z`.
pub fn parse_scheduler_timestamp(input: &str) -> Option<u64> {
    let trimmed = input.trim().trim_end_matches('Z');
    let (date, time) = trimmed.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<i32>().ok()?;
    let month = date_parts.next()?.parse::<i32>().ok()?;
    let day = date_parts.next()?.parse::<i32>().ok()?;
    if date_parts.next().is_some() {
        return None;
    }

    let mut time_parts = time.split(':');
    let hour = time_parts.next()?.parse::<i32>().ok()?;
    let minute = time_parts.next()?.parse::<i32>().ok()?;
    let second = time_parts.next()?.split('.').next()?.parse::<i32>().ok()?;
    if time_parts.next().is_some() {
        return None;
    }

    #[cfg(unix)]
    {
        let mut tm = libc::tm {
            tm_sec: second,
            tm_min: minute,
            tm_hour: hour,
            tm_mday: day,
            tm_mon: month - 1,
            tm_year: year - 1900,
            tm_wday: 0,
            tm_yday: 0,
            tm_isdst: 0,
            tm_gmtoff: 0,
            tm_zone: std::ptr::null_mut(),
        };
        let timestamp = unsafe { libc::timegm(&mut tm) };
        (timestamp >= 0).then_some(timestamp as u64)
    }

    #[cfg(not(unix))]
    {
        let _ = (year, month, day, hour, minute, second);
        None
    }
}

/// Derives live walltime progress for a running tracked job.
pub fn walltime_progress(
    record: &SubmissionRecord,
    scheduler: &SchedulerStatus,
    queue_diagnostics: Option<&QueueDiagnostics>,
    now: u64,
) -> Option<WalltimeProgress> {
    if JobState::parse(&scheduler.state) != JobState::Running {
        return None;
    }
    let requested = record.requested_walltime.as_ref()?;
    let started_at = queue_diagnostics
        .and_then(|queue| queue.start_time.as_deref())
        .and_then(parse_scheduler_timestamp)
        .or(Some(record.submitted_at))?;
    let elapsed_seconds = now.saturating_sub(started_at).min(requested.seconds);
    Some(WalltimeProgress {
        original: requested.original.clone(),
        elapsed_seconds,
        total_seconds: requested.seconds,
        remaining_seconds: requested.seconds.saturating_sub(elapsed_seconds),
    })
}

/// Builds the tracked status snapshot used by `hpc-compose status`.
pub fn build_status_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
) -> Result<StatusSnapshot> {
    build_status_snapshot_core(spec_path, job_id, options, false, None)
}

/// Builds the tracked status snapshot, optionally including Slurm array rows.
pub fn build_status_snapshot_with_array(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
    include_array: bool,
) -> Result<StatusSnapshot> {
    build_status_snapshot_core(spec_path, job_id, options, include_array, None)
}

/// Builds the tracked status snapshot reusing an already-probed raw Slurm
/// scheduler status (from [`probe_scheduler_status_many`]) instead of re-probing.
///
/// The prefetched pair is used only for Slurm-backed records; local records
/// derive their status from runtime state as usual. Callers that batch probes
/// over many jobs (sweep status, `diff --across`) thread the batched result
/// through here so each snapshot avoids a per-job squeue/sacct spawn.
pub fn build_status_snapshot_with_status(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
    prefetched: Option<(SchedulerStatus, Option<QueueDiagnostics>)>,
) -> Result<StatusSnapshot> {
    build_status_snapshot_core(spec_path, job_id, options, false, prefetched)
}

fn build_status_snapshot_core(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
    include_array: bool,
    prefetched: Option<(SchedulerStatus, Option<QueueDiagnostics>)>,
) -> Result<StatusSnapshot> {
    let record = load_submission_record(spec_path, job_id)?;
    let now = unix_timestamp_now();
    let runtime_state = load_runtime_state(&record);
    let (scheduler, queue_diagnostics) = match record.backend {
        SubmissionBackend::Slurm => {
            let (raw_scheduler, queue_diagnostics) =
                prefetched.unwrap_or_else(|| probe_status_components(&record.job_id, options));
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
            completed_successfully: runtime_state.and_then(|state| state.completed_successfully),
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
            started_at: runtime_state.and_then(|state| state.started_at),
            finished_at: runtime_state.and_then(|state| state.finished_at),
            duration_seconds: runtime_state.and_then(|state| {
                state.duration_seconds.or_else(|| {
                    derive_service_duration_seconds(
                        state.started_at,
                        state.finished_at,
                        now,
                        launcher_pid.is_some(),
                    )
                })
            }),
            assertions: runtime_state
                .and_then(|state| state.assertions.as_ref())
                .map(ServiceAssertionStatus::from),
            placement_mode: runtime_state.and_then(|state| state.placement_mode.clone()),
            nodes: runtime_state.and_then(|state| state.nodes),
            ntasks: runtime_state.and_then(|state| state.ntasks),
            ntasks_per_node: runtime_state.and_then(|state| state.ntasks_per_node),
            nodelist: runtime_state.and_then(|state| state.nodelist.clone()),
        });
    }
    let array = if include_array {
        Some(build_array_status_snapshot(&record, job_id, options)?)
    } else {
        None
    };
    let watchdog_started_at = queue_diagnostics
        .as_ref()
        .and_then(|queue| queue.start_time.as_deref())
        .and_then(parse_scheduler_timestamp);
    let watchdog = super::watchdog::build_watchdog_snapshot(
        spec_path,
        &record,
        &scheduler,
        watchdog_started_at,
        now,
    )
    .snapshot;

    Ok(StatusSnapshot {
        log_dir: log_dir_for_record(&record),
        batch_log,
        record,
        scheduler,
        queue_diagnostics,
        array,
        verification: None,
        services,
        watchdog,
        attempt: runtime_state.as_ref().and_then(|state| state.attempt),
        is_resume: runtime_state.as_ref().and_then(|state| state.is_resume),
        resume_dir: runtime_state
            .as_ref()
            .and_then(|state| state.resume_dir.clone()),
    })
}

fn derive_service_duration_seconds(
    started_at: Option<u64>,
    finished_at: Option<u64>,
    now: u64,
    active: bool,
) -> Option<u64> {
    let started_at = started_at?;
    match finished_at {
        Some(finished_at) => Some(finished_at.saturating_sub(started_at)),
        None if active => Some(now.saturating_sub(started_at)),
        None => None,
    }
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

    if state.completed_successfully.unwrap_or(false) {
        return "exited";
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
///
/// This returns only the state (no queue diagnostics), so sacct is skipped
/// whenever squeue already resolved a live state — no user-visible field is
/// derived from the discarded sacct probe in that case.
pub fn probe_scheduler_status(job_id: &str, options: &SchedulerOptions) -> SchedulerStatus {
    probe_status_components_inner(job_id, options, false).0
}

/// Probes scheduler state and returns queue-facing diagnostics when available.
pub fn probe_scheduler_status_with_queue_diagnostics(
    job_id: &str,
    options: &SchedulerOptions,
) -> (SchedulerStatus, Option<QueueDiagnostics>) {
    probe_status_components(job_id, options)
}

/// Returns the human-readable label for a scheduler source.
pub fn scheduler_source_label(source: SchedulerSource) -> &'static str {
    match source {
        SchedulerSource::Squeue => "squeue",
        SchedulerSource::Sacct => "sacct",
        SchedulerSource::LocalOnly => "local-only",
    }
}

pub(crate) fn build_batch_log_status(path: &Path, now: u64) -> BatchLogStatus {
    let status = build_log_status(path, now);
    BatchLogStatus {
        path: path.to_path_buf(),
        present: status.present,
        updated_at: status.updated_at,
        updated_age_seconds: status.updated_age_seconds,
    }
}

pub(super) fn build_log_status(path: &Path, now: u64) -> BatchLogStatus {
    let metadata = fs::metadata(path).ok();
    let updated_at = metadata
        .as_ref()
        .and_then(|meta| meta.modified().ok())
        .and_then(system_time_to_unix);
    BatchLogStatus {
        path: path.to_path_buf(),
        present: metadata.is_some(),
        updated_at,
        updated_age_seconds: updated_at.map(|ts| now.saturating_sub(ts)),
    }
}

#[allow(dead_code)]
pub(super) fn probe_squeue_queue_diagnostics(
    job_id: &str,
    binary: &str,
) -> Option<QueueDiagnosticsProbe> {
    match probe_squeue_queue_diagnostics_result(job_id, binary) {
        QueueProbeResult::Probe(probe) => probe,
        QueueProbeResult::Unavailable(_) => None,
    }
}

fn probe_squeue_queue_diagnostics_result(job_id: &str, binary: &str) -> QueueProbeResult {
    let mut command = Command::new(binary);
    command.args(["-h", "-j", job_id, "-o", "%T|%r|%S"]);
    let output = match run_scheduler_command(&mut command, "squeue", binary) {
        Ok(output) => output,
        Err(err) => match err.unavailable_detail() {
            Some(detail) => return QueueProbeResult::Unavailable(detail),
            None => return QueueProbeResult::Probe(None),
        },
    };
    if !output.status.success() {
        return QueueProbeResult::Probe(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let Some(row) = stdout.lines().map(str::trim).find(|line| !line.is_empty()) else {
        return QueueProbeResult::Probe(None);
    };
    let mut fields = row.split('|').map(str::trim);
    let state = fields.next().and_then(normalize_scheduler_state_field);
    let pending_reason = fields.next().and_then(normalize_scheduler_metadata);
    let start_time = fields.next().and_then(normalize_scheduler_metadata);
    QueueProbeResult::Probe(Some(QueueDiagnosticsProbe {
        state,
        pending_reason,
        start_time,
        ..QueueDiagnosticsProbe::default()
    }))
}

#[allow(dead_code)]
pub(super) fn probe_sacct_queue_diagnostics(
    job_id: &str,
    binary: &str,
) -> Option<QueueDiagnosticsProbe> {
    match probe_sacct_queue_diagnostics_result(job_id, binary) {
        QueueProbeResult::Probe(probe) => probe,
        QueueProbeResult::Unavailable(_) => None,
    }
}

enum QueueProbeResult {
    Probe(Option<QueueDiagnosticsProbe>),
    Unavailable(String),
}

fn probe_sacct_queue_diagnostics_result(job_id: &str, binary: &str) -> QueueProbeResult {
    let mut command = Command::new(binary);
    command.args([
        "-n",
        "-X",
        "-j",
        job_id,
        "--format=State,Eligible,Start,Reason",
        "--parsable2",
    ]);
    let output = match run_scheduler_command(&mut command, "sacct", binary) {
        Ok(output) => output,
        Err(err) => match err.unavailable_detail() {
            Some(detail) => return QueueProbeResult::Unavailable(detail),
            None => return QueueProbeResult::Probe(None),
        },
    };
    if !output.status.success() {
        return QueueProbeResult::Probe(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let Some(row) = stdout.lines().map(str::trim).find(|line| !line.is_empty()) else {
        return QueueProbeResult::Probe(None);
    };
    let mut fields = row.split('|').map(str::trim);
    let state = fields.next().and_then(normalize_scheduler_state_field);
    let eligible_time = fields.next().and_then(normalize_scheduler_metadata);
    let start_time = fields.next().and_then(normalize_scheduler_metadata);
    let pending_reason = fields.next().and_then(normalize_scheduler_metadata);
    QueueProbeResult::Probe(Some(QueueDiagnosticsProbe {
        state,
        pending_reason,
        eligible_time,
        start_time,
    }))
}

/// Builds a merged Slurm array task snapshot from live queue and accounting probes.
pub fn build_array_status_snapshot(
    record: &SubmissionRecord,
    requested_job_id: Option<&str>,
    options: &SchedulerOptions,
) -> Result<ArrayStatusSnapshot> {
    let (parent_job_id, filtered_task_id) =
        normalize_array_job_id(requested_job_id.unwrap_or(&record.job_id));
    let mut unavailable = Vec::new();
    let mut by_key = BTreeMap::<String, ArrayTaskStatus>::new();

    match probe_sacct_array_tasks(
        &parent_job_id,
        filtered_task_id,
        &options.sacct_bin,
        record.slurm_array.as_deref(),
    )? {
        ArrayProbeResult::Rows(rows) => {
            for row in rows {
                by_key.insert(array_task_key(&row), row);
            }
        }
        ArrayProbeResult::Unavailable(reason) => unavailable.push(reason),
    }
    match probe_squeue_array_tasks(&parent_job_id, filtered_task_id, &options.squeue_bin)? {
        ArrayProbeResult::Rows(rows) => {
            for row in rows {
                by_key.insert(array_task_key(&row), row);
            }
        }
        ArrayProbeResult::Unavailable(reason) => unavailable.push(reason),
    }

    let mut tasks = by_key.into_values().collect::<Vec<_>>();
    tasks.sort_by(|left, right| {
        left.task_id
            .cmp(&right.task_id)
            .then_with(|| left.job_id_raw.cmp(&right.job_id_raw))
    });
    let mut state_counts = BTreeMap::new();
    for task in &tasks {
        *state_counts.entry(task.state.clone()).or_insert(0) += 1;
    }

    let available = !tasks.is_empty();
    let reason = if available {
        if record.slurm_array.is_none() {
            Some(
                "tracked record does not include original x-slurm.array metadata; rows are scheduler-observed"
                    .to_string(),
            )
        } else {
            None
        }
    } else if !unavailable.is_empty() {
        Some(format!(
            "array task status is not available: {}",
            unavailable.join("; ")
        ))
    } else if record.slurm_array.is_none() {
        Some(
            "no array task rows found; tracked record does not include original x-slurm.array metadata"
                .to_string(),
        )
    } else {
        Some(format!(
            "no array task rows found for x-slurm.array={}",
            record.slurm_array.as_deref().unwrap_or("<unknown>")
        ))
    };

    Ok(ArrayStatusSnapshot {
        available,
        reason,
        parent_job_id,
        filtered_task_id,
        tasks,
        state_counts,
    })
}

enum ArrayProbeResult {
    Rows(Vec<ArrayTaskStatus>),
    Unavailable(String),
}

fn probe_squeue_array_tasks(
    parent_job_id: &str,
    filtered_task_id: Option<u32>,
    binary: &str,
) -> Result<ArrayProbeResult> {
    let mut command = Command::new(binary);
    command.args(["--array", "-h", "-j", parent_job_id, "-o", "%i|%T|%M|%R"]);
    let output = match run_scheduler_command(&mut command, "squeue", binary) {
        Ok(output) => output,
        Err(err) => match err {
            SchedulerCommandError::Unavailable(err) => {
                return Ok(ArrayProbeResult::Unavailable(err.detail().to_string()));
            }
            SchedulerCommandError::Io(err) => {
                return Err(err).with_context(|| format!("failed to execute '{binary}'"));
            }
        },
    };
    if !output.status.success() {
        bail!(
            "squeue --array failed: {}",
            command_failure_detail(&output.stdout, &output.stderr)
        );
    }
    let mut rows = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 4 {
            bail!("failed to parse squeue --array row '{line}': expected 4 pipe-separated fields");
        }
        let task_id = parse_array_task_id(fields[0], parent_job_id);
        if filtered_task_id.is_some() && task_id != filtered_task_id {
            continue;
        }
        rows.push(ArrayTaskStatus {
            task_id,
            job_id_raw: fields[0].to_string(),
            state: normalize_scheduler_state(fields[1]),
            exit_code: None,
            elapsed_seconds: None,
            elapsed: normalize_scheduler_metadata(fields[2]),
            reason: normalize_scheduler_metadata(fields[3]),
            source: SchedulerSource::Squeue,
        });
    }
    Ok(ArrayProbeResult::Rows(rows))
}

fn probe_sacct_array_tasks(
    parent_job_id: &str,
    filtered_task_id: Option<u32>,
    binary: &str,
    slurm_array: Option<&str>,
) -> Result<ArrayProbeResult> {
    let mut command = Command::new(binary);
    command.args([
        "--array",
        "-n",
        "-X",
        "-j",
        parent_job_id,
        "--parsable2",
        "--format=JobIDRaw,State,ExitCode,ElapsedRaw",
    ]);
    let output = match run_scheduler_command(&mut command, "sacct", binary) {
        Ok(output) => output,
        Err(err) => match err {
            SchedulerCommandError::Unavailable(err) => {
                return Ok(ArrayProbeResult::Unavailable(err.detail().to_string()));
            }
            SchedulerCommandError::Io(err) => {
                return Err(err).with_context(|| format!("failed to execute '{binary}'"));
            }
        },
    };
    if !output.status.success() {
        bail!(
            "sacct --array failed: {}",
            command_failure_detail(&output.stdout, &output.stderr)
        );
    }
    let mut rows = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() != 4 {
            bail!("failed to parse sacct --array row '{line}': expected 4 pipe-separated fields");
        }
        let task_id = parse_array_task_id(fields[0], parent_job_id);
        if filtered_task_id.is_some() && task_id != filtered_task_id {
            continue;
        }
        rows.push(ArrayTaskStatus {
            task_id,
            job_id_raw: fields[0].to_string(),
            state: normalize_scheduler_state_field(fields[1])
                .unwrap_or_else(|| normalize_scheduler_state(fields[1])),
            exit_code: normalize_scheduler_metadata(fields[2]),
            elapsed_seconds: fields[3].parse::<u64>().ok(),
            elapsed: fields[3].parse::<u64>().ok().map(format_walltime_duration),
            reason: slurm_array.map(|array| format!("array={array}")),
            source: SchedulerSource::Sacct,
        });
    }
    Ok(ArrayProbeResult::Rows(rows))
}

fn normalize_array_job_id(job_id: &str) -> (String, Option<u32>) {
    let Some((parent, task)) = job_id.split_once('_') else {
        return (job_id.to_string(), None);
    };
    (parent.to_string(), task.parse::<u32>().ok())
}

fn parse_array_task_id(raw_job_id: &str, parent_job_id: &str) -> Option<u32> {
    let suffix = raw_job_id.strip_prefix(parent_job_id)?;
    let suffix = suffix.strip_prefix('_')?;
    suffix
        .split_once('.')
        .map_or(suffix, |(task, _)| task)
        .parse::<u32>()
        .ok()
}

fn array_task_key(row: &ArrayTaskStatus) -> String {
    match row.task_id {
        Some(task_id) => format!("task:{task_id:010}"),
        None => format!("raw:{}", row.job_id_raw),
    }
}

pub(crate) fn command_unavailable_error(err: &std::io::Error) -> bool {
    process_probe::command_unavailable_error(err)
}

pub(crate) fn command_unavailable_detail(
    command_name: &str,
    binary: &str,
    err: &std::io::Error,
) -> String {
    process_probe::command_unavailable_detail(command_name, binary, err)
}

fn command_failure_detail(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    if stdout.is_empty() {
        "command exited with non-zero status".to_string()
    } else {
        stdout
    }
}

pub(super) fn build_scheduler_status(state: String, source: SchedulerSource) -> SchedulerStatus {
    let parsed = JobState::parse(&state);
    let terminal = parsed.is_terminal();
    SchedulerStatus {
        failed: terminal && !parsed.is_success(),
        terminal,
        source,
        state,
        detail: None,
    }
}

pub(crate) fn reconcile_scheduler_status(
    status: SchedulerStatus,
    submitted_at: u64,
    last_visible_at: Option<u64>,
    now: u64,
) -> SchedulerStatus {
    if status.source != SchedulerSource::LocalOnly {
        return status;
    }
    if status
        .detail
        .as_deref()
        .is_some_and(|detail| detail.contains("not available"))
    {
        return status;
    }

    if now.saturating_sub(submitted_at) <= INITIAL_SCHEDULER_LOOKUP_GRACE_SECONDS {
        return SchedulerStatus {
            state: JobState::WaitingForScheduler.as_str().to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "job is not visible in squeue or sacct yet; this is common just after submission"
                    .to_string(),
            ),
        };
    }

    if let Some(last_visible_at) = last_visible_at
        && now.saturating_sub(last_visible_at) <= ACCOUNTING_GAP_GRACE_SECONDS
    {
        return SchedulerStatus {
            state: JobState::WaitingForAccounting.as_str().to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "job just disappeared from squeue and has not appeared in sacct yet; waiting for accounting data"
                    .to_string(),
            ),
        };
    }

    status
}

pub(crate) fn is_transitional_local_only(status: &SchedulerStatus) -> bool {
    status.source == SchedulerSource::LocalOnly
        && matches!(
            JobState::parse(&status.state),
            JobState::WaitingForScheduler | JobState::WaitingForAccounting
        )
}

pub(crate) fn stats_unavailable_reason(scheduler: &SchedulerStatus) -> String {
    match JobState::parse(&scheduler.state) {
        JobState::Pending | JobState::Configuring | JobState::WaitingForScheduler => {
            "live step statistics are not available because the job is not running yet".to_string()
        }
        JobState::WaitingForAccounting => {
            "live step statistics are unavailable while Slurm accounting data is catching up"
                .to_string()
        }
        _ if scheduler.terminal => {
            "live step statistics are not available because the job is no longer running"
                .to_string()
        }
        JobState::Running => {
            "sstat did not report any numbered job steps for this running job".to_string()
        }
        _ => "sstat did not report any numbered job steps for this job".to_string(),
    }
}

pub(crate) fn unix_timestamp_now() -> u64 {
    crate::time_util::unix_timestamp_now()
}

fn probe_status_components(
    job_id: &str,
    options: &SchedulerOptions,
) -> (SchedulerStatus, Option<QueueDiagnostics>) {
    // The status/watch surfaces render sacct-sourced queue diagnostics
    // (eligible time) even for live jobs, so this entry point always probes
    // sacct to keep that output intact.
    probe_status_components_inner(job_id, options, true)
}

fn probe_status_components_inner(
    job_id: &str,
    options: &SchedulerOptions,
    always_probe_sacct: bool,
) -> (SchedulerStatus, Option<QueueDiagnostics>) {
    let (squeue, squeue_unavailable) =
        match probe_squeue_queue_diagnostics_result(job_id, &options.squeue_bin) {
            QueueProbeResult::Probe(probe) => (probe, None),
            QueueProbeResult::Unavailable(reason) => (None, Some(reason)),
        };
    let (sacct, sacct_unavailable) =
        if always_probe_sacct || squeue_state_needs_sacct(squeue.as_ref()) {
            match probe_sacct_queue_diagnostics_result(job_id, &options.sacct_bin) {
                QueueProbeResult::Probe(probe) => (probe, None),
                QueueProbeResult::Unavailable(reason) => (None, Some(reason)),
            }
        } else {
            (None, None)
        };
    let unavailable = [squeue_unavailable, sacct_unavailable]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assemble_status_components(squeue.as_ref(), sacct.as_ref(), &unavailable)
}

/// Assembles the raw `(SchedulerStatus, QueueDiagnostics)` pair from an optional
/// squeue probe (authoritative for live jobs) and sacct probe (authoritative for
/// terminal accounting). A job resolved by neither yields the shared
/// "scheduler state is unavailable" status.
fn assemble_status_components(
    squeue: Option<&QueueDiagnosticsProbe>,
    sacct: Option<&QueueDiagnosticsProbe>,
    unavailable: &[String],
) -> (SchedulerStatus, Option<QueueDiagnostics>) {
    let scheduler = scheduler_status_from_probe(squeue, SchedulerSource::Squeue)
        .or_else(|| scheduler_status_from_probe(sacct, SchedulerSource::Sacct))
        .unwrap_or_else(|| SchedulerStatus {
            state: JobState::Unknown.as_str().to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(if unavailable.is_empty() {
                "scheduler state is unavailable because squeue/sacct could not determine this job"
                    .to_string()
            } else {
                format!(
                    "scheduler state is not available: {}",
                    unavailable.join("; ")
                )
            }),
        });
    let queue_diagnostics = build_status_queue_diagnostics(&scheduler, squeue, sacct);
    (scheduler, queue_diagnostics)
}

/// Returns `true` when sacct is still required after a squeue probe: squeue
/// reports live (queued/running) jobs, so any state it returns that is not
/// terminal is authoritative and lets us skip sacct. When squeue is empty (job
/// left the queue) or the state is terminal, accounting remains authoritative.
fn squeue_state_needs_sacct(squeue: Option<&QueueDiagnosticsProbe>) -> bool {
    match squeue.and_then(|probe| probe.state.as_deref()) {
        Some(state) => is_terminal_state(state),
        None => true,
    }
}

/// Batched raw scheduler probe. Issues ONE squeue for every job id and, only for
/// the jobs squeue did not resolve to a live state, ONE sacct. Returns the same
/// raw `(SchedulerStatus, Option<QueueDiagnostics>)` pair `probe_scheduler_status`
/// / `probe_status_components` yield per job (before reconciliation). A job id
/// absent from all probe output maps to the same "scheduler state is unavailable"
/// status the single-job probe produces.
///
/// sacct is gated here (skipped for jobs squeue already reports as live): the
/// batched callers — sweep status/stats and `diff --across` — never render the
/// sacct-only `eligible_time` diagnostic, so gating is output-neutral for them.
pub fn probe_scheduler_status_many(
    job_ids: &[&str],
    options: &SchedulerOptions,
) -> BTreeMap<String, (SchedulerStatus, Option<QueueDiagnostics>)> {
    let mut result = BTreeMap::new();
    if job_ids.is_empty() {
        return result;
    }
    // Preserve order while de-duplicating for the comma-joined `-j` list.
    let mut unique = Vec::new();
    for id in job_ids {
        if !unique.contains(id) {
            unique.push(*id);
        }
    }

    let (squeue_map, squeue_unavailable) = probe_squeue_batch(&unique, &options.squeue_bin);
    let sacct_ids = unique
        .iter()
        .copied()
        .filter(|id| squeue_state_needs_sacct(squeue_map.get(*id)))
        .collect::<Vec<_>>();
    let (sacct_map, sacct_unavailable) = if sacct_ids.is_empty() {
        (BTreeMap::new(), None)
    } else {
        probe_sacct_batch(&sacct_ids, &options.sacct_bin)
    };

    for id in unique {
        let unavailable = [squeue_unavailable.clone(), sacct_unavailable.clone()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let components =
            assemble_status_components(squeue_map.get(id), sacct_map.get(id), &unavailable);
        result.insert(id.to_string(), components);
    }
    result
}

fn probe_squeue_batch(
    job_ids: &[&str],
    binary: &str,
) -> (BTreeMap<String, QueueDiagnosticsProbe>, Option<String>) {
    let joined = job_ids.join(",");
    let mut command = Command::new(binary);
    command.args(["-h", "-j", &joined, "-o", "%i|%T|%r|%S"]);
    let output = match run_scheduler_command(&mut command, "squeue", binary) {
        Ok(output) => output,
        Err(err) => match err.unavailable_detail() {
            Some(detail) => return (BTreeMap::new(), Some(detail)),
            None => return (BTreeMap::new(), None),
        },
    };
    if !output.status.success() {
        return (BTreeMap::new(), None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut map = BTreeMap::new();
    for line in stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split('|').map(str::trim);
        let Some(job_id) = fields.next().filter(|id| !id.is_empty()) else {
            continue;
        };
        let state = fields.next().and_then(normalize_scheduler_state_field);
        let pending_reason = fields.next().and_then(normalize_scheduler_metadata);
        let start_time = fields.next().and_then(normalize_scheduler_metadata);
        map.entry(job_id.to_string())
            .or_insert(QueueDiagnosticsProbe {
                state,
                pending_reason,
                start_time,
                ..QueueDiagnosticsProbe::default()
            });
    }
    (map, None)
}

fn probe_sacct_batch(
    job_ids: &[&str],
    binary: &str,
) -> (BTreeMap<String, QueueDiagnosticsProbe>, Option<String>) {
    let joined = job_ids.join(",");
    let mut command = Command::new(binary);
    command.args([
        "-n",
        "-X",
        "-j",
        &joined,
        "--format=JobID,State,Eligible,Start,Reason",
        "--parsable2",
    ]);
    let output = match run_scheduler_command(&mut command, "sacct", binary) {
        Ok(output) => output,
        Err(err) => match err.unavailable_detail() {
            Some(detail) => return (BTreeMap::new(), Some(detail)),
            None => return (BTreeMap::new(), None),
        },
    };
    if !output.status.success() {
        return (BTreeMap::new(), None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut map = BTreeMap::new();
    for line in stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split('|').map(str::trim);
        let Some(job_id) = fields.next().filter(|id| !id.is_empty()) else {
            continue;
        };
        let state = fields.next().and_then(normalize_scheduler_state_field);
        let eligible_time = fields.next().and_then(normalize_scheduler_metadata);
        let start_time = fields.next().and_then(normalize_scheduler_metadata);
        let pending_reason = fields.next().and_then(normalize_scheduler_metadata);
        map.entry(job_id.to_string())
            .or_insert(QueueDiagnosticsProbe {
                state,
                pending_reason,
                eligible_time,
                start_time,
            });
    }
    (map, None)
}

fn build_status_queue_diagnostics(
    scheduler: &SchedulerStatus,
    squeue: Option<&QueueDiagnosticsProbe>,
    sacct: Option<&QueueDiagnosticsProbe>,
) -> Option<QueueDiagnostics> {
    let pending_reason = if JobState::parse(&scheduler.state) == JobState::Pending {
        squeue
            .and_then(|probe| probe.pending_reason.clone())
            .or_else(|| {
                match sacct
                    .and_then(|probe| probe.state.as_deref())
                    .map(JobState::parse)
                {
                    Some(JobState::Pending) => sacct.and_then(|probe| probe.pending_reason.clone()),
                    _ => None,
                }
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
    if let Some(state) = runtime_state
        .and_then(|state| state.job_status.clone())
        .map(|state| normalize_scheduler_state(&state))
        && is_terminal_state(&state)
    {
        return build_scheduler_status(state, SchedulerSource::LocalOnly);
    }

    if let Some(pid) = supervisor_pid
        && pid_is_running(pid)
    {
        let state = runtime_state
            .and_then(|state| state.job_status.clone())
            .map(|state| normalize_scheduler_state(&state))
            .unwrap_or_else(|| JobState::Running.as_str().to_string());
        return SchedulerStatus {
            state,
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: None,
        };
    }

    if let Some(exit_code) = runtime_state.and_then(|state| state.job_exit_code) {
        return build_scheduler_status(
            if exit_code == 0 {
                JobState::Completed
            } else {
                JobState::Failed
            }
            .as_str()
            .to_string(),
            SchedulerSource::LocalOnly,
        );
    }

    if let Some(pid) = supervisor_pid {
        return SchedulerStatus {
            state: JobState::Failed.as_str().to_string(),
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
            state: JobState::WaitingForLocalRuntime.as_str().to_string(),
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
        state: JobState::WaitingForLocalRuntime.as_str().to_string(),
        source: SchedulerSource::LocalOnly,
        terminal: false,
        failed: false,
        detail: Some(
            "local runtime state has not been written yet; waiting for the launcher to initialize"
                .to_string(),
        ),
    }
}

pub(crate) fn pid_is_running(pid: u32) -> bool {
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

fn normalize_scheduler_state_field(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return None;
    }
    Some(normalize_scheduler_state(trimmed))
}

fn normalize_scheduler_metadata(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed.to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "n/a" | "na" | "none" | "null" | "unknown" | "invalid" | "not_set" | "not set"
    ) {
        return None;
    }
    Some(trimmed.to_string())
}

fn normalize_scheduler_state(raw: &str) -> String {
    raw.split_whitespace()
        .next()
        .unwrap_or(raw)
        .trim_end_matches('+')
        .to_ascii_uppercase()
}

/// A Slurm scheduler job state, parsed from raw squeue/sacct output or an
/// internal tracker sentinel.
///
/// Slurm reports job state as a free-form string; this enum models every state
/// the tracker's logic branches on: the terminal states (mirroring the historic
/// `is_terminal_state` list), the live states we name, and the internal
/// `WAITING_FOR_*` / `unknown` sentinels the tracker synthesizes when the
/// scheduler cannot answer. Any state Slurm emits that we do not model is
/// preserved verbatim — uppercased, exactly as [`JobState::parse`] normalizes —
/// in [`JobState::Other`], so it round-trips through serialization unchanged.
///
/// The serialized structs ([`SchedulerStatus`], [`ArrayTaskStatus`], the array
/// `state_counts` map) keep storing `state` as a `String`; `JobState` is the
/// typed lens used at comparison boundaries. [`JobState::as_str`] returns the
/// byte-identical wire string for every variant so converting in either
/// direction never perturbs on-disk state files or `--format json` output.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    BootFail,
    Cancelled,
    Completed,
    Deadline,
    Failed,
    LaunchFailed,
    NodeFail,
    OutOfMemory,
    Preempted,
    ReconfigFail,
    Revoked,
    Timeout,
    Pending,
    Running,
    Configuring,
    Completing,
    WaitingForScheduler,
    WaitingForAccounting,
    WaitingForLocalRuntime,
    Unknown,
    /// Any state we do not model, preserved uppercased (as today's normalize).
    Other(String),
}

impl JobState {
    /// Parses a raw scheduler state, absorbing the historic
    /// `normalize_scheduler_state` behaviour: take the first whitespace-delimited
    /// token (so sacct suffixes like `CANCELLED by 43` collapse to `CANCELLED`),
    /// trim a trailing `+`, and uppercase.
    pub fn parse(raw: &str) -> Self {
        Self::from_normalized(normalize_scheduler_state(raw))
    }

    fn from_normalized(normalized: String) -> Self {
        match normalized.as_str() {
            "BOOT_FAIL" => Self::BootFail,
            "CANCELLED" => Self::Cancelled,
            "COMPLETED" => Self::Completed,
            "DEADLINE" => Self::Deadline,
            "FAILED" => Self::Failed,
            "LAUNCH_FAILED" => Self::LaunchFailed,
            "NODE_FAIL" => Self::NodeFail,
            "OUT_OF_MEMORY" => Self::OutOfMemory,
            "PREEMPTED" => Self::Preempted,
            "RECONFIG_FAIL" => Self::ReconfigFail,
            "REVOKED" => Self::Revoked,
            "TIMEOUT" => Self::Timeout,
            "PENDING" => Self::Pending,
            "RUNNING" => Self::Running,
            "CONFIGURING" => Self::Configuring,
            "COMPLETING" => Self::Completing,
            "WAITING_FOR_SCHEDULER" => Self::WaitingForScheduler,
            "WAITING_FOR_ACCOUNTING" => Self::WaitingForAccounting,
            "WAITING_FOR_LOCAL_RUNTIME" => Self::WaitingForLocalRuntime,
            "UNKNOWN" => Self::Unknown,
            _ => Self::Other(normalized),
        }
    }

    /// Returns the exact wire string for this state — byte-identical to the
    /// strings the tracker has always stored and serialized. The `unknown`
    /// sentinel deliberately keeps its historic lowercase spelling.
    pub fn as_str(&self) -> &str {
        match self {
            Self::BootFail => "BOOT_FAIL",
            Self::Cancelled => "CANCELLED",
            Self::Completed => "COMPLETED",
            Self::Deadline => "DEADLINE",
            Self::Failed => "FAILED",
            Self::LaunchFailed => "LAUNCH_FAILED",
            Self::NodeFail => "NODE_FAIL",
            Self::OutOfMemory => "OUT_OF_MEMORY",
            Self::Preempted => "PREEMPTED",
            Self::ReconfigFail => "RECONFIG_FAIL",
            Self::Revoked => "REVOKED",
            Self::Timeout => "TIMEOUT",
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Configuring => "CONFIGURING",
            Self::Completing => "COMPLETING",
            Self::WaitingForScheduler => "WAITING_FOR_SCHEDULER",
            Self::WaitingForAccounting => "WAITING_FOR_ACCOUNTING",
            Self::WaitingForLocalRuntime => "WAITING_FOR_LOCAL_RUNTIME",
            Self::Unknown => "unknown",
            Self::Other(state) => state,
        }
    }

    /// Returns `true` for terminal Slurm states (the job has stopped). Matches
    /// the historic `is_terminal_state` set exactly.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::BootFail
                | Self::Cancelled
                | Self::Completed
                | Self::Deadline
                | Self::Failed
                | Self::LaunchFailed
                | Self::NodeFail
                | Self::OutOfMemory
                | Self::Preempted
                | Self::ReconfigFail
                | Self::Revoked
                | Self::Timeout
        )
    }

    /// Returns `true` for live Slurm states (the job is queued or executing).
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Running | Self::Configuring | Self::Completing
        )
    }

    /// Returns `true` only for the successful terminal state (`COMPLETED`).
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

pub(crate) fn is_terminal_state(state: &str) -> bool {
    JobState::parse(state).is_terminal()
}

pub(crate) fn cancel_job(job_id: &str, scancel_bin: &str) -> Result<()> {
    let output = Command::new(scancel_bin)
        .arg(job_id)
        .output()
        .context(format!("failed to execute '{scancel_bin}'"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("scancel failed for job {job_id}");
        }
        bail!("scancel failed for job {job_id}: {detail}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        println!("{stdout}");
    }
    println!("cancelled job: {job_id}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::job::runtime_state::{ServiceRuntimeStateEntry, ServiceRuntimeStateFile};

    use super::*;

    fn runtime_entry() -> ServiceRuntimeStateEntry {
        ServiceRuntimeStateEntry {
            service_name: "api".into(),
            step_name: Some("hpc-compose:api".into()),
            log_path: Some(PathBuf::from("/tmp/api.log")),
            launch_index: Some(0),
            launcher_pid: Some(std::process::id()),
            healthy: Some(false),
            completed_successfully: Some(false),
            readiness_configured: Some(false),
            failure_policy_mode: None,
            restart_count: Some(0),
            max_restarts: None,
            window_seconds: None,
            max_restarts_in_window: None,
            restart_failures_in_window: None,
            restart_failure_timestamps: None,
            last_exit_code: None,
            started_at: None,
            finished_at: None,
            duration_seconds: None,
            first_failure_at: None,
            first_failure_exit_code: None,
            first_failure_node: None,
            first_failure_rank: None,
            placement_mode: None,
            nodes: None,
            ntasks: None,
            ntasks_per_node: None,
            nodelist: None,
            assertions: None,
        }
    }

    fn walltime_record(requested_walltime: Option<RequestedWalltime>) -> SubmissionRecord {
        SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: "12345".into(),
            submitted_at: 1_200,
            compose_file: PathBuf::from("/tmp/compose.yaml"),
            submit_dir: PathBuf::from("/tmp"),
            script_path: PathBuf::from("/tmp/job.sbatch"),
            cache_dir: PathBuf::from("/tmp/cache"),
            runtime_root: None,
            batch_log: PathBuf::from("/tmp/slurm-12345.out"),
            batch_log_managed: false,
            service_logs: BTreeMap::new(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
            provenance: None,
            tags: Vec::new(),
            notes: Vec::new(),
        }
    }

    #[cfg(unix)]
    fn write_fake_probe(tmpdir: &Path, name: &str, stdout: &str) -> PathBuf {
        write_fake_script(
            tmpdir,
            name,
            &format!("#!/bin/sh\ncat <<'EOF'\n{stdout}\nEOF\n"),
        )
    }

    #[cfg(unix)]
    fn write_fake_script(tmpdir: &Path, name: &str, body: &str) -> PathBuf {
        let path = tmpdir.join(name);
        fs::write(&path, body).expect("fake probe");
        let mut perms = fs::metadata(&path).expect("metadata").permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        fs::set_permissions(&path, perms).expect("chmod");
        path
    }

    #[test]
    fn walltime_and_log_status_helpers_cover_edge_cases() {
        assert_eq!(format_walltime_duration(3_661), "01:01:01");
        assert_eq!(format_walltime_duration(90_061), "1-01:01:01");
        assert_eq!(parse_scheduler_timestamp("not-a-timestamp"), None);
        assert!(parse_scheduler_timestamp("2026-04-10T12:00:00Z").is_some());

        let running = build_scheduler_status("RUNNING".into(), SchedulerSource::Squeue);
        let pending = build_scheduler_status("PENDING".into(), SchedulerSource::Squeue);
        let record = walltime_record(Some(RequestedWalltime {
            original: "00:10:00".into(),
            seconds: 600,
        }));

        assert!(walltime_progress(&record, &pending, None, 1_500).is_none());
        assert!(walltime_progress(&walltime_record(None), &running, None, 1_500).is_none());

        let from_record_submit =
            walltime_progress(&record, &running, None, 1_500).expect("progress from submitted_at");
        assert_eq!(from_record_submit.elapsed_seconds, 300);
        assert_eq!(from_record_submit.remaining_seconds, 300);

        let queue = QueueDiagnostics {
            pending_reason: None,
            eligible_time: None,
            start_time: Some("1970-01-01T00:20:00Z".into()),
        };
        let from_queue_start = walltime_progress(&record, &running, Some(&queue), 1_500)
            .expect("progress from queue start");
        assert_eq!(from_queue_start.elapsed_seconds, 300);

        let saturated =
            walltime_progress(&record, &running, Some(&queue), 9_999).expect("saturated progress");
        assert_eq!(saturated.elapsed_seconds, 600);
        assert_eq!(saturated.remaining_seconds, 0);
        assert_eq!(walltime_progress_percent(&saturated), 100);

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = build_log_status(&tmpdir.path().join("missing.log"), 10);
        assert!(!missing.present);
        assert!(missing.updated_at.is_none());
        assert!(missing.updated_age_seconds.is_none());

        let log = tmpdir.path().join("app.log");
        fs::write(&log, "ready\n").expect("log");
        let present = build_log_status(&log, unix_timestamp_now());
        assert!(present.present);
        assert!(present.updated_at.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn probe_scheduler_status_many_batches_and_maps_states() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        // squeue emits one `%i|%T|%r|%S` row per requested id (skipping the
        // ones it does not know), recording its argv so we can assert a single
        // comma-joined invocation.
        let squeue_log = tmpdir.path().join("squeue.argv");
        let squeue = write_fake_script(
            tmpdir.path(),
            "squeue",
            &format!(
                r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
job=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-j" ]]; then job="$arg"; fi
  prev="$arg"
done
IFS=',' read -ra ids <<< "$job"
for id in "${{ids[@]}}"; do
  case "$id" in
    100) echo "100|RUNNING|N/A|N/A" ;;
    101) echo "101|PENDING|Resources|N/A" ;;
  esac
done
"#,
                squeue_log.display()
            ),
        );
        // sacct records any invocation and resolves only the terminal id (102).
        let sacct_log = tmpdir.path().join("sacct.argv");
        let sacct = write_fake_script(
            tmpdir.path(),
            "sacct",
            &format!(
                r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
job=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-j" ]]; then job="$arg"; fi
  prev="$arg"
done
IFS=',' read -ra ids <<< "$job"
for id in "${{ids[@]}}"; do
  case "$id" in
    102) echo "102|COMPLETED|2026-01-01T00:00:00|2026-01-01T00:01:00|None" ;;
  esac
done
"#,
                sacct_log.display()
            ),
        );

        let options = SchedulerOptions {
            squeue_bin: squeue.to_string_lossy().to_string(),
            sacct_bin: sacct.to_string_lossy().to_string(),
        };
        let result = probe_scheduler_status_many(&["100", "101", "102", "103"], &options);
        assert_eq!(result["100"].0.state, "RUNNING");
        assert_eq!(result["100"].0.source, SchedulerSource::Squeue);
        assert_eq!(result["101"].0.state, "PENDING");
        assert_eq!(result["102"].0.state, "COMPLETED");
        assert_eq!(result["102"].0.source, SchedulerSource::Sacct);
        // 103 is absent from both probes -> shared "unavailable" status.
        assert_eq!(result["103"].0.state, "unknown");
        assert_eq!(result["103"].0.source, SchedulerSource::LocalOnly);

        // squeue ran exactly once with a comma-joined `-j` list of every id.
        let squeue_calls = fs::read_to_string(&squeue_log).expect("squeue log");
        assert_eq!(squeue_calls.lines().count(), 1);
        assert!(squeue_calls.contains("100,101,102,103"));
        // sacct ran exactly once, only for the ids squeue left unresolved
        // (the two live states are gated out).
        let sacct_calls = fs::read_to_string(&sacct_log).expect("sacct log");
        assert_eq!(sacct_calls.lines().count(), 1);
        assert!(sacct_calls.contains("102,103"));
        assert!(!sacct_calls.contains("-j 100"));
    }

    #[cfg(unix)]
    #[test]
    fn probe_scheduler_status_skips_sacct_for_live_squeue_state() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let squeue = write_fake_probe(tmpdir.path(), "squeue", "RUNNING|N/A|N/A");
        let sacct_log = tmpdir.path().join("sacct.argv");
        let sacct = write_fake_script(
            tmpdir.path(),
            "sacct",
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\necho 'RUNNING|2026-01-01T00:00:00|2026-01-01T00:01:00|None'\n",
                sacct_log.display()
            ),
        );
        let options = SchedulerOptions {
            squeue_bin: squeue.to_string_lossy().to_string(),
            sacct_bin: sacct.to_string_lossy().to_string(),
        };

        // State-only probe: squeue already reports RUNNING, so sacct is skipped.
        let status = probe_scheduler_status("100", &options);
        assert_eq!(status.state, "RUNNING");
        assert_eq!(status.source, SchedulerSource::Squeue);
        assert!(
            !sacct_log.exists(),
            "sacct must not run when squeue reports a live state for a state-only probe"
        );

        // The status/watch path keeps probing sacct so eligible-time diagnostics
        // survive for live jobs.
        let (with_diag, diagnostics) =
            probe_scheduler_status_with_queue_diagnostics("100", &options);
        assert_eq!(with_diag.state, "RUNNING");
        assert!(sacct_log.exists(), "status path must still probe sacct");
        assert_eq!(
            diagnostics.and_then(|queue| queue.eligible_time).as_deref(),
            Some("2026-01-01T00:00:00")
        );
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_probe_parses_fake_squeue_and_sacct_diagnostics() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let squeue = write_fake_probe(
            tmpdir.path(),
            "squeue",
            "PENDING|Resources|2026-04-10T12:00:00",
        );
        let sacct = write_fake_probe(
            tmpdir.path(),
            "sacct",
            "PENDING|2026-04-10T11:55:00|2026-04-10T12:05:00|Priority",
        );

        let squeue_probe =
            probe_squeue_queue_diagnostics("12345", squeue.to_str().expect("squeue path"))
                .expect("squeue probe");
        assert_eq!(squeue_probe.state.as_deref(), Some("PENDING"));
        assert_eq!(squeue_probe.pending_reason.as_deref(), Some("Resources"));
        assert_eq!(
            squeue_probe.start_time.as_deref(),
            Some("2026-04-10T12:00:00")
        );

        let sacct_probe =
            probe_sacct_queue_diagnostics("12345", sacct.to_str().expect("sacct path"))
                .expect("sacct probe");
        assert_eq!(sacct_probe.state.as_deref(), Some("PENDING"));
        assert_eq!(sacct_probe.pending_reason.as_deref(), Some("Priority"));
        assert_eq!(
            sacct_probe.eligible_time.as_deref(),
            Some("2026-04-10T11:55:00")
        );

        let (status, diagnostics) = probe_scheduler_status_with_queue_diagnostics(
            "12345",
            &SchedulerOptions {
                squeue_bin: squeue.to_string_lossy().to_string(),
                sacct_bin: sacct.to_string_lossy().to_string(),
            },
        );
        assert_eq!(status.source, SchedulerSource::Squeue);
        assert_eq!(status.state, "PENDING");
        let diagnostics = diagnostics.expect("diagnostics");
        assert_eq!(diagnostics.pending_reason.as_deref(), Some("Resources"));
        assert_eq!(
            diagnostics.eligible_time.as_deref(),
            Some("2026-04-10T11:55:00")
        );
        assert_eq!(
            diagnostics.start_time.as_deref(),
            Some("2026-04-10T12:00:00")
        );
    }

    #[cfg(unix)]
    #[cfg(unix)]
    #[test]
    fn scheduler_command_reads_full_output() {
        // Regression guard for the pipe-read refactor: a successful command's stdout
        // must be captured in full. A genuine `read_to_end` error can't be forced
        // deterministically via the fake-binary fixtures (a closed pipe yields EOF,
        // i.e. `Ok(0)`, not an error), so we assert the happy path stays intact and
        // rely on the type system to route any real read error into `Io`.
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let printer = write_fake_probe(tmpdir.path(), "printy-squeue", "JOBID STATE\n42 RUNNING");
        let binary = printer.to_string_lossy().to_string();
        let mut command = Command::new(&printer);

        let output = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_secs(5),
        )
        .expect("fake command should succeed");

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("JOBID STATE"));
        assert!(stdout.contains("42 RUNNING"));
    }

    #[test]
    fn scheduler_command_timeout_reports_unavailable() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let sleeper = write_fake_script(tmpdir.path(), "sleepy-squeue", "#!/bin/sh\nsleep 5\n");
        let binary = sleeper.to_string_lossy().to_string();
        let mut command = Command::new(&sleeper);

        let err = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_millis(50),
        )
        .expect_err("sleeping command should time out");

        match err {
            SchedulerCommandError::Unavailable(err) => {
                assert!(err.detail().contains("squeue timed out"));
                assert!(err.detail().contains(&binary));
            }
            SchedulerCommandError::Io(err) => panic!("expected timeout detail, got {err}"),
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

        let terminal_with_live_pid = ServiceRuntimeStateFile {
            backend: Some(SubmissionBackend::Local),
            job_status: Some("completed".into()),
            job_exit_code: Some(0),
            supervisor_pid: Some(current_pid),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            services: Vec::new(),
        };
        let completed = build_local_scheduler_status(Some(&terminal_with_live_pid));
        assert_eq!(completed.state, "COMPLETED");
        assert!(completed.terminal);
        assert!(!completed.failed);

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

    #[test]
    fn normalize_scheduler_metadata_filters_sentinels() {
        assert_eq!(normalize_scheduler_metadata(""), None);
        assert_eq!(normalize_scheduler_metadata("   "), None);
        for sentinel in [
            "n/a", "N/A", "na", "NA", "none", "None", "NONE", "null", "NULL", "unknown", "Unknown",
            "invalid", "INVALID", "not_set", "NOT_SET", "not set", "Not Set",
        ] {
            assert_eq!(
                normalize_scheduler_metadata(sentinel),
                None,
                "expected sentinel {sentinel:?} to normalize to None",
            );
        }
        assert_eq!(
            normalize_scheduler_metadata("Resources"),
            Some("Resources".to_string())
        );
        assert_eq!(
            normalize_scheduler_metadata("  Priority  "),
            Some("Priority".to_string())
        );
        assert_eq!(
            normalize_scheduler_metadata("none-of-the-above"),
            Some("none-of-the-above".to_string())
        );
    }

    #[test]
    fn derive_service_duration_seconds_covers_all_branches() {
        assert_eq!(
            derive_service_duration_seconds(None, Some(50), 100, true),
            None
        );
        assert_eq!(
            derive_service_duration_seconds(Some(10), Some(50), 999, false),
            Some(40)
        );
        assert_eq!(
            derive_service_duration_seconds(Some(50), Some(10), 999, true),
            Some(0)
        );
        assert_eq!(
            derive_service_duration_seconds(Some(10), None, 100, true),
            Some(90)
        );
        assert_eq!(
            derive_service_duration_seconds(Some(200), None, 100, true),
            Some(0)
        );
        assert_eq!(
            derive_service_duration_seconds(Some(10), None, 100, false),
            None
        );
    }

    #[test]
    fn job_state_round_trips_every_variant() {
        let variants = [
            JobState::BootFail,
            JobState::Cancelled,
            JobState::Completed,
            JobState::Deadline,
            JobState::Failed,
            JobState::LaunchFailed,
            JobState::NodeFail,
            JobState::OutOfMemory,
            JobState::Preempted,
            JobState::ReconfigFail,
            JobState::Revoked,
            JobState::Timeout,
            JobState::Pending,
            JobState::Running,
            JobState::Configuring,
            JobState::Completing,
            JobState::WaitingForScheduler,
            JobState::WaitingForAccounting,
            JobState::WaitingForLocalRuntime,
            JobState::Unknown,
            // Other must carry the uppercased form parse would produce.
            JobState::Other("SUSPENDED".to_string()),
        ];
        for variant in variants {
            assert_eq!(
                JobState::parse(variant.as_str()),
                variant.clone(),
                "round-trip failed for {variant:?} (as_str {:?})",
                variant.as_str()
            );
        }
        // The `unknown` sentinel keeps its historic lowercase wire spelling.
        assert_eq!(JobState::Unknown.as_str(), "unknown");
    }

    #[test]
    fn job_state_parse_absorbs_normalize_and_preserves_unknown_states() {
        // Absorbs normalize_scheduler_state: first token, trailing '+', uppercase.
        assert_eq!(JobState::parse("running"), JobState::Running);
        assert_eq!(JobState::parse("RUNNING+"), JobState::Running);
        assert_eq!(JobState::parse("CANCELLED by 43"), JobState::Cancelled);
        // Unmodeled states round-trip verbatim (uppercased), exactly as the old
        // normalize_scheduler_state produced them.
        assert_eq!(
            JobState::parse("suspended"),
            JobState::Other("SUSPENDED".to_string())
        );
        assert_eq!(
            JobState::parse("suspended").as_str(),
            normalize_scheduler_state("suspended")
        );
    }

    #[test]
    fn job_state_terminal_and_live_agree_with_legacy_oracle() {
        // Oracle: the exact literal list the historic is_terminal_state matched.
        fn legacy_is_terminal(state: &str) -> bool {
            matches!(
                state,
                "BOOT_FAIL"
                    | "CANCELLED"
                    | "COMPLETED"
                    | "DEADLINE"
                    | "FAILED"
                    | "LAUNCH_FAILED"
                    | "NODE_FAIL"
                    | "OUT_OF_MEMORY"
                    | "PREEMPTED"
                    | "RECONFIG_FAIL"
                    | "REVOKED"
                    | "TIMEOUT"
            )
        }
        let all_states = [
            "BOOT_FAIL",
            "CANCELLED",
            "COMPLETED",
            "DEADLINE",
            "FAILED",
            "LAUNCH_FAILED",
            "NODE_FAIL",
            "OUT_OF_MEMORY",
            "PREEMPTED",
            "RECONFIG_FAIL",
            "REVOKED",
            "TIMEOUT",
            "PENDING",
            "RUNNING",
            "CONFIGURING",
            "COMPLETING",
            "WAITING_FOR_SCHEDULER",
            "WAITING_FOR_ACCOUNTING",
            "WAITING_FOR_LOCAL_RUNTIME",
            "unknown",
            "SUSPENDED",
        ];
        for state in all_states {
            let parsed = JobState::parse(state);
            assert_eq!(
                parsed.is_terminal(),
                legacy_is_terminal(state),
                "is_terminal disagreed with legacy oracle for {state}"
            );
            // The is_terminal_state wrapper must agree with the enum too.
            assert_eq!(is_terminal_state(state), parsed.is_terminal());
        }
        for state in ["PENDING", "RUNNING", "CONFIGURING", "COMPLETING"] {
            assert!(JobState::parse(state).is_live(), "{state} should be live");
        }
        for state in [
            "COMPLETED",
            "FAILED",
            "WAITING_FOR_SCHEDULER",
            "unknown",
            "SUSPENDED",
        ] {
            assert!(
                !JobState::parse(state).is_live(),
                "{state} should not be live"
            );
        }
        assert!(JobState::parse("COMPLETED").is_success());
        assert!(!JobState::parse("FAILED").is_success());
    }
}
