//! Attempt/requeue history reconstructed from LOCAL tracked state only.
//!
//! `checkpoints` reads the per-attempt `state.json` files under
//! `.hpc-compose/<job>/attempts/<n>/` (written only when `x-slurm.resume` is
//! configured and the job is requeued) and falls back to the single latest
//! `state.json` when no attempt directories exist. It contacts no scheduler and
//! reads nothing from the cluster filesystem: a missing or unreadable attempt is
//! skipped and surfaced in `degraded`, never fatal.

use super::runtime_state::ServiceRuntimeStateFile;
use super::*;

/// Attempt/requeue history for one tracked job, derived purely from local
/// tracked state. Hand-rolled output (not a [`StatusSnapshot`]); read-only.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointHistory {
    pub job_id: String,
    pub compose_file: PathBuf,
    pub submitted_at: u64,
    /// True when per-attempt directories exist (i.e. `x-slurm.resume` produced a
    /// requeue layout) or the record pins a resume directory.
    pub resume_configured: bool,
    /// Number of attempts observed: `max(attempt dir count, latest.attempt + 1)`,
    /// or `1` for the single-state fallback.
    pub attempts: u32,
    /// Requeues = `attempts - 1` (never underflows).
    pub requeues: u32,
    /// The highest attempt index observed (0-based), if any attempt was read.
    pub current_attempt: Option<u32>,
    /// `is_resume` flag of the latest readable attempt state, if present.
    pub is_resume: Option<bool>,
    /// Resume directory pinned in the latest readable attempt state, if present.
    pub resume_dir: Option<PathBuf>,
    pub entries: Vec<CheckpointAttempt>,
    /// Non-fatal degradation notes (missing/unreadable state, count
    /// disagreements, truncated history).
    pub degraded: Vec<String>,
}

/// One reconstructed attempt derived from a single `state.json`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointAttempt {
    /// 0-based attempt index. For the single-state fallback this is the state's
    /// own `attempt` field when present, otherwise `0`.
    pub attempt: u32,
    pub is_resume: Option<bool>,
    pub job_status: Option<String>,
    pub job_exit_code: Option<i32>,
    /// Earliest `started_at` over the attempt's services.
    pub started_at: Option<u64>,
    /// Latest `finished_at` over the attempt's services.
    pub finished_at: Option<u64>,
    /// `finished_at - started_at` when both bounds are known.
    pub duration_seconds: Option<u64>,
    pub services: Vec<CheckpointAttemptService>,
}

/// Per-service timing for one attempt.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointAttemptService {
    pub service_name: String,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
    pub duration_seconds: Option<u64>,
    pub last_exit_code: Option<i32>,
    pub restart_count: Option<u32>,
}

/// Reads numeric attempt subdirectories of `attempts_dir`, sorted ascending,
/// ignoring non-numeric entries (and recording them in `degraded`).
fn list_attempt_numbers(attempts_dir: &Path, degraded: &mut Vec<String>) -> Vec<u32> {
    let entries = match fs::read_dir(attempts_dir) {
        Ok(entries) => entries,
        Err(err) => {
            degraded.push(format!(
                "could not read attempts directory {}: {err}",
                attempts_dir.display()
            ));
            return Vec::new();
        }
    };
    let mut numbers = Vec::new();
    for entry in entries.flatten() {
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) => {
                degraded.push(format!("could not stat {}: {err}", entry.path().display()));
                continue;
            }
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        match name.parse::<u32>() {
            Ok(number) => numbers.push(number),
            Err(_) => degraded.push(format!(
                "ignoring non-numeric attempt directory {}",
                entry.path().display()
            )),
        }
    }
    numbers.sort_unstable();
    numbers
}

/// Folds a single runtime state file into a [`CheckpointAttempt`].
fn attempt_from_state(attempt: u32, state: &ServiceRuntimeStateFile) -> CheckpointAttempt {
    let services: Vec<CheckpointAttemptService> = state
        .services
        .iter()
        .map(|service| {
            let duration_seconds = service.duration_seconds.or_else(|| {
                service
                    .started_at
                    .zip(service.finished_at)
                    .map(|(started, finished)| finished.saturating_sub(started))
            });
            CheckpointAttemptService {
                service_name: service.service_name.clone(),
                started_at: service.started_at,
                finished_at: service.finished_at,
                duration_seconds,
                last_exit_code: service.last_exit_code,
                restart_count: service.restart_count,
            }
        })
        .collect();

    let started_at = services
        .iter()
        .filter_map(|service| service.started_at)
        .min();
    let finished_at = services
        .iter()
        .filter_map(|service| service.finished_at)
        .max();
    let duration_seconds = started_at
        .zip(finished_at)
        .map(|(started, finished)| finished.saturating_sub(started));

    CheckpointAttempt {
        attempt,
        is_resume: state.is_resume,
        job_status: state.job_status.clone(),
        job_exit_code: state.job_exit_code,
        started_at,
        finished_at,
        duration_seconds,
        services,
    }
}

/// Collects the attempt/requeue history for `record` from LOCAL tracked state.
///
/// Never contacts the scheduler or reads cluster state. Missing/unreadable
/// state is skipped and recorded in [`CheckpointHistory::degraded`], so the
/// result is always coherent (possibly `attempts == 0`) and never panics.
pub fn collect_checkpoint_history(record: &SubmissionRecord) -> CheckpointHistory {
    let job_root = runtime_job_root_for_record(record);
    let attempts_dir = tracked_paths::attempts_dir(&job_root);
    let mut degraded = Vec::new();
    let mut entries = Vec::new();
    // The `attempt` field stored *inside* the latest readable state.json, used
    // only to cross-check the directory-derived index (open question 3).
    let mut latest_state_attempt: Option<u32> = None;
    // Highest 0-based attempt *directory* index that exists on disk, regardless
    // of whether its state.json could be read (so a reaped/corrupt attempt still
    // bumps the count instead of silently lowering the requeue total).
    let mut max_dir_index: Option<u32> = None;

    let resume_layout = attempts_dir.is_dir();
    let resume_configured = resume_layout || record.resume_dir.is_some();

    if resume_layout {
        let numbers = list_attempt_numbers(&attempts_dir, &mut degraded);
        max_dir_index = numbers.last().copied();
        // Flag a gap in 0-based indices (e.g. attempt 0 reaped by GC while a
        // higher index survives) so requeue counts are not silently miscounted.
        if let Some(max) = max_dir_index {
            let expected = max as usize + 1;
            if numbers.len() < expected {
                degraded.push(format!(
                    "truncated attempt history: found {} of {} expected 0-based attempt directories",
                    numbers.len(),
                    expected
                ));
            }
        }
        for number in numbers {
            let attempt_root = tracked_paths::attempt_root(&job_root, number);
            let state_path = tracked_paths::attempt_state_path(&attempt_root);
            match read_json::<ServiceRuntimeStateFile>(&state_path) {
                Ok(state) => {
                    latest_state_attempt = state.attempt.or(latest_state_attempt);
                    entries.push(attempt_from_state(number, &state));
                }
                Err(err) => degraded.push(format!(
                    "could not read attempt state at {}: {err}",
                    state_path.display()
                )),
            }
        }
    } else {
        // Single-state fallback: a non-resume job writes one state.json at the
        // job root with no attempt subdirectories. It always counts as a single
        // attempt with no requeues and no per-attempt index.
        let state_path = tracked_paths::latest_state_path(&job_root);
        match read_json::<ServiceRuntimeStateFile>(&state_path) {
            Ok(state) => {
                latest_state_attempt = state.attempt;
                let attempt = state.attempt.unwrap_or(0);
                entries.push(attempt_from_state(attempt, &state));
            }
            Err(err) => degraded.push(format!(
                "could not read state at {}: {err}",
                state_path.display()
            )),
        }
    }

    // Cross-check the highest 0-based directory index against the latest state's
    // own `attempt` field; prefer max(dir_index, state_attempt) and record any
    // disagreement (e.g. an in-flight attempt whose state.json isn't written).
    if let (Some(dir_index), Some(state_attempt)) = (max_dir_index, latest_state_attempt)
        && dir_index != state_attempt
    {
        degraded.push(format!(
            "attempt index disagreement: highest directory index {dir_index} vs latest state attempt {state_attempt}"
        ));
    }

    let (current_attempt, attempts) = if resume_layout {
        let highest = match (max_dir_index, latest_state_attempt) {
            (Some(dir), Some(state)) => Some(dir.max(state)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        };
        // Empty/unreadable attempts tree reports 0 cleanly without panicking.
        (highest, highest.map_or(0, |max| max.saturating_add(1)))
    } else {
        // Fallback: one attempt (when state was readable), no per-attempt index.
        (None, u32::try_from(entries.len()).unwrap_or(u32::MAX))
    };
    let requeues = attempts.saturating_sub(1);
    let is_resume = entries.last().and_then(|entry| entry.is_resume);
    let resume_dir = record.resume_dir.clone();

    CheckpointHistory {
        job_id: record.job_id.clone(),
        compose_file: record.compose_file.clone(),
        submitted_at: record.submitted_at,
        resume_configured,
        attempts,
        requeues,
        current_attempt,
        is_resume,
        resume_dir,
        entries,
        degraded,
    }
}
