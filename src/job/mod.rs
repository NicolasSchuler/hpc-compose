//! Tracking, status inspection, log streaming, metrics, and artifact export
//! for submitted jobs.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::Builder;

use crate::render::log_file_name_for_service;
use crate::runtime_plan::RuntimePlan;
use crate::tracked_paths;

mod accounting;
mod artifacts;
mod bundle;
mod checkpoints;
mod deep_clean;
mod diff;
mod evidence;
mod logs;
mod metrics_probe;
mod model;
mod provenance;
mod ps;
mod record;
mod replay;
mod rightsize;
mod runtime_state;
mod scheduler;
mod score;
mod stats;
mod stats_rollup;
mod sweep;
mod verify;
mod watchdog;

#[cfg(test)]
use artifacts::{copy_path_recursive, remove_existing_destination, resolve_export_dir};
#[cfg(test)]
use logs::{read_new_lines, selected_service_logs, tail_lines};
#[cfg(test)]
use stats::{
    load_sampler_snapshot, parse_sstat_output, probe_step_stats, step_from_slurm_sample_row,
};

pub use accounting::{AccountingRow, AccountingSnapshot, AccountingSummary};
pub use artifacts::{
    ArtifactBundleManifest, ArtifactBundleProvenance, ArtifactEntryMetadata, ArtifactExportOptions,
    ArtifactExportReport, ArtifactManifest, BundleExportReport, artifact_manifest_path_for_record,
    artifact_payload_dir_for_record, artifacts_dir_for_record, export_artifacts,
};
pub use bundle::{
    ExperimentBundleFileEntry, ExperimentBundleManifest, ExperimentBundleOptions,
    write_experiment_bundle,
};
pub use checkpoints::{
    CheckpointAttempt, CheckpointAttemptService, CheckpointHistory, collect_checkpoint_history,
};
pub use deep_clean::{
    DeepCleanupDetails, OrphanRuntimeDirReport, build_deep_cleanup_report, run_deep_cleanup_report,
};
pub use diff::{
    JobDiffChange, JobDiffReport, JobDiffServiceStatus, JobDiffSide, JobMatrixReport, JobMatrixRow,
    JobMatrixRun, SpecDiffReport, build_job_diff_report, build_job_matrix_report,
    build_spec_diff_report,
};
pub use logs::{
    LogPrintOptions, WatchOutcome, parse_log_since_duration, parse_queue_warn_after_duration,
    print_logs, wait_for_job_start, watch_submission,
};
pub use metrics_probe::{
    MetricsProbeOptions, MetricsProbeReport, build_metrics_probe_report,
    serialize_metrics_probe_report, validate_metrics_probe_options,
};
pub use model::{
    JobNote, RequestedWalltime, SchedulerSource, SubmissionBackend, SubmissionKind,
    SubmissionRecord, SubmissionRecordBuildOptions, SweepTrialMetadata,
};
pub use provenance::{GitProvenance, JobProvenance, collect_provenance, read_git_provenance};
pub use ps::{PsSnapshot, build_ps_snapshot};
pub use record::{
    CleanupJobReport, CleanupMode, CleanupReport, JobInventoryEntry, JobInventoryScan,
    MAX_NOTE_LEN, MAX_TAG_LEN, MAX_TAGS_PER_RECORD, append_job_note, apply_tag_changes,
    build_cleanup_report, build_submission_record, build_submission_record_with_backend,
    build_submission_record_with_backend_and_options, build_submission_record_with_options,
    clean_all_except_latest, clean_by_age, find_submission_record_in_repo, jobs_dir_for,
    latest_canary_record_path_for, latest_notebook_record_path_for, latest_record_path_for,
    latest_run_record_path_for, load_submission_record, load_submission_record_optional,
    log_dir_for_record, metadata_root_for, persist_submission_record, remove_submission_record,
    run_cleanup_report, runtime_job_root_for_record, scan_job_inventory, scan_job_records,
    state_path_for_record, update_submission_record, validate_note_text, validate_tag,
    write_submission_record,
};
pub use replay::{
    ReplayArtifactPaths, ReplayEvent, ReplayEventKind, ReplayFrame, ReplayReport,
    ReplayServiceFrame, build_replay_report,
};
pub use rightsize::{
    RightsizeConfidence, RightsizeObservation, RightsizeRecommendation, RightsizeReport,
    build_rightsize_report,
};
pub(crate) use scheduler::cancel_job;
pub(crate) use scheduler::pid_is_running;
pub use scheduler::{
    ArrayStatusSnapshot, ArrayTaskStatus, BatchLogStatus, JobState, PsServiceRow, QueueDiagnostics,
    SchedulerStatus, ServiceAssertionStatus, ServiceLogStatus, StatusSnapshot, WalltimeProgress,
    build_array_status_snapshot, build_status_snapshot, build_status_snapshot_with_array,
    build_status_snapshot_with_status, format_walltime_duration, format_walltime_summary,
    parse_scheduler_timestamp, probe_scheduler_status, probe_scheduler_status_many,
    probe_scheduler_status_with_queue_diagnostics, scheduler_source_label, walltime_progress,
    walltime_progress_percent,
};
pub use score::{
    EfficiencyScoreComponent, EfficiencyScoreConfidence, EfficiencyScoreOptions,
    EfficiencyScoreReport, build_efficiency_score_report,
};
pub use stats::{
    CollectorCoverage, CollectorCoverageScope, CollectorCoverageSummary, CollectorStatus,
    CpuNodeSample, CpuSnapshot, CpuSummary, FirstFailure, GpuDeviceSample, GpuNodeSummary,
    GpuProcessSample, GpuSnapshot, SamplerSnapshot, SchedulerOptions, SlurmSamplerSnapshot,
    StatsOptions, StatsSnapshot, StepStats, build_stats_snapshot, build_stats_snapshot_with_status,
    collector_coverage_summaries, load_collector_coverage_summaries, metrics_dir_for_record,
    telemetry_coverage_warnings,
};
pub use stats_rollup::{ReplicateStats, group_by_config, replicate_rollup};
pub use sweep::{
    SWEEP_MANIFEST_SCHEMA_VERSION, SweepExpansion, SweepExpansionTrial, SweepManifest,
    SweepManifestTrial, compose_file_sha256, detect_sweep_drift, expand_sweep,
    expand_sweep_with_limit, generate_sweep_id, interpolation_vars_for_sweep_metadata,
    interpolation_vars_for_sweep_trial, latest_sweep_manifest_path_for, load_sweep_manifest,
    resume_trial_positions, scan_sweep_manifests, sweep_manifest_path_for, write_sweep_manifest,
};
pub use verify::{
    StatusVerificationCheck, StatusVerificationReport, build_status_verification_report,
};
pub use watchdog::{
    WatchdogClassification, WatchdogObservation, WatchdogResource, WatchdogSnapshot, WatchdogStatus,
};

const SUBMISSION_SCHEMA_VERSION: u32 = 3;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const INITIAL_SCHEDULER_LOOKUP_GRACE_SECONDS: u64 = 15;
const ACCOUNTING_GAP_GRACE_SECONDS: u64 = 15;
const ARTIFACT_MANIFEST_SCHEMA_VERSION: u32 = 3;
const ARTIFACT_PROVENANCE_SCHEMA_VERSION: u32 = 2;

fn absolute_path(path: &Path) -> Result<PathBuf> {
    crate::path_util::absolute_path_cwd(path)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let serialized =
        serde_json::to_vec_pretty(value).context("failed to serialize job metadata")?;
    // Atomic, owner-only write via a per-writer unique temp file + rename, so
    // concurrent runs on a shared filesystem never publish (or observe) a torn
    // record, do not collide on a fixed `*.json.tmp` name, and do not expose
    // potentially sensitive command/sweep/config metadata to other users.
    crate::secure_io::write_atomic(path, &serialized, true)
        .context(format!("failed to write {}", path.display()))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).context(format!("failed to parse {}", path.display()))
}

/// Read an optional JSON file, distinguishing "legitimately absent" from "broken".
///
/// A missing file (`NotFound`) is an expected, silent `None`. A corrupt/truncated
/// file or any other IO error is a *degraded* `None`: we emit a single `WARN` line
/// naming the path and error so tracked jobs no longer vanish silently, then return
/// `None` to preserve the caller's fall-through behavior.
fn read_json_optional<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    match read_json::<T>(path) {
        Ok(value) => Some(value),
        Err(err) => {
            let is_not_found = err
                .chain()
                .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
                .any(|io_err| io_err.kind() == std::io::ErrorKind::NotFound);
            if !is_not_found {
                crate::diagnostics::warn_with_code(
                    "corrupt_job_record",
                    format!("{}: {err:#}", path.display()),
                );
            }
            None
        }
    }
}

fn batch_log_path_for_backend(
    plan: &RuntimePlan,
    submit_dir: &Path,
    job_id: &str,
    backend: SubmissionBackend,
) -> PathBuf {
    // When the user pins x-slurm.output, honor it verbatim (relative -> submit_dir).
    if let Some(raw) = plan.slurm.output.clone() {
        let rendered = expand_slurm_filename_pattern(
            &raw,
            job_id,
            &plan.name,
            current_user_name().as_deref(),
            false,
        );
        let candidate = PathBuf::from(rendered);
        return if candidate.is_absolute() {
            candidate
        } else {
            submit_dir.join(candidate)
        };
    }
    // Default: a hidden, job-id-free parent under the resolved runtime root that
    // can be pre-created host-side before sbatch (Slurm opens --output before the
    // script body runs). %j is client-expanded here so the persisted record
    // points at the concrete file Slurm will write. Keep the basename independent
    // of the raw Slurm job name because names can contain path separators.
    let _ = backend; // both backends share the same default shape
    let runtime_root =
        crate::tracked_paths::resolve_runtime_root(submit_dir, plan.slurm.runtime_root.as_deref());
    let file = expand_slurm_filename_pattern(
        crate::tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN,
        job_id,
        &plan.name,
        current_user_name().as_deref(),
        false,
    );
    runtime_root
        .join(crate::tracked_paths::LOGS_DIR_NAME)
        .join(file)
}

#[cfg(test)]
fn batch_log_path_for(plan: &RuntimePlan, submit_dir: &Path, job_id: &str) -> PathBuf {
    batch_log_path_for_backend(plan, submit_dir, job_id, SubmissionBackend::Slurm)
}

/// Returns a filesystem glob (with `%t`/`%N`/`%n`/`%s`/`%a` -> `*`) matching the
/// batch log(s) Slurm wrote for a job. `%j`/`%A`/`%x`/`%u` are still expanded
/// from submit-time values. For read-back/discovery only; callers that stat a
/// single concrete path must use [`batch_log_path_for_backend`].
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn batch_log_glob_for_backend(
    plan: &RuntimePlan,
    submit_dir: &Path,
    job_id: &str,
    _backend: SubmissionBackend,
) -> PathBuf {
    if let Some(raw) = plan.slurm.output.clone() {
        let rendered = expand_slurm_filename_pattern(
            &raw,
            job_id,
            &plan.name,
            current_user_name().as_deref(),
            true,
        );
        let candidate = PathBuf::from(rendered);
        return if candidate.is_absolute() {
            candidate
        } else {
            submit_dir.join(candidate)
        };
    }
    let runtime_root =
        crate::tracked_paths::resolve_runtime_root(submit_dir, plan.slurm.runtime_root.as_deref());
    let file = expand_slurm_filename_pattern(
        crate::tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN,
        job_id,
        &plan.name,
        current_user_name().as_deref(),
        true,
    );
    runtime_root
        .join(crate::tracked_paths::LOGS_DIR_NAME)
        .join(file)
}

fn expand_slurm_filename_pattern(
    pattern: &str,
    job_id: &str,
    job_name: &str,
    user_name: Option<&str>,
    glob_per_task: bool,
) -> String {
    let mut rendered = String::new();
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch != '%' {
            rendered.push(ch);
            continue;
        }

        if matches!(chars.peek(), Some('%')) {
            chars.next();
            rendered.push('%');
            continue;
        }

        let mut width = String::new();
        while let Some(peek) = chars.peek() {
            if peek.is_ascii_digit() {
                width.push(*peek);
                chars.next();
            } else {
                break;
            }
        }

        let Some(specifier) = chars.next() else {
            rendered.push('%');
            rendered.push_str(&width);
            break;
        };
        let padded_width = width.parse::<usize>().ok().map(|value| value.min(10));

        match specifier {
            'j' | 'A' => rendered.push_str(&zero_pad(job_id, padded_width)),
            'x' => rendered.push_str(job_name),
            'u' => {
                if let Some(user_name) = user_name {
                    rendered.push_str(user_name);
                } else {
                    rendered.push('%');
                    rendered.push_str(&width);
                    rendered.push(specifier);
                }
            }
            // Per-task / per-node specifiers are only known to Slurm at runtime.
            // For read-back matching, collapse each to a single glob `*`; for the
            // record path (glob_per_task = false) leave them literal so the path
            // stays stable (callers that stat directly must not glob).
            't' | 'N' | 'n' | 's' | 'a' if glob_per_task => rendered.push('*'),
            _ => {
                rendered.push('%');
                rendered.push_str(&width);
                rendered.push(specifier);
            }
        }
    }

    rendered
}

fn zero_pad(value: &str, width: Option<usize>) -> String {
    if let Some(width) = width {
        format!("{value:0>width$}")
    } else {
        value.to_string()
    }
}

fn current_user_name() -> Option<String> {
    env::var("USER").ok().or_else(|| env::var("LOGNAME").ok())
}

#[cfg(test)]
mod tests;
