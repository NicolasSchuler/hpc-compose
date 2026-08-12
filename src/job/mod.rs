//! Tracking, status inspection, log streaming, metrics, and artifact export
//! for submitted jobs.

// The broad fixture suite in `job/tests.rs` intentionally exercises the
// public facade as a whole. Keep its support imports test-only so production
// submodules cannot acquire dependencies through the parent namespace.
#[cfg(test)]
use crate::tracked_paths;
#[cfg(test)]
use std::fs;

mod accounting;
mod analytics;
mod annotation_policy;
mod artifacts;
mod batch_log;
mod bundle;
mod checkpoints;
mod config_snapshot;
mod deep_clean;
mod diff;
mod evidence;
mod file_digest;
mod logs;
mod metadata_io;
mod metrics_probe;
mod model;
mod provenance;
mod ps;
mod record;
mod replay;
mod rightsize;
mod runtime_state;
mod sampler_protocol;
mod scheduler;
mod scheduler_command;
mod score;
mod stats;
mod stats_rollup;
mod sweep;
mod verify;
mod watchdog;

#[cfg(test)]
use artifacts::{copy_path_recursive, remove_existing_destination, resolve_export_dir};
#[cfg(test)]
use logs::{read_new_lines, selected_service_logs};
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
pub(crate) use config_snapshot::effective_config_snapshot_yaml;
pub use deep_clean::{
    DeepCleanupDetails, OrphanRuntimeDirReport, build_deep_cleanup_report, run_deep_cleanup_report,
};
pub use diff::{
    JobDiffChange, JobDiffReport, JobDiffServiceStatus, JobDiffSide, JobMatrixReport, JobMatrixRow,
    JobMatrixRun, SpecDiffReport, build_job_diff_report, build_job_matrix_report,
    build_spec_diff_report,
};
pub(crate) use logs::tail_lines;
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
const ARTIFACT_MANIFEST_SCHEMA_VERSION: u32 = 3;
const ARTIFACT_PROVENANCE_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests;
