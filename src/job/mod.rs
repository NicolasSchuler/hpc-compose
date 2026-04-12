//! Tracking, status inspection, log streaming, metrics, and artifact export
//! for submitted jobs.

use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::Builder;

use crate::prepare::RuntimePlan;
use crate::render::log_file_name_for_service;
use crate::tracked_paths;

mod artifacts;
mod logs;
mod model;
mod ps;
mod record;
mod runtime_state;
mod scheduler;
mod stats;

#[cfg(test)]
use artifacts::{copy_path_recursive, remove_existing_destination, resolve_export_dir};
#[cfg(test)]
use logs::{read_new_lines, selected_service_logs, tail_lines};
#[cfg(test)]
use stats::{
    load_sampler_snapshot, parse_sstat_output, probe_step_stats, step_from_slurm_sample_row,
};

pub use artifacts::{
    ArtifactBundleManifest, ArtifactBundleProvenance, ArtifactEntryMetadata, ArtifactExportOptions,
    ArtifactExportReport, ArtifactManifest, BundleExportReport, artifact_manifest_path_for_record,
    artifact_payload_dir_for_record, artifacts_dir_for_record, export_artifacts,
};
pub use logs::{WatchOutcome, print_logs, watch_submission};
pub use model::{
    RequestedWalltime, SchedulerSource, SubmissionBackend, SubmissionKind, SubmissionRecord,
    SubmissionRecordBuildOptions,
};
pub use ps::{PsSnapshot, build_ps_snapshot};
pub use record::{
    CleanupJobReport, CleanupMode, CleanupReport, JobInventoryEntry, JobInventoryScan,
    build_cleanup_report, build_submission_record, build_submission_record_with_backend,
    build_submission_record_with_backend_and_options, build_submission_record_with_options,
    clean_all_except_latest, clean_by_age, find_submission_record_in_repo, jobs_dir_for,
    latest_record_path_for, latest_run_record_path_for, load_submission_record, log_dir_for_record,
    metadata_root_for, persist_submission_record, remove_submission_record, run_cleanup_report,
    runtime_job_root_for_record, scan_job_inventory, scan_job_records, state_path_for_record,
    write_submission_record,
};
pub use scheduler::{
    BatchLogStatus, PsServiceRow, QueueDiagnostics, SchedulerStatus, ServiceLogStatus,
    StatusSnapshot, WalltimeProgress, build_status_snapshot, format_walltime_duration,
    format_walltime_summary, parse_scheduler_timestamp, probe_scheduler_status,
    probe_scheduler_status_with_queue_diagnostics, scheduler_source_label, walltime_progress,
    walltime_progress_percent,
};
pub use stats::{
    CollectorStatus, GpuDeviceSample, GpuProcessSample, GpuSnapshot, SamplerSnapshot,
    SchedulerOptions, SlurmSamplerSnapshot, StatsOptions, StatsSnapshot, StepStats,
    build_stats_snapshot, metrics_dir_for_record,
};

const SUBMISSION_SCHEMA_VERSION: u32 = 2;
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
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, &serialized).context(format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).context(format!(
        "failed to rename {} to {}",
        tmp_path.display(),
        path.display()
    ))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).context(format!("failed to parse {}", path.display()))
}

fn batch_log_path_for_backend(
    plan: &RuntimePlan,
    submit_dir: &Path,
    job_id: &str,
    backend: SubmissionBackend,
) -> PathBuf {
    let raw = plan.slurm.output.clone().unwrap_or_else(|| match backend {
        SubmissionBackend::Slurm => "slurm-%j.out".to_string(),
        SubmissionBackend::Local => "hpc-compose-local-%j.out".to_string(),
    });
    let rendered =
        expand_slurm_filename_pattern(&raw, job_id, &plan.name, current_user_name().as_deref());
    let candidate = PathBuf::from(rendered);
    if candidate.is_absolute() {
        candidate
    } else {
        submit_dir.join(candidate)
    }
}

#[cfg(test)]
fn batch_log_path_for(plan: &RuntimePlan, submit_dir: &Path, job_id: &str) -> PathBuf {
    batch_log_path_for_backend(plan, submit_dir, job_id, SubmissionBackend::Slurm)
}

fn expand_slurm_filename_pattern(
    pattern: &str,
    job_id: &str,
    job_name: &str,
    user_name: Option<&str>,
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
