use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::prepare::RuntimePlan;
use crate::render::log_file_name_for_service;

const SUBMISSION_SCHEMA_VERSION: u32 = 1;
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const INITIAL_SCHEDULER_LOOKUP_GRACE_SECONDS: u64 = 15;
const ACCOUNTING_GAP_GRACE_SECONDS: u64 = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmissionRecord {
    pub schema_version: u32,
    pub job_id: String,
    pub submitted_at: u64,
    pub compose_file: PathBuf,
    pub submit_dir: PathBuf,
    pub script_path: PathBuf,
    pub cache_dir: PathBuf,
    pub batch_log: PathBuf,
    pub service_logs: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerSource {
    Squeue,
    Sacct,
    LocalOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerStatus {
    pub state: String,
    pub source: SchedulerSource,
    pub terminal: bool,
    pub failed: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLogStatus {
    pub service_name: String,
    pub path: PathBuf,
    pub present: bool,
    pub updated_at: Option<u64>,
    pub updated_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchLogStatus {
    pub path: PathBuf,
    pub present: bool,
    pub updated_at: Option<u64>,
    pub updated_age_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub record: SubmissionRecord,
    pub scheduler: SchedulerStatus,
    pub log_dir: PathBuf,
    pub batch_log: BatchLogStatus,
    pub services: Vec<ServiceLogStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatsSnapshot {
    pub job_id: String,
    pub record: Option<SubmissionRecord>,
    pub metrics_dir: Option<PathBuf>,
    pub scheduler: SchedulerStatus,
    pub available: bool,
    pub reason: Option<String>,
    pub source: String,
    pub notes: Vec<String>,
    pub sampler: Option<SamplerSnapshot>,
    pub steps: Vec<StepStats>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct StepStats {
    pub step_id: String,
    pub ntasks: String,
    pub ave_cpu: String,
    pub ave_rss: String,
    pub max_rss: String,
    pub alloc_tres: String,
    pub tres_usage_in_ave: String,
    pub alloc_tres_map: BTreeMap<String, String>,
    pub usage_tres_in_ave_map: BTreeMap<String, String>,
    pub gpu_count: Option<String>,
    pub gpu_util: Option<String>,
    pub gpu_mem: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SamplerSnapshot {
    pub interval_seconds: u64,
    pub collectors: Vec<CollectorStatus>,
    pub gpu: Option<GpuSnapshot>,
    pub slurm: Option<SlurmSamplerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorStatus {
    pub name: String,
    pub enabled: bool,
    pub available: bool,
    pub note: Option<String>,
    pub last_sampled_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GpuSnapshot {
    pub sampled_at: String,
    pub gpus: Vec<GpuDeviceSample>,
    pub processes: Vec<GpuProcessSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GpuDeviceSample {
    pub index: Option<String>,
    pub uuid: Option<String>,
    pub name: Option<String>,
    pub utilization_gpu: Option<String>,
    pub utilization_memory: Option<String>,
    pub memory_used_mib: Option<String>,
    pub memory_total_mib: Option<String>,
    pub temperature_c: Option<String>,
    pub power_draw_w: Option<String>,
    pub power_limit_w: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GpuProcessSample {
    pub gpu_uuid: Option<String>,
    pub pid: Option<String>,
    pub process_name: Option<String>,
    pub used_memory_mib: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SlurmSamplerSnapshot {
    pub sampled_at: String,
    pub steps: Vec<StepStats>,
}

#[derive(Debug, Clone)]
pub struct SchedulerOptions {
    pub squeue_bin: String,
    pub sacct_bin: String,
}

impl Default for SchedulerOptions {
    fn default() -> Self {
        Self {
            squeue_bin: "squeue".to_string(),
            sacct_bin: "sacct".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StatsOptions {
    pub scheduler: SchedulerOptions,
    pub sstat_bin: String,
}

impl Default for StatsOptions {
    fn default() -> Self {
        Self {
            scheduler: SchedulerOptions::default(),
            sstat_bin: "sstat".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SamplerMetaFile {
    interval_seconds: u64,
    collectors: Vec<CollectorStatus>,
}

#[derive(Debug, Deserialize)]
struct GpuDeviceSampleRow {
    sampled_at: String,
    index: Option<String>,
    uuid: Option<String>,
    name: Option<String>,
    utilization_gpu: Option<String>,
    utilization_memory: Option<String>,
    memory_used_mib: Option<String>,
    memory_total_mib: Option<String>,
    temperature_c: Option<String>,
    power_draw_w: Option<String>,
    power_limit_w: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GpuProcessSampleRow {
    sampled_at: String,
    gpu_uuid: Option<String>,
    pid: Option<String>,
    process_name: Option<String>,
    used_memory_mib: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SlurmSampleRow {
    sampled_at: String,
    step_id: Option<String>,
    ntasks: Option<String>,
    ave_cpu: Option<String>,
    ave_rss: Option<String>,
    max_rss: Option<String>,
    alloc_tres: Option<String>,
    tres_usage_in_ave: Option<String>,
}

#[derive(Debug, Default)]
struct SamplerLoadOutcome {
    sampler: Option<SamplerSnapshot>,
    notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchOutcome {
    Completed(SchedulerStatus),
    Failed(SchedulerStatus),
    Unknown(SchedulerStatus),
}

#[derive(Debug, Clone)]
struct LogCursor {
    service_name: String,
    path: PathBuf,
    offset: u64,
    pending: String,
}

pub fn metadata_root_for(spec_path: &Path) -> PathBuf {
    spec_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".hpc-compose")
}

pub fn jobs_dir_for(spec_path: &Path) -> PathBuf {
    metadata_root_for(spec_path).join("jobs")
}

pub fn latest_record_path_for(spec_path: &Path) -> PathBuf {
    metadata_root_for(spec_path).join("latest.json")
}

pub fn persist_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    let record = build_submission_record(spec_path, submit_dir, script_path, plan, job_id)?;
    write_submission_record(&record)?;
    Ok(record)
}

pub fn build_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let submit_dir = absolute_path(submit_dir)?;
    let script_path = absolute_path(script_path)?;
    let log_dir = submit_dir.join(".hpc-compose").join(job_id).join("logs");
    let service_logs = plan
        .ordered_services
        .iter()
        .map(|service| {
            (
                service.name.clone(),
                log_dir.join(log_file_name_for_service(&service.name)),
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(SubmissionRecord {
        schema_version: SUBMISSION_SCHEMA_VERSION,
        job_id: job_id.to_string(),
        submitted_at: unix_timestamp_now(),
        compose_file,
        submit_dir: submit_dir.clone(),
        script_path,
        cache_dir: plan.cache_dir.clone(),
        batch_log: batch_log_path_for(plan, &submit_dir, job_id),
        service_logs,
    })
}

pub fn write_submission_record(record: &SubmissionRecord) -> Result<()> {
    let jobs_dir = jobs_dir_for(&record.compose_file);
    fs::create_dir_all(&jobs_dir)
        .with_context(|| format!("failed to create {}", jobs_dir.display()))?;
    write_json(&jobs_dir.join(format!("{}.json", record.job_id)), record)?;
    write_json(&latest_record_path_for(&record.compose_file), record)?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CleanResult {
    pub removed_jobs: Vec<String>,
}

pub fn scan_job_records(spec_path: &Path) -> Result<Vec<SubmissionRecord>> {
    let compose_file = absolute_path(spec_path)?;
    let jobs_dir = jobs_dir_for(&compose_file);
    if !jobs_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in
        fs::read_dir(&jobs_dir).with_context(|| format!("failed to read {}", jobs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if let Ok(record) = read_json::<SubmissionRecord>(&path) {
            records.push(record);
        }
    }
    Ok(records)
}

pub fn clean_by_age(spec_path: &Path, age_days: u64) -> Result<CleanResult> {
    let compose_file = absolute_path(spec_path)?;
    let records = scan_job_records(&compose_file)?;
    let cutoff = unix_timestamp_now().saturating_sub(age_days * 86400);
    let mut removed = Vec::new();
    for record in &records {
        if record.submitted_at < cutoff {
            remove_job_artifacts(&compose_file, &record.job_id)?;
            removed.push(record.job_id.clone());
        }
    }
    Ok(CleanResult {
        removed_jobs: removed,
    })
}

pub fn clean_all_except_latest(spec_path: &Path) -> Result<CleanResult> {
    let compose_file = absolute_path(spec_path)?;
    let latest_path = latest_record_path_for(&compose_file);
    let latest_job_id = if latest_path.exists() {
        read_json::<SubmissionRecord>(&latest_path)
            .ok()
            .map(|record| record.job_id)
    } else {
        None
    };

    let records = scan_job_records(&compose_file)?;
    let mut removed = Vec::new();
    for record in &records {
        if latest_job_id.as_deref() == Some(&record.job_id) {
            continue;
        }
        remove_job_artifacts(&compose_file, &record.job_id)?;
        removed.push(record.job_id.clone());
    }
    Ok(CleanResult {
        removed_jobs: removed,
    })
}

fn remove_job_artifacts(compose_file: &Path, job_id: &str) -> Result<()> {
    let jobs_dir = jobs_dir_for(compose_file);
    let record_path = jobs_dir.join(format!("{job_id}.json"));
    if record_path.exists() {
        fs::remove_file(&record_path)
            .with_context(|| format!("failed to remove {}", record_path.display()))?;
    }
    let job_dir = metadata_root_for(compose_file).join(job_id);
    if job_dir.is_dir() {
        fs::remove_dir_all(&job_dir)
            .with_context(|| format!("failed to remove {}", job_dir.display()))?;
    }
    Ok(())
}

pub fn load_submission_record(spec_path: &Path, job_id: Option<&str>) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let path = match job_id {
        Some(job_id) => jobs_dir_for(&compose_file).join(format!("{job_id}.json")),
        None => latest_record_path_for(&compose_file),
    };
    if !path.exists() {
        if let Some(job_id) = job_id {
            bail!(
                "no tracked submission metadata exists for job '{}' under {}; run 'hpc-compose submit -f {}' first",
                job_id,
                metadata_root_for(&compose_file).display(),
                compose_file.display()
            );
        }
        bail!(
            "no tracked submission metadata exists for {}; run 'hpc-compose submit -f {}' first",
            compose_file.display(),
            compose_file.display()
        );
    }
    read_json(&path)
}

pub fn build_status_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &SchedulerOptions,
) -> Result<StatusSnapshot> {
    let record = load_submission_record(spec_path, job_id)?;
    let scheduler = reconcile_scheduler_status(
        probe_scheduler_status(&record.job_id, options),
        record.submitted_at,
        None,
        unix_timestamp_now(),
    );
    let now = unix_timestamp_now();
    let batch_log = build_batch_log_status(&record.batch_log, now);
    let services = record
        .service_logs
        .iter()
        .map(|(service_name, path)| {
            let log_status = build_log_status(path, now);
            ServiceLogStatus {
                service_name: service_name.clone(),
                path: path.clone(),
                present: log_status.present,
                updated_age_seconds: log_status.updated_age_seconds,
                updated_at: log_status.updated_at,
            }
        })
        .collect::<Vec<_>>();
    Ok(StatusSnapshot {
        log_dir: log_dir_for_record(&record),
        batch_log,
        record,
        scheduler,
        services,
    })
}

pub fn build_stats_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &StatsOptions,
) -> Result<StatsSnapshot> {
    let (job_id, record) = match job_id {
        Some(job_id) => (
            job_id.to_string(),
            load_submission_record(spec_path, Some(job_id)).ok(),
        ),
        None => {
            let record = load_submission_record(spec_path, None)?;
            (record.job_id.clone(), Some(record))
        }
    };
    let raw_scheduler = probe_scheduler_status(&job_id, &options.scheduler);
    let scheduler = if let Some(record) = &record {
        reconcile_scheduler_status(
            raw_scheduler,
            record.submitted_at,
            None,
            unix_timestamp_now(),
        )
    } else {
        raw_scheduler
    };
    let metrics_dir = record.as_ref().map(metrics_dir_for_record);
    let SamplerLoadOutcome { sampler, mut notes } = if let Some(metrics_dir) = metrics_dir.as_ref()
    {
        load_sampler_snapshot(metrics_dir)
    } else {
        SamplerLoadOutcome::default()
    };

    let mut steps = sampler
        .as_ref()
        .and_then(|snapshot| snapshot.slurm.as_ref())
        .map(|snapshot| snapshot.steps.clone())
        .unwrap_or_default();
    let sampler_contributed = sampler.as_ref().is_some_and(|snapshot| {
        snapshot.gpu.is_some()
            || snapshot
                .slurm
                .as_ref()
                .is_some_and(|slurm| !slurm.steps.is_empty())
    });
    let used_sampler_steps = !steps.is_empty();
    let mut used_live_sstat = false;

    if steps.is_empty() {
        match probe_step_stats(&job_id, &options.sstat_bin) {
            Ok(probed_steps) => {
                steps = probed_steps;
                used_live_sstat = !steps.is_empty();
            }
            Err(err) if sampler_contributed => {
                notes.push(format!(
                    "live sstat fallback failed while reading sampler-backed stats: {err}"
                ));
            }
            Err(err) => return Err(err),
        }
    }

    let available = !steps.is_empty()
        || sampler
            .as_ref()
            .and_then(|snapshot| snapshot.gpu.as_ref())
            .is_some();
    if available
        && sampler
            .as_ref()
            .and_then(|snapshot| snapshot.gpu.as_ref())
            .is_none()
        && !steps.is_empty()
        && steps.iter().all(|step| !step.has_live_gpu_metrics())
    {
        notes.push(
            "GPU accounting metrics are unavailable for this job; this cluster may not expose GPU TRES accounting via sstat".to_string(),
        );
    }
    let source =
        if sampler_contributed && (used_live_sstat || (!used_sampler_steps && !steps.is_empty())) {
            "sampler+sstat"
        } else if sampler_contributed {
            "sampler"
        } else {
            "sstat"
        };

    Ok(StatsSnapshot {
        job_id,
        record,
        metrics_dir,
        scheduler: scheduler.clone(),
        available,
        reason: (!available).then(|| stats_unavailable_reason(&scheduler)),
        source: source.to_string(),
        notes,
        sampler,
        steps,
    })
}

pub fn probe_scheduler_status(job_id: &str, options: &SchedulerOptions) -> SchedulerStatus {
    probe_squeue(job_id, &options.squeue_bin)
        .or_else(|| probe_sacct(job_id, &options.sacct_bin))
        .unwrap_or_else(|| SchedulerStatus {
            state: "unknown".to_string(),
            source: SchedulerSource::LocalOnly,
            terminal: false,
            failed: false,
            detail: Some(
                "scheduler state is unavailable because squeue/sacct could not determine this job"
                    .to_string(),
            ),
        })
}

pub fn log_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    record
        .service_logs
        .values()
        .next()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| {
            record
                .submit_dir
                .join(".hpc-compose")
                .join(&record.job_id)
                .join("logs")
        })
}

pub fn metrics_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    record
        .submit_dir
        .join(".hpc-compose")
        .join(&record.job_id)
        .join("metrics")
}

fn load_sampler_snapshot(metrics_dir: &Path) -> SamplerLoadOutcome {
    if !metrics_dir.is_dir() {
        return SamplerLoadOutcome::default();
    }

    let meta_path = metrics_dir.join("meta.json");
    let meta: SamplerMetaFile = match read_json(&meta_path) {
        Ok(meta) => meta,
        Err(err) => {
            return SamplerLoadOutcome {
                sampler: None,
                notes: vec![format!(
                    "failed to parse metrics sampler metadata at {}: {err}",
                    meta_path.display()
                )],
            };
        }
    };

    let mut notes = meta
        .collectors
        .iter()
        .filter(|collector| collector.enabled)
        .filter_map(|collector| {
            collector
                .note
                .as_ref()
                .map(|note| format!("metrics collector '{}': {note}", collector.name))
        })
        .collect::<Vec<_>>();

    let gpu = if collector_enabled(&meta.collectors, "gpu") {
        match load_gpu_snapshot(metrics_dir) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                notes.push(format!(
                    "failed to parse GPU sampler data under {}: {err}",
                    metrics_dir.display()
                ));
                None
            }
        }
    } else {
        None
    };

    let slurm = if collector_enabled(&meta.collectors, "slurm") {
        match load_slurm_sampler_snapshot(metrics_dir) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                notes.push(format!(
                    "failed to parse Slurm sampler data under {}: {err}",
                    metrics_dir.display()
                ));
                None
            }
        }
    } else {
        None
    };

    SamplerLoadOutcome {
        sampler: Some(SamplerSnapshot {
            interval_seconds: meta.interval_seconds,
            collectors: meta.collectors,
            gpu,
            slurm,
        }),
        notes,
    }
}

fn collector_enabled(collectors: &[CollectorStatus], name: &str) -> bool {
    collectors
        .iter()
        .find(|collector| collector.name == name)
        .is_some_and(|collector| collector.enabled)
}

fn load_gpu_snapshot(metrics_dir: &Path) -> Result<Option<GpuSnapshot>> {
    let gpu_path = metrics_dir.join("gpu.jsonl");
    let Some((sampled_at, devices)) = load_latest_gpu_devices(&gpu_path)? else {
        return Ok(None);
    };
    let processes =
        load_gpu_processes_for_timestamp(&metrics_dir.join("gpu_processes.jsonl"), &sampled_at)?;
    Ok(Some(GpuSnapshot {
        sampled_at,
        gpus: devices,
        processes,
    }))
}

fn load_latest_gpu_devices(path: &Path) -> Result<Option<(String, Vec<GpuDeviceSample>)>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut latest_sampled_at: Option<String> = None;
    let mut devices = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: GpuDeviceSampleRow = serde_json::from_str(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), index + 1))?;
        match latest_sampled_at.as_deref() {
            None => {
                latest_sampled_at = Some(row.sampled_at.clone());
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            Some(current) if row.sampled_at.as_str() > current => {
                latest_sampled_at = Some(row.sampled_at.clone());
                devices.clear();
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            Some(current) if row.sampled_at == current => {
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            _ => {}
        }
    }

    Ok(latest_sampled_at.map(|sampled_at| (sampled_at, devices)))
}

fn load_gpu_processes_for_timestamp(
    path: &Path,
    sampled_at: &str,
) -> Result<Vec<GpuProcessSample>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut processes = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: GpuProcessSampleRow = serde_json::from_str(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), index + 1))?;
        if row.sampled_at != sampled_at {
            continue;
        }
        processes.push(GpuProcessSample {
            gpu_uuid: row.gpu_uuid,
            pid: row.pid,
            process_name: row.process_name,
            used_memory_mib: row.used_memory_mib,
        });
    }

    Ok(processes)
}

fn load_slurm_sampler_snapshot(metrics_dir: &Path) -> Result<Option<SlurmSamplerSnapshot>> {
    let path = metrics_dir.join("slurm.jsonl");
    if !path.exists() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut latest_sampled_at: Option<String> = None;
    let mut steps = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: SlurmSampleRow = serde_json::from_str(line)
            .with_context(|| format!("failed to parse {} line {}", path.display(), index + 1))?;
        let sampled_at = row.sampled_at.clone();
        let step = step_from_slurm_sample_row(row)
            .with_context(|| format!("failed to parse {} line {}", path.display(), index + 1))?;
        match latest_sampled_at.as_deref() {
            None => {
                latest_sampled_at = Some(sampled_at);
                steps.push(step);
            }
            Some(current) if sampled_at.as_str() > current => {
                latest_sampled_at = Some(sampled_at);
                steps.clear();
                steps.push(step);
            }
            Some(current) if sampled_at == current => {
                steps.push(step);
            }
            _ => {}
        }
    }

    Ok(latest_sampled_at.map(|sampled_at| SlurmSamplerSnapshot { sampled_at, steps }))
}

fn step_from_slurm_sample_row(row: SlurmSampleRow) -> Result<StepStats> {
    let step_id = required_json_string("step_id", row.step_id)?;
    let alloc_tres = row.alloc_tres.unwrap_or_default();
    let tres_usage_in_ave = row.tres_usage_in_ave.unwrap_or_default();
    let alloc_tres_map = parse_tres_map(&alloc_tres)
        .with_context(|| format!("failed to parse AllocTRES for step '{step_id}'"))?;
    let usage_tres_in_ave_map = parse_tres_map(&tres_usage_in_ave)
        .with_context(|| format!("failed to parse TRESUsageInAve for step '{step_id}'"))?;

    Ok(StepStats {
        step_id,
        ntasks: row.ntasks.unwrap_or_default(),
        ave_cpu: row.ave_cpu.unwrap_or_default(),
        ave_rss: row.ave_rss.unwrap_or_default(),
        max_rss: row.max_rss.unwrap_or_default(),
        alloc_tres: alloc_tres.clone(),
        tres_usage_in_ave: tres_usage_in_ave.clone(),
        gpu_count: find_tres_value(&alloc_tres_map, "gres/gpu"),
        gpu_util: find_tres_value(&usage_tres_in_ave_map, "gres/gpuutil"),
        gpu_mem: find_tres_value(&usage_tres_in_ave_map, "gres/gpumem"),
        alloc_tres_map,
        usage_tres_in_ave_map,
    })
}

fn required_json_string(field: &str, value: Option<String>) -> Result<String> {
    value.with_context(|| format!("missing required field '{field}'"))
}

pub fn print_logs(
    record: &SubmissionRecord,
    service: Option<&str>,
    lines: usize,
    follow: bool,
) -> Result<()> {
    let selected = selected_service_logs(record, service)?;
    let mut stdout = io::stdout();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    if !follow {
        stdout.flush().context("failed to flush log output")?;
        return Ok(());
    }

    let mut cursors = build_cursors(&selected);
    loop {
        let emitted = drain_log_cursors(&mut cursors, &mut stdout)?;
        stdout.flush().context("failed to flush log output")?;
        if !emitted {
            thread::sleep(POLL_INTERVAL);
        }
    }
}

pub fn watch_submission(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    lines: usize,
) -> Result<WatchOutcome> {
    let selected = selected_service_logs(record, None)?;
    let mut stdout = io::stdout();
    writeln!(stdout, "watching job {}...", record.job_id).ok();
    emit_initial_tail(&selected, lines, &mut stdout)?;
    let mut cursors = build_cursors(&selected);
    let mut last_state: Option<(String, SchedulerSource)> = None;
    let mut last_visible_at: Option<u64> = None;

    loop {
        let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
        let raw_status = probe_scheduler_status(&record.job_id, options);
        let now = unix_timestamp_now();
        if raw_status.source != SchedulerSource::LocalOnly {
            last_visible_at = Some(now);
        }
        let status =
            reconcile_scheduler_status(raw_status, record.submitted_at, last_visible_at, now);
        let state_key = (status.state.clone(), status.source);
        if last_state.as_ref() != Some(&state_key) {
            writeln!(
                stdout,
                "scheduler state: {} ({})",
                status.state,
                scheduler_source_label(status.source)
            )
            .ok();
            if let Some(detail) = &status.detail {
                writeln!(stdout, "note: {detail}").ok();
            }
            stdout.flush().ok();
            last_state = Some(state_key);
        }

        match status.source {
            SchedulerSource::LocalOnly if is_transitional_local_only(&status) => {
                thread::sleep(POLL_INTERVAL);
            }
            SchedulerSource::LocalOnly => return Ok(WatchOutcome::Unknown(status)),
            _ if status.terminal && status.failed => {
                let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
                stdout.flush().ok();
                return Ok(WatchOutcome::Failed(status));
            }
            _ if status.terminal => {
                let _ = drain_log_cursors(&mut cursors, &mut stdout)?;
                stdout.flush().ok();
                return Ok(WatchOutcome::Completed(status));
            }
            _ => thread::sleep(POLL_INTERVAL),
        }
    }
}

pub fn scheduler_source_label(source: SchedulerSource) -> &'static str {
    match source {
        SchedulerSource::Squeue => "squeue",
        SchedulerSource::Sacct => "sacct",
        SchedulerSource::LocalOnly => "local-only",
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("failed to determine current directory")?
            .join(path)
    };
    Ok(normalize_path(path))
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let serialized =
        serde_json::to_vec_pretty(value).context("failed to serialize job metadata")?;
    fs::write(path, serialized).with_context(|| format!("failed to write {}", path.display()))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn build_batch_log_status(path: &Path, now: u64) -> BatchLogStatus {
    let status = build_log_status(path, now);
    BatchLogStatus {
        path: path.to_path_buf(),
        present: status.present,
        updated_at: status.updated_at,
        updated_age_seconds: status.updated_age_seconds,
    }
}

fn batch_log_path_for(plan: &RuntimePlan, submit_dir: &Path, job_id: &str) -> PathBuf {
    let raw = plan
        .slurm
        .output
        .clone()
        .unwrap_or_else(|| "slurm-%j.out".to_string());
    let rendered =
        expand_slurm_filename_pattern(&raw, job_id, &plan.name, current_user_name().as_deref());
    let candidate = PathBuf::from(rendered);
    if candidate.is_absolute() {
        candidate
    } else {
        submit_dir.join(candidate)
    }
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

fn build_log_status(path: &Path, now: u64) -> BatchLogStatus {
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

fn probe_squeue(job_id: &str, binary: &str) -> Option<SchedulerStatus> {
    let output = Command::new(binary)
        .args(["-h", "-j", job_id, "-o", "%T"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let state = stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?
        .to_string();
    Some(build_scheduler_status(
        normalize_scheduler_state(&state),
        SchedulerSource::Squeue,
    ))
}

fn probe_sacct(job_id: &str, binary: &str) -> Option<SchedulerStatus> {
    let output = Command::new(binary)
        .args(["-n", "-j", job_id, "--format=State", "--parsable2"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let state = stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())?
        .split('|')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if state.is_empty() {
        return None;
    }
    Some(build_scheduler_status(
        normalize_scheduler_state(&state),
        SchedulerSource::Sacct,
    ))
}

fn probe_step_stats(job_id: &str, binary: &str) -> Result<Vec<StepStats>> {
    let output = Command::new(binary)
        .args([
            "--allsteps",
            "--jobs",
            job_id,
            "--parsable2",
            "--noconvert",
            "--format=JobID,NTasks,AveCPU,AveRSS,MaxRSS,AllocTRES,TRESUsageInAve",
        ])
        .output()
        .with_context(|| format!("failed to execute '{binary}'"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("sstat failed for job {job_id}");
        }
        bail!("sstat failed for job {job_id}: {detail}");
    }

    parse_sstat_output(job_id, &String::from_utf8_lossy(&output.stdout))
}

fn parse_sstat_output(job_id: &str, stdout: &str) -> Result<Vec<StepStats>> {
    let mut steps = Vec::new();

    for (index, raw_line) in stdout.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields
            .first()
            .is_some_and(|field| field.eq_ignore_ascii_case("JobID"))
        {
            continue;
        }
        if fields.len() != 7 {
            bail!(
                "malformed sstat output on line {}: expected 7 fields, found {}",
                index + 1,
                fields.len()
            );
        }

        let step_id = fields[0];
        if !is_numbered_step(job_id, step_id) {
            continue;
        }

        let alloc_tres_map = parse_tres_map(fields[5])
            .with_context(|| format!("failed to parse AllocTRES for step '{step_id}'"))?;
        let usage_tres_in_ave_map = parse_tres_map(fields[6])
            .with_context(|| format!("failed to parse TRESUsageInAve for step '{step_id}'"))?;
        steps.push(StepStats {
            step_id: step_id.to_string(),
            ntasks: fields[1].to_string(),
            ave_cpu: fields[2].to_string(),
            ave_rss: fields[3].to_string(),
            max_rss: fields[4].to_string(),
            alloc_tres: fields[5].to_string(),
            tres_usage_in_ave: fields[6].to_string(),
            gpu_count: find_tres_value(&alloc_tres_map, "gres/gpu"),
            gpu_util: find_tres_value(&usage_tres_in_ave_map, "gres/gpuutil"),
            gpu_mem: find_tres_value(&usage_tres_in_ave_map, "gres/gpumem"),
            alloc_tres_map,
            usage_tres_in_ave_map,
        });
    }

    Ok(steps)
}

fn parse_tres_map(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for segment in raw.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (key, value) = segment
            .split_once('=')
            .with_context(|| format!("invalid TRES entry '{segment}'"))?;
        values.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(values)
}

fn find_tres_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    values.get(key).cloned().or_else(|| {
        values
            .iter()
            .find(|(candidate, _)| candidate.starts_with(&format!("{key}:")))
            .map(|(_, value)| value.clone())
    })
}

fn is_numbered_step(job_id: &str, step_id: &str) -> bool {
    let Some(suffix) = step_id
        .strip_prefix(job_id)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())
}

fn build_scheduler_status(state: String, source: SchedulerSource) -> SchedulerStatus {
    let terminal = is_terminal_state(&state);
    SchedulerStatus {
        failed: terminal && state != "COMPLETED",
        terminal,
        source,
        state,
        detail: None,
    }
}

fn reconcile_scheduler_status(
    status: SchedulerStatus,
    submitted_at: u64,
    last_visible_at: Option<u64>,
    now: u64,
) -> SchedulerStatus {
    if status.source != SchedulerSource::LocalOnly {
        return status;
    }

    if now.saturating_sub(submitted_at) <= INITIAL_SCHEDULER_LOOKUP_GRACE_SECONDS {
        return SchedulerStatus {
            state: "WAITING_FOR_SCHEDULER".to_string(),
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
            state: "WAITING_FOR_ACCOUNTING".to_string(),
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

fn normalize_scheduler_state(raw: &str) -> String {
    raw.split_whitespace()
        .next()
        .unwrap_or(raw)
        .trim_end_matches('+')
        .to_ascii_uppercase()
}

fn is_terminal_state(state: &str) -> bool {
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

fn is_transitional_local_only(status: &SchedulerStatus) -> bool {
    status.source == SchedulerSource::LocalOnly
        && matches!(
            status.state.as_str(),
            "WAITING_FOR_SCHEDULER" | "WAITING_FOR_ACCOUNTING"
        )
}

fn stats_unavailable_reason(scheduler: &SchedulerStatus) -> String {
    match scheduler.state.as_str() {
        "PENDING" | "CONFIGURING" | "WAITING_FOR_SCHEDULER" => {
            "live step statistics are not available because the job is not running yet".to_string()
        }
        "WAITING_FOR_ACCOUNTING" => {
            "live step statistics are unavailable while Slurm accounting data is catching up"
                .to_string()
        }
        _ if scheduler.terminal => {
            "live step statistics are not available because the job is no longer running"
                .to_string()
        }
        "RUNNING" => "sstat did not report any numbered job steps for this running job".to_string(),
        _ => "sstat did not report any numbered job steps for this job".to_string(),
    }
}

fn selected_service_logs(
    record: &SubmissionRecord,
    service: Option<&str>,
) -> Result<Vec<(String, PathBuf)>> {
    if let Some(service) = service {
        let path = record.service_logs.get(service).cloned().with_context(|| {
            format!(
                "service '{}' does not exist in tracked job {}",
                service, record.job_id
            )
        })?;
        return Ok(vec![(service.to_string(), path)]);
    }
    Ok(record
        .service_logs
        .iter()
        .map(|(name, path)| (name.clone(), path.clone()))
        .collect())
}

fn emit_initial_tail(
    selected: &[(String, PathBuf)],
    lines: usize,
    writer: &mut impl Write,
) -> Result<()> {
    let tailed = selected
        .iter()
        .map(|(service, path)| Ok((service.clone(), tail_lines(path, lines)?)))
        .collect::<Result<Vec<_>>>()?;
    let max_len = tailed
        .iter()
        .map(|(_, lines)| lines.len())
        .max()
        .unwrap_or(0);
    for index in 0..max_len {
        for (service, lines) in &tailed {
            if let Some(line) = lines.get(index) {
                writeln!(writer, "[{service}] {line}").context("failed to write log output")?;
            }
        }
    }
    Ok(())
}

fn build_cursors(selected: &[(String, PathBuf)]) -> Vec<LogCursor> {
    selected
        .iter()
        .map(|(service_name, path)| LogCursor {
            service_name: service_name.clone(),
            offset: fs::metadata(path).map(|meta| meta.len()).unwrap_or(0),
            path: path.clone(),
            pending: String::new(),
        })
        .collect()
}

fn drain_log_cursors(cursors: &mut [LogCursor], writer: &mut impl Write) -> Result<bool> {
    let mut emitted = false;
    for cursor in cursors {
        for line in read_new_lines(cursor)? {
            writeln!(writer, "[{}] {}", cursor.service_name, line)
                .context("failed to write log output")?;
            emitted = true;
        }
    }
    Ok(emitted)
}

fn read_new_lines(cursor: &mut LogCursor) -> Result<Vec<String>> {
    let Ok(mut file) = File::open(&cursor.path) else {
        return Ok(Vec::new());
    };
    let len = file
        .metadata()
        .with_context(|| format!("failed to read metadata for {}", cursor.path.display()))?
        .len();
    if cursor.offset > len {
        cursor.offset = 0;
        cursor.pending.clear();
    }
    if cursor.offset == len {
        return Ok(Vec::new());
    }

    file.seek(SeekFrom::Start(cursor.offset))
        .with_context(|| format!("failed to seek {}", cursor.path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", cursor.path.display()))?;
    cursor.offset = len;

    let mut combined = std::mem::take(&mut cursor.pending);
    combined.push_str(&String::from_utf8_lossy(&bytes));
    let mut lines = Vec::new();

    if combined.is_empty() {
        return Ok(lines);
    }

    let ends_with_newline = combined.ends_with('\n');
    for segment in combined.split_inclusive('\n') {
        lines.push(
            segment
                .trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string(),
        );
    }

    if !ends_with_newline {
        cursor.pending = lines.pop().unwrap_or_default();
    }

    Ok(lines)
}

fn tail_lines(path: &Path, lines: usize) -> Result<Vec<String>> {
    let Ok(raw) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut collected = raw.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    if collected.len() > lines {
        collected.drain(0..(collected.len() - lines));
    }
    Ok(collected)
}

fn system_time_to_unix(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

fn unix_timestamp_now() -> u64 {
    system_time_to_unix(SystemTime::now()).unwrap_or(0)
}

impl StepStats {
    fn has_live_gpu_metrics(&self) -> bool {
        self.gpu_util.is_some() || self.gpu_mem.is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::planner::{ExecutionSpec, ImageSource};
    use crate::prepare::RuntimeService;
    use crate::spec::{ServiceSlurmConfig, SlurmConfig};

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
            expand_slurm_filename_pattern(
                "logs/%u-%4j-%%-%x.out",
                "77",
                "custom-name",
                Some("alice")
            ),
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
        assert_eq!(result.removed_jobs, vec!["100"]);

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
        assert_eq!(result.removed_jobs, vec!["300"]);

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
        assert!(result.removed_jobs.is_empty());

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
        assert_eq!(result.removed_jobs, vec!["500"]);
        assert!(!job_dir.exists());
    }
}
