use std::collections::BTreeSet;

use super::accounting::{AccountingSnapshot, build_accounting_snapshot};
use super::runtime_state::load_runtime_state;
use super::scheduler::{
    SchedulerCommandError, SchedulerCommandUnavailable, build_local_scheduler_status,
    command_unavailable_detail, command_unavailable_error, reconcile_scheduler_status,
    run_scheduler_command, stats_unavailable_reason, unix_timestamp_now,
};
use super::*;

/// Combined metrics and scheduler view returned by the `stats` command.
#[allow(missing_docs)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accounting: Option<AccountingSnapshot>,
    pub first_failure: Option<FirstFailure>,
    pub attempt: Option<u32>,
    pub is_resume: Option<bool>,
    pub resume_dir: Option<PathBuf>,
}

/// Best-effort first service failure observed by the runtime supervisor.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct FirstFailure {
    pub service: String,
    pub exit_code: i32,
    pub at_unix: Option<u64>,
    pub node: Option<String>,
    pub rank: Option<String>,
}

/// One Slurm step metrics row as presented by `hpc-compose stats`.
#[allow(missing_docs)]
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

/// Snapshot of the job-local metrics sampler outputs.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct SamplerSnapshot {
    pub interval_seconds: u64,
    pub collectors: Vec<CollectorStatus>,
    pub gpu: Option<GpuSnapshot>,
    pub slurm: Option<SlurmSamplerSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuSnapshot>,
}

/// Sampled host CPU telemetry collected from `/proc/stat` by the job-local
/// sampler.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct CpuSnapshot {
    pub sampled_at: String,
    pub nodes: Vec<CpuNodeSample>,
    pub summary: CpuSummary,
}

/// One node's latest sampled CPU utilization row.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CpuNodeSample {
    pub node: Option<String>,
    pub cpu_util_pct: Option<f64>,
    pub core_count: Option<u64>,
    pub loadavg_1m: Option<f64>,
}

/// Cross-node rollup of the latest CPU utilization sample.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CpuSummary {
    pub node_count: usize,
    pub mean_util_pct: Option<f64>,
    pub max_util_pct: Option<f64>,
    pub total_core_count: Option<u64>,
}

/// Availability metadata for one configured metrics collector.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorStatus {
    pub name: String,
    pub enabled: bool,
    pub available: bool,
    pub note: Option<String>,
    pub last_sampled_at: Option<String>,
}

/// GPU telemetry snapshot collected by the job-local sampler.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct GpuSnapshot {
    pub sampled_at: String,
    pub nodes: Vec<GpuNodeSummary>,
    pub gpus: Vec<GpuDeviceSample>,
    pub processes: Vec<GpuProcessSample>,
}

/// Per-node GPU summary derived from the latest sampler rows.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GpuNodeSummary {
    pub node: Option<String>,
    pub gpu_count: usize,
    pub avg_utilization_gpu: Option<f64>,
    pub memory_used_mib: Option<u64>,
    pub memory_total_mib: Option<u64>,
    pub power_draw_w: Option<f64>,
    pub power_limit_w: Option<f64>,
}

/// One sampled GPU device record.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GpuDeviceSample {
    pub node: Option<String>,
    pub rank: Option<String>,
    pub local_rank: Option<String>,
    pub service: Option<String>,
    pub collector: Option<String>,
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

/// One sampled GPU process record.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GpuProcessSample {
    pub node: Option<String>,
    pub rank: Option<String>,
    pub local_rank: Option<String>,
    pub service: Option<String>,
    pub collector: Option<String>,
    pub gpu_uuid: Option<String>,
    pub pid: Option<String>,
    pub process_name: Option<String>,
    pub used_memory_mib: Option<String>,
}

/// Slurm metrics sampler output for all observed steps.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct SlurmSamplerSnapshot {
    pub sampled_at: String,
    pub steps: Vec<StepStats>,
}

/// External binaries used to query scheduler state.
#[allow(missing_docs)]
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

/// Options for building a metrics snapshot.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct StatsOptions {
    pub scheduler: SchedulerOptions,
    pub sstat_bin: String,
    pub accounting: bool,
}

impl Default for StatsOptions {
    fn default() -> Self {
        Self {
            scheduler: SchedulerOptions::default(),
            sstat_bin: "sstat".to_string(),
            accounting: false,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct SamplerMetaFile {
    pub(super) interval_seconds: u64,
    pub(super) collectors: Vec<CollectorStatus>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GpuDeviceSampleRow {
    pub(super) sampled_at: String,
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) rank: Option<String>,
    #[serde(default)]
    pub(super) local_rank: Option<String>,
    #[serde(default)]
    pub(super) service: Option<String>,
    #[serde(default)]
    pub(super) collector: Option<String>,
    pub(super) index: Option<String>,
    pub(super) uuid: Option<String>,
    pub(super) name: Option<String>,
    pub(super) utilization_gpu: Option<String>,
    pub(super) utilization_memory: Option<String>,
    pub(super) memory_used_mib: Option<String>,
    pub(super) memory_total_mib: Option<String>,
    pub(super) temperature_c: Option<String>,
    pub(super) power_draw_w: Option<String>,
    pub(super) power_limit_w: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GpuProcessSampleRow {
    pub(super) sampled_at: String,
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) rank: Option<String>,
    #[serde(default)]
    pub(super) local_rank: Option<String>,
    #[serde(default)]
    pub(super) service: Option<String>,
    #[serde(default)]
    pub(super) collector: Option<String>,
    pub(super) gpu_uuid: Option<String>,
    pub(super) pid: Option<String>,
    pub(super) process_name: Option<String>,
    pub(super) used_memory_mib: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CpuSampleRow {
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) cpu_util_pct: Option<f64>,
    #[serde(default)]
    pub(super) core_count: Option<u64>,
    #[serde(default)]
    pub(super) loadavg_1m: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SlurmSampleRow {
    pub(super) sampled_at: String,
    pub(super) step_id: Option<String>,
    pub(super) ntasks: Option<String>,
    pub(super) ave_cpu: Option<String>,
    pub(super) ave_rss: Option<String>,
    pub(super) max_rss: Option<String>,
    pub(super) alloc_tres: Option<String>,
    pub(super) tres_usage_in_ave: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct SamplerLoadOutcome {
    pub(super) sampler: Option<SamplerSnapshot>,
    pub(super) notes: Vec<String>,
}

const JSONL_TAIL_CHUNK_SIZE: u64 = 64 * 1024;

#[derive(Debug, Deserialize)]
struct JsonlSampleProbe {
    sampled_at: String,
}

#[derive(Debug, Clone)]
struct JsonlSampleLine {
    number: usize,
    text: String,
    sampled_at: String,
}

/// Builds the tracked metrics snapshot used by `hpc-compose stats`.
pub fn build_stats_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &StatsOptions,
) -> Result<StatsSnapshot> {
    build_stats_snapshot_core(spec_path, job_id, options, None)
}

/// Builds the tracked metrics snapshot reusing an already-probed raw scheduler
/// status (from [`probe_scheduler_status_many`]) instead of re-probing.
///
/// The prefetched status is used only for Slurm-backed records; local records
/// derive their status from runtime state. Callers batching probes over a whole
/// sweep (`sweep stats`) thread the batched result through here so each snapshot
/// avoids a per-job squeue/sacct spawn (sstat is still probed per job).
pub fn build_stats_snapshot_with_status(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &StatsOptions,
    prefetched: Option<SchedulerStatus>,
) -> Result<StatsSnapshot> {
    build_stats_snapshot_core(spec_path, job_id, options, prefetched)
}

fn build_stats_snapshot_core(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &StatsOptions,
    prefetched: Option<SchedulerStatus>,
) -> Result<StatsSnapshot> {
    let (job_id, record) = match job_id {
        Some(job_id) => (
            job_id.to_string(),
            load_submission_record_optional(spec_path, Some(job_id)),
        ),
        None => {
            let record = load_submission_record(spec_path, None)?;
            (record.job_id.clone(), Some(record))
        }
    };
    let runtime_state = record.as_ref().and_then(load_runtime_state);
    let raw_scheduler = match (record.as_ref().map(|record| record.backend), prefetched) {
        (Some(SubmissionBackend::Local), _) => build_local_scheduler_status(runtime_state.as_ref()),
        (_, Some(status)) => status,
        (_, None) => probe_scheduler_status(&job_id, &options.scheduler),
    };
    let scheduler = if let Some(record) = &record {
        match record.backend {
            SubmissionBackend::Slurm => reconcile_scheduler_status(
                raw_scheduler,
                record.submitted_at,
                None,
                unix_timestamp_now(),
            ),
            SubmissionBackend::Local => raw_scheduler,
        }
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
    let mut sstat_unavailable_reason = None;

    if steps.is_empty()
        && record.as_ref().map(|record| record.backend) != Some(SubmissionBackend::Local)
    {
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
            Err(err) if command_unavailable_anyhow(&err) => {
                let reason = command_unavailable_anyhow_detail("sstat", &options.sstat_bin, &err);
                notes.push(reason.clone());
                sstat_unavailable_reason = Some(reason);
            }
            Err(err) => return Err(err),
        }
    }

    // Backfill the allocation-derived GPU count from observed nvidia-smi device
    // samples when TRES data is absent. On clusters where sstat rejects
    // AllocTRES the TRES-parsed gpu_count is always None, yet the sampler still
    // records the real device list; surface that count so `stats` output and CSV
    // are not permanently blank. Only fills steps missing a count, never
    // overwrites a genuine TRES-derived value.
    if let Some(observed) = sampler
        .as_ref()
        .and_then(|snapshot| snapshot.gpu.as_ref())
        .and_then(observed_gpu_device_count)
    {
        for step in &mut steps {
            if step.gpu_count.is_none() {
                step.gpu_count = Some(observed.to_string());
            }
        }
    }

    let available = !steps.is_empty()
        || sampler
            .as_ref()
            .and_then(|snapshot| snapshot.gpu.as_ref())
            .is_some();
    if record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local) {
        notes.push("Slurm step statistics are unavailable for locally launched jobs".to_string());
    }
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
    let source = if record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local) {
        "sampler"
    } else if sampler_contributed && (used_live_sstat || (!used_sampler_steps && !steps.is_empty()))
    {
        "sampler+sstat"
    } else if sampler_contributed {
        "sampler"
    } else {
        "sstat"
    };
    let reason = if !available
        && record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local)
    {
        Some("runtime metrics are not available because no local sampler data has been collected yet".to_string())
    } else if !available && sstat_unavailable_reason.is_some() {
        sstat_unavailable_reason
    } else {
        (!available).then(|| stats_unavailable_reason(&scheduler))
    };
    let accounting = if options.accounting {
        Some(build_accounting_snapshot(
            &job_id,
            record.as_ref(),
            &options.scheduler.sacct_bin,
        )?)
    } else {
        None
    };

    Ok(StatsSnapshot {
        job_id,
        record,
        metrics_dir,
        scheduler: scheduler.clone(),
        available,
        reason,
        source: source.to_string(),
        notes,
        sampler,
        steps,
        accounting,
        first_failure: runtime_state
            .as_ref()
            .and_then(first_failure_from_runtime_state),
        attempt: runtime_state.as_ref().and_then(|state| state.attempt),
        is_resume: runtime_state.as_ref().and_then(|state| state.is_resume),
        resume_dir: runtime_state
            .as_ref()
            .and_then(|state| state.resume_dir.clone()),
    })
}

/// Returns the tracked metrics directory for a submission record.
pub fn metrics_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    // Honor an explicit x-slurm.runtime_root override (schema v3+); rebuilding
    // the default root here silently lost all metrics for override jobs.
    tracked_paths::latest_metrics_dir(&runtime_job_root_for_record(record))
}

fn command_unavailable_anyhow(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<SchedulerCommandUnavailable>()
            .is_some()
            || cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(command_unavailable_error)
    })
}

fn command_unavailable_anyhow_detail(
    command_name: &str,
    binary: &str,
    err: &anyhow::Error,
) -> String {
    err.chain()
        .find_map(|cause| {
            cause
                .downcast_ref::<SchedulerCommandUnavailable>()
                .map(|err| err.detail().to_string())
        })
        .or_else(|| {
            err.chain()
                .find_map(|cause| cause.downcast_ref::<std::io::Error>())
                .map(|io| command_unavailable_detail(command_name, binary, io))
        })
        .unwrap_or_else(|| format!("{command_name} not available at '{binary}' ({err})"))
}

impl StepStats {
    pub(super) fn has_live_gpu_metrics(&self) -> bool {
        self.gpu_util.is_some() || self.gpu_mem.is_some()
    }
}

pub(crate) fn load_sampler_snapshot(metrics_dir: &Path) -> SamplerLoadOutcome {
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

    let cpu = if collector_enabled(&meta.collectors, "cpu") {
        match load_cpu_snapshot(metrics_dir) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                notes.push(format!(
                    "failed to parse CPU sampler data under {}: {err}",
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
            cpu,
        }),
        notes,
    }
}

/// Loads the latest per-node CPU sample group from `cpu.jsonl` and rolls it up
/// into a cross-node summary. Multiple rows share the newest `sampled_at` (one
/// per node on multi-node jobs); the last row seen for a node wins.
fn load_cpu_snapshot(metrics_dir: &Path) -> Result<Option<CpuSnapshot>> {
    let path = metrics_dir.join("cpu.jsonl");
    let lines = latest_ordered_jsonl_group(&path)?;
    let Some(first_line) = lines.first() else {
        return Ok(None);
    };
    let sampled_at = first_line.sampled_at.clone();
    let mut nodes: BTreeMap<Option<String>, CpuNodeSample> = BTreeMap::new();
    for line in lines {
        let row: CpuSampleRow = serde_json::from_str(&line.text).context(format!(
            "failed to parse {} line {}",
            path.display(),
            line.number
        ))?;
        nodes.insert(
            row.node.clone(),
            CpuNodeSample {
                node: row.node,
                cpu_util_pct: row.cpu_util_pct,
                core_count: row.core_count,
                loadavg_1m: row.loadavg_1m,
            },
        );
    }
    let nodes: Vec<CpuNodeSample> = nodes.into_values().collect();
    let summary = summarize_cpu_nodes(&nodes);
    Ok(Some(CpuSnapshot {
        sampled_at,
        nodes,
        summary,
    }))
}

fn summarize_cpu_nodes(nodes: &[CpuNodeSample]) -> CpuSummary {
    let utils: Vec<f64> = nodes.iter().filter_map(|node| node.cpu_util_pct).collect();
    let mean_util_pct = (!utils.is_empty()).then(|| utils.iter().sum::<f64>() / utils.len() as f64);
    let max_util_pct = utils.iter().copied().fold(None, |acc, value| {
        Some(acc.map_or(value, |m: f64| m.max(value)))
    });
    let cores: Vec<u64> = nodes.iter().filter_map(|node| node.core_count).collect();
    let total_core_count = (!cores.is_empty()).then(|| cores.iter().sum());
    CpuSummary {
        node_count: nodes.len(),
        mean_util_pct,
        max_util_pct,
        total_core_count,
    }
}

fn collector_enabled(collectors: &[CollectorStatus], name: &str) -> bool {
    collectors
        .iter()
        .find(|collector| collector.name == name)
        .is_some_and(|collector| collector.enabled)
}

fn latest_ordered_jsonl_group(path: &Path) -> Result<Vec<JsonlSampleLine>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path).context(format!("failed to read {}", path.display()))?;
    let mut position = file
        .metadata()
        .context(format!("failed to stat {}", path.display()))?
        .len();
    if position == 0 {
        return Ok(Vec::new());
    }
    let mut chunks = Vec::new();

    while position > 0 {
        let read_len = position.min(JSONL_TAIL_CHUNK_SIZE);
        position -= read_len;
        file.seek(SeekFrom::Start(position))
            .context(format!("failed to seek {}", path.display()))?;
        let mut chunk = vec![0_u8; read_len as usize];
        file.read_exact(&mut chunk)
            .context(format!("failed to read {}", path.display()))?;
        chunks.push(chunk);

        let lines = decode_ordered_jsonl_suffix(path, position, &chunks)?;
        if lines.is_empty() {
            continue;
        }

        let latest_sampled_at = lines
            .last()
            .map(|line| line.sampled_at.as_str())
            .unwrap_or_default();
        let mut group_start = lines.len() - 1;
        while group_start > 0 && lines[group_start - 1].sampled_at == latest_sampled_at {
            group_start -= 1;
        }
        if group_start > 0 || position == 0 {
            return Ok(lines[group_start..].to_vec());
        }
    }

    Ok(Vec::new())
}

fn ordered_jsonl_group_for_timestamp(
    path: &Path,
    sampled_at: &str,
) -> Result<Vec<JsonlSampleLine>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path).context(format!("failed to read {}", path.display()))?;
    let mut position = file
        .metadata()
        .context(format!("failed to stat {}", path.display()))?
        .len();
    if position == 0 {
        return Ok(Vec::new());
    }
    let mut chunks = Vec::new();

    while position > 0 {
        let read_len = position.min(JSONL_TAIL_CHUNK_SIZE);
        position -= read_len;
        file.seek(SeekFrom::Start(position))
            .context(format!("failed to seek {}", path.display()))?;
        let mut chunk = vec![0_u8; read_len as usize];
        file.read_exact(&mut chunk)
            .context(format!("failed to read {}", path.display()))?;
        chunks.push(chunk);

        let lines = decode_ordered_jsonl_suffix(path, position, &chunks)?;
        if lines.is_empty() {
            continue;
        }

        if let Some(group_end) = lines.iter().rposition(|line| line.sampled_at == sampled_at) {
            let mut group_start = group_end;
            while group_start > 0 && lines[group_start - 1].sampled_at == sampled_at {
                group_start -= 1;
            }
            if group_start > 0 || position == 0 {
                return Ok(lines[group_start..=group_end].to_vec());
            }
        } else if lines
            .last()
            .is_some_and(|line| line.sampled_at.as_str() < sampled_at)
        {
            // The sampler appends monotonically timestamped groups. If the
            // newest process row is older than the GPU sample, no process rows
            // were collected for that sample.
            return Ok(Vec::new());
        }
    }

    Ok(Vec::new())
}

fn decode_ordered_jsonl_suffix(
    path: &Path,
    start_offset: u64,
    chunks_from_tail: &[Vec<u8>],
) -> Result<Vec<JsonlSampleLine>> {
    let total_len = chunks_from_tail.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks_from_tail.iter().rev() {
        bytes.extend_from_slice(chunk);
    }

    let mut first_line_number = count_newlines_before(path, start_offset)? + 1;
    if start_offset > 0 {
        let Some(newline_index) = bytes.iter().position(|byte| *byte == b'\n') else {
            return Ok(Vec::new());
        };
        bytes.drain(..=newline_index);
        first_line_number += 1;
    }

    let text =
        std::str::from_utf8(&bytes).context(format!("failed to decode {}", path.display()))?;
    let mut lines = Vec::new();
    for (offset, raw_line) in text.lines().enumerate() {
        let line_number = first_line_number + offset;
        let line = raw_line.trim();
        if !line.is_empty() {
            let sampled_at = parse_sampled_at_probe(path, line_number, line)?;
            lines.push(JsonlSampleLine {
                number: line_number,
                text: line.to_string(),
                sampled_at,
            });
        }
    }
    Ok(lines)
}

fn count_newlines_before(path: &Path, byte_len: u64) -> Result<usize> {
    if byte_len == 0 {
        return Ok(0);
    }
    let mut file = File::open(path).context(format!("failed to read {}", path.display()))?;
    let mut remaining = byte_len;
    let mut count = 0;
    let mut buffer = [0_u8; 8192];
    while remaining > 0 {
        let limit = buffer.len().min(remaining as usize);
        let read = file
            .read(&mut buffer[..limit])
            .context(format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        count += buffer[..read].iter().filter(|byte| **byte == b'\n').count();
        remaining -= read as u64;
    }
    Ok(count)
}

fn parse_sampled_at_probe(path: &Path, line_number: usize, line: &str) -> Result<String> {
    let row: JsonlSampleProbe = serde_json::from_str(line).context(format!(
        "failed to parse {} line {}",
        path.display(),
        line_number
    ))?;
    Ok(row.sampled_at)
}

fn load_gpu_snapshot(metrics_dir: &Path) -> Result<Option<GpuSnapshot>> {
    let gpu_path = metrics_dir.join("gpu.jsonl");
    let Some((sampled_at, devices)) = load_latest_gpu_devices(&gpu_path)? else {
        return Ok(None);
    };
    let processes =
        load_gpu_processes_for_timestamp(&metrics_dir.join("gpu_processes.jsonl"), &sampled_at)?;
    let nodes = summarize_gpu_nodes(&devices);
    Ok(Some(GpuSnapshot {
        sampled_at,
        nodes,
        gpus: devices,
        processes,
    }))
}

fn load_latest_gpu_devices(path: &Path) -> Result<Option<(String, Vec<GpuDeviceSample>)>> {
    let lines = latest_ordered_jsonl_group(path)?;
    let Some(first_line) = lines.first() else {
        return Ok(None);
    };
    let latest_sampled_at = first_line.sampled_at.clone();
    let mut devices = Vec::with_capacity(lines.len());

    for line in lines {
        let row: GpuDeviceSampleRow = serde_json::from_str(&line.text).context(format!(
            "failed to parse {} line {}",
            path.display(),
            line.number
        ))?;
        devices.push(gpu_device_from_row(row));
    }

    Ok(Some((latest_sampled_at, devices)))
}

fn gpu_device_from_row(row: GpuDeviceSampleRow) -> GpuDeviceSample {
    GpuDeviceSample {
        node: row.node,
        rank: row.rank,
        local_rank: row.local_rank,
        service: row.service,
        collector: row.collector,
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
    }
}

fn summarize_gpu_nodes(devices: &[GpuDeviceSample]) -> Vec<GpuNodeSummary> {
    let mut grouped: BTreeMap<Option<String>, Vec<&GpuDeviceSample>> = BTreeMap::new();
    for device in devices {
        grouped.entry(device.node.clone()).or_default().push(device);
    }
    grouped
        .into_iter()
        .map(|(node, devices)| {
            let gpu_count = devices.len();
            let util_values = devices
                .iter()
                .filter_map(|device| parse_u64_stats(device.utilization_gpu.as_deref()))
                .collect::<Vec<_>>();
            let avg_utilization_gpu = (!util_values.is_empty())
                .then(|| util_values.iter().sum::<u64>() as f64 / util_values.len() as f64);
            let memory_used_mib = sum_optional_stats(
                devices
                    .iter()
                    .map(|device| device.memory_used_mib.as_deref()),
            );
            let memory_total_mib = sum_optional_stats(
                devices
                    .iter()
                    .map(|device| device.memory_total_mib.as_deref()),
            );
            let power_draw_w =
                sum_optional_f64_stats(devices.iter().map(|device| device.power_draw_w.as_deref()));
            let power_limit_w = sum_optional_f64_stats(
                devices.iter().map(|device| device.power_limit_w.as_deref()),
            );
            GpuNodeSummary {
                node,
                gpu_count,
                avg_utilization_gpu,
                memory_used_mib,
                memory_total_mib,
                power_draw_w,
                power_limit_w,
            }
        })
        .collect()
}

/// Counts the distinct GPU devices observed by the nvidia-smi sampler across
/// every node, summing per-node distinct device identities. Each device is keyed
/// by its UUID when present, falling back to its index, so repeated sample rows
/// for the same device are not double-counted. Returns `None` when no devices
/// were observed. Used to backfill the allocation-derived `StepStats::gpu_count`
/// on clusters where `sstat` rejects `AllocTRES` (so TRES data is absent).
fn observed_gpu_device_count(gpu: &GpuSnapshot) -> Option<usize> {
    let mut per_node: BTreeMap<Option<String>, BTreeSet<String>> = BTreeMap::new();
    for device in &gpu.gpus {
        let identity = device
            .uuid
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or(device
                .index
                .as_deref()
                .filter(|value| !value.trim().is_empty()));
        if let Some(identity) = identity {
            per_node
                .entry(device.node.clone())
                .or_default()
                .insert(identity.trim().to_string());
        }
    }
    let total: usize = per_node.values().map(BTreeSet::len).sum();
    (total > 0).then_some(total)
}

fn parse_u64_stats(value: Option<&str>) -> Option<u64> {
    value?.trim().parse::<u64>().ok()
}

fn sum_optional_stats<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Option<u64> {
    let parsed = values.filter_map(parse_u64_stats).collect::<Vec<_>>();
    (!parsed.is_empty()).then(|| parsed.iter().sum())
}

fn parse_f64_stats(value: Option<&str>) -> Option<f64> {
    value?.trim().parse::<f64>().ok()
}

fn sum_optional_f64_stats<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Option<f64> {
    let parsed = values.filter_map(parse_f64_stats).collect::<Vec<_>>();
    (!parsed.is_empty()).then(|| parsed.iter().sum())
}

fn load_gpu_processes_for_timestamp(
    path: &Path,
    sampled_at: &str,
) -> Result<Vec<GpuProcessSample>> {
    let lines = ordered_jsonl_group_for_timestamp(path, sampled_at)?;
    let mut processes = Vec::with_capacity(lines.len());

    for line in lines {
        let row: GpuProcessSampleRow = serde_json::from_str(&line.text).context(format!(
            "failed to parse {} line {}",
            path.display(),
            line.number
        ))?;
        processes.push(GpuProcessSample {
            node: row.node,
            rank: row.rank,
            local_rank: row.local_rank,
            service: row.service,
            collector: row.collector,
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
    let lines = latest_ordered_jsonl_group(&path)?;
    let Some(first_line) = lines.first() else {
        return Ok(None);
    };
    let latest_sampled_at = first_line.sampled_at.clone();
    let mut steps = Vec::with_capacity(lines.len());

    for line in lines {
        let row: SlurmSampleRow = serde_json::from_str(&line.text).context(format!(
            "failed to parse {} line {}",
            path.display(),
            line.number
        ))?;
        let step = step_from_slurm_sample_row(row).context(format!(
            "failed to parse {} line {}",
            path.display(),
            line.number
        ))?;
        steps.push(step);
    }

    Ok(Some(SlurmSamplerSnapshot {
        sampled_at: latest_sampled_at,
        steps,
    }))
}

pub(crate) fn step_from_slurm_sample_row(row: SlurmSampleRow) -> Result<StepStats> {
    let step_id = required_json_string("step_id", row.step_id)?;
    let alloc_tres = row.alloc_tres.unwrap_or_default();
    let tres_usage_in_ave = row.tres_usage_in_ave.unwrap_or_default();
    let alloc_tres_map = parse_tres_map(&alloc_tres)
        .context(format!("failed to parse AllocTRES for step '{step_id}'"))?;
    let usage_tres_in_ave_map = parse_tres_map(&tres_usage_in_ave).context(format!(
        "failed to parse TRESUsageInAve for step '{step_id}'"
    ))?;

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
    value.context(format!("missing required field '{field}'"))
}

pub(crate) fn probe_step_stats(job_id: &str, binary: &str) -> Result<Vec<StepStats>> {
    let mut command = Command::new(binary);
    command.args([
        "--allsteps",
        "--jobs",
        job_id,
        "--parsable2",
        "--noconvert",
        // AllocTRES is a sacct (allocation) field that sstat rejects
        // ("Invalid field requested"); sstat reports live step usage only.
        // Allocation is sourced from accounting/the plan instead.
        "--format=JobID,NTasks,AveCPU,AveRSS,MaxRSS,TRESUsageInAve",
    ]);
    let output = run_scheduler_command(&mut command, "sstat", binary)
        .map_err(SchedulerCommandError::into_anyhow)
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

pub(crate) fn parse_sstat_output(job_id: &str, stdout: &str) -> Result<Vec<StepStats>> {
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
        if fields.len() != 6 {
            bail!(
                "malformed sstat output on line {}: expected 6 fields, found {}",
                index + 1,
                fields.len()
            );
        }

        let step_id = fields[0];
        if !is_numbered_step(job_id, step_id) {
            continue;
        }

        // sstat reports live step usage only; allocation (AllocTRES) is a sacct
        // field, so alloc_tres / gpu_count are left empty here and resolved from
        // accounting downstream.
        let usage_tres_in_ave_map = parse_tres_map(fields[5]).context(format!(
            "failed to parse TRESUsageInAve for step '{step_id}'"
        ))?;
        steps.push(StepStats {
            step_id: step_id.to_string(),
            ntasks: fields[1].to_string(),
            ave_cpu: fields[2].to_string(),
            ave_rss: fields[3].to_string(),
            max_rss: fields[4].to_string(),
            alloc_tres: String::new(),
            tres_usage_in_ave: fields[5].to_string(),
            gpu_count: None,
            gpu_util: find_tres_value(&usage_tres_in_ave_map, "gres/gpuutil"),
            gpu_mem: find_tres_value(&usage_tres_in_ave_map, "gres/gpumem"),
            alloc_tres_map: BTreeMap::new(),
            usage_tres_in_ave_map,
        });
    }

    Ok(steps)
}

pub(super) fn parse_tres_map(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for segment in raw.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (key, value) = segment
            .split_once('=')
            .context(format!("invalid TRES entry '{segment}'"))?;
        values.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(values)
}

pub(super) fn find_tres_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    if let Some(value) = values.get(key) {
        return Some(value.clone());
    }
    let prefix = format!("{key}:");
    for (candidate, value) in values {
        if candidate.starts_with(&prefix) {
            return Some(value.clone());
        }
    }
    None
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

fn first_failure_from_runtime_state(
    state: &super::runtime_state::ServiceRuntimeStateFile,
) -> Option<FirstFailure> {
    state
        .services
        .iter()
        .filter_map(|service| {
            let exit_code = service.first_failure_exit_code?;
            Some(FirstFailure {
                service: service.service_name.clone(),
                exit_code,
                at_unix: service.first_failure_at,
                node: service.first_failure_node.clone(),
                rank: service.first_failure_rank.clone(),
            })
        })
        .min_by_key(|failure| failure.at_unix.unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampler_helpers_cover_latest_rows_and_missing_files() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = load_sampler_snapshot(&tmpdir.path().join("missing"));
        assert!(missing.sampler.is_none());
        assert!(missing.notes.is_empty());

        let metrics_dir = tmpdir.path().join("metrics");
        fs::create_dir_all(&metrics_dir).expect("metrics dir");
        fs::write(
            metrics_dir.join("meta.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "interval_seconds": 5,
                "collectors": [
                    {"name": "gpu", "enabled": true, "available": true, "note": "nvidia-smi", "last_sampled_at": "2026-04-10T10:00:00Z"},
                    {"name": "slurm", "enabled": true, "available": true, "note": null, "last_sampled_at": "2026-04-10T10:00:00Z"}
                ]
            }))
            .expect("meta json"),
        )
        .expect("write meta");
        fs::write(
            metrics_dir.join("gpu.jsonl"),
            concat!(
                "\n",
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"index\":\"0\",\"uuid\":\"gpu-old\",\"name\":\"A100\",\"utilization_gpu\":\"10\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"index\":\"0\",\"uuid\":\"gpu-new-0\",\"name\":\"A100\",\"utilization_gpu\":\"80\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"index\":\"1\",\"uuid\":\"gpu-new-1\",\"name\":\"A100\",\"utilization_gpu\":\"75\"}\n"
            ),
        )
        .expect("write gpu");
        fs::write(
            metrics_dir.join("gpu_processes.jsonl"),
            concat!(
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"gpu_uuid\":\"gpu-old\",\"pid\":\"1\",\"process_name\":\"old\",\"used_memory_mib\":\"64\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"gpu_uuid\":\"gpu-new-0\",\"pid\":\"42\",\"process_name\":\"python\",\"used_memory_mib\":\"512\"}\n"
            ),
        )
        .expect("write gpu processes");
        fs::write(
            metrics_dir.join("slurm.jsonl"),
            concat!(
                "\n",
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"step_id\":\"123.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:01\",\"alloc_tres\":\"cpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:01\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"123.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:02\",\"alloc_tres\":\"cpu=1,gres/gpu:tesla=2\",\"tres_usage_in_ave\":\"cpu=00:00:02,gres/gpuutil:tesla=80\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"123.1\",\"ntasks\":\"2\",\"ave_cpu\":\"00:00:03\",\"alloc_tres\":\"cpu=2\",\"tres_usage_in_ave\":\"cpu=00:00:03\"}\n"
            ),
        )
        .expect("write slurm");

        let loaded = load_sampler_snapshot(&metrics_dir);
        let sampler = loaded.sampler.expect("sampler");
        assert_eq!(sampler.interval_seconds, 5);
        assert!(loaded.notes.iter().any(|note| note.contains("nvidia-smi")));
        let gpu = sampler.gpu.expect("gpu snapshot");
        assert_eq!(gpu.sampled_at, "2026-04-10T10:00:00Z");
        assert_eq!(gpu.gpus.len(), 2);
        assert_eq!(gpu.gpus[0].node, None);
        assert_eq!(gpu.nodes.len(), 1);
        assert_eq!(gpu.nodes[0].node, None);
        assert_eq!(gpu.nodes[0].gpu_count, 2);
        assert_eq!(gpu.nodes[0].avg_utilization_gpu, Some(77.5));
        assert_eq!(gpu.processes.len(), 1);
        let slurm = sampler.slurm.expect("slurm snapshot");
        assert_eq!(slurm.sampled_at, "2026-04-10T10:00:00Z");
        assert_eq!(slurm.steps.len(), 2);
    }

    #[test]
    fn observed_gpu_device_count_sums_distinct_devices_per_node() {
        fn device(node: Option<&str>, index: &str, uuid: Option<&str>) -> GpuDeviceSample {
            GpuDeviceSample {
                node: node.map(str::to_string),
                rank: None,
                local_rank: None,
                service: None,
                collector: None,
                index: Some(index.to_string()),
                uuid: uuid.map(str::to_string),
                name: None,
                utilization_gpu: None,
                utilization_memory: None,
                memory_used_mib: None,
                memory_total_mib: None,
                temperature_c: None,
                power_draw_w: None,
                power_limit_w: None,
            }
        }

        // Two nodes with two devices each; the repeated uuid on node02 must not
        // be double-counted, and the missing-uuid device falls back to index.
        let gpu = GpuSnapshot {
            sampled_at: "2026-04-10T10:00:00Z".into(),
            nodes: Vec::new(),
            gpus: vec![
                device(Some("node01"), "0", Some("gpu-a")),
                device(Some("node01"), "1", Some("gpu-b")),
                device(Some("node02"), "0", Some("gpu-c")),
                device(Some("node02"), "0", Some("gpu-c")),
                device(Some("node02"), "1", None),
            ],
            processes: Vec::new(),
        };
        // node01: {gpu-a, gpu-b} = 2; node02: {gpu-c, index "1"} = 2 (dup gpu-c
        // collapses). Total distinct devices across the fleet = 4.
        assert_eq!(observed_gpu_device_count(&gpu), Some(4));

        let empty = GpuSnapshot {
            sampled_at: "2026-04-10T10:00:00Z".into(),
            nodes: Vec::new(),
            gpus: Vec::new(),
            processes: Vec::new(),
        };
        assert_eq!(observed_gpu_device_count(&empty), None);
    }

    #[test]
    fn sampler_jsonl_loaders_read_complete_latest_tail_group() {
        use std::fmt::Write as _;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let metrics_dir = tmpdir.path();
        let old_sampled_at = "2026-04-10T09:59:00Z";
        let latest_sampled_at = "2026-04-10T10:00:00Z";

        let mut gpu_rows = String::new();
        for index in 0..1_500 {
            writeln!(
                gpu_rows,
                r#"{{"sampled_at":"{old_sampled_at}","index":"{index}","uuid":"gpu-old-{index}","name":"A100","utilization_gpu":"10"}}"#
            )
            .expect("write old gpu row");
        }
        for index in 0..900 {
            writeln!(
                gpu_rows,
                r#"{{"sampled_at":"{latest_sampled_at}","index":"{index}","uuid":"gpu-new-{index}","name":"A100","utilization_gpu":"80"}}"#
            )
            .expect("write latest gpu row");
        }
        fs::write(metrics_dir.join("gpu.jsonl"), gpu_rows).expect("write gpu");

        let (sampled_at, devices) = load_latest_gpu_devices(&metrics_dir.join("gpu.jsonl"))
            .expect("gpu")
            .expect("rows");
        assert_eq!(sampled_at, latest_sampled_at);
        assert_eq!(devices.len(), 900);
        assert_eq!(devices[0].uuid.as_deref(), Some("gpu-new-0"));
        assert_eq!(devices[899].uuid.as_deref(), Some("gpu-new-899"));

        let mut process_rows = String::new();
        for index in 0..1_000 {
            writeln!(
                process_rows,
                r#"{{"sampled_at":"{old_sampled_at}","gpu_uuid":"gpu-old-{index}","pid":"{index}","process_name":"old","used_memory_mib":"64"}}"#
            )
            .expect("write old process row");
        }
        for index in 0..700 {
            writeln!(
                process_rows,
                r#"{{"sampled_at":"{latest_sampled_at}","gpu_uuid":"gpu-new-{index}","pid":"{index}","process_name":"python","used_memory_mib":"512"}}"#
            )
            .expect("write latest process row");
        }
        fs::write(metrics_dir.join("gpu_processes.jsonl"), process_rows)
            .expect("write gpu processes");
        let processes = load_gpu_processes_for_timestamp(
            &metrics_dir.join("gpu_processes.jsonl"),
            latest_sampled_at,
        )
        .expect("processes");
        assert_eq!(processes.len(), 700);
        assert_eq!(processes[0].gpu_uuid.as_deref(), Some("gpu-new-0"));
        assert_eq!(processes[699].gpu_uuid.as_deref(), Some("gpu-new-699"));

        let mut slurm_rows = String::new();
        for index in 0..1_500 {
            writeln!(
                slurm_rows,
                r#"{{"sampled_at":"{old_sampled_at}","step_id":"123.{index}","ntasks":"1","ave_cpu":"00:00:01","alloc_tres":"cpu=1","tres_usage_in_ave":"cpu=00:00:01"}}"#
            )
            .expect("write old slurm row");
        }
        for index in 0..800 {
            writeln!(
                slurm_rows,
                r#"{{"sampled_at":"{latest_sampled_at}","step_id":"123.{index}","ntasks":"2","ave_cpu":"00:00:02","alloc_tres":"cpu=2","tres_usage_in_ave":"cpu=00:00:02"}}"#
            )
            .expect("write latest slurm row");
        }
        fs::write(metrics_dir.join("slurm.jsonl"), slurm_rows).expect("write slurm");
        let slurm = load_slurm_sampler_snapshot(metrics_dir)
            .expect("slurm")
            .expect("rows");
        assert_eq!(slurm.sampled_at, latest_sampled_at);
        assert_eq!(slurm.steps.len(), 800);
        assert_eq!(slurm.steps[0].step_id, "123.0");
        assert_eq!(slurm.steps[799].step_id, "123.799");
    }

    #[test]
    fn stats_parser_helpers_cover_error_and_prefix_paths() {
        let mut tres = BTreeMap::new();
        tres.insert("gres/gpu:tesla".to_string(), "2".to_string());
        assert_eq!(find_tres_value(&tres, "gres/gpu").as_deref(), Some("2"));
        assert!(!is_numbered_step("123", "123.batch"));
        assert!(is_numbered_step("123", "123.0"));

        let parsed = parse_tres_map(" , cpu=1 ,, gres/gpumem:tesla=8192M ").expect("tres");
        assert_eq!(parsed.get("cpu").map(String::as_str), Some("1"));
        assert!(
            parse_tres_map("broken-entry")
                .expect_err("invalid tres")
                .to_string()
                .contains("invalid TRES entry")
        );

        let row = SlurmSampleRow {
            sampled_at: "2026-04-10T10:00:00Z".into(),
            step_id: Some("123.0".into()),
            ntasks: None,
            ave_cpu: None,
            ave_rss: None,
            max_rss: None,
            alloc_tres: Some("gres/gpu:tesla=2".into()),
            tres_usage_in_ave: Some("gres/gpumem:tesla=8192M".into()),
        };
        let step = step_from_slurm_sample_row(row).expect("step");
        assert_eq!(step.gpu_count.as_deref(), Some("2"));
        assert_eq!(step.gpu_mem.as_deref(), Some("8192M"));

        let device = gpu_device_from_row(
            serde_json::from_str::<GpuDeviceSampleRow>(
                r#"{"sampled_at":"2026-04-10T10:00:00Z","node":"node01","rank":"7","local_rank":"3","service":"trainer","collector":"nvidia-smi","index":"0","memory_used_mib":"1024","memory_total_mib":"8192"}"#,
            )
            .expect("gpu row"),
        );
        assert_eq!(device.node.as_deref(), Some("node01"));
        assert_eq!(device.rank.as_deref(), Some("7"));
        assert_eq!(device.local_rank.as_deref(), Some("3"));
        assert_eq!(device.service.as_deref(), Some("trainer"));
        assert_eq!(device.collector.as_deref(), Some("nvidia-smi"));
        assert!(
            step_from_slurm_sample_row(SlurmSampleRow {
                sampled_at: "2026-04-10T10:00:00Z".into(),
                step_id: None,
                ntasks: None,
                ave_cpu: None,
                ave_rss: None,
                max_rss: None,
                alloc_tres: None,
                tres_usage_in_ave: None,
            })
            .expect_err("missing step id")
            .to_string()
            .contains("missing required field 'step_id'")
        );

        let false_bin = ["/usr/bin/false", "/bin/false"]
            .into_iter()
            .find(|path| Path::new(path).exists())
            .unwrap_or("false");
        let sstat_err = probe_step_stats("123", false_bin).expect_err("sstat failure");
        assert!(sstat_err.to_string().contains("sstat failed for job 123"));
    }
}
