use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::prepare::{RuntimePlan, RuntimeService};
use crate::spec::parse_slurm_time_limit;

use super::StatsOptions;
use super::accounting::{AccountingRow, AccountingSnapshot, build_accounting_snapshot};
use super::model::{SubmissionBackend, SubmissionRecord};
use super::runtime_state::{load_runtime_state, runtime_state_by_service};
use super::scheduler::{build_status_snapshot, scheduler_source_label};
use super::stats::{
    GpuDeviceSampleRow, GpuProcessSampleRow, SlurmSampleRow, StepStats, metrics_dir_for_record,
    probe_step_stats, step_from_slurm_sample_row,
};

const GIB: u64 = 1_024 * 1_024 * 1_024;
const MEMORY_HEADROOM_PERCENT: f64 = 1.25;
const MEMORY_ABSOLUTE_HEADROOM_BYTES: u64 = 2 * GIB;
const CPU_HEADROOM_PERCENT: f64 = 1.25;
const TIME_HEADROOM_PERCENT: f64 = 1.25;
const MEANINGFUL_REDUCTION_RATIO: f64 = 0.80;
const GPU_ACTIVE_UTILIZATION_PERCENT: u64 = 5;
const GPU_ACTIVE_MEMORY_MIB: u64 = 512;
const TIME_ROUNDING_SECONDS: u64 = 5 * 60;

/// Conservative resource right-sizing report for a completed tracked job.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RightsizeReport {
    pub job_id: String,
    pub scheduler_state: String,
    pub scheduler_source: String,
    pub complete: bool,
    pub sources: Vec<String>,
    pub notes: Vec<String>,
    pub observations: Vec<RightsizeObservation>,
    pub recommendations: Vec<RightsizeRecommendation>,
}

/// One resource usage observation used by the right-sizing assistant.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RightsizeObservation {
    pub resource: String,
    pub scope: String,
    pub target_path: String,
    pub requested: Option<String>,
    pub observed: Option<String>,
    pub utilization: Option<f64>,
    pub source: String,
    pub confidence: RightsizeConfidence,
    pub note: Option<String>,
}

/// One concrete resource setting recommendation.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RightsizeRecommendation {
    pub resource: String,
    pub scope: String,
    pub target_path: String,
    pub current: String,
    pub suggested: String,
    pub observed: String,
    pub reason: String,
    pub confidence: RightsizeConfidence,
}

/// Confidence label for a right-sizing observation or recommendation.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RightsizeConfidence {
    High,
    Medium,
    Low,
}

#[derive(Debug, Default)]
struct SamplerHistory {
    slurm_steps: Vec<StepStats>,
    gpu: Option<GpuActivitySummary>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct GpuActivitySummary {
    sample_count: usize,
    max_active_devices: u32,
    max_seen_devices: u32,
}

#[derive(Debug, Default)]
struct GpuSampleAccumulator {
    seen_devices: BTreeSet<String>,
    active_devices: BTreeSet<String>,
}

/// Builds a conservative right-sizing report for one tracked Slurm job.
pub fn build_rightsize_report(
    plan: &RuntimePlan,
    record: &SubmissionRecord,
    options: &StatsOptions,
) -> Result<RightsizeReport> {
    if record.backend != SubmissionBackend::Slurm {
        bail!(
            "inspect --rightsize requires a tracked Slurm submission; local runs are not supported"
        );
    }

    let status = build_status_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &options.scheduler,
    )
    .context("failed to inspect tracked scheduler state for right-sizing")?;
    let mut notes = Vec::new();
    let mut sources = BTreeSet::new();

    let mut steps = match probe_step_stats(&record.job_id, &options.sstat_bin) {
        Ok(steps) => {
            if steps.is_empty() {
                notes.push("sstat returned no numbered step rows for this job".to_string());
            } else {
                sources.insert("sstat".to_string());
            }
            steps
        }
        Err(err) => {
            notes.push(format!("sstat step statistics unavailable: {err}"));
            Vec::new()
        }
    };

    let metrics_dir = metrics_dir_for_record(record);
    let sampler = load_sampler_history(&metrics_dir, &mut notes);
    if !sampler.slurm_steps.is_empty() {
        sources.insert("sampler/slurm".to_string());
        steps.extend(sampler.slurm_steps.clone());
    }
    if sampler.gpu.is_some() {
        sources.insert("sampler/gpu".to_string());
    }

    let accounting =
        match build_accounting_snapshot(&record.job_id, Some(record), &options.scheduler.sacct_bin)
        {
            Ok(snapshot) => {
                if snapshot.available {
                    sources.insert("sacct".to_string());
                } else if let Some(reason) = &snapshot.reason {
                    notes.push(format!("sacct accounting unavailable: {reason}"));
                }
                Some(snapshot)
            }
            Err(err) => {
                notes.push(format!("sacct accounting unavailable: {err}"));
                None
            }
        };

    if !status.scheduler.terminal {
        notes.push(format!(
            "job is still {}; recommendations are provisional until the run reaches a terminal state",
            status.scheduler.state
        ));
    }

    let runtime_state = load_runtime_state(record);
    let launch_index_by_service = runtime_state
        .as_ref()
        .map(runtime_state_by_service)
        .map(|states| {
            states
                .into_values()
                .filter_map(|state| state.launch_index.map(|index| (index, state.service_name)))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let mut observations = Vec::new();
    let mut recommendations = Vec::new();
    add_memory_rightsize(
        plan,
        accounting.as_ref(),
        &steps,
        &mut observations,
        &mut recommendations,
        &mut notes,
    );
    add_cpu_rightsize(
        plan,
        accounting.as_ref(),
        &mut observations,
        &mut recommendations,
        &mut notes,
    );
    add_gpu_rightsize(
        plan,
        sampler.gpu.as_ref(),
        &mut observations,
        &mut recommendations,
        &mut notes,
    );
    add_time_rightsize(
        plan,
        accounting.as_ref(),
        &mut observations,
        &mut recommendations,
        &mut notes,
    );
    add_step_attribution_note(plan, &steps, &launch_index_by_service, &mut notes);

    Ok(RightsizeReport {
        job_id: record.job_id.clone(),
        scheduler_state: status.scheduler.state,
        scheduler_source: scheduler_source_label(status.scheduler.source).to_string(),
        complete: status.scheduler.terminal
            && accounting
                .as_ref()
                .is_some_and(|snapshot| snapshot.available),
        sources: sources.into_iter().collect(),
        notes,
        observations,
        recommendations,
    })
}

fn add_memory_rightsize(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
    steps: &[StepStats],
    observations: &mut Vec<RightsizeObservation>,
    recommendations: &mut Vec<RightsizeRecommendation>,
    notes: &mut Vec<String>,
) {
    let Some(raw_requested) = plan.slurm.mem.as_deref() else {
        notes.push("x-slurm.mem is not set; memory right-sizing needs an explicit request to compare against".to_string());
        return;
    };
    let Some(requested_bytes) = parse_memory_bytes(raw_requested) else {
        notes.push(format!(
            "could not parse x-slurm.mem='{raw_requested}' for right-sizing"
        ));
        return;
    };

    let mut observed_candidates = Vec::new();
    observed_candidates.extend(steps.iter().filter_map(estimated_step_memory_bytes));
    if let Some(accounting) = accounting {
        observed_candidates.extend(accounting.rows.iter().filter_map(|row| row.max_rss_bytes));
    }
    let Some(observed_bytes) = observed_candidates.into_iter().max() else {
        observations.push(RightsizeObservation {
            resource: "memory".to_string(),
            scope: "allocation".to_string(),
            target_path: "x-slurm.mem".to_string(),
            requested: Some(format_bytes_gib(requested_bytes)),
            observed: None,
            utilization: None,
            source: "unavailable".to_string(),
            confidence: RightsizeConfidence::Low,
            note: Some("no MaxRSS/AveRSS evidence was available".to_string()),
        });
        return;
    };

    observations.push(RightsizeObservation {
        resource: "memory".to_string(),
        scope: "allocation".to_string(),
        target_path: "x-slurm.mem".to_string(),
        requested: Some(format_bytes_gib(requested_bytes)),
        observed: Some(format_bytes_gib(observed_bytes)),
        utilization: Some(observed_bytes as f64 / requested_bytes as f64),
        source: "sacct/sstat/sampler".to_string(),
        confidence: RightsizeConfidence::High,
        note: None,
    });

    let target = ((observed_bytes as f64) * MEMORY_HEADROOM_PERCENT).ceil() as u64;
    let target = target.max(observed_bytes.saturating_add(MEMORY_ABSOLUTE_HEADROOM_BYTES));
    let suggested_bytes = ceil_nice_memory_bytes(target);
    if should_recommend_u64(suggested_bytes, requested_bytes) {
        recommendations.push(RightsizeRecommendation {
            resource: "memory".to_string(),
            scope: "allocation".to_string(),
            target_path: "x-slurm.mem".to_string(),
            current: raw_requested.to_string(),
            suggested: format_slurm_memory(suggested_bytes),
            observed: format_bytes_gib(observed_bytes),
            reason: format!(
                "allocation used {} of {}; keep 25% plus at least 2 GiB headroom",
                format_bytes_gib(observed_bytes),
                format_bytes_gib(requested_bytes)
            ),
            confidence: RightsizeConfidence::High,
        });
    }
}

fn add_cpu_rightsize(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
    observations: &mut Vec<RightsizeObservation>,
    recommendations: &mut Vec<RightsizeRecommendation>,
    notes: &mut Vec<String>,
) {
    let Some(accounting) = accounting.filter(|snapshot| snapshot.available) else {
        notes.push(
            "CPU right-sizing needs sacct accounting rows with elapsed and CPU time".to_string(),
        );
        return;
    };

    let single_service = plan.ordered_services.len() == 1;
    for service in &plan.ordered_services {
        let Some(requested_cpus) = service.slurm.cpus_per_task.or(plan.slurm.cpus_per_task) else {
            continue;
        };
        let rows = accounting_rows_for_service(
            &record_job_id(accounting),
            service,
            accounting,
            single_service,
        );
        if rows.is_empty() {
            observations.push(RightsizeObservation {
                resource: "cpus_per_task".to_string(),
                scope: service.name.clone(),
                target_path: cpu_target_path(plan, service),
                requested: Some(requested_cpus.to_string()),
                observed: None,
                utilization: None,
                source: "sacct".to_string(),
                confidence: RightsizeConfidence::Low,
                note: Some("no service-attributed sacct rows were available".to_string()),
            });
            continue;
        }
        let task_count = service_task_count(service).max(1) as f64;
        let observed_per_task = rows
            .iter()
            .filter_map(|row| observed_cpu_cores_per_task(row, task_count))
            .fold(None, max_f64);
        let Some(observed_per_task) = observed_per_task else {
            continue;
        };
        let target_path = cpu_target_path(plan, service);
        observations.push(RightsizeObservation {
            resource: "cpus_per_task".to_string(),
            scope: service.name.clone(),
            target_path: target_path.clone(),
            requested: Some(requested_cpus.to_string()),
            observed: Some(format!("{observed_per_task:.1}")),
            utilization: Some(observed_per_task / requested_cpus as f64),
            source: "sacct".to_string(),
            confidence: RightsizeConfidence::Medium,
            note: Some(
                "CPU usage is derived from average accounting CPU time, not peak runnable threads"
                    .to_string(),
            ),
        });

        let suggested = ((observed_per_task * CPU_HEADROOM_PERCENT).ceil() as u32).max(1);
        if should_recommend_u32(suggested, requested_cpus) {
            recommendations.push(RightsizeRecommendation {
                resource: "cpus_per_task".to_string(),
                scope: service.name.clone(),
                target_path,
                current: requested_cpus.to_string(),
                suggested: suggested.to_string(),
                observed: format!("{observed_per_task:.1}"),
                reason: format!(
                    "service {} used {:.1} of {} CPUs per task on average",
                    service.name, observed_per_task, requested_cpus
                ),
                confidence: RightsizeConfidence::Medium,
            });
        }
    }
}

fn add_gpu_rightsize(
    plan: &RuntimePlan,
    gpu: Option<&GpuActivitySummary>,
    observations: &mut Vec<RightsizeObservation>,
    recommendations: &mut Vec<RightsizeRecommendation>,
    notes: &mut Vec<String>,
) {
    let Some(summary) = gpu else {
        notes.push("GPU right-sizing needs nvidia-smi sampler history".to_string());
        return;
    };
    let Some((scope, target_path, requested_gpus)) = gpu_request_target(plan) else {
        notes.push("GPU sampler data is available, but no explicit x-slurm.gpus request was found to compare against".to_string());
        return;
    };
    observations.push(RightsizeObservation {
        resource: "gpus".to_string(),
        scope: scope.clone(),
        target_path: target_path.clone(),
        requested: Some(requested_gpus.to_string()),
        observed: Some(summary.max_active_devices.to_string()),
        utilization: Some(summary.max_active_devices as f64 / requested_gpus as f64),
        source: "sampler/gpu".to_string(),
        confidence: gpu_confidence(summary),
        note: Some(format!(
            "observed {} sampler snapshot(s), with up to {} visible device(s)",
            summary.sample_count, summary.max_seen_devices
        )),
    });

    let suggested = summary.max_active_devices.saturating_add(1).max(1);
    if should_recommend_u32(suggested, requested_gpus) {
        recommendations.push(RightsizeRecommendation {
            resource: "gpus".to_string(),
            scope,
            target_path,
            current: requested_gpus.to_string(),
            suggested: suggested.to_string(),
            observed: summary.max_active_devices.to_string(),
            reason: format!(
                "GPU sampler saw at most {} active of {} requested GPUs",
                summary.max_active_devices, requested_gpus
            ),
            confidence: gpu_confidence(summary),
        });
    }
}

fn add_time_rightsize(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
    observations: &mut Vec<RightsizeObservation>,
    recommendations: &mut Vec<RightsizeRecommendation>,
    notes: &mut Vec<String>,
) {
    let Some(raw_requested) = plan.slurm.time.as_deref() else {
        notes.push(
            "x-slurm.time is not set; walltime right-sizing needs an explicit request".to_string(),
        );
        return;
    };
    let Ok(requested_seconds) = parse_slurm_time_limit(raw_requested) else {
        notes.push(format!(
            "could not parse x-slurm.time='{raw_requested}' for right-sizing"
        ));
        return;
    };
    let Some(accounting) = accounting.filter(|snapshot| snapshot.available) else {
        notes.push("walltime right-sizing needs sacct elapsed accounting".to_string());
        return;
    };
    let Some(elapsed_seconds) = observed_elapsed_seconds(accounting) else {
        notes.push("sacct did not report elapsed seconds for walltime right-sizing".to_string());
        return;
    };
    observations.push(RightsizeObservation {
        resource: "time".to_string(),
        scope: "allocation".to_string(),
        target_path: "x-slurm.time".to_string(),
        requested: Some(format_slurm_duration(requested_seconds)),
        observed: Some(format_slurm_duration(elapsed_seconds)),
        utilization: Some(elapsed_seconds as f64 / requested_seconds as f64),
        source: "sacct".to_string(),
        confidence: RightsizeConfidence::High,
        note: None,
    });

    let suggested_seconds = ceil_to_multiple(
        ((elapsed_seconds as f64) * TIME_HEADROOM_PERCENT).ceil() as u64,
        TIME_ROUNDING_SECONDS,
    );
    if should_recommend_u64(suggested_seconds, requested_seconds) {
        recommendations.push(RightsizeRecommendation {
            resource: "time".to_string(),
            scope: "allocation".to_string(),
            target_path: "x-slurm.time".to_string(),
            current: raw_requested.to_string(),
            suggested: format_slurm_duration(suggested_seconds),
            observed: format_slurm_duration(elapsed_seconds),
            reason: format!(
                "job elapsed {} of requested {}",
                format_slurm_duration(elapsed_seconds),
                format_slurm_duration(requested_seconds)
            ),
            confidence: RightsizeConfidence::High,
        });
    }
}

fn add_step_attribution_note(
    plan: &RuntimePlan,
    steps: &[StepStats],
    launch_index_by_service: &BTreeMap<u32, String>,
    notes: &mut Vec<String>,
) {
    if plan.ordered_services.len() <= 1 || steps.is_empty() || !launch_index_by_service.is_empty() {
        return;
    }
    notes.push(
        "multi-service sstat rows could not be attributed to services because runtime state did not include launch indices".to_string(),
    );
}

fn load_sampler_history(metrics_dir: &Path, notes: &mut Vec<String>) -> SamplerHistory {
    if !metrics_dir.is_dir() {
        return SamplerHistory::default();
    }
    SamplerHistory {
        slurm_steps: match load_slurm_step_history(&metrics_dir.join("slurm.jsonl")) {
            Ok(steps) => steps,
            Err(err) => {
                notes.push(format!("failed to parse Slurm sampler history: {err}"));
                Vec::new()
            }
        },
        gpu: match load_gpu_activity_history(metrics_dir) {
            Ok(summary) => summary,
            Err(err) => {
                notes.push(format!("failed to parse GPU sampler history: {err}"));
                None
            }
        },
    }
}

fn load_slurm_step_history(path: &Path) -> Result<Vec<StepStats>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    let mut steps = Vec::new();
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row = serde_json::from_str::<SlurmSampleRow>(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        steps.push(step_from_slurm_sample_row(row).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?);
    }
    Ok(steps)
}

fn load_gpu_activity_history(metrics_dir: &Path) -> Result<Option<GpuActivitySummary>> {
    let gpu_path = metrics_dir.join("gpu.jsonl");
    if !gpu_path.exists() {
        return Ok(None);
    }
    let mut samples: BTreeMap<String, GpuSampleAccumulator> = BTreeMap::new();
    let raw =
        fs::read_to_string(&gpu_path).context(format!("failed to read {}", gpu_path.display()))?;
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row = serde_json::from_str::<GpuDeviceSampleRow>(line).context(format!(
            "failed to parse {} line {}",
            gpu_path.display(),
            index + 1
        ))?;
        let key = gpu_device_key(&row);
        let sample = samples.entry(row.sampled_at.clone()).or_default();
        sample.seen_devices.insert(key.clone());
        if gpu_device_is_active(&row) {
            sample.active_devices.insert(key);
        }
    }

    let processes_path = metrics_dir.join("gpu_processes.jsonl");
    if processes_path.exists() {
        let raw = fs::read_to_string(&processes_path)
            .context(format!("failed to read {}", processes_path.display()))?;
        for (index, raw_line) in raw.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            let row = serde_json::from_str::<GpuProcessSampleRow>(line).context(format!(
                "failed to parse {} line {}",
                processes_path.display(),
                index + 1
            ))?;
            let Some(key) = process_gpu_key(&row) else {
                continue;
            };
            samples
                .entry(row.sampled_at.clone())
                .or_default()
                .active_devices
                .insert(key);
        }
    }

    if samples.is_empty() {
        return Ok(None);
    }
    Ok(Some(GpuActivitySummary {
        sample_count: samples.len(),
        max_active_devices: samples
            .values()
            .map(|sample| sample.active_devices.len() as u32)
            .max()
            .unwrap_or(0),
        max_seen_devices: samples
            .values()
            .map(|sample| sample.seen_devices.len() as u32)
            .max()
            .unwrap_or(0),
    }))
}

fn gpu_device_key(row: &GpuDeviceSampleRow) -> String {
    row.uuid
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}:{}",
                row.node.as_deref().unwrap_or("unknown-node"),
                row.index.as_deref().unwrap_or("unknown-index")
            )
        })
}

fn process_gpu_key(row: &GpuProcessSampleRow) -> Option<String> {
    row.gpu_uuid
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn gpu_device_is_active(row: &GpuDeviceSampleRow) -> bool {
    parse_u64(row.utilization_gpu.as_deref())
        .is_some_and(|value| value > GPU_ACTIVE_UTILIZATION_PERCENT)
        || parse_u64(row.memory_used_mib.as_deref())
            .is_some_and(|value| value > GPU_ACTIVE_MEMORY_MIB)
}

fn estimated_step_memory_bytes(step: &StepStats) -> Option<u64> {
    let max_rss = parse_memory_bytes(&step.max_rss);
    let ave_rss_total = parse_memory_bytes(&step.ave_rss)
        .map(|value| value.saturating_mul(step.ntasks.trim().parse::<u64>().unwrap_or(1).max(1)));
    max_option(max_rss, ave_rss_total)
}

fn accounting_rows_for_service<'a>(
    job_id: &str,
    service: &RuntimeService,
    accounting: &'a AccountingSnapshot,
    single_service: bool,
) -> Vec<&'a AccountingRow> {
    let step_name = service_step_name(&service.name);
    accounting
        .rows
        .iter()
        .filter(|row| {
            row.job_name == step_name
                || (single_service
                    && is_numbered_job_step(job_id, &row.job_id_raw)
                    && !row.job_name.is_empty())
        })
        .collect()
}

fn record_job_id(accounting: &AccountingSnapshot) -> String {
    accounting
        .rows
        .iter()
        .map(|row| row.job_id_raw.as_str())
        .find(|value| !value.contains('.') && !value.contains('_'))
        .or_else(|| {
            accounting.rows.iter().find_map(|row| {
                row.job_id_raw
                    .split_once('.')
                    .map(|(prefix, _)| prefix)
                    .filter(|prefix| !prefix.is_empty())
            })
        })
        .unwrap_or("")
        .to_string()
}

fn is_numbered_job_step(job_id: &str, row_job_id: &str) -> bool {
    row_job_id
        .strip_prefix(job_id)
        .and_then(|rest| rest.strip_prefix('.'))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
}

fn observed_cpu_cores_per_task(row: &AccountingRow, task_count: f64) -> Option<f64> {
    let elapsed = row.elapsed_raw_seconds?;
    if elapsed == 0 {
        return None;
    }
    let total_cpu_seconds = row.total_cpu_seconds.or(row.cpu_time_raw_seconds)?;
    Some((total_cpu_seconds as f64 / elapsed as f64) / task_count.max(1.0))
}

fn service_task_count(service: &RuntimeService) -> u32 {
    service
        .placement
        .ntasks
        .or_else(|| {
            service
                .placement
                .ntasks_per_node
                .map(|per_node| per_node.saturating_mul(service.placement.nodes))
        })
        .unwrap_or(1)
}

fn cpu_target_path(plan: &RuntimePlan, service: &RuntimeService) -> String {
    if service.slurm.cpus_per_task.is_some() || plan.ordered_services.len() > 1 {
        format!("services.{}.x-slurm.cpus_per_task", service.name)
    } else {
        "x-slurm.cpus_per_task".to_string()
    }
}

fn gpu_request_target(plan: &RuntimePlan) -> Option<(String, String, u32)> {
    if plan.ordered_services.len() == 1 {
        let service = &plan.ordered_services[0];
        if let Some(gpus) = service.slurm.gpus {
            return Some((
                service.name.clone(),
                format!("services.{}.x-slurm.gpus", service.name),
                gpus,
            ));
        }
    }
    plan.slurm
        .gpus
        .map(|gpus| ("allocation".to_string(), "x-slurm.gpus".to_string(), gpus))
}

fn gpu_confidence(summary: &GpuActivitySummary) -> RightsizeConfidence {
    if summary.sample_count >= 3 {
        RightsizeConfidence::Medium
    } else {
        RightsizeConfidence::Low
    }
}

fn observed_elapsed_seconds(accounting: &AccountingSnapshot) -> Option<u64> {
    let allocation_rows = accounting
        .rows
        .iter()
        .filter(|row| !row.job_id_raw.contains('.') && !row.job_id_raw.contains('_'))
        .filter_map(|row| row.elapsed_raw_seconds)
        .collect::<Vec<_>>();
    if !allocation_rows.is_empty() {
        return allocation_rows.into_iter().max();
    }
    accounting
        .rows
        .iter()
        .filter_map(|row| row.elapsed_raw_seconds)
        .max()
}

fn parse_memory_bytes(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    let number_end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_digit()).then_some(index))
        .unwrap_or(trimmed.len());
    let value = trimmed[..number_end].parse::<u64>().ok()?;
    let unit = trimmed[number_end..].trim().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1_024,
        "M" | "MB" | "MIB" => 1_024_u64.pow(2),
        "G" | "GB" | "GIB" => GIB,
        "T" | "TB" | "TIB" => 1_024_u64.pow(4),
        "P" | "PB" | "PIB" => 1_024_u64.pow(5),
        _ => return None,
    };
    Some(value.saturating_mul(multiplier))
}

fn parse_u64(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse::<u64>().ok()
}

fn ceil_nice_memory_bytes(bytes: u64) -> u64 {
    let gib = bytes.div_ceil(GIB).max(1);
    let nice = [
        1, 2, 4, 8, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 768, 1024,
    ];
    if let Some(value) = nice.into_iter().find(|value| *value >= gib) {
        value * GIB
    } else {
        gib.div_ceil(256) * 256 * GIB
    }
}

fn format_slurm_memory(bytes: u64) -> String {
    format!("{}G", bytes.div_ceil(GIB).max(1))
}

fn format_bytes_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

fn format_slurm_duration(seconds: u64) -> String {
    let days = seconds / 86_400;
    let rest = seconds % 86_400;
    let hours = rest / 3_600;
    let minutes = (rest % 3_600) / 60;
    let seconds = rest % 60;
    if days > 0 {
        format!("{days}-{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

fn ceil_to_multiple(value: u64, multiple: u64) -> u64 {
    if multiple == 0 {
        value
    } else {
        value.div_ceil(multiple) * multiple
    }
}

fn should_recommend_u64(suggested: u64, requested: u64) -> bool {
    suggested < requested && (suggested as f64) <= (requested as f64 * MEANINGFUL_REDUCTION_RATIO)
}

fn should_recommend_u32(suggested: u32, requested: u32) -> bool {
    suggested < requested && (suggested as f64) <= (requested as f64 * MEANINGFUL_REDUCTION_RATIO)
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_f64(left: Option<f64>, right: f64) -> Option<f64> {
    Some(left.map_or(right, |left| left.max(right)))
}

fn service_step_name(value: &str) -> String {
    format!("hpc-compose:{}", service_token(value))
}

fn service_token(value: &str) -> String {
    let mut token = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            token.push(byte as char);
        } else {
            token.push_str(&format!("_x{byte:02x}_"));
        }
    }
    token
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_parsing_and_rounding_are_conservative() {
        assert_eq!(parse_memory_bytes("64G"), Some(64 * GIB));
        assert_eq!(parse_memory_bytes("512M"), Some(512 * 1_024 * 1_024));
        assert_eq!(format_slurm_memory(ceil_nice_memory_bytes(15 * GIB)), "16G");
        assert_eq!(format_slurm_memory(ceil_nice_memory_bytes(17 * GIB)), "24G");
    }

    #[test]
    fn step_memory_uses_max_rss_or_average_total() {
        let step = StepStats {
            step_id: "123.0".into(),
            ntasks: "4".into(),
            ave_cpu: String::new(),
            ave_rss: "2G".into(),
            max_rss: "3G".into(),
            alloc_tres: String::new(),
            tres_usage_in_ave: String::new(),
            alloc_tres_map: BTreeMap::new(),
            usage_tres_in_ave_map: BTreeMap::new(),
            gpu_count: None,
            gpu_util: None,
            gpu_mem: None,
        };
        assert_eq!(estimated_step_memory_bytes(&step), Some(8 * GIB));
    }

    #[test]
    fn gpu_activity_counts_active_devices_by_sample() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let metrics_dir = tmpdir.path();
        fs::write(
            metrics_dir.join("gpu.jsonl"),
            concat!(
                "{\"sampled_at\":\"t1\",\"index\":\"0\",\"uuid\":\"a\",\"utilization_gpu\":\"90\",\"memory_used_mib\":\"10\"}\n",
                "{\"sampled_at\":\"t1\",\"index\":\"1\",\"uuid\":\"b\",\"utilization_gpu\":\"0\",\"memory_used_mib\":\"10\"}\n",
                "{\"sampled_at\":\"t2\",\"index\":\"0\",\"uuid\":\"a\",\"utilization_gpu\":\"0\",\"memory_used_mib\":\"20\"}\n",
                "{\"sampled_at\":\"t2\",\"index\":\"1\",\"uuid\":\"b\",\"utilization_gpu\":\"0\",\"memory_used_mib\":\"900\"}\n"
            ),
        )
        .expect("gpu jsonl");
        fs::write(
            metrics_dir.join("gpu_processes.jsonl"),
            "{\"sampled_at\":\"t2\",\"gpu_uuid\":\"a\",\"pid\":\"42\"}\n",
        )
        .expect("process jsonl");
        let summary = load_gpu_activity_history(metrics_dir)
            .expect("history")
            .expect("summary");
        assert_eq!(summary.sample_count, 2);
        assert_eq!(summary.max_seen_devices, 2);
        assert_eq!(summary.max_active_devices, 2);
    }

    #[test]
    fn recommendation_threshold_requires_meaningful_reduction() {
        assert!(should_recommend_u32(4, 8));
        assert!(!should_recommend_u32(7, 8));
        assert!(should_recommend_u64(16, 64));
        assert!(!should_recommend_u64(52, 64));
    }

    #[test]
    fn walltime_rounds_to_five_minutes() {
        assert_eq!(ceil_to_multiple(4_501, TIME_ROUNDING_SECONDS), 4_800);
        assert_eq!(format_slurm_duration(4_500), "01:15:00");
        assert_eq!(format_slurm_duration(90_000), "1-01:00:00");
    }
}
