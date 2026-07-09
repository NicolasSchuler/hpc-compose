use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::runtime_plan::RuntimePlan;
use crate::spec::{GIB, parse_memory_bytes, parse_slurm_time_limit};

use super::accounting::{AccountingSnapshot, build_accounting_snapshot};
use super::model::{SubmissionBackend, SubmissionRecord};
use super::read_json;
use super::rightsize::{RightsizeConfidence, RightsizeRecommendation, build_rightsize_report};
use super::scheduler::{build_status_snapshot, parse_scheduler_timestamp, scheduler_source_label};
use super::stats::{
    GpuDeviceSampleRow, GpuProcessSampleRow, SamplerMetaFile, SchedulerOptions, SlurmSampleRow,
    StepStats, find_tres_value, metrics_dir_for_record, probe_step_stats,
    step_from_slurm_sample_row,
};

const GPU_ACTIVE_UTILIZATION_PERCENT: f64 = 5.0;
const GPU_ACTIVE_MEMORY_MIB: u64 = 512;
const CPU_ACTIVE_SECONDS: u64 = 1;

const GPU_WEIGHT: f64 = 0.35;
const MEMORY_WEIGHT: f64 = 0.25;
const COMPUTE_TIME_WEIGHT: f64 = 0.25;
const ENERGY_WEIGHT: f64 = 0.15;

/// Options for building an efficiency score report.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct EfficiencyScoreOptions {
    pub scheduler: SchedulerOptions,
    pub sstat_bin: String,
    pub pue: f64,
    pub gpu_tdp_w: f64,
    pub cpu_watts_per_core: f64,
}

impl Default for EfficiencyScoreOptions {
    fn default() -> Self {
        Self {
            scheduler: SchedulerOptions::default(),
            sstat_bin: "sstat".to_string(),
            pue: 1.20,
            gpu_tdp_w: 300.0,
            cpu_watts_per_core: 8.0,
        }
    }
}

/// Confidence label for an efficiency score component.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EfficiencyScoreConfidence {
    High,
    Medium,
    Low,
}

/// One scored efficiency dimension.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, schemars::JsonSchema)]
pub struct EfficiencyScoreComponent {
    pub name: String,
    pub label: String,
    pub available: bool,
    pub weight: f64,
    pub score: Option<u8>,
    pub utilization: Option<f64>,
    pub observed: Option<String>,
    pub requested: Option<String>,
    pub source: String,
    pub confidence: EfficiencyScoreConfidence,
    pub note: Option<String>,
}

/// Post-run resource efficiency report for one tracked Slurm job.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, schemars::JsonSchema)]
pub struct EfficiencyScoreReport {
    pub job_id: String,
    pub scheduler_state: String,
    pub scheduler_source: String,
    pub complete: bool,
    pub score: u8,
    pub grade: String,
    pub components: Vec<EfficiencyScoreComponent>,
    pub energy_kwh: Option<f64>,
    pub energy_basis: String,
    pub confidence: EfficiencyScoreConfidence,
    pub tips: Vec<String>,
    pub sources: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Default)]
struct ScoreSamplerHistory {
    interval_seconds: Option<u64>,
    gpu_samples: BTreeMap<String, GpuSampleSummary>,
    slurm_steps: Vec<StepStats>,
    slurm_active_timestamps: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct GpuSampleSummary {
    seen_devices: BTreeSet<String>,
    utilization_values: Vec<f64>,
    memory_used_mib: u64,
    power_draw_w: Option<f64>,
    power_limit_w: Option<f64>,
    active: bool,
}

#[derive(Debug, Clone, Copy)]
struct EnergyEstimate {
    actual_kwh: Option<f64>,
    budget_kwh: Option<f64>,
    basis: &'static str,
    confidence: EfficiencyScoreConfidence,
}

/// Builds the post-run efficiency score for one tracked Slurm job.
pub fn build_efficiency_score_report(
    plan: &RuntimePlan,
    record: &SubmissionRecord,
    options: &EfficiencyScoreOptions,
) -> Result<EfficiencyScoreReport> {
    if options.pue <= 0.0 {
        bail!("score --pue must be greater than 0");
    }
    if options.gpu_tdp_w < 0.0 {
        bail!("score --gpu-tdp-w must be non-negative");
    }
    if options.cpu_watts_per_core < 0.0 {
        bail!("score --cpu-watts-per-core must be non-negative");
    }
    if record.backend != SubmissionBackend::Slurm {
        bail!("score requires a tracked Slurm submission; local runs are not supported");
    }

    let status = build_status_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &options.scheduler,
    )
    .context("failed to inspect tracked scheduler state for efficiency scoring")?;
    let mut notes = Vec::new();
    let mut sources = BTreeSet::new();

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

    let mut steps = match probe_step_stats(&record.job_id, &options.sstat_bin) {
        Ok(steps) => {
            if !steps.is_empty() {
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
    let sampler = load_score_sampler_history(&metrics_dir, &mut notes);
    if !sampler.gpu_samples.is_empty() {
        sources.insert("sampler/gpu".to_string());
    }
    if !sampler.slurm_steps.is_empty() {
        sources.insert("sampler/slurm".to_string());
        steps.extend(sampler.slurm_steps.clone());
    }

    let requested_walltime_seconds = requested_walltime_seconds(plan, &mut notes);
    let elapsed_seconds = accounting
        .as_ref()
        .filter(|snapshot| snapshot.available)
        .and_then(observed_elapsed_seconds);

    let energy = estimate_energy(
        plan,
        accounting.as_ref(),
        &sampler,
        requested_walltime_seconds,
        elapsed_seconds,
        options,
    );

    let components = vec![
        gpu_utilization_component(&sampler, &steps),
        memory_utilization_component(plan, accounting.as_ref(), &steps),
        compute_time_component(&sampler, requested_walltime_seconds),
        energy_component(energy),
    ];

    let score = weighted_score(&components);
    let grade = grade_for_score(score).to_string();
    let confidence = overall_confidence(&components, energy.confidence);

    let rightsize = build_rightsize_report(
        plan,
        record,
        &super::StatsOptions {
            scheduler: options.scheduler.clone(),
            sstat_bin: options.sstat_bin.clone(),
            accounting: false,
        },
    );
    let tips = match rightsize {
        Ok(report) => {
            sources.extend(report.sources);
            notes.extend(
                report
                    .notes
                    .into_iter()
                    .map(|note| format!("rightsize: {note}")),
            );
            strongest_tip(&report.recommendations).into_iter().collect()
        }
        Err(err) => {
            notes.push(format!("rightsize tips unavailable: {err}"));
            Vec::new()
        }
    };

    let complete = status.scheduler.terminal
        && accounting
            .as_ref()
            .is_some_and(|snapshot| snapshot.available);

    Ok(EfficiencyScoreReport {
        job_id: record.job_id.clone(),
        scheduler_state: status.scheduler.state,
        scheduler_source: scheduler_source_label(status.scheduler.source).to_string(),
        complete,
        score,
        grade,
        components,
        energy_kwh: energy.actual_kwh.map(round3),
        energy_basis: energy.basis.to_string(),
        confidence,
        tips,
        sources: sources.into_iter().collect(),
        notes,
    })
}

fn load_score_sampler_history(metrics_dir: &Path, notes: &mut Vec<String>) -> ScoreSamplerHistory {
    if !metrics_dir.is_dir() {
        return ScoreSamplerHistory::default();
    }

    let mut history = ScoreSamplerHistory::default();
    let meta_path = metrics_dir.join("meta.json");
    if meta_path.exists() {
        match read_json::<SamplerMetaFile>(&meta_path) {
            Ok(meta) => history.interval_seconds = Some(meta.interval_seconds),
            Err(err) => notes.push(format!(
                "failed to parse metrics sampler metadata at {}: {err}",
                meta_path.display()
            )),
        }
    }

    history.gpu_samples = match load_gpu_sample_history(metrics_dir) {
        Ok(samples) => samples,
        Err(err) => {
            notes.push(format!("failed to parse GPU sampler history: {err}"));
            BTreeMap::new()
        }
    };
    let (slurm_steps, active_timestamps) =
        match load_slurm_sample_history(&metrics_dir.join("slurm.jsonl")) {
            Ok(history) => history,
            Err(err) => {
                notes.push(format!("failed to parse Slurm sampler history: {err}"));
                (Vec::new(), BTreeSet::new())
            }
        };
    history.slurm_steps = slurm_steps;
    history.slurm_active_timestamps = active_timestamps;
    history
}

fn load_gpu_sample_history(metrics_dir: &Path) -> Result<BTreeMap<String, GpuSampleSummary>> {
    let gpu_path = metrics_dir.join("gpu.jsonl");
    let mut samples: BTreeMap<String, GpuSampleSummary> = BTreeMap::new();
    if !gpu_path.exists() {
        return Ok(samples);
    }

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
        let sample = samples.entry(row.sampled_at.clone()).or_default();
        let device_key = gpu_device_key(&row);
        sample.seen_devices.insert(device_key);
        if let Some(value) = parse_f64(row.utilization_gpu.as_deref()) {
            sample.utilization_values.push(value);
            if value > GPU_ACTIVE_UTILIZATION_PERCENT {
                sample.active = true;
            }
        }
        if let Some(value) = parse_u64(row.memory_used_mib.as_deref()) {
            sample.memory_used_mib = sample.memory_used_mib.saturating_add(value);
            if value > GPU_ACTIVE_MEMORY_MIB {
                sample.active = true;
            }
        }
        add_optional_f64(
            &mut sample.power_draw_w,
            parse_f64(row.power_draw_w.as_deref()),
        );
        add_optional_f64(
            &mut sample.power_limit_w,
            parse_f64(row.power_limit_w.as_deref()),
        );
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
            if row
                .gpu_uuid
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
                || parse_u64(row.used_memory_mib.as_deref()).is_some_and(|value| value > 0)
            {
                samples.entry(row.sampled_at).or_default().active = true;
            }
        }
    }

    Ok(samples)
}

fn load_slurm_sample_history(path: &Path) -> Result<(Vec<StepStats>, BTreeSet<String>)> {
    if !path.exists() {
        return Ok((Vec::new(), BTreeSet::new()));
    }
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    let mut steps = Vec::new();
    let mut active_timestamps = BTreeSet::new();
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
        let sampled_at = row.sampled_at.clone();
        let step = step_from_slurm_sample_row(row).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        if step_has_cpu_activity(&step) || estimated_step_memory_bytes(&step).is_some() {
            active_timestamps.insert(sampled_at);
        }
        steps.push(step);
    }
    Ok((steps, active_timestamps))
}

fn gpu_utilization_component(
    sampler: &ScoreSamplerHistory,
    steps: &[StepStats],
) -> EfficiencyScoreComponent {
    if let Some(percent) = sampler.mean_gpu_utilization_percent() {
        let score = percent_score(percent / 100.0);
        return EfficiencyScoreComponent {
            name: "gpu_utilization".to_string(),
            label: "GPU Util".to_string(),
            available: true,
            weight: GPU_WEIGHT,
            score: Some(score),
            utilization: Some(percent / 100.0),
            observed: Some(format!("{percent:.1}%")),
            requested: None,
            source: "sampler/gpu".to_string(),
            confidence: if sampler.gpu_samples.len() >= 3 {
                EfficiencyScoreConfidence::High
            } else {
                EfficiencyScoreConfidence::Low
            },
            note: None,
        };
    }

    let values = steps
        .iter()
        .filter_map(|step| step.gpu_util.as_deref())
        .filter_map(parse_f64_from_slurm_value)
        .collect::<Vec<_>>();
    if !values.is_empty() {
        let percent = values.iter().sum::<f64>() / values.len() as f64;
        let score = percent_score(percent / 100.0);
        return EfficiencyScoreComponent {
            name: "gpu_utilization".to_string(),
            label: "GPU Util".to_string(),
            available: true,
            weight: GPU_WEIGHT,
            score: Some(score),
            utilization: Some(percent / 100.0),
            observed: Some(format!("{percent:.1}%")),
            requested: None,
            source: "sstat".to_string(),
            confidence: EfficiencyScoreConfidence::Medium,
            note: Some(
                "GPU utilization came from Slurm TRES accounting, not sampler history".into(),
            ),
        };
    }

    unavailable_component(
        "gpu_utilization",
        "GPU Util",
        GPU_WEIGHT,
        "GPU utilization needs nvidia-smi sampler history or GPU TRES usage from sstat",
    )
}

fn memory_utilization_component(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
    steps: &[StepStats],
) -> EfficiencyScoreComponent {
    let requested = requested_memory_bytes(plan, accounting);
    let observed = observed_memory_bytes(accounting, steps);
    match (requested, observed) {
        (Some((requested_bytes, requested_label)), Some(observed_bytes)) if requested_bytes > 0 => {
            let utilization = observed_bytes as f64 / requested_bytes as f64;
            EfficiencyScoreComponent {
                name: "memory_utilization".to_string(),
                label: "Memory".to_string(),
                available: true,
                weight: MEMORY_WEIGHT,
                score: Some(percent_score(utilization)),
                utilization: Some(utilization),
                observed: Some(format_bytes_gib(observed_bytes)),
                requested: Some(requested_label),
                source: "sacct/sstat/sampler".to_string(),
                confidence: EfficiencyScoreConfidence::High,
                note: None,
            }
        }
        (None, _) => unavailable_component(
            "memory_utilization",
            "Memory",
            MEMORY_WEIGHT,
            "memory utilization needs an explicit memory request or Slurm memory TRES",
        ),
        (_, None) => unavailable_component(
            "memory_utilization",
            "Memory",
            MEMORY_WEIGHT,
            "memory utilization needs MaxRSS/AveRSS evidence from sacct, sstat, or sampler history",
        ),
        _ => unavailable_component(
            "memory_utilization",
            "Memory",
            MEMORY_WEIGHT,
            "memory utilization could not be computed from the available evidence",
        ),
    }
}

fn compute_time_component(
    sampler: &ScoreSamplerHistory,
    requested_walltime_seconds: Option<u64>,
) -> EfficiencyScoreComponent {
    let Some(requested_seconds) = requested_walltime_seconds else {
        return unavailable_component(
            "compute_time_utilization",
            "Walltime",
            COMPUTE_TIME_WEIGHT,
            "compute-time utilization needs an explicit x-slurm.time request",
        );
    };
    let Some(active_seconds) = sampler.active_seconds() else {
        return unavailable_component(
            "compute_time_utilization",
            "Walltime",
            COMPUTE_TIME_WEIGHT,
            "compute-time utilization needs historical GPU or Slurm sampler rows",
        );
    };
    let utilization = active_seconds / requested_seconds.max(1) as f64;
    EfficiencyScoreComponent {
        name: "compute_time_utilization".to_string(),
        label: "Walltime".to_string(),
        available: true,
        weight: COMPUTE_TIME_WEIGHT,
        score: Some(percent_score(utilization)),
        utilization: Some(utilization),
        observed: Some(format_duration_seconds(active_seconds.round() as u64)),
        requested: Some(format_duration_seconds(requested_seconds)),
        source: "sampler".to_string(),
        confidence: if sampler.sample_count() >= 3 {
            EfficiencyScoreConfidence::Medium
        } else {
            EfficiencyScoreConfidence::Low
        },
        note: Some(
            "active time excludes sampler intervals without GPU, GPU-process, or CPU-step activity"
                .into(),
        ),
    }
}

fn energy_component(energy: EnergyEstimate) -> EfficiencyScoreComponent {
    match (energy.actual_kwh, energy.budget_kwh) {
        (Some(actual), Some(budget)) if budget > 0.0 => {
            let utilization = actual / budget;
            EfficiencyScoreComponent {
                name: "energy_budget_utilization".to_string(),
                label: "Energy".to_string(),
                available: true,
                weight: ENERGY_WEIGHT,
                score: Some(percent_score(utilization)),
                utilization: Some(utilization),
                observed: Some(format!("{:.2} kWh", round2(actual))),
                requested: Some(format!("{:.2} kWh", round2(budget))),
                source: energy.basis.to_string(),
                confidence: energy.confidence,
                note: Some(
                    "kWh is a best-effort energy estimate, not a carbon-emissions claim".into(),
                ),
            }
        }
        (Some(actual), None) => EfficiencyScoreComponent {
            name: "energy_budget_utilization".to_string(),
            label: "Energy".to_string(),
            available: false,
            weight: ENERGY_WEIGHT,
            score: None,
            utilization: None,
            observed: Some(format!("{:.2} kWh", round2(actual))),
            requested: None,
            source: energy.basis.to_string(),
            confidence: energy.confidence,
            note: Some("energy budget needs requested walltime and allocation power".into()),
        },
        _ => unavailable_component(
            "energy_budget_utilization",
            "Energy",
            ENERGY_WEIGHT,
            "energy estimate needs walltime, accounting, or sampler duration evidence",
        ),
    }
}

fn estimate_energy(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
    sampler: &ScoreSamplerHistory,
    requested_walltime_seconds: Option<u64>,
    elapsed_seconds: Option<u64>,
    options: &EfficiencyScoreOptions,
) -> EnergyEstimate {
    let gpu_count = allocated_gpu_count(accounting)
        .or_else(|| requested_gpu_count(plan))
        .or_else(|| sampler.max_seen_gpu_devices().map(|value| value as u64))
        .unwrap_or(0);
    let cpu_count = allocated_cpu_count(accounting)
        .or_else(|| requested_cpu_count(plan))
        .unwrap_or(1);
    let allocation_power_w =
        gpu_count as f64 * options.gpu_tdp_w + cpu_count as f64 * options.cpu_watts_per_core;
    let budget_seconds = requested_walltime_seconds.or(elapsed_seconds).or_else(|| {
        sampler
            .total_sampled_seconds()
            .map(|value| value.round() as u64)
    });
    let budget_kwh = match (allocation_power_w > 0.0, budget_seconds) {
        (true, Some(seconds)) => Some(kwh(allocation_power_w, seconds as f64, options.pue)),
        _ => None,
    };

    if let Some((gpu_wh, seconds)) = sampler.integrated_gpu_power_draw_wh() {
        let cpu_wh = cpu_count as f64 * options.cpu_watts_per_core * seconds / 3_600.0;
        return EnergyEstimate {
            actual_kwh: Some((gpu_wh + cpu_wh) * options.pue / 1_000.0),
            budget_kwh,
            basis: "sampler_power_draw+pue",
            confidence: EfficiencyScoreConfidence::High,
        };
    }

    if let Some((gpu_wh, seconds)) = sampler.integrated_gpu_power_limit_wh() {
        let cpu_wh = cpu_count as f64 * options.cpu_watts_per_core * seconds / 3_600.0;
        return EnergyEstimate {
            actual_kwh: Some((gpu_wh + cpu_wh) * options.pue / 1_000.0),
            budget_kwh,
            basis: "sampler_power_limit+pue",
            confidence: EfficiencyScoreConfidence::Medium,
        };
    }

    let fallback_seconds = elapsed_seconds
        .map(|value| value as f64)
        .or_else(|| sampler.active_seconds())
        .or_else(|| budget_seconds.map(|value| value as f64));
    EnergyEstimate {
        actual_kwh: fallback_seconds.map(|seconds| kwh(allocation_power_w, seconds, options.pue)),
        budget_kwh,
        basis: "configured_tdp+pue",
        confidence: EfficiencyScoreConfidence::Low,
    }
}

impl ScoreSamplerHistory {
    fn sample_count(&self) -> usize {
        self.all_sample_timestamps().len()
    }

    fn all_sample_timestamps(&self) -> BTreeSet<String> {
        let mut timestamps = self.gpu_samples.keys().cloned().collect::<BTreeSet<_>>();
        timestamps.extend(self.slurm_active_timestamps.iter().cloned());
        timestamps
    }

    fn duration_by_timestamp(&self) -> BTreeMap<String, f64> {
        let timestamps = self.all_sample_timestamps();
        duration_by_timestamp_for(timestamps, self.interval_seconds)
    }

    fn mean_gpu_utilization_percent(&self) -> Option<f64> {
        if self.gpu_samples.is_empty() {
            return None;
        }
        let durations = self.duration_by_timestamp();
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        for (sampled_at, sample) in &self.gpu_samples {
            if sample.utilization_values.is_empty() {
                continue;
            }
            let avg = sample.utilization_values.iter().sum::<f64>()
                / sample.utilization_values.len() as f64;
            let weight = durations.get(sampled_at).copied().unwrap_or(1.0).max(1.0);
            weighted_sum += avg * weight;
            total_weight += weight;
        }
        (total_weight > 0.0).then_some(weighted_sum / total_weight)
    }

    fn active_seconds(&self) -> Option<f64> {
        let timestamps = self.all_sample_timestamps();
        if timestamps.is_empty() {
            return None;
        }
        let durations = self.duration_by_timestamp();
        let active = timestamps
            .iter()
            .filter(|timestamp| {
                self.gpu_samples
                    .get(*timestamp)
                    .is_some_and(|sample| sample.active)
                    || self.slurm_active_timestamps.contains(*timestamp)
            })
            .map(|timestamp| durations.get(timestamp).copied().unwrap_or(1.0).max(0.0))
            .sum::<f64>();
        Some(active)
    }

    fn total_sampled_seconds(&self) -> Option<f64> {
        let durations = self.duration_by_timestamp();
        (!durations.is_empty()).then(|| durations.values().sum())
    }

    fn max_seen_gpu_devices(&self) -> Option<usize> {
        self.gpu_samples
            .values()
            .map(|sample| sample.seen_devices.len())
            .max()
            .filter(|value| *value > 0)
    }

    fn integrated_gpu_power_draw_wh(&self) -> Option<(f64, f64)> {
        self.integrated_gpu_power_wh(|sample| sample.power_draw_w)
    }

    fn integrated_gpu_power_limit_wh(&self) -> Option<(f64, f64)> {
        self.integrated_gpu_power_wh(|sample| sample.power_limit_w)
    }

    fn integrated_gpu_power_wh(
        &self,
        power: impl Fn(&GpuSampleSummary) -> Option<f64>,
    ) -> Option<(f64, f64)> {
        if self.gpu_samples.is_empty() {
            return None;
        }
        let durations = self.duration_by_timestamp();
        let mut watt_seconds = 0.0;
        let mut seconds = 0.0;
        for (sampled_at, sample) in &self.gpu_samples {
            let Some(power_w) = power(sample) else {
                continue;
            };
            let duration = durations.get(sampled_at).copied().unwrap_or(1.0).max(1.0);
            watt_seconds += power_w * duration;
            seconds += duration;
        }
        (seconds > 0.0).then_some((watt_seconds / 3_600.0, seconds))
    }
}

fn duration_by_timestamp_for(
    timestamps: BTreeSet<String>,
    interval_seconds: Option<u64>,
) -> BTreeMap<String, f64> {
    let parsed = timestamps
        .iter()
        .filter_map(|timestamp| parse_scheduler_timestamp(timestamp).map(|unix| (timestamp, unix)))
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        return timestamps
            .into_iter()
            .map(|timestamp| (timestamp, interval_seconds.unwrap_or(1) as f64))
            .collect();
    }

    let mut durations = BTreeMap::new();
    for (index, (timestamp, unix)) in parsed.iter().enumerate() {
        let duration = parsed
            .get(index + 1)
            .and_then(|(_, next)| next.checked_sub(*unix))
            .filter(|value| *value > 0)
            .or(interval_seconds)
            .unwrap_or(1);
        let duration = interval_seconds
            .map(|interval| duration.min(interval))
            .unwrap_or(duration);
        durations.insert((*timestamp).clone(), duration.max(1) as f64);
    }
    durations
}

fn weighted_score(components: &[EfficiencyScoreComponent]) -> u8 {
    let mut total_weight = 0.0;
    let mut weighted_sum = 0.0;
    for component in components {
        let Some(score) = component.score else {
            continue;
        };
        total_weight += component.weight;
        weighted_sum += f64::from(score) * component.weight;
    }
    if total_weight == 0.0 {
        return 0;
    }
    (weighted_sum / total_weight).round().clamp(0.0, 100.0) as u8
}

fn grade_for_score(score: u8) -> &'static str {
    match score {
        85..=100 => "A",
        70..=84 => "B",
        55..=69 => "C",
        40..=54 => "D",
        _ => "F",
    }
}

fn overall_confidence(
    components: &[EfficiencyScoreComponent],
    energy_confidence: EfficiencyScoreConfidence,
) -> EfficiencyScoreConfidence {
    let available = components
        .iter()
        .filter(|component| component.available)
        .collect::<Vec<_>>();
    if available.is_empty() {
        return EfficiencyScoreConfidence::Low;
    }
    if available.len() >= 3
        && available
            .iter()
            .all(|component| component.confidence == EfficiencyScoreConfidence::High)
        && energy_confidence != EfficiencyScoreConfidence::Low
    {
        return EfficiencyScoreConfidence::High;
    }
    if available.len() >= 2 {
        EfficiencyScoreConfidence::Medium
    } else {
        EfficiencyScoreConfidence::Low
    }
}

fn percent_score(utilization: f64) -> u8 {
    (utilization * 100.0).round().clamp(0.0, 100.0) as u8
}

fn unavailable_component(
    name: &str,
    label: &str,
    weight: f64,
    note: &str,
) -> EfficiencyScoreComponent {
    EfficiencyScoreComponent {
        name: name.to_string(),
        label: label.to_string(),
        available: false,
        weight,
        score: None,
        utilization: None,
        observed: None,
        requested: None,
        source: "unavailable".to_string(),
        confidence: EfficiencyScoreConfidence::Low,
        note: Some(note.to_string()),
    }
}

fn strongest_tip(recommendations: &[RightsizeRecommendation]) -> Option<String> {
    recommendations
        .iter()
        .min_by_key(|recommendation| {
            let confidence_rank = match recommendation.confidence {
                RightsizeConfidence::High => 0,
                RightsizeConfidence::Medium => 1,
                RightsizeConfidence::Low => 2,
            };
            let resource_rank = match recommendation.resource.as_str() {
                "memory" => 0,
                "time" => 1,
                "gpus" => 2,
                "cpus_per_task" => 3,
                _ => 4,
            };
            (confidence_rank, resource_rank)
        })
        .map(|recommendation| {
            format!(
                "Consider {}: {} (was {}; observed {}).",
                recommendation.target_path,
                recommendation.suggested,
                recommendation.current,
                recommendation.observed
            )
        })
}

fn requested_walltime_seconds(plan: &RuntimePlan, notes: &mut Vec<String>) -> Option<u64> {
    let raw = plan.slurm.time.as_deref()?;
    match parse_slurm_time_limit(raw) {
        Ok(seconds) => Some(seconds),
        Err(err) => {
            notes.push(format!(
                "could not parse x-slurm.time='{raw}' for efficiency scoring: {err}"
            ));
            None
        }
    }
}

fn requested_memory_bytes(
    plan: &RuntimePlan,
    accounting: Option<&AccountingSnapshot>,
) -> Option<(u64, String)> {
    if let Some(raw) = plan.slurm.mem.as_deref()
        && let Some(bytes) = parse_memory_bytes(raw)
    {
        return Some((bytes, format_bytes_gib(bytes)));
    }
    accounting
        .filter(|snapshot| snapshot.available)
        .and_then(primary_accounting_rows)
        .and_then(|rows| {
            rows.iter()
                .find_map(|row| tres_memory_bytes(&row.alloc_tres_map))
                .or_else(|| {
                    rows.iter()
                        .find_map(|row| tres_memory_bytes(&row.req_tres_map))
                })
        })
        .map(|bytes| (bytes, format_bytes_gib(bytes)))
}

fn observed_memory_bytes(
    accounting: Option<&AccountingSnapshot>,
    steps: &[StepStats],
) -> Option<u64> {
    let mut observed = steps
        .iter()
        .filter_map(estimated_step_memory_bytes)
        .collect::<Vec<_>>();
    if let Some(accounting) = accounting.filter(|snapshot| snapshot.available) {
        if let Some(summary) = &accounting.summary
            && let Some(bytes) = summary.max_rss_bytes
        {
            observed.push(bytes);
        }
        observed.extend(accounting.rows.iter().filter_map(|row| row.max_rss_bytes));
    }
    observed.into_iter().max()
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

fn primary_accounting_rows(accounting: &AccountingSnapshot) -> Option<Vec<&super::AccountingRow>> {
    if accounting.rows.is_empty() {
        return None;
    }
    let allocation = accounting
        .rows
        .iter()
        .filter(|row| !row.job_id_raw.contains('.') && !row.job_id_raw.contains('_'))
        .collect::<Vec<_>>();
    if allocation.is_empty() {
        Some(accounting.rows.iter().collect())
    } else {
        Some(allocation)
    }
}

fn allocated_gpu_count(accounting: Option<&AccountingSnapshot>) -> Option<u64> {
    accounting
        .filter(|snapshot| snapshot.available)
        .and_then(primary_accounting_rows)
        .and_then(|rows| {
            rows.iter()
                .find_map(|row| tres_gpu_count(&row.alloc_tres_map))
                .or_else(|| {
                    rows.iter()
                        .find_map(|row| tres_gpu_count(&row.req_tres_map))
                })
        })
}

fn allocated_cpu_count(accounting: Option<&AccountingSnapshot>) -> Option<u64> {
    accounting
        .filter(|snapshot| snapshot.available)
        .and_then(primary_accounting_rows)
        .and_then(|rows| rows.iter().filter_map(|row| row.alloc_cpus).max())
}

fn requested_gpu_count(plan: &RuntimePlan) -> Option<u64> {
    if let Some(gpus) = plan.slurm.gpus {
        return Some(u64::from(gpus));
    }
    if let (Some(per_node), Some(nodes)) = (plan.slurm.gpus_per_node, plan.slurm.nodes) {
        return Some(u64::from(per_node.saturating_mul(nodes)));
    }
    if let Some(gres) = plan.slurm.gres.as_deref()
        && let Some(gpus) = parse_gres_gpu_count(gres)
    {
        return Some(u64::from(gpus));
    }
    let service_total = plan
        .ordered_services
        .iter()
        .filter_map(|service| service.slurm.gpus)
        .map(u64::from)
        .sum::<u64>();
    (service_total > 0).then_some(service_total)
}

fn requested_cpu_count(plan: &RuntimePlan) -> Option<u64> {
    let allocation_tasks = plan.slurm.ntasks.or_else(|| {
        plan.slurm
            .ntasks_per_node
            .zip(plan.slurm.nodes)
            .map(|(per_node, nodes)| per_node.saturating_mul(nodes))
    });
    if let Some(tasks) = allocation_tasks {
        return Some(u64::from(tasks) * u64::from(plan.slurm.cpus_per_task.unwrap_or(1)));
    }
    let service_total = plan
        .ordered_services
        .iter()
        .map(|service| {
            let tasks = service
                .placement
                .ntasks
                .or_else(|| {
                    service
                        .placement
                        .ntasks_per_node
                        .map(|per_node| per_node.saturating_mul(service.placement.nodes))
                })
                .unwrap_or(1);
            u64::from(tasks) * u64::from(service.slurm.cpus_per_task.unwrap_or(1))
        })
        .sum::<u64>();
    (service_total > 0).then_some(service_total)
}

fn estimated_step_memory_bytes(step: &StepStats) -> Option<u64> {
    let max_rss = parse_memory_bytes(&step.max_rss);
    let ave_rss_total = parse_memory_bytes(&step.ave_rss)
        .map(|value| value.saturating_mul(step.ntasks.trim().parse::<u64>().unwrap_or(1).max(1)));
    max_option(max_rss, ave_rss_total)
}

fn step_has_cpu_activity(step: &StepStats) -> bool {
    parse_slurm_duration_seconds(&step.ave_cpu).is_some_and(|seconds| seconds >= CPU_ACTIVE_SECONDS)
        || find_tres_value(&step.usage_tres_in_ave_map, "cpu")
            .as_deref()
            .and_then(parse_slurm_duration_seconds)
            .is_some_and(|seconds| seconds >= CPU_ACTIVE_SECONDS)
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

fn parse_gres_gpu_count(gres: &str) -> Option<u32> {
    for part in gres.split(',') {
        let part = part.trim();
        if !part.to_ascii_lowercase().contains("gpu") {
            continue;
        }
        if let Some(value) = part
            .rsplit(':')
            .next()
            .and_then(|last| last.parse::<u32>().ok())
        {
            return Some(value);
        }
        return Some(1);
    }
    None
}

fn tres_gpu_count(values: &BTreeMap<String, String>) -> Option<u64> {
    find_tres_value(values, "gres/gpu")
        .or_else(|| find_tres_value(values, "gpu"))
        .and_then(|value| parse_u64(Some(&value)))
}

fn tres_memory_bytes(values: &BTreeMap<String, String>) -> Option<u64> {
    values
        .get("mem")
        .or_else(|| values.get("memory"))
        .and_then(|value| parse_memory_bytes(value))
}

fn parse_slurm_duration_seconds(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(seconds);
    }
    let without_fraction = trimmed.split('.').next().unwrap_or(trimmed);
    let has_days = without_fraction.contains('-');
    let (days, time) = match without_fraction.split_once('-') {
        Some((days, time)) => (days.parse::<u64>().ok()?, time),
        None => (0, without_fraction),
    };
    let parts = time
        .split(':')
        .map(|part| part.parse::<u64>().ok())
        .collect::<Option<Vec<_>>>()?;
    // Mirror the spec walltime range rules so utilization math agrees with the
    // validator: minutes and seconds are 0-59, and the hours field is bounded to
    // 0-23 whenever a day prefix carries the unbounded magnitude.
    let out_of_range = match (parts.as_slice(), has_days) {
        ([_minutes, seconds], false) => *seconds > 59,
        ([hours, minutes], true) => *hours > 23 || *minutes > 59,
        ([_hours, minutes, seconds], false) => *minutes > 59 || *seconds > 59,
        ([hours, minutes, seconds], true) => *hours > 23 || *minutes > 59 || *seconds > 59,
        _ => false,
    };
    if out_of_range {
        return None;
    }
    let seconds = match parts.as_slice() {
        [minutes, seconds] => minutes.saturating_mul(60).saturating_add(*seconds),
        [hours, minutes, seconds] => hours
            .saturating_mul(3_600)
            .saturating_add(minutes.saturating_mul(60))
            .saturating_add(*seconds),
        _ => return None,
    };
    Some(days.saturating_mul(86_400).saturating_add(seconds))
}

fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw?.trim().parse::<f64>().ok()
}

fn parse_u64(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse::<u64>().ok()
}

fn parse_f64_from_slurm_value(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    let number = trimmed
        .trim_end_matches('%')
        .trim_end_matches('M')
        .trim_end_matches('G')
        .trim();
    number.parse::<f64>().ok()
}

fn add_optional_f64(target: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or(0.0) + value);
    }
}

fn kwh(power_w: f64, seconds: f64, pue: f64) -> f64 {
    power_w * seconds * pue / 3_600_000.0
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round3(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn format_bytes_gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

fn format_duration_seconds(seconds: u64) -> String {
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_duration_parser_applies_walltime_range_rules() {
        assert_eq!(parse_slurm_duration_seconds("90"), Some(90));
        assert_eq!(parse_slurm_duration_seconds("01:30"), Some(90));
        assert_eq!(parse_slurm_duration_seconds("1:02:03"), Some(3723));
        // day-HH:MM form.
        assert_eq!(parse_slurm_duration_seconds("1-00:00"), Some(86_400));
        // Fractional seconds are stripped.
        assert_eq!(parse_slurm_duration_seconds("00:01.500"), Some(1));
        assert_eq!(parse_slurm_duration_seconds(""), None);
        assert_eq!(parse_slurm_duration_seconds("UNKNOWN"), None);
        // Range rules reject out-of-range minutes/seconds and hours-with-day,
        // unlike the accounting parser (see job::accounting::tests).
        assert_eq!(parse_slurm_duration_seconds("00:90"), None);
        assert_eq!(parse_slurm_duration_seconds("01:60:00"), None);
        assert_eq!(parse_slurm_duration_seconds("1-24:00"), None);
    }

    #[test]
    fn score_grades_follow_thresholds() {
        assert_eq!(grade_for_score(100), "A");
        assert_eq!(grade_for_score(85), "A");
        assert_eq!(grade_for_score(84), "B");
        assert_eq!(grade_for_score(70), "B");
        assert_eq!(grade_for_score(69), "C");
        assert_eq!(grade_for_score(55), "C");
        assert_eq!(grade_for_score(54), "D");
        assert_eq!(grade_for_score(40), "D");
        assert_eq!(grade_for_score(39), "F");
    }

    #[test]
    fn weighted_score_renormalizes_missing_components() {
        let components = vec![
            EfficiencyScoreComponent {
                name: "gpu".into(),
                label: "GPU".into(),
                available: true,
                weight: 0.75,
                score: Some(80),
                utilization: Some(0.8),
                observed: None,
                requested: None,
                source: "test".into(),
                confidence: EfficiencyScoreConfidence::High,
                note: None,
            },
            unavailable_component("missing", "Missing", 0.25, "missing"),
        ];
        assert_eq!(weighted_score(&components), 80);
    }

    #[test]
    fn sampler_activity_counts_only_active_intervals() {
        let mut history = ScoreSamplerHistory {
            interval_seconds: Some(10),
            ..ScoreSamplerHistory::default()
        };
        history.gpu_samples.insert(
            "2026-04-10T10:00:00Z".into(),
            GpuSampleSummary {
                utilization_values: vec![90.0],
                active: true,
                ..GpuSampleSummary::default()
            },
        );
        history.gpu_samples.insert(
            "2026-04-10T10:00:10Z".into(),
            GpuSampleSummary {
                utilization_values: vec![0.0],
                active: false,
                ..GpuSampleSummary::default()
            },
        );
        history
            .slurm_active_timestamps
            .insert("2026-04-10T10:00:20Z".into());
        assert_eq!(history.active_seconds(), Some(20.0));
        assert_eq!(history.mean_gpu_utilization_percent(), Some(45.0));
    }

    #[test]
    fn energy_prefers_power_draw_then_power_limit_then_tdp() {
        let mut history = ScoreSamplerHistory {
            interval_seconds: Some(10),
            ..ScoreSamplerHistory::default()
        };
        history.gpu_samples.insert(
            "2026-04-10T10:00:00Z".into(),
            GpuSampleSummary {
                power_draw_w: Some(100.0),
                power_limit_w: Some(300.0),
                seen_devices: BTreeSet::from(["GPU-0".to_string()]),
                ..GpuSampleSummary::default()
            },
        );
        let options = EfficiencyScoreOptions {
            pue: 1.0,
            gpu_tdp_w: 300.0,
            cpu_watts_per_core: 0.0,
            ..EfficiencyScoreOptions::default()
        };
        let estimate = estimate_energy(
            &minimal_plan(),
            None,
            &history,
            Some(100),
            Some(10),
            &options,
        );
        assert_eq!(estimate.basis, "sampler_power_draw+pue");
        assert_eq!(estimate.actual_kwh.map(round2), Some(0.0));

        history
            .gpu_samples
            .get_mut("2026-04-10T10:00:00Z")
            .expect("sample")
            .power_draw_w = None;
        let estimate = estimate_energy(
            &minimal_plan(),
            None,
            &history,
            Some(100),
            Some(10),
            &options,
        );
        assert_eq!(estimate.basis, "sampler_power_limit+pue");

        history.gpu_samples.clear();
        let estimate = estimate_energy(
            &minimal_plan(),
            None,
            &history,
            Some(100),
            Some(10),
            &options,
        );
        assert_eq!(estimate.basis, "configured_tdp+pue");
    }

    #[test]
    fn score_invalid_options_fail_before_scheduler_queries() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = SubmissionRecord {
            schema_version: super::super::SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: super::super::SubmissionKind::Main,
            job_id: "12345".into(),
            submitted_at: 100,
            compose_file: tmpdir.path().join("compose.yaml"),
            submit_dir: tmpdir.path().to_path_buf(),
            script_path: tmpdir.path().join("job.sbatch"),
            cache_dir: tmpdir.path().join("cache"),
            runtime_root: None,
            batch_log: tmpdir.path().join("slurm-12345.out"),
            batch_log_managed: false,
            service_logs: BTreeMap::new(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
            provenance: None,
            tags: Vec::new(),
            notes: Vec::new(),
        };
        let cases = [
            (
                EfficiencyScoreOptions {
                    pue: 0.0,
                    scheduler: SchedulerOptions {
                        squeue_bin: "/definitely/not/squeue".into(),
                        sacct_bin: "/definitely/not/sacct".into(),
                    },
                    sstat_bin: "/definitely/not/sstat".into(),
                    ..EfficiencyScoreOptions::default()
                },
                "score --pue must be greater than 0",
            ),
            (
                EfficiencyScoreOptions {
                    gpu_tdp_w: -1.0,
                    scheduler: SchedulerOptions {
                        squeue_bin: "/definitely/not/squeue".into(),
                        sacct_bin: "/definitely/not/sacct".into(),
                    },
                    sstat_bin: "/definitely/not/sstat".into(),
                    ..EfficiencyScoreOptions::default()
                },
                "score --gpu-tdp-w must be non-negative",
            ),
            (
                EfficiencyScoreOptions {
                    cpu_watts_per_core: -1.0,
                    scheduler: SchedulerOptions {
                        squeue_bin: "/definitely/not/squeue".into(),
                        sacct_bin: "/definitely/not/sacct".into(),
                    },
                    sstat_bin: "/definitely/not/sstat".into(),
                    ..EfficiencyScoreOptions::default()
                },
                "score --cpu-watts-per-core must be non-negative",
            ),
        ];

        for (options, expected) in cases {
            let err = build_efficiency_score_report(&minimal_plan(), &record, &options)
                .expect_err("invalid score option should fail before probing scheduler");
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn score_rejects_local_record_before_scheduler_queries() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = SubmissionRecord {
            schema_version: super::super::SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Local,
            kind: super::super::SubmissionKind::Main,
            job_id: "local-123".into(),
            submitted_at: 100,
            compose_file: tmpdir.path().join("compose.yaml"),
            submit_dir: tmpdir.path().to_path_buf(),
            script_path: tmpdir.path().join("job.sh"),
            cache_dir: tmpdir.path().join("cache"),
            runtime_root: None,
            batch_log: tmpdir.path().join("local.out"),
            batch_log_managed: false,
            service_logs: BTreeMap::new(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
            provenance: None,
            tags: Vec::new(),
            notes: Vec::new(),
        };
        let err = build_efficiency_score_report(
            &minimal_plan(),
            &record,
            &EfficiencyScoreOptions {
                scheduler: SchedulerOptions {
                    squeue_bin: "/definitely/not/squeue".into(),
                    sacct_bin: "/definitely/not/sacct".into(),
                },
                sstat_bin: "/definitely/not/sstat".into(),
                ..EfficiencyScoreOptions::default()
            },
        )
        .expect_err("local score should fail before scheduler probing");

        assert_eq!(
            err.to_string(),
            "score requires a tracked Slurm submission; local runs are not supported"
        );
    }

    #[test]
    fn score_uses_sstat_gpu_utilization_when_sampler_gpu_history_absent() {
        let component = gpu_utilization_component(
            &ScoreSamplerHistory::default(),
            &[StepStats {
                step_id: "12345.0".into(),
                ntasks: "1".into(),
                ave_cpu: String::new(),
                ave_rss: String::new(),
                max_rss: String::new(),
                alloc_tres: "cpu=1,gres/gpu=1".into(),
                tres_usage_in_ave: "gres/gpuutil=80".into(),
                alloc_tres_map: BTreeMap::from([
                    ("cpu".into(), "1".into()),
                    ("gres/gpu".into(), "1".into()),
                ]),
                usage_tres_in_ave_map: BTreeMap::from([("gres/gpuutil".into(), "80".into())]),
                gpu_count: Some("1".into()),
                gpu_util: Some("80".into()),
                gpu_mem: None,
            }],
        );

        assert!(component.available);
        assert_eq!(component.name, "gpu_utilization");
        assert_eq!(component.source, "sstat");
        assert_eq!(component.confidence, EfficiencyScoreConfidence::Medium);
        assert_eq!(component.observed.as_deref(), Some("80.0%"));
        assert!(
            component
                .note
                .as_deref()
                .is_some_and(|note| note.contains("Slurm TRES accounting"))
        );
    }

    fn minimal_plan() -> RuntimePlan {
        RuntimePlan {
            name: "score-test".into(),
            cache_dir: std::env::temp_dir(),
            runtime: Default::default(),
            slurm: Default::default(),
            ordered_services: Vec::new(),
        }
    }
}
