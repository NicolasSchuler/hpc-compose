use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::spec::{
    ComposeSpec, EffectiveComposeConfig, EffectiveWatchdogConfig, EffectiveWatchdogResourceConfig,
    WatchdogAction, parse_memory_bytes,
};

use super::model::SubmissionRecord;
use super::read_json;
use super::scheduler::{JobState, SchedulerStatus, parse_scheduler_timestamp};
use super::stats::{
    GpuDeviceSampleRow, SamplerMetaFile, SlurmSampleRow, StepStats, find_tres_value,
    metrics_dir_for_record, step_from_slurm_sample_row,
};

/// Status of the advisory idle-resource watchdog.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogStatus {
    Ok,
    Warning,
    Pending,
    Unavailable,
}

impl WatchdogStatus {
    /// Returns the stable lowercase status label used in human-readable output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Pending => "pending",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Resource classified by the idle-resource watchdog.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogResource {
    Gpu,
    Cpu,
}

impl WatchdogResource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Gpu => "gpu",
            Self::Cpu => "cpu",
        }
    }
}

/// Resource state inferred from sustained sampler history.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogClassification {
    Active,
    Idle,
    ResidentIdle,
    WrapperStuck,
    IoWaitLike,
    InsufficientWindow,
    NoSamples,
    MissingComputeSignal,
}

impl WatchdogClassification {
    /// Returns the stable lowercase classification label used in human-readable output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Idle => "idle",
            Self::ResidentIdle => "resident_idle",
            Self::WrapperStuck => "wrapper_stuck",
            Self::IoWaitLike => "io_wait_like",
            Self::InsufficientWindow => "insufficient_window",
            Self::NoSamples => "no_samples",
            Self::MissingComputeSignal => "missing_compute_signal",
        }
    }
}

/// One watchdog observation over a sustained history window.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct WatchdogObservation {
    pub resource: WatchdogResource,
    pub status: WatchdogStatus,
    pub classification: WatchdogClassification,
    pub window_seconds: u64,
    pub observed_seconds: u64,
    pub sample_count: usize,
    pub mean_compute_pct: Option<f64>,
    pub max_compute_pct: Option<f64>,
    pub memory_resident_pct: Option<f64>,
    pub memory_signal: Option<String>,
    pub message: String,
}

/// Advisory watchdog snapshot included in status/stats outputs.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct WatchdogSnapshot {
    pub enabled: bool,
    pub action: WatchdogAction,
    pub status: WatchdogStatus,
    pub message: String,
    pub grace_period_seconds: u64,
    pub observations: Vec<WatchdogObservation>,
}

impl WatchdogSnapshot {
    /// Returns the first warning message, if the watchdog is warning.
    #[must_use]
    pub fn warning_message(&self) -> Option<&str> {
        (self.status == WatchdogStatus::Warning).then_some(self.message.as_str())
    }
}

#[derive(Debug, Default)]
pub(crate) struct WatchdogBuildOutcome {
    pub(crate) snapshot: Option<WatchdogSnapshot>,
    pub(crate) notes: Vec<String>,
}

#[derive(Debug, Default)]
struct WatchdogHistory {
    interval_seconds: Option<u64>,
    gpu_samples: BTreeMap<String, ResourceWindowSample>,
    cpu_samples: BTreeMap<String, ResourceWindowSample>,
    notes: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct ResourceWindowSample {
    compute_values_pct: Vec<f64>,
    memory_resident_pct: Option<f64>,
    memory_signal: Option<&'static str>,
}

#[derive(Debug, Default)]
struct GpuAccumulator {
    compute_values_pct: Vec<f64>,
    memory_used_mib: u64,
    memory_total_mib: u64,
    saw_memory: bool,
}

#[derive(Debug, Default)]
struct CpuAccumulator {
    compute_values_pct: Vec<f64>,
}

/// Builds an advisory idle-resource watchdog snapshot for a tracked job.
pub(crate) fn build_watchdog_snapshot(
    spec_path: &Path,
    record: &SubmissionRecord,
    scheduler: &SchedulerStatus,
    started_at: Option<u64>,
    now: u64,
) -> WatchdogBuildOutcome {
    let mut notes = Vec::new();
    let Some(config) = watchdog_config_for_record(spec_path, record, &mut notes) else {
        return WatchdogBuildOutcome {
            snapshot: None,
            notes,
        };
    };
    if !config.enabled {
        return WatchdogBuildOutcome {
            snapshot: None,
            notes,
        };
    }

    let snapshot = classify_watchdog_for_record(record, scheduler, started_at, now, config);
    WatchdogBuildOutcome {
        snapshot: Some(snapshot),
        notes,
    }
}

fn watchdog_config_for_record(
    spec_path: &Path,
    record: &SubmissionRecord,
    notes: &mut Vec<String>,
) -> Option<EffectiveWatchdogConfig> {
    if let Some(snapshot_yaml) = record.config_snapshot_yaml.as_deref() {
        match serde_norway::from_str::<EffectiveComposeConfig>(snapshot_yaml) {
            Ok(config) => return config.slurm.watchdog,
            Err(err) => notes.push(format!(
                "watchdog config snapshot unavailable; falling back to current spec: {err}"
            )),
        }
    }

    match ComposeSpec::load(spec_path) {
        Ok(spec) => spec.slurm.effective_watchdog_config(),
        Err(err) => {
            notes.push(format!("watchdog config unavailable: {err}"));
            None
        }
    }
}

fn classify_watchdog_for_record(
    record: &SubmissionRecord,
    scheduler: &SchedulerStatus,
    started_at: Option<u64>,
    now: u64,
    config: EffectiveWatchdogConfig,
) -> WatchdogSnapshot {
    if JobState::parse(&scheduler.state) != JobState::Running {
        return WatchdogSnapshot {
            enabled: true,
            action: config.action,
            status: WatchdogStatus::Pending,
            message: format!(
                "watchdog waits for RUNNING jobs; scheduler state is {}",
                scheduler.state
            ),
            grace_period_seconds: config.grace_period_seconds,
            observations: Vec::new(),
        };
    }

    let started_at = started_at.unwrap_or(record.submitted_at);
    let age = now.saturating_sub(started_at);
    if age < config.grace_period_seconds {
        return WatchdogSnapshot {
            enabled: true,
            action: config.action,
            status: WatchdogStatus::Pending,
            message: format!(
                "watchdog grace period active ({age}/{}s)",
                config.grace_period_seconds
            ),
            grace_period_seconds: config.grace_period_seconds,
            observations: Vec::new(),
        };
    }

    let metrics_dir = metrics_dir_for_record(record);
    let history = load_watchdog_history(&metrics_dir);
    let observations = vec![
        classify_resource_window(
            WatchdogResource::Gpu,
            &config.gpu,
            &history.gpu_samples,
            history.interval_seconds,
        ),
        classify_resource_window(
            WatchdogResource::Cpu,
            &config.cpu,
            &history.cpu_samples,
            history.interval_seconds,
        ),
    ];

    let status = overall_status(&observations, &history.notes);
    let message = overall_message(status, &observations, &history.notes);
    WatchdogSnapshot {
        enabled: true,
        action: config.action,
        status,
        message,
        grace_period_seconds: config.grace_period_seconds,
        observations,
    }
}

fn load_watchdog_history(metrics_dir: &Path) -> WatchdogHistory {
    let mut history = WatchdogHistory::default();
    if !metrics_dir.is_dir() {
        history.notes.push(format!(
            "metrics directory not found: {}",
            metrics_dir.display()
        ));
        return history;
    }

    let meta_path = metrics_dir.join("meta.json");
    match read_json::<SamplerMetaFile>(&meta_path) {
        Ok(meta) => history.interval_seconds = Some(meta.interval_seconds),
        Err(err) => history.notes.push(format!(
            "failed to parse metrics sampler metadata at {}: {err}",
            meta_path.display()
        )),
    }

    match load_gpu_sample_history(metrics_dir) {
        Ok(samples) => history.gpu_samples = samples,
        Err(err) => history
            .notes
            .push(format!("failed to parse GPU sampler history: {err}")),
    }
    match load_cpu_sample_history(metrics_dir) {
        Ok(samples) => history.cpu_samples = samples,
        Err(err) => history
            .notes
            .push(format!("failed to parse CPU sampler history: {err}")),
    }
    match load_slurm_memory_history(&metrics_dir.join("slurm.jsonl")) {
        Ok(samples) => merge_cpu_memory_samples(&mut history.cpu_samples, samples),
        Err(err) => history.notes.push(format!(
            "failed to parse Slurm memory sampler history: {err}"
        )),
    }
    history
}

fn load_gpu_sample_history(metrics_dir: &Path) -> Result<BTreeMap<String, ResourceWindowSample>> {
    let path = metrics_dir.join("gpu.jsonl");
    let mut samples: BTreeMap<String, GpuAccumulator> = BTreeMap::new();
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&path).context(format!("failed to read {}", path.display()))?;
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row = serde_json::from_str::<GpuDeviceSampleRow>(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        let sample = samples.entry(row.sampled_at).or_default();
        if let Some(value) = parse_f64(row.utilization_gpu.as_deref()) {
            sample.compute_values_pct.push(value);
        }
        if let (Some(used), Some(total)) = (
            parse_u64(row.memory_used_mib.as_deref()),
            parse_u64(row.memory_total_mib.as_deref()),
        ) {
            sample.saw_memory = true;
            sample.memory_used_mib = sample.memory_used_mib.saturating_add(used);
            sample.memory_total_mib = sample.memory_total_mib.saturating_add(total);
        }
    }

    Ok(samples
        .into_iter()
        .map(|(timestamp, sample)| {
            let memory_resident_pct = (sample.saw_memory && sample.memory_total_mib > 0)
                .then(|| sample.memory_used_mib as f64 * 100.0 / sample.memory_total_mib as f64);
            (
                timestamp,
                ResourceWindowSample {
                    compute_values_pct: sample.compute_values_pct,
                    memory_resident_pct,
                    memory_signal: memory_resident_pct.map(|_| "gpu_memory_used_total"),
                },
            )
        })
        .collect())
}

#[derive(Debug, Deserialize)]
struct CpuSampleHistoryRow {
    sampled_at: String,
    #[serde(default)]
    cpu_util_pct: Option<f64>,
}

fn load_cpu_sample_history(metrics_dir: &Path) -> Result<BTreeMap<String, ResourceWindowSample>> {
    let path = metrics_dir.join("cpu.jsonl");
    let mut samples: BTreeMap<String, CpuAccumulator> = BTreeMap::new();
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(&path).context(format!("failed to read {}", path.display()))?;
    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row = serde_json::from_str::<CpuSampleHistoryRow>(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        if let Some(value) = row.cpu_util_pct {
            samples
                .entry(row.sampled_at)
                .or_default()
                .compute_values_pct
                .push(value);
        } else {
            samples.entry(row.sampled_at).or_default();
        }
    }

    Ok(samples
        .into_iter()
        .map(|(timestamp, sample)| {
            (
                timestamp,
                ResourceWindowSample {
                    compute_values_pct: sample.compute_values_pct,
                    memory_resident_pct: None,
                    memory_signal: None,
                },
            )
        })
        .collect())
}

fn load_slurm_memory_history(path: &Path) -> Result<BTreeMap<String, f64>> {
    let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
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
        if let Some(percent) = step_memory_resident_pct(&step) {
            samples.entry(sampled_at).or_default().push(percent);
        }
    }

    Ok(samples
        .into_iter()
        .map(|(timestamp, values)| {
            let max = values.into_iter().fold(0.0_f64, f64::max);
            (timestamp, max)
        })
        .collect())
}

fn merge_cpu_memory_samples(
    cpu_samples: &mut BTreeMap<String, ResourceWindowSample>,
    memory_samples: BTreeMap<String, f64>,
) {
    for (timestamp, memory_resident_pct) in memory_samples {
        let sample = cpu_samples.entry(timestamp).or_default();
        sample.memory_resident_pct = Some(memory_resident_pct);
        sample.memory_signal = Some("slurm_rss_alloc_tres");
    }
}

fn step_memory_resident_pct(step: &StepStats) -> Option<f64> {
    let observed = estimated_step_memory_bytes(step)?;
    let allocated = find_tres_value(&step.alloc_tres_map, "mem")
        .or_else(|| find_tres_value(&step.alloc_tres_map, "memory"))
        .and_then(|value| parse_memory_bytes(&value))?;
    (allocated > 0).then(|| observed as f64 * 100.0 / allocated as f64)
}

fn estimated_step_memory_bytes(step: &StepStats) -> Option<u64> {
    let max_rss = parse_memory_bytes(&step.max_rss);
    let ave_rss_total = parse_memory_bytes(&step.ave_rss)
        .map(|value| value.saturating_mul(step.ntasks.trim().parse::<u64>().unwrap_or(1).max(1)));
    max_option(max_rss, ave_rss_total)
}

fn max_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn classify_resource_window(
    resource: WatchdogResource,
    policy: &EffectiveWatchdogResourceConfig,
    samples: &BTreeMap<String, ResourceWindowSample>,
    interval_seconds: Option<u64>,
) -> WatchdogObservation {
    if samples.is_empty() {
        return WatchdogObservation {
            resource,
            status: WatchdogStatus::Pending,
            classification: WatchdogClassification::NoSamples,
            window_seconds: policy.window_seconds,
            observed_seconds: 0,
            sample_count: 0,
            mean_compute_pct: None,
            max_compute_pct: None,
            memory_resident_pct: None,
            memory_signal: None,
            message: format!("{} watchdog has no sampler samples yet", resource.as_str()),
        };
    }

    let selected = recent_window_samples(samples, policy.window_seconds, interval_seconds);
    if selected.observed_seconds < policy.window_seconds {
        return WatchdogObservation {
            resource,
            status: WatchdogStatus::Pending,
            classification: WatchdogClassification::InsufficientWindow,
            window_seconds: policy.window_seconds,
            observed_seconds: selected.observed_seconds,
            sample_count: selected.samples.len(),
            mean_compute_pct: selected.mean_compute_pct,
            max_compute_pct: selected.max_compute_pct,
            memory_resident_pct: selected.mean_memory_resident_pct,
            memory_signal: selected.memory_signal.map(str::to_string),
            message: format!(
                "{} watchdog collecting history ({}/{}s)",
                resource.as_str(),
                selected.observed_seconds,
                policy.window_seconds
            ),
        };
    }

    let Some(max_compute) = selected.max_compute_pct else {
        return WatchdogObservation {
            resource,
            status: WatchdogStatus::Pending,
            classification: WatchdogClassification::MissingComputeSignal,
            window_seconds: policy.window_seconds,
            observed_seconds: selected.observed_seconds,
            sample_count: selected.samples.len(),
            mean_compute_pct: selected.mean_compute_pct,
            max_compute_pct: selected.max_compute_pct,
            memory_resident_pct: selected.mean_memory_resident_pct,
            memory_signal: selected.memory_signal.map(str::to_string),
            message: format!(
                "{} watchdog has no compute-utilization signal",
                resource.as_str()
            ),
        };
    };

    if max_compute >= policy.compute_below_pct_f64() {
        return WatchdogObservation {
            resource,
            status: WatchdogStatus::Ok,
            classification: WatchdogClassification::Active,
            window_seconds: policy.window_seconds,
            observed_seconds: selected.observed_seconds,
            sample_count: selected.samples.len(),
            mean_compute_pct: selected.mean_compute_pct,
            max_compute_pct: selected.max_compute_pct,
            memory_resident_pct: selected.mean_memory_resident_pct,
            memory_signal: selected.memory_signal.map(str::to_string),
            message: format!(
                "{} compute reached {:.1}% within the watchdog window",
                resource.as_str(),
                max_compute
            ),
        };
    }

    let (classification, message) = match (resource, selected.mean_memory_resident_pct) {
        (WatchdogResource::Gpu, Some(memory))
            if memory >= policy.memory_resident_above_pct_f64() =>
        {
            (
                WatchdogClassification::ResidentIdle,
                format!(
                    "low GPU compute (<{}%) with resident VRAM ({memory:.1}% >= {}%)",
                    policy.compute_below_pct, policy.memory_resident_above_pct
                ),
            )
        }
        (WatchdogResource::Gpu, Some(memory)) => (
            WatchdogClassification::Idle,
            format!(
                "low GPU compute (<{}%) and low resident VRAM ({memory:.1}% < {}%)",
                policy.compute_below_pct, policy.memory_resident_above_pct
            ),
        ),
        (WatchdogResource::Gpu, None) => (
            WatchdogClassification::Idle,
            format!(
                "low GPU compute (<{}%); VRAM residency signal is unavailable",
                policy.compute_below_pct
            ),
        ),
        (WatchdogResource::Cpu, Some(memory))
            if memory >= policy.memory_resident_above_pct_f64() =>
        {
            (
                WatchdogClassification::IoWaitLike,
                format!(
                    "low CPU compute (<{}%) with high RSS residency ({memory:.1}% >= {}%)",
                    policy.compute_below_pct, policy.memory_resident_above_pct
                ),
            )
        }
        (WatchdogResource::Cpu, Some(memory)) => (
            WatchdogClassification::WrapperStuck,
            format!(
                "low CPU compute (<{}%) and low RSS residency ({memory:.1}% < {}%)",
                policy.compute_below_pct, policy.memory_resident_above_pct
            ),
        ),
        (WatchdogResource::Cpu, None) => (
            WatchdogClassification::WrapperStuck,
            format!(
                "low CPU compute (<{}%); CPU memory residency signal is unavailable",
                policy.compute_below_pct
            ),
        ),
    };

    WatchdogObservation {
        resource,
        status: WatchdogStatus::Warning,
        classification,
        window_seconds: policy.window_seconds,
        observed_seconds: selected.observed_seconds,
        sample_count: selected.samples.len(),
        mean_compute_pct: selected.mean_compute_pct,
        max_compute_pct: selected.max_compute_pct,
        memory_resident_pct: selected.mean_memory_resident_pct,
        memory_signal: selected.memory_signal.map(str::to_string),
        message,
    }
}

struct RecentWindow<'a> {
    samples: Vec<(&'a str, &'a ResourceWindowSample, u64)>,
    observed_seconds: u64,
    mean_compute_pct: Option<f64>,
    max_compute_pct: Option<f64>,
    mean_memory_resident_pct: Option<f64>,
    memory_signal: Option<&'static str>,
}

fn recent_window_samples<'a>(
    samples: &'a BTreeMap<String, ResourceWindowSample>,
    window_seconds: u64,
    interval_seconds: Option<u64>,
) -> RecentWindow<'a> {
    let durations = duration_by_timestamp_for(samples.keys().cloned().collect(), interval_seconds);
    let mut selected = Vec::new();
    let mut observed_seconds = 0_u64;
    for (timestamp, sample) in samples.iter().rev() {
        let duration = durations.get(timestamp).copied().unwrap_or(1).max(1);
        selected.push((timestamp.as_str(), sample, duration));
        observed_seconds = observed_seconds.saturating_add(duration);
        if observed_seconds >= window_seconds {
            break;
        }
    }
    selected.reverse();

    let mut weighted_compute = 0.0;
    let mut compute_weight = 0_u64;
    let mut max_compute_pct = None;
    let mut weighted_memory = 0.0;
    let mut memory_weight = 0_u64;
    let mut memory_signal = None;
    for (_, sample, duration) in &selected {
        if !sample.compute_values_pct.is_empty() {
            let mean = sample.compute_values_pct.iter().sum::<f64>()
                / sample.compute_values_pct.len() as f64;
            weighted_compute += mean * *duration as f64;
            compute_weight = compute_weight.saturating_add(*duration);
            max_compute_pct = Some(max_compute_pct.map_or(mean, |value: f64| value.max(mean)));
        }
        if let Some(memory) = sample.memory_resident_pct {
            weighted_memory += memory * *duration as f64;
            memory_weight = memory_weight.saturating_add(*duration);
            memory_signal = sample.memory_signal.or(memory_signal);
        }
    }

    RecentWindow {
        samples: selected,
        observed_seconds,
        mean_compute_pct: (compute_weight > 0).then(|| weighted_compute / compute_weight as f64),
        max_compute_pct,
        mean_memory_resident_pct: (memory_weight > 0)
            .then(|| weighted_memory / memory_weight as f64),
        memory_signal,
    }
}

fn duration_by_timestamp_for(
    timestamps: BTreeSet<String>,
    interval_seconds: Option<u64>,
) -> BTreeMap<String, u64> {
    let parsed = timestamps
        .iter()
        .filter_map(|timestamp| parse_scheduler_timestamp(timestamp).map(|unix| (timestamp, unix)))
        .collect::<Vec<_>>();
    if parsed.is_empty() {
        return timestamps
            .into_iter()
            .map(|timestamp| (timestamp, interval_seconds.unwrap_or(1).max(1)))
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
        durations.insert((*timestamp).clone(), duration.max(1));
    }
    durations
}

fn overall_status(observations: &[WatchdogObservation], notes: &[String]) -> WatchdogStatus {
    if observations
        .iter()
        .any(|observation| observation.status == WatchdogStatus::Warning)
    {
        return WatchdogStatus::Warning;
    }
    if observations
        .iter()
        .any(|observation| observation.status == WatchdogStatus::Ok)
    {
        return WatchdogStatus::Ok;
    }
    if !notes.is_empty() {
        return WatchdogStatus::Unavailable;
    }
    WatchdogStatus::Pending
}

fn overall_message(
    status: WatchdogStatus,
    observations: &[WatchdogObservation],
    notes: &[String],
) -> String {
    match status {
        WatchdogStatus::Warning => observations
            .iter()
            .filter(|observation| observation.status == WatchdogStatus::Warning)
            .map(|observation| observation.message.as_str())
            .collect::<Vec<_>>()
            .join("; "),
        WatchdogStatus::Ok => "watchdog sees recent resource activity".to_string(),
        WatchdogStatus::Pending => observations
            .first()
            .map(|observation| observation.message.clone())
            .unwrap_or_else(|| "watchdog is collecting sampler history".to_string()),
        WatchdogStatus::Unavailable => notes
            .first()
            .cloned()
            .unwrap_or_else(|| "watchdog sampler history unavailable".to_string()),
    }
}

fn parse_f64(raw: Option<&str>) -> Option<f64> {
    raw?.trim().parse::<f64>().ok()
}

fn parse_u64(raw: Option<&str>) -> Option<u64> {
    raw?.trim().parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> EffectiveWatchdogResourceConfig {
        EffectiveWatchdogResourceConfig {
            window_seconds: 120,
            compute_below_pct: 2,
            memory_resident_above_pct: 20,
        }
    }

    fn sample(compute: f64, memory: Option<f64>) -> ResourceWindowSample {
        ResourceWindowSample {
            compute_values_pct: vec![compute],
            memory_resident_pct: memory,
            memory_signal: memory.map(|_| "test"),
        }
    }

    #[test]
    fn gpu_classifier_distinguishes_idle_and_resident_idle() {
        let samples = BTreeMap::from([
            ("2026-04-10T10:00:00Z".to_string(), sample(0.5, Some(5.0))),
            ("2026-04-10T10:01:00Z".to_string(), sample(0.4, Some(5.0))),
        ]);
        let idle = classify_resource_window(WatchdogResource::Gpu, &policy(), &samples, Some(60));
        assert_eq!(idle.status, WatchdogStatus::Warning);
        assert_eq!(idle.classification, WatchdogClassification::Idle);

        let resident = BTreeMap::from([
            ("2026-04-10T10:00:00Z".to_string(), sample(0.5, Some(60.0))),
            ("2026-04-10T10:01:00Z".to_string(), sample(0.4, Some(60.0))),
        ]);
        let resident =
            classify_resource_window(WatchdogResource::Gpu, &policy(), &resident, Some(60));
        assert_eq!(resident.status, WatchdogStatus::Warning);
        assert_eq!(
            resident.classification,
            WatchdogClassification::ResidentIdle
        );
    }

    #[test]
    fn gpu_classifier_uses_compute_not_memory_bandwidth() {
        let samples = BTreeMap::from([
            ("2026-04-10T10:00:00Z".to_string(), sample(0.0, Some(1.0))),
            ("2026-04-10T10:01:00Z".to_string(), sample(0.0, Some(1.0))),
        ]);
        let observation =
            classify_resource_window(WatchdogResource::Gpu, &policy(), &samples, Some(60));
        assert_eq!(observation.classification, WatchdogClassification::Idle);
        assert_eq!(observation.memory_resident_pct, Some(1.0));
    }

    #[test]
    fn cpu_classifier_distinguishes_wrapper_and_high_memory() {
        let low_memory = BTreeMap::from([
            ("2026-04-10T10:00:00Z".to_string(), sample(1.0, Some(3.0))),
            ("2026-04-10T10:01:00Z".to_string(), sample(1.0, Some(3.0))),
        ]);
        let wrapper =
            classify_resource_window(WatchdogResource::Cpu, &policy(), &low_memory, Some(60));
        assert_eq!(wrapper.classification, WatchdogClassification::WrapperStuck);

        let high_memory = BTreeMap::from([
            ("2026-04-10T10:00:00Z".to_string(), sample(1.0, Some(70.0))),
            ("2026-04-10T10:01:00Z".to_string(), sample(1.0, Some(70.0))),
        ]);
        let io_wait =
            classify_resource_window(WatchdogResource::Cpu, &policy(), &high_memory, Some(60));
        assert_eq!(io_wait.classification, WatchdogClassification::IoWaitLike);
    }

    #[test]
    fn classifier_waits_for_sustained_window() {
        let samples =
            BTreeMap::from([("2026-04-10T10:01:00Z".to_string(), sample(0.0, Some(1.0)))]);
        let observation =
            classify_resource_window(WatchdogResource::Gpu, &policy(), &samples, Some(60));
        assert_eq!(observation.status, WatchdogStatus::Pending);
        assert_eq!(
            observation.classification,
            WatchdogClassification::InsufficientWindow
        );
    }
}
