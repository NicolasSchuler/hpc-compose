//! Advisory live cluster weather probes.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{Result, bail};
use serde::Serialize;

use crate::cluster::{ClusterProfile, discover_cluster_profile_path, load_cluster_profile};
use crate::context::ResolvedBinaries;
use crate::time_util::unix_timestamp_now;

#[derive(Debug, Clone)]
pub struct WeatherOptions<'a> {
    pub binaries: &'a ResolvedBinaries,
    pub cwd: &'a Path,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WeatherReport {
    pub timestamp_unix: u64,
    pub cluster: Option<String>,
    pub condition: WeatherCondition,
    pub nodes: Option<NodeSummary>,
    pub queue: Option<QueueSummary>,
    pub user: UserJobSummary,
    pub fairshare: Option<FairshareSummary>,
    pub priority: Option<PrioritySummary>,
    pub maintenance: Vec<MaintenanceNote>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WeatherCondition {
    Clear,
    PartlyBusy,
    Busy,
    Stormy,
    Unknown,
}

impl WeatherCondition {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Clear => "Clear",
            Self::PartlyBusy => "Partly Busy",
            Self::Busy => "Busy",
            Self::Stormy => "Stormy",
            Self::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct NodeSummary {
    pub total_nodes: u32,
    pub free_nodes: u32,
    pub unavailable_nodes: u32,
    pub cpu: NodeClassSummary,
    pub gpu: GpuSummary,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct NodeClassSummary {
    pub total_nodes: u32,
    pub free_nodes: u32,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct GpuSummary {
    pub total_nodes: u32,
    pub free_nodes: u32,
    pub total_devices: u32,
    pub free_devices: u32,
    pub models: Vec<GpuModelSummary>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct GpuModelSummary {
    pub model: String,
    pub total_nodes: u32,
    pub free_nodes: u32,
    pub total_devices: u32,
    pub free_devices: u32,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct QueueSummary {
    pub total_jobs: u32,
    pub running_jobs: u32,
    pub pending_jobs: u32,
    pub other_jobs: u32,
    pub average_pending_wait_seconds: Option<u64>,
    pub start_sample_count: u32,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct UserJobSummary {
    pub user: Option<String>,
    pub total_jobs: u32,
    pub running_jobs: u32,
    pub pending_jobs: u32,
    pub other_jobs: u32,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FairshareSummary {
    pub account: Option<String>,
    pub user: Option<String>,
    pub fairshare: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PrioritySummary {
    pub pending_jobs: u32,
    pub top_job_id: Option<String>,
    pub highest_priority: Option<i64>,
    pub average_priority: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MaintenanceNote {
    pub partition: Option<String>,
    pub state: String,
    pub nodes: u32,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartEstimate {
    pub average_wait_seconds: u64,
    pub sample_count: u32,
}

#[derive(Debug, Clone, Default)]
struct ParsedSinfo {
    nodes: NodeSummary,
    maintenance: Vec<MaintenanceNote>,
}

#[derive(Debug, Clone, Default)]
struct GpuModelAccumulator {
    total_nodes: u32,
    free_nodes: u32,
    total_devices: u32,
    free_devices: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GresGpu {
    model: Option<String>,
    count: u32,
}

pub fn collect_weather(options: &WeatherOptions<'_>) -> Result<WeatherReport> {
    let timestamp_unix = unix_timestamp_now();
    let mut warnings = Vec::new();
    let profile = load_discovered_cluster_profile(options.cwd, &mut warnings);
    let cluster = profile
        .as_ref()
        .and_then(cluster_label_from_profile)
        .or_else(cluster_label_from_env);
    let user = current_user();

    let sinfo = match run_capture(
        &options.binaries.sinfo.value,
        &["-h", "-o", "%P|%T|%D|%G|%E"],
    ) {
        Ok(raw) => Some(parse_sinfo(&raw)),
        Err(warning) => {
            warnings.push(warning);
            None
        }
    };

    let mut queue = match run_capture(&options.binaries.squeue.value, &["-h", "-o", "%T|%u"]) {
        Ok(raw) => Some(parse_squeue(&raw, user.as_deref())),
        Err(warning) => {
            warnings.push(warning);
            None
        }
    };

    if let Some(queue) = queue.as_mut() {
        match run_capture(
            &options.binaries.squeue.value,
            &["--start", "-h", "-o", "%S"],
        ) {
            Ok(raw) => {
                if let Some(estimate) = parse_squeue_start(&raw, timestamp_unix) {
                    queue.queue.average_pending_wait_seconds = Some(estimate.average_wait_seconds);
                    queue.queue.start_sample_count = estimate.sample_count;
                }
            }
            Err(warning) => warnings.push(warning),
        }
    }

    if sinfo.is_none() && queue.is_none() {
        bail!(
            "weather probes failed: {}",
            if warnings.is_empty() {
                "no live Slurm data was available".to_string()
            } else {
                warnings.join("; ")
            }
        );
    }

    let fairshare = user.as_deref().and_then(|user| {
        match run_capture(
            &options.binaries.sshare.value,
            &["-n", "-P", "-u", user, "-o", "Account,User,FairShare"],
        ) {
            Ok(raw) => parse_sshare(&raw, Some(user)),
            Err(warning) => {
                warnings.push(warning);
                None
            }
        }
    });

    let priority = user.as_deref().and_then(|user| {
        match run_capture(
            &options.binaries.sprio.value,
            &["-h", "-u", user, "-o", "%.18i|%Y|%F|%P|%Q"],
        ) {
            Ok(raw) => parse_sprio(&raw),
            Err(warning) => {
                warnings.push(warning);
                None
            }
        }
    });

    let nodes = sinfo.as_ref().map(|parsed| parsed.nodes.clone());
    let condition = condition_for_nodes(nodes.as_ref());
    let maintenance = sinfo
        .as_ref()
        .map(|parsed| parsed.maintenance.clone())
        .unwrap_or_default();
    let user_summary = queue
        .as_ref()
        .map(|parsed| parsed.user.clone())
        .unwrap_or_else(|| UserJobSummary {
            user,
            ..UserJobSummary::default()
        });
    let queue_summary = queue.map(|parsed| parsed.queue);

    Ok(WeatherReport {
        timestamp_unix,
        cluster,
        condition,
        nodes,
        queue: queue_summary,
        user: user_summary,
        fairshare,
        priority,
        maintenance,
        warnings,
    })
}

/// Public-ish convenience wrapper around [`parse_sinfo`] returning the parsed
/// nodes and maintenance notes as a tuple. Currently exercised only by tests
/// (the production weather flow calls [`parse_sinfo`] directly), but kept
/// available for future internal callers and external integration tests.
#[allow(dead_code)]
pub fn parse_sinfo_nodes(raw: &str) -> (NodeSummary, Vec<MaintenanceNote>) {
    let parsed = parse_sinfo(raw);
    (parsed.nodes, parsed.maintenance)
}

fn parse_sinfo(raw: &str) -> ParsedSinfo {
    let mut parsed = ParsedSinfo::default();
    let mut models: BTreeMap<String, GpuModelAccumulator> = BTreeMap::new();

    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 4 {
            continue;
        }
        let partition = clean_partition_name(fields[0]);
        let state = fields[1].to_string();
        let Some(nodes) = parse_u32(fields[2]) else {
            continue;
        };
        let gres_gpus = parse_gres_gpus(fields[3]);
        let free = state_is_free(fields[1]);
        let unavailable = state_is_unavailable(fields[1]);

        parsed.nodes.total_nodes = parsed.nodes.total_nodes.saturating_add(nodes);
        if free {
            parsed.nodes.free_nodes = parsed.nodes.free_nodes.saturating_add(nodes);
        }
        if unavailable {
            parsed.nodes.unavailable_nodes = parsed.nodes.unavailable_nodes.saturating_add(nodes);
            parsed.maintenance.push(MaintenanceNote {
                partition,
                state: state.clone(),
                nodes,
                reason: fields
                    .get(4)
                    .and_then(|value| non_empty_reason(value))
                    .map(str::to_string),
            });
        }

        if gres_gpus.is_empty() {
            parsed.nodes.cpu.total_nodes = parsed.nodes.cpu.total_nodes.saturating_add(nodes);
            if free {
                parsed.nodes.cpu.free_nodes = parsed.nodes.cpu.free_nodes.saturating_add(nodes);
            }
            continue;
        }

        parsed.nodes.gpu.total_nodes = parsed.nodes.gpu.total_nodes.saturating_add(nodes);
        if free {
            parsed.nodes.gpu.free_nodes = parsed.nodes.gpu.free_nodes.saturating_add(nodes);
        }
        for gpu in gres_gpus {
            let model = gpu.model.unwrap_or_else(|| "unknown".to_string());
            let devices = nodes.saturating_mul(gpu.count);
            parsed.nodes.gpu.total_devices = parsed.nodes.gpu.total_devices.saturating_add(devices);
            if free {
                parsed.nodes.gpu.free_devices =
                    parsed.nodes.gpu.free_devices.saturating_add(devices);
            }
            let entry = models.entry(model).or_default();
            entry.total_nodes = entry.total_nodes.saturating_add(nodes);
            entry.total_devices = entry.total_devices.saturating_add(devices);
            if free {
                entry.free_nodes = entry.free_nodes.saturating_add(nodes);
                entry.free_devices = entry.free_devices.saturating_add(devices);
            }
        }
    }

    parsed.nodes.gpu.models = models
        .into_iter()
        .map(|(model, counts)| GpuModelSummary {
            model,
            total_nodes: counts.total_nodes,
            free_nodes: counts.free_nodes,
            total_devices: counts.total_devices,
            free_devices: counts.free_devices,
        })
        .collect();
    parsed
}

#[derive(Debug, Clone, Default)]
struct ParsedQueue {
    queue: QueueSummary,
    user: UserJobSummary,
}

fn parse_squeue(raw: &str, user: Option<&str>) -> ParsedQueue {
    let mut parsed = ParsedQueue {
        user: UserJobSummary {
            user: user.map(str::to_string),
            ..UserJobSummary::default()
        },
        ..ParsedQueue::default()
    };
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 2 {
            continue;
        }
        let state = fields[0];
        let owner = fields[1];
        count_job_state(
            state,
            &mut parsed.queue.running_jobs,
            &mut parsed.queue.pending_jobs,
            &mut parsed.queue.other_jobs,
        );
        parsed.queue.total_jobs = parsed.queue.total_jobs.saturating_add(1);
        if user.is_some_and(|user| user == owner) {
            count_job_state(
                state,
                &mut parsed.user.running_jobs,
                &mut parsed.user.pending_jobs,
                &mut parsed.user.other_jobs,
            );
            parsed.user.total_jobs = parsed.user.total_jobs.saturating_add(1);
        }
    }
    parsed
}

pub fn parse_squeue_start(raw: &str, now_unix: u64) -> Option<StartEstimate> {
    let waits = raw
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter_map(parse_slurm_datetime)
        .filter_map(|start| start.checked_sub(now_unix))
        .collect::<Vec<_>>();
    if waits.is_empty() {
        return None;
    }
    let total = waits.iter().copied().sum::<u64>();
    Some(StartEstimate {
        average_wait_seconds: total / waits.len() as u64,
        sample_count: waits.len() as u32,
    })
}

pub fn parse_sshare(raw: &str, user: Option<&str>) -> Option<FairshareSummary> {
    let mut first = None;
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 3 {
            continue;
        }
        let summary = FairshareSummary {
            account: non_empty(fields[0]).map(str::to_string),
            user: non_empty(fields[1]).map(str::to_string),
            fairshare: parse_f64(fields[2]),
        };
        if user.is_some_and(|user| summary.user.as_deref() == Some(user)) {
            return Some(summary);
        }
        first.get_or_insert(summary);
    }
    first
}

pub fn parse_sprio(raw: &str) -> Option<PrioritySummary> {
    let mut pending_jobs = 0_u32;
    let mut top_job_id = None;
    let mut highest_priority = None;
    let mut priority_total = 0_i128;

    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields.len() < 2 {
            continue;
        }
        let Some(priority) = parse_i64(fields[1]) else {
            continue;
        };
        pending_jobs = pending_jobs.saturating_add(1);
        priority_total += i128::from(priority);
        if highest_priority.is_none_or(|current| priority > current) {
            highest_priority = Some(priority);
            top_job_id = non_empty(fields[0]).map(str::to_string);
        }
    }

    if pending_jobs == 0 {
        return None;
    }
    Some(PrioritySummary {
        pending_jobs,
        top_job_id,
        highest_priority,
        average_priority: Some(priority_total as f64 / f64::from(pending_jobs)),
    })
}

#[must_use]
pub fn condition_for_nodes(nodes: Option<&NodeSummary>) -> WeatherCondition {
    let Some(nodes) = nodes else {
        return WeatherCondition::Unknown;
    };
    if nodes.total_nodes == 0 {
        return WeatherCondition::Unknown;
    }
    let free_ratio = f64::from(nodes.free_nodes) / f64::from(nodes.total_nodes);
    let unavailable_ratio = f64::from(nodes.unavailable_nodes) / f64::from(nodes.total_nodes);
    if free_ratio < 0.10 || unavailable_ratio >= 0.25 {
        WeatherCondition::Stormy
    } else if free_ratio < 0.30 {
        WeatherCondition::Busy
    } else if free_ratio < 0.60 {
        WeatherCondition::PartlyBusy
    } else {
        WeatherCondition::Clear
    }
}

fn run_capture(bin: &str, args: &[&str]) -> std::result::Result<String, String> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .map_err(|err| format!("failed to run {} {}: {err}", bin, args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{} {} exited with status {}{}",
            bin,
            args.join(" "),
            output.status,
            non_empty(&stderr)
                .map(|detail| format!(": {detail}"))
                .unwrap_or_default()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn load_discovered_cluster_profile(
    cwd: &Path,
    warnings: &mut Vec<String>,
) -> Option<ClusterProfile> {
    let path = discover_cluster_profile_path(cwd)?;
    match load_cluster_profile(&path) {
        Ok(profile) => Some(profile),
        Err(err) => {
            warnings.push(format!(
                "failed to load cluster profile {}: {err}",
                path.display()
            ));
            None
        }
    }
}

fn cluster_label_from_profile(profile: &ClusterProfile) -> Option<String> {
    profile
        .site
        .name
        .as_ref()
        .and_then(|name| non_empty(name))
        .map(str::to_string)
}

fn cluster_label_from_env() -> Option<String> {
    ["SLURM_CLUSTER_NAME", "SLURM_CLUSTER"]
        .into_iter()
        .find_map(|key| env::var(key).ok().and_then(non_empty_owned))
}

fn current_user() -> Option<String> {
    ["USER", "LOGNAME"]
        .into_iter()
        .find_map(|key| env::var(key).ok().and_then(non_empty_owned))
}

fn clean_partition_name(value: &str) -> Option<String> {
    non_empty(value.trim_end_matches('*')).map(str::to_string)
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn non_empty_owned(value: String) -> Option<String> {
    non_empty(&value).map(str::to_string)
}

fn non_empty_reason(value: &str) -> Option<&str> {
    let value = non_empty(value)?;
    (!matches!(value, "none" | "None" | "NONE" | "(null)" | "N/A")).then_some(value)
}

fn parse_u32(value: &str) -> Option<u32> {
    value.trim().parse::<u32>().ok()
}

fn parse_i64(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn parse_f64(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("infinity") {
        return None;
    }
    value.parse::<f64>().ok()
}

fn count_job_state(state: &str, running: &mut u32, pending: &mut u32, other: &mut u32) {
    let state = state.to_ascii_lowercase();
    if matches!(state.as_str(), "running" | "r") {
        *running = running.saturating_add(1);
    } else if matches!(state.as_str(), "pending" | "pd") {
        *pending = pending.saturating_add(1);
    } else {
        *other = other.saturating_add(1);
    }
}

fn state_is_free(state: &str) -> bool {
    let state = state.to_ascii_lowercase();
    state.contains("idle") && !state_is_unavailable(&state) && !state.contains("reserved")
}

fn state_is_unavailable(state: &str) -> bool {
    let state = state.to_ascii_lowercase();
    [
        "down",
        "drain",
        "maint",
        "fail",
        "no_respond",
        "not_responding",
        "unknown",
    ]
    .iter()
    .any(|needle| state.contains(needle))
}

fn parse_gres_gpus(gres: &str) -> Vec<GresGpu> {
    let normalized = gres.trim();
    if normalized.is_empty() || matches!(normalized, "N/A" | "(null)" | "none") {
        return Vec::new();
    }
    normalized
        .split(',')
        .filter_map(|part| parse_gres_gpu_part(part.trim()))
        .collect()
}

fn parse_gres_gpu_part(part: &str) -> Option<GresGpu> {
    let without_details = part.split_once('(').map_or(part, |(head, _)| head);
    let tokens = without_details
        .split(':')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let gpu_index = tokens
        .iter()
        .position(|token| *token == "gpu" || token.ends_with("/gpu") || token.ends_with("gpu"))?;
    let rest = &tokens[gpu_index + 1..];
    let count = rest
        .iter()
        .rev()
        .find_map(|token| parse_u32(token))
        .unwrap_or(1);
    let model = rest
        .iter()
        .find(|token| parse_u32(token).is_none())
        .map(|token| token.to_ascii_lowercase());
    Some(GresGpu { model, count })
}

fn parse_slurm_datetime(value: &str) -> Option<u64> {
    let value = value.trim();
    if value.is_empty()
        || value.eq_ignore_ascii_case("n/a")
        || value.eq_ignore_ascii_case("unknown")
    {
        return None;
    }
    let value = value.trim_end_matches('Z');
    let (date, time) = value
        .split_once('T')
        .or_else(|| value.split_once(' '))
        .unwrap_or((value, "00:00:00"));
    let date_parts = date.split('-').collect::<Vec<_>>();
    if date_parts.len() != 3 {
        return None;
    }
    let time_parts = time.split(':').collect::<Vec<_>>();
    if time_parts.len() < 2 {
        return None;
    }
    let year = date_parts[0].parse::<i32>().ok()?;
    let month = date_parts[1].parse::<u32>().ok()?;
    let day = date_parts[2].parse::<u32>().ok()?;
    let hour = time_parts[0].parse::<u32>().ok()?;
    let minute = time_parts[1].parse::<u32>().ok()?;
    let second = time_parts
        .get(2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let days = days_from_civil(year, month, day)?;
    let seconds = days
        .saturating_mul(86_400)
        .saturating_add(i64::from(hour) * 3_600)
        .saturating_add(i64::from(minute) * 60)
        .saturating_add(i64::from(second));
    u64::try_from(seconds).ok()
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    let month = i32::try_from(month).ok()?;
    let day = i32::try_from(day).ok()?;
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month_adjusted = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month_adjusted + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(i64::from(era) * 146_097 + i64::from(doe) - 719_468)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sinfo_parser_counts_grouped_cpu_gpu_and_unavailable_rows() {
        let raw = "\
gpu*|idle|2|gpu:a100:4|none
gpu|allocated|3|gpu:a100:4|
gpu|down|1|gpu:h100:8|maintenance Tue
cpu|idle|5|N/A|
cpu|drain|2|(null)|hardware
";
        let (nodes, maintenance) = parse_sinfo_nodes(raw);
        assert_eq!(nodes.total_nodes, 13);
        assert_eq!(nodes.free_nodes, 7);
        assert_eq!(nodes.unavailable_nodes, 3);
        assert_eq!(nodes.gpu.total_nodes, 6);
        assert_eq!(nodes.gpu.free_nodes, 2);
        assert_eq!(nodes.gpu.total_devices, 28);
        assert_eq!(nodes.gpu.free_devices, 8);
        assert_eq!(nodes.cpu.total_nodes, 7);
        assert_eq!(nodes.cpu.free_nodes, 5);
        assert_eq!(nodes.gpu.models[0].model, "a100");
        assert_eq!(nodes.gpu.models[0].free_nodes, 2);
        assert_eq!(nodes.gpu.models[1].model, "h100");
        assert_eq!(maintenance.len(), 2);
        assert_eq!(maintenance[0].reason.as_deref(), Some("maintenance Tue"));
    }

    #[test]
    fn squeue_parser_counts_global_and_user_jobs() {
        let parsed = parse_squeue(
            "\
RUNNING|alice
PENDING|alice
PD|bob
COMPLETING|alice
",
            Some("alice"),
        );
        assert_eq!(parsed.queue.total_jobs, 4);
        assert_eq!(parsed.queue.running_jobs, 1);
        assert_eq!(parsed.queue.pending_jobs, 2);
        assert_eq!(parsed.queue.other_jobs, 1);
        assert_eq!(parsed.user.total_jobs, 3);
        assert_eq!(parsed.user.pending_jobs, 1);
    }

    #[test]
    fn squeue_start_parser_averages_future_start_times() {
        let now = parse_slurm_datetime("2026-05-15T12:00:00").expect("now");
        let estimate = parse_squeue_start(
            "\
2026-05-15T12:10:00
N/A
2026-05-15T12:20:00
",
            now,
        )
        .expect("estimate");
        assert_eq!(estimate.sample_count, 2);
        assert_eq!(estimate.average_wait_seconds, 900);
    }

    #[test]
    fn sshare_and_sprio_parsers_extract_advisory_signals() {
        let fairshare =
            parse_sshare("project|alice|0.75\nother|bob|0.1\n", Some("alice")).expect("fairshare");
        assert_eq!(fairshare.account.as_deref(), Some("project"));
        assert_eq!(fairshare.fairshare, Some(0.75));

        let priority =
            parse_sprio("42|62000|51000|1000|0\n43|61000|50000|1000|0\n").expect("priority");
        assert_eq!(priority.pending_jobs, 2);
        assert_eq!(priority.top_job_id.as_deref(), Some("42"));
        assert_eq!(priority.highest_priority, Some(62000));
        assert_eq!(priority.average_priority, Some(61500.0));
    }

    #[test]
    fn condition_heuristic_uses_free_and_unavailable_ratios() {
        let clear = NodeSummary {
            total_nodes: 10,
            free_nodes: 7,
            ..NodeSummary::default()
        };
        assert_eq!(condition_for_nodes(Some(&clear)), WeatherCondition::Clear);
        let partly = NodeSummary {
            total_nodes: 10,
            free_nodes: 4,
            ..NodeSummary::default()
        };
        assert_eq!(
            condition_for_nodes(Some(&partly)),
            WeatherCondition::PartlyBusy
        );
        let stormy = NodeSummary {
            total_nodes: 10,
            free_nodes: 5,
            unavailable_nodes: 3,
            ..NodeSummary::default()
        };
        assert_eq!(condition_for_nodes(Some(&stormy)), WeatherCondition::Stormy);
    }
}
