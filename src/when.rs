use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::spec::JobDependencyCondition;

const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 60;
const MIN_POLL_INTERVAL_SECONDS: u64 = 5;
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;

#[derive(Debug, Clone)]
pub(crate) struct WhenConditions {
    pub free_nodes: Option<FreeNodesCondition>,
    pub after_job: Option<AfterJobCondition>,
    pub time_window: Option<TimeWindow>,
}

#[derive(Debug, Clone)]
pub(crate) struct FreeNodesCondition {
    pub partition: String,
    pub minimum_idle_nodes: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct AfterJobCondition {
    pub job_id: String,
    pub condition: JobDependencyCondition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeWindow {
    start_minutes: u16,
    end_minutes: u16,
    label: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WhenConditionSummary {
    pub kind: &'static str,
    pub description: String,
    pub satisfied: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WhenTrigger {
    pub conditions: Vec<WhenConditionSummary>,
}

#[derive(Debug, Clone)]
pub(crate) struct MonitorOptions {
    pub conditions: WhenConditions,
    pub poll_interval: Duration,
    pub timeout: Option<Duration>,
    pub sinfo_bin: String,
    pub squeue_bin: String,
    pub sacct_bin: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ProbeOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub(crate) trait MonitorRuntime {
    fn monotonic_seconds(&self) -> u64;
    fn local_minutes_since_midnight(&self) -> u16;
    fn sleep(&mut self, duration: Duration);
    fn command_output(&mut self, bin: &str, args: &[String]) -> Result<ProbeOutput>;
}

pub(crate) struct RealMonitorRuntime {
    start: Instant,
}

impl RealMonitorRuntime {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl MonitorRuntime for RealMonitorRuntime {
    fn monotonic_seconds(&self) -> u64 {
        self.start.elapsed().as_secs()
    }

    fn local_minutes_since_midnight(&self) -> u16 {
        current_local_minutes_since_midnight()
    }

    fn sleep(&mut self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn command_output(&mut self, bin: &str, args: &[String]) -> Result<ProbeOutput> {
        let output = Command::new(bin)
            .args(args)
            .output()
            .with_context(|| format!("failed to execute '{bin}'"))?;
        Ok(ProbeOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        })
    }
}

impl WhenConditions {
    pub(crate) fn is_empty(&self) -> bool {
        self.free_nodes.is_none() && self.after_job.is_none() && self.time_window.is_none()
    }
}

impl FreeNodesCondition {
    fn description(&self) -> String {
        format!(
            "partition {} has at least {} idle node(s)",
            self.partition, self.minimum_idle_nodes
        )
    }
}

impl AfterJobCondition {
    fn description(&self) -> String {
        format!("job {} satisfies {}", self.job_id, self.condition.as_str())
    }
}

impl TimeWindow {
    pub(crate) fn parse(raw: &str) -> Result<Self> {
        let Some((start, end)) = raw.split_once('-') else {
            bail!("--between must use HH:MM-HH:MM syntax");
        };
        let start_minutes = parse_hhmm(start, "--between start")?;
        let end_minutes = parse_hhmm(end, "--between end")?;
        if start_minutes == end_minutes {
            bail!("--between start and end must be different times");
        }
        Ok(Self {
            start_minutes,
            end_minutes,
            label: format!(
                "{}-{}",
                format_minutes(start_minutes),
                format_minutes(end_minutes)
            ),
        })
    }

    fn contains(&self, minutes: u16) -> bool {
        if self.start_minutes < self.end_minutes {
            minutes >= self.start_minutes && minutes <= self.end_minutes
        } else {
            minutes >= self.start_minutes || minutes <= self.end_minutes
        }
    }

    pub(crate) fn description(&self) -> String {
        format!("local time is between {}", self.label)
    }
}

pub(crate) fn parse_duration(raw: &str) -> Result<Duration> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("duration must not be empty");
    }
    let (digits, unit) = raw.split_at(
        raw.find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(raw.len()),
    );
    if digits.is_empty() || unit.is_empty() {
        bail!("duration '{raw}' must use a unit suffix such as s, m, h, or d");
    }
    if !digits.chars().all(|ch| ch.is_ascii_digit()) {
        bail!("duration '{raw}' must start with a non-negative integer");
    }
    let value = digits
        .parse::<u64>()
        .with_context(|| format!("duration '{raw}' is too large"))?;
    let multiplier = match unit {
        "s" => 1,
        "m" => SECONDS_PER_MINUTE,
        "h" => SECONDS_PER_HOUR,
        "d" => SECONDS_PER_DAY,
        _ => bail!("duration '{raw}' must use one of these units: s, m, h, d"),
    };
    Ok(Duration::from_secs(value.saturating_mul(multiplier)))
}

pub(crate) fn parse_poll_interval(raw: Option<&str>) -> Result<Duration> {
    let duration = match raw {
        Some(raw) => parse_duration(raw)?,
        None => Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS),
    };
    if duration < Duration::from_secs(MIN_POLL_INTERVAL_SECONDS) {
        bail!("--poll-interval must be at least {MIN_POLL_INTERVAL_SECONDS}s");
    }
    Ok(duration)
}

pub(crate) fn parse_after_job_condition(raw: &str) -> Result<JobDependencyCondition> {
    match raw {
        "afterany" => Ok(JobDependencyCondition::AfterAny),
        "afterok" => Ok(JobDependencyCondition::AfterOk),
        "afternotok" => Ok(JobDependencyCondition::AfterNotOk),
        other => {
            bail!("unknown --after-job-condition '{other}'; use afterany, afterok, or afternotok")
        }
    }
}

pub(crate) fn parse_idle_node_count(raw: &str) -> u32 {
    raw.lines()
        .filter_map(|line| {
            let (state, nodes) = line.split_once('|')?;
            if state.trim().eq_ignore_ascii_case("idle") {
                nodes.trim().parse::<u32>().ok()
            } else {
                None
            }
        })
        .sum()
}

pub(crate) fn monitor_until_ready(
    options: &MonitorOptions,
    runtime: &mut impl MonitorRuntime,
) -> Result<WhenTrigger> {
    if options.conditions.is_empty() {
        bail!("when requires at least one condition");
    }
    let started_at = runtime.monotonic_seconds();
    loop {
        let summaries = evaluate_conditions(options, runtime)?;
        if summaries.iter().all(|summary| summary.satisfied) {
            return Ok(WhenTrigger {
                conditions: summaries,
            });
        }

        if let Some(timeout) = options.timeout
            && runtime.monotonic_seconds().saturating_sub(started_at) >= timeout.as_secs()
        {
            bail!("when conditions were not satisfied before timeout");
        }
        runtime.sleep(options.poll_interval);
    }
}

fn evaluate_conditions(
    options: &MonitorOptions,
    runtime: &mut impl MonitorRuntime,
) -> Result<Vec<WhenConditionSummary>> {
    let mut summaries = Vec::new();
    if let Some(condition) = &options.conditions.free_nodes {
        summaries.push(check_free_nodes(condition, &options.sinfo_bin, runtime)?);
    }
    if let Some(condition) = &options.conditions.after_job {
        summaries.push(check_after_job(
            condition,
            &options.squeue_bin,
            &options.sacct_bin,
            runtime,
        )?);
    }
    if let Some(window) = &options.conditions.time_window {
        let now = runtime.local_minutes_since_midnight();
        let satisfied = window.contains(now);
        summaries.push(WhenConditionSummary {
            kind: "time_window",
            description: window.description(),
            satisfied,
            detail: format!(
                "local time {} is {} {}",
                format_minutes(now),
                if satisfied { "inside" } else { "outside" },
                window.label
            ),
        });
    }
    Ok(summaries)
}

fn check_free_nodes(
    condition: &FreeNodesCondition,
    sinfo_bin: &str,
    runtime: &mut impl MonitorRuntime,
) -> Result<WhenConditionSummary> {
    let args = vec![
        "-h".to_string(),
        "-p".to_string(),
        condition.partition.clone(),
        "-o".to_string(),
        "%T|%D".to_string(),
    ];
    let output = runtime.command_output(sinfo_bin, &args)?;
    if !output.success {
        bail!(
            "sinfo failed while checking partition {}: {}",
            condition.partition,
            command_failure_detail(&output)
        );
    }
    let idle_nodes = parse_idle_node_count(&output.stdout);
    let satisfied = idle_nodes >= condition.minimum_idle_nodes;
    Ok(WhenConditionSummary {
        kind: "free_nodes",
        description: condition.description(),
        satisfied,
        detail: format!(
            "partition {} has {} idle node(s); need at least {}",
            condition.partition, idle_nodes, condition.minimum_idle_nodes
        ),
    })
}

fn check_after_job(
    condition: &AfterJobCondition,
    squeue_bin: &str,
    sacct_bin: &str,
    runtime: &mut impl MonitorRuntime,
) -> Result<WhenConditionSummary> {
    let squeue_args = vec![
        "-h".to_string(),
        "-j".to_string(),
        condition.job_id.clone(),
        "-o".to_string(),
        "%T".to_string(),
    ];
    let squeue = runtime.command_output(squeue_bin, &squeue_args)?;
    if !squeue.success {
        bail!(
            "squeue failed while checking job {}: {}",
            condition.job_id,
            command_failure_detail(&squeue)
        );
    }
    if let Some(state) = first_state(&squeue.stdout) {
        let outcome = classify_job_state(&state, condition.condition);
        return summary_for_job_outcome(condition, &state, outcome, true);
    }

    let sacct_args = vec![
        "-n".to_string(),
        "-j".to_string(),
        condition.job_id.clone(),
        "--format=State".to_string(),
        "--parsable2".to_string(),
    ];
    let sacct = runtime.command_output(sacct_bin, &sacct_args)?;
    if !sacct.success {
        bail!(
            "sacct failed while checking job {}: {}",
            condition.job_id,
            command_failure_detail(&sacct)
        );
    }
    let Some(state) = first_state(&sacct.stdout) else {
        return Ok(WhenConditionSummary {
            kind: "after_job",
            description: condition.description(),
            satisfied: false,
            detail: format!(
                "job {} is not visible in squeue or sacct yet",
                condition.job_id
            ),
        });
    };
    let outcome = classify_job_state(&state, condition.condition);
    summary_for_job_outcome(condition, &state, outcome, false)
}

fn summary_for_job_outcome(
    condition: &AfterJobCondition,
    state: &str,
    outcome: JobConditionOutcome,
    from_squeue: bool,
) -> Result<WhenConditionSummary> {
    let source = if from_squeue { "squeue" } else { "sacct" };
    match outcome {
        JobConditionOutcome::Satisfied => Ok(WhenConditionSummary {
            kind: "after_job",
            description: condition.description(),
            satisfied: true,
            detail: format!(
                "job {} state {} from {} satisfies {}",
                condition.job_id,
                normalize_state_label(state),
                source,
                condition.condition.as_str()
            ),
        }),
        JobConditionOutcome::Pending => Ok(WhenConditionSummary {
            kind: "after_job",
            description: condition.description(),
            satisfied: false,
            detail: format!(
                "job {} state {} from {} does not satisfy {} yet",
                condition.job_id,
                normalize_state_label(state),
                source,
                condition.condition.as_str()
            ),
        }),
        JobConditionOutcome::Impossible => {
            bail!(
                "job {} reached state {}, which can never satisfy {}",
                condition.job_id,
                normalize_state_label(state),
                condition.condition.as_str()
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobConditionOutcome {
    Satisfied,
    Pending,
    Impossible,
}

fn classify_job_state(raw: &str, condition: JobDependencyCondition) -> JobConditionOutcome {
    let Some(state) = normalized_job_state(raw) else {
        return JobConditionOutcome::Pending;
    };
    if !is_terminal_state(&state) {
        return JobConditionOutcome::Pending;
    }
    let success = state == "COMPLETED";
    match condition {
        JobDependencyCondition::AfterAny => JobConditionOutcome::Satisfied,
        JobDependencyCondition::AfterOk if success => JobConditionOutcome::Satisfied,
        JobDependencyCondition::AfterOk => JobConditionOutcome::Impossible,
        JobDependencyCondition::AfterNotOk if success => JobConditionOutcome::Impossible,
        JobDependencyCondition::AfterNotOk => JobConditionOutcome::Satisfied,
    }
}

fn is_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "BOOT_FAIL"
            | "CANCELLED"
            | "COMPLETED"
            | "DEADLINE"
            | "FAILED"
            | "NODE_FAIL"
            | "OUT_OF_MEMORY"
            | "PREEMPTED"
            | "REVOKED"
            | "TIMEOUT"
    )
}

fn first_state(raw: &str) -> Option<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.eq_ignore_ascii_case("state"))
        .find_map(normalized_job_state)
}

fn normalized_job_state(raw: &str) -> Option<String> {
    let token = raw.trim().split(['|', ' ', '\t', '+', '(']).next()?.trim();
    (!token.is_empty()).then(|| token.to_ascii_uppercase())
}

fn normalize_state_label(raw: &str) -> String {
    normalized_job_state(raw).unwrap_or_else(|| raw.trim().to_string())
}

fn command_failure_detail(output: &ProbeOutput) -> String {
    let stderr = output.stderr.trim();
    if !stderr.is_empty() {
        return stderr.to_string();
    }
    let stdout = output.stdout.trim();
    if !stdout.is_empty() {
        return stdout.to_string();
    }
    "command exited unsuccessfully".to_string()
}

fn parse_hhmm(raw: &str, label: &str) -> Result<u16> {
    let Some((hours, minutes)) = raw.trim().split_once(':') else {
        bail!("{label} must use HH:MM syntax");
    };
    if hours.len() != 2 || minutes.len() != 2 {
        bail!("{label} must use zero-padded HH:MM syntax");
    }
    let hours = hours
        .parse::<u16>()
        .with_context(|| format!("{label} has invalid hour"))?;
    let minutes = minutes
        .parse::<u16>()
        .with_context(|| format!("{label} has invalid minute"))?;
    if hours > 23 || minutes > 59 {
        bail!("{label} is outside the valid 00:00-23:59 range");
    }
    Ok(hours * 60 + minutes)
}

fn format_minutes(minutes: u16) -> String {
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

#[cfg(unix)]
fn current_local_minutes_since_midnight() -> u16 {
    unsafe {
        let now = libc::time(std::ptr::null_mut());
        let mut local: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&now, &mut local).is_null() {
            return current_utc_minutes_since_midnight();
        }
        (u16::try_from(local.tm_hour).unwrap_or(0) * 60) + u16::try_from(local.tm_min).unwrap_or(0)
    }
}

#[cfg(not(unix))]
fn current_local_minutes_since_midnight() -> u16 {
    current_utc_minutes_since_midnight()
}

fn current_utc_minutes_since_midnight() -> u16 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        % SECONDS_PER_DAY;
    u16::try_from(seconds / SECONDS_PER_MINUTE).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Debug)]
    struct FakeRuntime {
        now_seconds: u64,
        local_minutes: u16,
        outputs: VecDeque<ProbeOutput>,
        sleeps: Vec<Duration>,
    }

    impl FakeRuntime {
        fn new(outputs: Vec<ProbeOutput>) -> Self {
            Self {
                now_seconds: 0,
                local_minutes: 12 * 60,
                outputs: VecDeque::from(outputs),
                sleeps: Vec::new(),
            }
        }
    }

    impl MonitorRuntime for FakeRuntime {
        fn monotonic_seconds(&self) -> u64 {
            self.now_seconds
        }

        fn local_minutes_since_midnight(&self) -> u16 {
            self.local_minutes
        }

        fn sleep(&mut self, duration: Duration) {
            self.sleeps.push(duration);
            self.now_seconds += duration.as_secs();
        }

        fn command_output(&mut self, _bin: &str, _args: &[String]) -> Result<ProbeOutput> {
            Ok(self.outputs.pop_front().unwrap_or_else(|| ProbeOutput {
                success: true,
                stdout: String::new(),
                stderr: String::new(),
            }))
        }
    }

    fn success(stdout: &str) -> ProbeOutput {
        ProbeOutput {
            success: true,
            stdout: stdout.to_string(),
            stderr: String::new(),
        }
    }

    #[test]
    fn duration_parser_accepts_basic_units_and_rejects_bad_input() {
        assert_eq!(parse_duration("0s").unwrap(), Duration::from_secs(0));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
        assert!(parse_duration("").is_err());
        assert!(parse_duration("5").is_err());
        assert!(parse_duration("1w").is_err());
        assert!(parse_poll_interval(Some("0s")).is_err());
        assert!(parse_poll_interval(Some("4s")).is_err());
        assert_eq!(
            parse_poll_interval(None).unwrap(),
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS)
        );
    }

    #[test]
    fn time_window_supports_same_day_wraparound_and_boundaries() {
        let day = TimeWindow::parse("09:30-17:00").unwrap();
        assert!(day.contains(9 * 60 + 30));
        assert!(day.contains(17 * 60));
        assert!(!day.contains(17 * 60 + 1));

        let night = TimeWindow::parse("22:00-06:00").unwrap();
        assert!(night.contains(22 * 60));
        assert!(night.contains(23 * 60 + 59));
        assert!(night.contains(6 * 60));
        assert!(!night.contains(12 * 60));

        assert!(TimeWindow::parse("9:00-17:00").is_err());
        assert!(TimeWindow::parse("24:00-02:00").is_err());
        assert!(TimeWindow::parse("10:00-10:00").is_err());
    }

    #[test]
    fn sinfo_parser_counts_only_idle_rows() {
        let raw = "idle|2\nmixed|8\nalloc|4\ndown|1\nIDLE|3\nidle~|7\nbroken\nidle|bad\n";
        assert_eq!(parse_idle_node_count(raw), 5);
    }

    #[test]
    fn job_state_classifier_handles_terminal_and_mismatch_cases() {
        assert_eq!(
            classify_job_state("COMPLETED", JobDependencyCondition::AfterOk),
            JobConditionOutcome::Satisfied
        );
        assert_eq!(
            classify_job_state("COMPLETED+", JobDependencyCondition::AfterOk),
            JobConditionOutcome::Satisfied
        );
        assert_eq!(
            classify_job_state("FAILED", JobDependencyCondition::AfterNotOk),
            JobConditionOutcome::Satisfied
        );
        assert_eq!(
            classify_job_state("CANCELLED", JobDependencyCondition::AfterAny),
            JobConditionOutcome::Satisfied
        );
        assert_eq!(
            classify_job_state("TIMEOUT", JobDependencyCondition::AfterAny),
            JobConditionOutcome::Satisfied
        );
        assert_eq!(
            classify_job_state("FAILED", JobDependencyCondition::AfterOk),
            JobConditionOutcome::Impossible
        );
        assert_eq!(
            classify_job_state("COMPLETED", JobDependencyCondition::AfterNotOk),
            JobConditionOutcome::Impossible
        );
        assert_eq!(
            classify_job_state("RUNNING", JobDependencyCondition::AfterAny),
            JobConditionOutcome::Pending
        );
    }

    #[test]
    fn monitor_loop_uses_and_semantics_and_fake_sleep() {
        let options = MonitorOptions {
            conditions: WhenConditions {
                free_nodes: Some(FreeNodesCondition {
                    partition: "gpu8".to_string(),
                    minimum_idle_nodes: 4,
                }),
                after_job: None,
                time_window: Some(TimeWindow::parse("22:00-06:00").unwrap()),
            },
            poll_interval: Duration::from_secs(5),
            timeout: Some(Duration::from_secs(10)),
            sinfo_bin: "sinfo".to_string(),
            squeue_bin: "squeue".to_string(),
            sacct_bin: "sacct".to_string(),
        };
        let mut runtime = FakeRuntime::new(vec![success("idle|2\n"), success("idle|4\n")]);
        runtime.local_minutes = 23 * 60;

        let trigger = monitor_until_ready(&options, &mut runtime).unwrap();
        assert_eq!(trigger.conditions.len(), 2);
        assert_eq!(runtime.sleeps, vec![Duration::from_secs(5)]);
    }

    #[test]
    fn monitor_loop_times_out_without_real_sleep() {
        let options = MonitorOptions {
            conditions: WhenConditions {
                free_nodes: Some(FreeNodesCondition {
                    partition: "gpu8".to_string(),
                    minimum_idle_nodes: 4,
                }),
                after_job: None,
                time_window: None,
            },
            poll_interval: Duration::from_secs(5),
            timeout: Some(Duration::from_secs(0)),
            sinfo_bin: "sinfo".to_string(),
            squeue_bin: "squeue".to_string(),
            sacct_bin: "sacct".to_string(),
        };
        let mut runtime = FakeRuntime::new(vec![success("idle|2\n")]);
        assert!(monitor_until_ready(&options, &mut runtime).is_err());
        assert!(runtime.sleeps.is_empty());
    }
}
