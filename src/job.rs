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
}
