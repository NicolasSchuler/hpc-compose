use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use hpc_compose::job::{
    PsServiceRow, PsSnapshot, SchedulerOptions, SubmissionRecord, WatchOutcome, build_ps_snapshot,
};

const DATA_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_WIDTH: usize = 120;
const DEFAULT_HEIGHT: usize = 30;
const MIN_TABLE_WIDTH: usize = 54;
const FORCE_WATCH_UI_ENV: &str = "HPC_COMPOSE_FORCE_WATCH_UI";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchKey {
    Up,
    Down,
    First,
    Last,
    Tab,
    Quit,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchModel {
    pub(crate) snapshot: PsSnapshot,
    pub(crate) selected_index: usize,
    pub(crate) log_lines: Vec<String>,
}

#[derive(Debug, Clone)]
struct SelectedLogBuffer {
    service_name: String,
    path: PathBuf,
    offset: u64,
    pending: String,
    lines: Vec<String>,
    capacity: usize,
}

#[derive(Debug)]
struct TerminalGuard {
    saved_mode: String,
}

impl TerminalGuard {
    fn enter() -> Result<Self> {
        let saved_mode = String::from_utf8(
            Command::new("stty")
                .arg("-g")
                .output()
                .context("failed to execute 'stty -g'")?
                .stdout,
        )
        .context("failed to parse terminal mode from stty")?
        .trim()
        .to_string();
        if saved_mode.is_empty() {
            bail!("failed to read terminal mode from stty");
        }
        let status = Command::new("stty")
            .args(["-echo", "-icanon", "min", "0", "time", "0"])
            .status()
            .context("failed to execute 'stty' while entering watch UI")?;
        if !status.success() {
            bail!("stty failed while entering watch UI");
        }
        print!("\x1b[?1049h\x1b[?25l");
        io::stdout()
            .flush()
            .context("failed to flush alternate-screen entry")?;
        Ok(Self { saved_mode })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = Command::new("stty").arg(&self.saved_mode).status();
        let _ = write!(io::stdout(), "\x1b[?25h\x1b[?1049l");
        let _ = io::stdout().flush();
    }
}

impl SelectedLogBuffer {
    fn seed(row: Option<&PsServiceRow>, lines: usize, capacity: usize) -> Self {
        let (service_name, path) = match row {
            Some(row) => (row.service_name.clone(), row.path.clone()),
            None => ("<none>".to_string(), PathBuf::new()),
        };
        let log_lines = if path.as_os_str().is_empty() {
            Vec::new()
        } else {
            tail_lines(&path, lines).unwrap_or_default()
        };
        let offset = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
        Self {
            service_name,
            path,
            offset,
            pending: String::new(),
            lines: capped_lines(log_lines, capacity),
            capacity,
        }
    }

    fn reseed_if_needed(&mut self, row: Option<&PsServiceRow>, lines: usize, capacity: usize) {
        let Some(row) = row else {
            *self = Self::seed(None, lines, capacity);
            return;
        };
        if self.service_name != row.service_name
            || self.path != row.path
            || self.capacity != capacity
        {
            *self = Self::seed(Some(row), lines, capacity);
        }
    }

    fn refresh(&mut self) -> Result<()> {
        if self.path.as_os_str().is_empty() {
            self.lines.clear();
            return Ok(());
        }
        for line in read_new_lines(&self.path, &mut self.offset, &mut self.pending)? {
            self.lines.push(line);
        }
        self.lines = capped_lines(std::mem::take(&mut self.lines), self.capacity);
        Ok(())
    }
}

pub(crate) fn can_use_watch_ui() -> bool {
    force_watch_ui() || (io::stdin().is_terminal() && io::stdout().is_terminal())
}

pub(crate) fn run_watch_ui(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    initial_service: Option<&str>,
    lines: usize,
) -> Result<WatchOutcome> {
    let _guard = TerminalGuard::enter()?;
    let mut input = io::stdin().lock();
    let mut pending_input = Vec::new();

    let mut snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
    let mut selected_index = initial_selected_index(&snapshot, initial_service)?;
    let (_, height) = terminal_size();
    let mut log_buffer = SelectedLogBuffer::seed(
        snapshot.services.get(selected_index),
        lines,
        log_capacity(height),
    );
    let mut last_refresh = Instant::now();

    loop {
        if last_refresh.elapsed() >= DATA_REFRESH_INTERVAL {
            snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
            selected_index = clamp_selected_index(&snapshot, selected_index);
            let (_, height) = terminal_size();
            log_buffer.reseed_if_needed(
                snapshot.services.get(selected_index),
                lines,
                log_capacity(height),
            );
            log_buffer.refresh()?;
            last_refresh = Instant::now();
        }

        render_model(
            &WatchModel {
                snapshot: snapshot.clone(),
                selected_index,
                log_lines: log_buffer.lines.clone(),
            },
            terminal_size(),
        )?;

        if snapshot.scheduler.terminal {
            return Ok(if snapshot.scheduler.failed {
                WatchOutcome::Failed(snapshot.scheduler.clone())
            } else {
                WatchOutcome::Completed(snapshot.scheduler.clone())
            });
        }

        let mut bytes = [0_u8; 64];
        let read = input
            .read(&mut bytes)
            .context("failed to read watch UI input")?;
        if read > 0 {
            pending_input.extend_from_slice(&bytes[..read]);
            for key in parse_keys(&mut pending_input) {
                match key {
                    WatchKey::Quit => {
                        return Ok(WatchOutcome::Interrupted(snapshot.scheduler.clone()));
                    }
                    other => {
                        selected_index =
                            apply_watch_key(selected_index, snapshot.services.len(), other);
                        let (_, height) = terminal_size();
                        log_buffer.reseed_if_needed(
                            snapshot.services.get(selected_index),
                            lines,
                            log_capacity(height),
                        );
                    }
                }
            }
        }

        thread::sleep(INPUT_POLL_INTERVAL);
    }
}

fn force_watch_ui() -> bool {
    std::env::var_os(FORCE_WATCH_UI_ENV).is_some_and(|value| value != "0")
}

pub(crate) fn apply_watch_key(selected_index: usize, service_count: usize, key: WatchKey) -> usize {
    if service_count == 0 {
        return 0;
    }
    match key {
        WatchKey::Up => selected_index.saturating_sub(1),
        WatchKey::Down | WatchKey::Tab => (selected_index + 1).min(service_count - 1),
        WatchKey::First => 0,
        WatchKey::Last => service_count - 1,
        WatchKey::Quit => selected_index,
    }
}

pub(crate) fn render_watch_frame(model: &WatchModel, width: usize, height: usize) -> String {
    let width = width.max(80);
    let height = height.max(12);
    let selected = model.snapshot.services.get(model.selected_index);
    let scheduler = format!(
        "{} ({})",
        model.snapshot.scheduler.state,
        hpc_compose::job::scheduler_source_label(model.snapshot.scheduler.source)
    );
    let selected_name = selected
        .map(|service| service.service_name.as_str())
        .unwrap_or("<none>");

    let mut lines = vec![
        fit_line(
            &format!("hpc-compose watch | job {}", model.snapshot.record.job_id),
            width,
        ),
        fit_line(
            &format!(
                "scheduler: {} | services: {} | selected: {}",
                scheduler,
                model.snapshot.services.len(),
                selected_name
            ),
            width,
        ),
    ];
    if let Some(detail) = model.snapshot.scheduler.detail.as_deref() {
        lines.push(fit_line(&format!("note: {detail}"), width));
    } else if let Some(queue) = &model.snapshot.queue_diagnostics {
        if let Some(reason) = queue.pending_reason.as_deref() {
            lines.push(fit_line(&format!("pending reason: {reason}"), width));
        }
    }
    lines.push("-".repeat(width));

    let table_width = MIN_TABLE_WIDTH.min(width.saturating_sub(20));
    let log_width = width.saturating_sub(table_width + 3);
    let body_height = height.saturating_sub(lines.len());
    let mut table_lines = Vec::with_capacity(body_height);
    table_lines.push(fit_line(
        "svc              step         pid    ready status   restarts exit",
        table_width,
    ));
    for (index, service) in model.snapshot.services.iter().enumerate() {
        let marker = if index == model.selected_index {
            '>'
        } else {
            ' '
        };
        let step = service.step_name.as_deref().unwrap_or("-");
        let pid = service
            .launcher_pid
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let ready = service.healthy.map(yes_no_short).unwrap_or("-");
        let status = service.status.as_deref().unwrap_or("unknown");
        let restarts = service
            .restart_count
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        let exit = service
            .last_exit_code
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());
        table_lines.push(fit_line(
            &format!(
                "{marker} {:<16} {:<12} {:<6} {:<5} {:<8} {:<8} {:<4}",
                truncate_cell(&service.service_name, 16),
                truncate_cell(step, 12),
                pid,
                ready,
                truncate_cell(status, 8),
                truncate_cell(&restarts, 8),
                exit
            ),
            table_width,
        ));
    }

    let mut log_lines = Vec::with_capacity(body_height);
    log_lines.push(fit_line(&format!("logs: {}", selected_name), log_width));
    for line in &model.log_lines {
        log_lines.push(fit_line(line, log_width));
    }

    let row_count = body_height.max(1);
    for row in 0..row_count {
        let left = table_lines.get(row).map(String::as_str).unwrap_or("");
        let right = log_lines.get(row).map(String::as_str).unwrap_or("");
        lines.push(format!(
            "{} │ {}",
            pad_line(left, table_width),
            pad_line(right, log_width)
        ));
    }

    lines.join("\n")
}

fn render_model(model: &WatchModel, (width, height): (usize, usize)) -> Result<()> {
    let frame = render_watch_frame(model, width, height);
    print!("\x1b[2J\x1b[H{frame}");
    io::stdout()
        .flush()
        .context("failed to flush watch UI frame")
}

fn initial_selected_index(snapshot: &PsSnapshot, initial_service: Option<&str>) -> Result<usize> {
    match initial_service {
        Some(service_name) => snapshot
            .services
            .iter()
            .position(|service| service.service_name == service_name)
            .with_context(|| {
                format!(
                    "service '{}' does not exist in tracked job {}",
                    service_name, snapshot.record.job_id
                )
            }),
        None => Ok(0),
    }
}

fn clamp_selected_index(snapshot: &PsSnapshot, selected_index: usize) -> usize {
    if snapshot.services.is_empty() {
        0
    } else {
        selected_index.min(snapshot.services.len() - 1)
    }
}

fn log_capacity(height: usize) -> usize {
    height.saturating_sub(6).max(4)
}

fn terminal_size() -> (usize, usize) {
    if let Ok(output) = Command::new("stty").arg("size").output()
        && output.status.success()
        && let Ok(raw) = String::from_utf8(output.stdout)
    {
        let mut parts = raw.split_whitespace();
        if let (Some(rows), Some(cols)) = (parts.next(), parts.next())
            && let (Ok(rows), Ok(cols)) = (rows.parse::<usize>(), cols.parse::<usize>())
        {
            return (cols, rows);
        }
    }
    let cols = std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_WIDTH);
    let rows = std::env::var("LINES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_HEIGHT);
    (cols, rows)
}

fn parse_keys(buffer: &mut Vec<u8>) -> Vec<WatchKey> {
    let mut keys = Vec::new();
    let mut index = 0;
    while index < buffer.len() {
        match buffer[index] {
            b'q' => {
                keys.push(WatchKey::Quit);
                index += 1;
            }
            b'j' => {
                keys.push(WatchKey::Down);
                index += 1;
            }
            b'k' => {
                keys.push(WatchKey::Up);
                index += 1;
            }
            b'g' => {
                keys.push(WatchKey::First);
                index += 1;
            }
            b'G' => {
                keys.push(WatchKey::Last);
                index += 1;
            }
            b'\t' => {
                keys.push(WatchKey::Tab);
                index += 1;
            }
            0x1b if buffer.len().saturating_sub(index) < 3 => break,
            0x1b if buffer.len().saturating_sub(index) >= 3
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'A' =>
            {
                keys.push(WatchKey::Up);
                index += 3;
            }
            0x1b if buffer.len().saturating_sub(index) >= 3
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'B' =>
            {
                keys.push(WatchKey::Down);
                index += 3;
            }
            _ => {
                index += 1;
            }
        }
    }
    if index > 0 {
        buffer.drain(0..index);
    }
    keys
}

fn read_new_lines(path: &Path, offset: &mut u64, pending: &mut String) -> Result<Vec<String>> {
    let Ok(mut file) = File::open(path) else {
        *offset = 0;
        pending.clear();
        return Ok(Vec::new());
    };
    let len = file
        .metadata()
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .len();
    if *offset > len {
        *offset = 0;
        pending.clear();
    }
    if *offset == len {
        return Ok(Vec::new());
    }

    file.seek(SeekFrom::Start(*offset))
        .with_context(|| format!("failed to seek {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    *offset = len;

    let mut combined = std::mem::take(pending);
    combined.push_str(&String::from_utf8_lossy(&bytes));
    let ends_with_newline = combined.ends_with('\n');
    let mut lines = combined
        .split_inclusive('\n')
        .map(|segment| {
            segment
                .trim_end_matches('\n')
                .trim_end_matches('\r')
                .to_string()
        })
        .collect::<Vec<_>>();

    if !ends_with_newline {
        *pending = lines.pop().unwrap_or_default();
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

fn capped_lines(mut lines: Vec<String>, capacity: usize) -> Vec<String> {
    if lines.len() > capacity {
        lines.drain(0..(lines.len() - capacity));
    }
    lines
}

fn yes_no_short(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn truncate_cell(value: &str, width: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(width) {
        out.push(ch);
    }
    out
}

fn fit_line(value: &str, width: usize) -> String {
    pad_line(&truncate_cell(value, width), width)
}

fn pad_line(value: &str, width: usize) -> String {
    let len = value.chars().count();
    if len >= width {
        truncate_cell(value, width)
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hpc_compose::job::{
        PsSnapshot, QueueDiagnostics, SchedulerSource, SchedulerStatus, SubmissionRecord,
    };

    fn sample_snapshot() -> PsSnapshot {
        PsSnapshot {
            record: SubmissionRecord {
                schema_version: 1,
                backend: hpc_compose::job::SubmissionBackend::Slurm,
                job_id: "12345".into(),
                submitted_at: 0,
                compose_file: PathBuf::from("/tmp/compose.yaml"),
                submit_dir: PathBuf::from("/tmp"),
                script_path: PathBuf::from("/tmp/job.sbatch"),
                cache_dir: PathBuf::from("/tmp/cache"),
                batch_log: PathBuf::from("/tmp/slurm-12345.out"),
                service_logs: Default::default(),
                artifact_export_dir: None,
                resume_dir: None,
            },
            scheduler: SchedulerStatus {
                state: "RUNNING".into(),
                source: SchedulerSource::Squeue,
                terminal: false,
                failed: false,
                detail: None,
            },
            queue_diagnostics: Some(QueueDiagnostics {
                pending_reason: None,
                eligible_time: None,
                start_time: None,
            }),
            log_dir: PathBuf::from("/tmp/.hpc-compose/12345/logs"),
            services: vec![
                PsServiceRow {
                    service_name: "api".into(),
                    path: PathBuf::from("/tmp/api.log"),
                    present: true,
                    updated_at: None,
                    updated_age_seconds: None,
                    log_path: Some(PathBuf::from("/tmp/api.log")),
                    step_name: Some("hpc-compose:api".into()),
                    launch_index: Some(0),
                    launcher_pid: Some(4242),
                    healthy: Some(true),
                    readiness_configured: Some(true),
                    status: Some("ready".into()),
                    failure_policy_mode: Some("restart_on_failure".into()),
                    restart_count: Some(1),
                    max_restarts: Some(3),
                    window_seconds: Some(60),
                    max_restarts_in_window: Some(3),
                    restart_failures_in_window: Some(1),
                    last_exit_code: None,
                    placement_mode: Some("primary".into()),
                    nodes: Some(1),
                    ntasks: Some(1),
                    ntasks_per_node: Some(1),
                    nodelist: Some("node001".into()),
                },
                PsServiceRow {
                    service_name: "worker".into(),
                    path: PathBuf::from("/tmp/worker.log"),
                    present: true,
                    updated_at: None,
                    updated_age_seconds: None,
                    log_path: Some(PathBuf::from("/tmp/worker.log")),
                    step_name: Some("hpc-compose:worker".into()),
                    launch_index: Some(1),
                    launcher_pid: Some(5252),
                    healthy: Some(false),
                    readiness_configured: Some(false),
                    status: Some("running".into()),
                    failure_policy_mode: None,
                    restart_count: None,
                    max_restarts: None,
                    window_seconds: None,
                    max_restarts_in_window: None,
                    restart_failures_in_window: None,
                    last_exit_code: None,
                    placement_mode: None,
                    nodes: None,
                    ntasks: None,
                    ntasks_per_node: None,
                    nodelist: None,
                },
            ],
            attempt: None,
            is_resume: None,
            resume_dir: None,
        }
    }

    #[test]
    fn watch_key_navigation_clamps_to_bounds() {
        assert_eq!(apply_watch_key(0, 2, WatchKey::Up), 0);
        assert_eq!(apply_watch_key(0, 2, WatchKey::Down), 1);
        assert_eq!(apply_watch_key(1, 2, WatchKey::Down), 1);
        assert_eq!(apply_watch_key(1, 2, WatchKey::First), 0);
        assert_eq!(apply_watch_key(0, 2, WatchKey::Last), 1);
        assert_eq!(apply_watch_key(0, 0, WatchKey::Down), 0);
    }

    #[test]
    fn parse_keys_recognizes_navigation_sequences() {
        let mut raw = vec![
            b'j', b'k', b'g', b'G', b'\t', 0x1b, b'[', b'A', 0x1b, b'[', b'B', b'q',
        ];
        assert_eq!(
            parse_keys(&mut raw),
            vec![
                WatchKey::Down,
                WatchKey::Up,
                WatchKey::First,
                WatchKey::Last,
                WatchKey::Tab,
                WatchKey::Up,
                WatchKey::Down,
                WatchKey::Quit,
            ]
        );
        assert!(raw.is_empty());
    }

    #[test]
    fn parse_keys_preserves_partial_escape_sequences() {
        let mut raw = vec![0x1b, b'['];
        assert!(parse_keys(&mut raw).is_empty());
        assert_eq!(raw, vec![0x1b, b'[']);

        raw.push(b'A');
        assert_eq!(parse_keys(&mut raw), vec![WatchKey::Up]);
        assert!(raw.is_empty());
    }

    #[test]
    fn render_watch_frame_includes_table_and_log_pane() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                log_lines: vec!["booting".into(), "ready".into()],
            },
            100,
            18,
        );
        assert!(frame.contains("hpc-compose watch | job 12345"));
        assert!(frame.contains("logs: api"));
        assert!(frame.contains("> api"));
        assert!(frame.contains("ready"));
        assert!(frame.contains("worker"));
    }
}
