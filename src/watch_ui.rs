use crate::term;

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossterm::cursor::MoveTo;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{self, Clear, ClearType};
use hpc_compose::cli::HoldOnExit;
use hpc_compose::job::{
    PsServiceRow, PsSnapshot, SchedulerOptions, StatsOptions, StatsSnapshot, SubmissionRecord,
    WalltimeProgress, WatchOutcome, build_ps_snapshot, build_stats_snapshot,
    format_walltime_summary, walltime_progress, walltime_progress_percent,
};

const DATA_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_WIDTH: usize = 120;
const DEFAULT_HEIGHT: usize = 30;
const MIN_TABLE_WIDTH: usize = 58;
const FORCE_WATCH_UI_ENV: &str = "HPC_COMPOSE_FORCE_WATCH_UI";
const METRICS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchKey {
    Up,
    Down,
    PageUp,
    PageDown,
    First,
    Last,
    End,
    Tab,
    TogglePause,
    ToggleAllLogs,
    DebugHint,
    LogsHint,
    StatsHint,
    Quit,
    Help,
    Search,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputMode {
    Normal,
    Search,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogViewMode {
    Selected,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WatchHoldState {
    pub(crate) failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct WatchModel {
    pub(crate) snapshot: PsSnapshot,
    pub(crate) selected_index: usize,
    pub(crate) walltime_progress: Option<WalltimeProgress>,
    pub(crate) log_lines: Vec<String>,
    pub(crate) follow_logs: bool,
    pub(crate) log_scroll: usize,
    pub(crate) log_view_mode: LogViewMode,
    pub(crate) hold_state: Option<WatchHoldState>,
    pub(crate) metrics_line: Option<String>,
    pub(crate) show_help: bool,
    pub(crate) filter: Option<String>,
    pub(crate) search_buffer: String,
    pub(crate) input_mode: InputMode,
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
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        #[cfg(test)]
        {
            Ok(Self)
        }

        #[cfg(not(test))]
        {
            terminal::enable_raw_mode().context("failed to enable terminal raw mode")?;
            let mut stdout = io::stdout();
            execute!(
                stdout,
                crossterm::terminal::EnterAlternateScreen,
                crossterm::cursor::Hide
            )
            .context("failed to enter alternate-screen watch UI")?;
            stdout
                .flush()
                .context("failed to flush alternate-screen entry")?;
            Ok(Self)
        }
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        #[cfg(not(test))]
        {
            let mut stdout = io::stdout();
            let _ = execute!(
                stdout,
                crossterm::cursor::Show,
                crossterm::terminal::LeaveAlternateScreen
            );
            let _ = terminal::disable_raw_mode();
            let _ = stdout.flush();
        }
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
    watch_ui_available(
        force_watch_ui(),
        io::stdin().is_terminal(),
        io::stdout().is_terminal(),
    )
}

pub(crate) fn run_watch_ui(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    initial_service: Option<&str>,
    lines: usize,
    hold_on_exit: HoldOnExit,
) -> Result<WatchOutcome> {
    let guard = TerminalGuard::enter()?;
    let result = run_watch_ui_loop(record, options, initial_service, lines, hold_on_exit);
    drop(guard);
    let result = result?;
    if let Some(command) = result.command_hint {
        println!("next command: {command}");
    }
    Ok(result.outcome)
}

#[derive(Debug)]
struct WatchLoopResult {
    outcome: WatchOutcome,
    command_hint: Option<String>,
}

fn run_watch_ui_loop(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    initial_service: Option<&str>,
    lines: usize,
    hold_on_exit: HoldOnExit,
) -> Result<WatchLoopResult> {
    let mut snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
    let mut selected_index = initial_selected_index(&snapshot, initial_service)?;
    let (_, height) = terminal_size();
    let mut log_buffer = SelectedLogBuffer::seed(
        snapshot.services.get(selected_index),
        lines,
        log_capacity(height),
    );
    let mut all_log_lines = build_all_log_lines(&snapshot, lines, log_capacity(height));
    let mut last_refresh = Instant::now();
    let mut last_metrics_refresh = Instant::now()
        .checked_sub(METRICS_REFRESH_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut metrics_line = None;
    let mut show_help = false;
    let mut filter: Option<String> = None;
    let mut input_mode = InputMode::Normal;
    let mut search_buffer = String::new();
    let mut follow_logs = true;
    let mut log_scroll = 0usize;
    let mut log_view_mode = LogViewMode::Selected;
    let mut terminal_outcome: Option<WatchOutcome> = None;

    loop {
        if last_refresh.elapsed() >= DATA_REFRESH_INTERVAL {
            snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
            let effective = filtered_services(&snapshot.services, filter.as_deref());
            selected_index = clamp_selected_index_raw(&effective, selected_index);
            let (_, height) = terminal_size();
            let resolved = effective.get(selected_index);
            let original_index = resolved.and_then(|r| {
                snapshot
                    .services
                    .iter()
                    .position(|s| s.service_name == r.service_name)
            });
            log_buffer.reseed_if_needed(
                original_index.map(|i| &snapshot.services[i]),
                lines,
                log_capacity(height),
            );
            if follow_logs {
                log_buffer.refresh()?;
                all_log_lines = build_all_log_lines(&snapshot, lines, log_capacity(height));
                log_scroll = 0;
            }
            last_refresh = Instant::now();
        }
        if last_metrics_refresh.elapsed() >= METRICS_REFRESH_INTERVAL {
            metrics_line = load_watch_metrics_line(record, options);
            last_metrics_refresh = Instant::now();
        }
        let walltime_progress = walltime_progress(
            &snapshot.record,
            &snapshot.scheduler,
            snapshot.queue_diagnostics.as_ref(),
            current_unix_timestamp(),
        );
        let current_outcome = terminal_outcome.clone().or_else(|| {
            snapshot.scheduler.terminal.then(|| {
                if snapshot.scheduler.failed {
                    WatchOutcome::Failed(snapshot.scheduler.clone())
                } else {
                    WatchOutcome::Completed(snapshot.scheduler.clone())
                }
            })
        });

        if terminal_outcome.is_none()
            && let Some(outcome) = current_outcome.clone()
        {
            if matches!(outcome, WatchOutcome::Failed(_))
                && let Some(failed_index) = first_failed_service_index(&snapshot.services)
            {
                filter = None;
                selected_index = failed_index;
                log_buffer.reseed_if_needed(
                    snapshot.services.get(selected_index),
                    lines,
                    log_capacity(terminal_size().1),
                );
            }
            if should_hold_on_exit(hold_on_exit, &outcome) {
                terminal_outcome = Some(outcome);
            } else {
                return Ok(WatchLoopResult {
                    outcome,
                    command_hint: None,
                });
            }
        }

        let displayed_log_lines = match log_view_mode {
            LogViewMode::Selected => log_buffer.lines.clone(),
            LogViewMode::All => all_log_lines.clone(),
        };

        render_model(
            &WatchModel {
                snapshot: snapshot.clone(),
                selected_index,
                walltime_progress,
                log_lines: displayed_log_lines,
                follow_logs,
                log_scroll,
                log_view_mode,
                hold_state: terminal_outcome.as_ref().map(|outcome| WatchHoldState {
                    failed: matches!(outcome, WatchOutcome::Failed(_)),
                }),
                metrics_line: metrics_line.clone(),
                show_help,
                filter: filter.clone(),
                search_buffer: search_buffer.clone(),
                input_mode,
            },
            terminal_size(),
        )?;

        if let Some(event) = read_watch_event(INPUT_POLL_INTERVAL)? {
            if input_mode == InputMode::Search {
                let key = match event {
                    WatchInput::Search(key) => key,
                    WatchInput::Normal(WatchKey::Quit) => SearchKey::Cancel,
                    _ => continue,
                };
                match key {
                    SearchKey::Char(ch) => search_buffer.push(ch),
                    SearchKey::Backspace => {
                        search_buffer.pop();
                    }
                    SearchKey::Clear => {
                        search_buffer.clear();
                    }
                    SearchKey::Submit => {
                        filter = if search_buffer.is_empty() {
                            None
                        } else {
                            Some(search_buffer.clone())
                        };
                        input_mode = InputMode::Normal;
                        selected_index = 0;
                    }
                    SearchKey::Cancel => {
                        search_buffer.clear();
                        input_mode = InputMode::Normal;
                    }
                }
            } else if let WatchInput::Normal(key) = event {
                let held_outcome = terminal_outcome.clone();
                match key {
                    WatchKey::Quit => {
                        return Ok(WatchLoopResult {
                            outcome: held_outcome.unwrap_or_else(|| {
                                WatchOutcome::Interrupted(snapshot.scheduler.clone())
                            }),
                            command_hint: None,
                        });
                    }
                    WatchKey::Help => {
                        show_help = !show_help;
                    }
                    WatchKey::Search => {
                        input_mode = InputMode::Search;
                        search_buffer = filter.clone().unwrap_or_default();
                    }
                    WatchKey::TogglePause => {
                        follow_logs = !follow_logs;
                        if follow_logs {
                            log_scroll = 0;
                        }
                    }
                    WatchKey::ToggleAllLogs => {
                        log_view_mode = match log_view_mode {
                            LogViewMode::Selected => LogViewMode::All,
                            LogViewMode::All => LogViewMode::Selected,
                        };
                        log_scroll = 0;
                    }
                    WatchKey::PageUp => {
                        follow_logs = false;
                        log_scroll = log_scroll.saturating_add(10);
                    }
                    WatchKey::PageDown => {
                        log_scroll = log_scroll.saturating_sub(10);
                        follow_logs = log_scroll == 0;
                    }
                    WatchKey::End => {
                        follow_logs = true;
                        log_scroll = 0;
                    }
                    WatchKey::DebugHint | WatchKey::LogsHint | WatchKey::StatsHint
                        if held_outcome.is_some() =>
                    {
                        let command = command_hint_for_key(
                            key,
                            record,
                            filtered_services(&snapshot.services, filter.as_deref())
                                .get(selected_index)
                                .map(|row| row.service_name.as_str()),
                        );
                        return Ok(WatchLoopResult {
                            outcome: held_outcome.expect("held outcome checked above"),
                            command_hint: Some(command),
                        });
                    }
                    other => {
                        let effective = filtered_services(&snapshot.services, filter.as_deref());
                        selected_index = apply_watch_key(selected_index, effective.len(), other);
                        let (_, height) = terminal_size();
                        let resolved = effective.get(selected_index);
                        let original_index = resolved.and_then(|r| {
                            snapshot
                                .services
                                .iter()
                                .position(|s| s.service_name == r.service_name)
                        });
                        log_buffer.reseed_if_needed(
                            original_index.map(|i| &snapshot.services[i]),
                            lines,
                            log_capacity(height),
                        );
                        log_scroll = 0;
                    }
                }
            }
        }
    }
}

fn force_watch_ui() -> bool {
    force_watch_ui_from_value(std::env::var_os(FORCE_WATCH_UI_ENV).as_deref())
}

fn force_watch_ui_from_value(value: Option<&OsStr>) -> bool {
    value.is_some_and(|value| value != OsStr::new("0"))
}

fn watch_ui_available(force: bool, stdin_is_terminal: bool, stdout_is_terminal: bool) -> bool {
    force || (stdin_is_terminal && stdout_is_terminal)
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
        WatchKey::PageUp
        | WatchKey::PageDown
        | WatchKey::End
        | WatchKey::TogglePause
        | WatchKey::ToggleAllLogs
        | WatchKey::DebugHint
        | WatchKey::LogsHint
        | WatchKey::StatsHint
        | WatchKey::Quit
        | WatchKey::Help
        | WatchKey::Search => selected_index,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchInput {
    Normal(WatchKey),
    Search(SearchKey),
}

fn read_watch_event(timeout: Duration) -> Result<Option<WatchInput>> {
    if !event::poll(timeout).context("failed to poll watch UI input")? {
        return Ok(None);
    }
    match event::read().context("failed to read watch UI input")? {
        Event::Key(key) => Ok(map_key_event(key)),
        _ => Ok(None),
    }
}

fn map_key_event(key: KeyEvent) -> Option<WatchInput> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('u')) {
        return Some(WatchInput::Search(SearchKey::Clear));
    }

    match key.code {
        KeyCode::Char('q') => Some(WatchInput::Normal(WatchKey::Quit)),
        KeyCode::Char('j') | KeyCode::Down => Some(WatchInput::Normal(WatchKey::Down)),
        KeyCode::Char('k') | KeyCode::Up => Some(WatchInput::Normal(WatchKey::Up)),
        KeyCode::Char('g') => Some(WatchInput::Normal(WatchKey::First)),
        KeyCode::Char('G') => Some(WatchInput::Normal(WatchKey::Last)),
        KeyCode::Tab => Some(WatchInput::Normal(WatchKey::Tab)),
        KeyCode::PageUp => Some(WatchInput::Normal(WatchKey::PageUp)),
        KeyCode::PageDown => Some(WatchInput::Normal(WatchKey::PageDown)),
        KeyCode::End => Some(WatchInput::Normal(WatchKey::End)),
        KeyCode::Char(' ') => Some(WatchInput::Normal(WatchKey::TogglePause)),
        KeyCode::Char('a') => Some(WatchInput::Normal(WatchKey::ToggleAllLogs)),
        KeyCode::Char('d') => Some(WatchInput::Normal(WatchKey::DebugHint)),
        KeyCode::Char('l') => Some(WatchInput::Normal(WatchKey::LogsHint)),
        KeyCode::Char('s') => Some(WatchInput::Normal(WatchKey::StatsHint)),
        KeyCode::Char('?') => Some(WatchInput::Normal(WatchKey::Help)),
        KeyCode::Char('/') => Some(WatchInput::Normal(WatchKey::Search)),
        KeyCode::Enter => Some(WatchInput::Search(SearchKey::Submit)),
        KeyCode::Esc => Some(WatchInput::Search(SearchKey::Cancel)),
        KeyCode::Backspace => Some(WatchInput::Search(SearchKey::Backspace)),
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(WatchInput::Search(SearchKey::Char(ch)))
        }
        _ => None,
    }
}

pub(crate) fn render_watch_frame(model: &WatchModel, width: usize, height: usize) -> String {
    let width = width.max(1);
    let height = height.max(1);
    let effective = filtered_services(&model.snapshot.services, model.filter.as_deref());
    let selected = effective.get(model.selected_index);
    if width < 80 || height < 12 {
        return render_compact_watch_frame(model, &effective, selected.copied(), width, height);
    }

    let scheduler = format!(
        "{} ({})",
        term::styled_scheduler_state(&model.snapshot.scheduler.state),
        hpc_compose::job::scheduler_source_label(model.snapshot.scheduler.source)
    );
    let selected_name = selected
        .map(|service| service.service_name.as_str())
        .unwrap_or("<none>");

    let filter_indicator = model
        .filter
        .as_deref()
        .map(|f| format!(" | {}", term::styled_warning(&format!("filter: {f}"))))
        .unwrap_or_default();

    let mut lines = vec![
        fit_line(
            &format!(
                "{} | {} | job {}{}{}",
                term::styled_bold("hpc-compose watch"),
                scheduler,
                model.snapshot.record.job_id,
                filter_indicator,
                hold_indicator(model.hold_state)
            ),
            width,
        ),
        fit_line(
            &format!(
                "services: {} | selected: {} | logs: {} {}",
                effective.len(),
                selected_name,
                log_view_label(model.log_view_mode),
                if model.follow_logs {
                    "FOLLOW"
                } else {
                    "PAUSED"
                }
            ),
            width,
        ),
    ];
    if let Some(progress) = &model.walltime_progress {
        lines.push(fit_line(&render_walltime_bar(progress, width), width));
    }
    if let Some(metrics) = model.metrics_line.as_deref() {
        lines.push(fit_line(metrics, width));
    }
    if let Some(detail) = model.snapshot.scheduler.detail.as_deref() {
        lines.push(fit_line(&format!("note: {detail}"), width));
    } else if let Some(queue) = &model.snapshot.queue_diagnostics
        && let Some(reason) = queue.pending_reason.as_deref()
    {
        lines.push(fit_line(
            &format!("{}: {reason}", term::styled_warning("pending reason")),
            width,
        ));
    }
    lines.push("-".repeat(width));

    let mut search_lines = Vec::new();
    if model.input_mode == InputMode::Search {
        search_lines.push("-".repeat(width));
        search_lines.push(fit_line(&format!("filter: {}", model.search_buffer), width));
    }

    let mut help_lines = Vec::new();
    if model.show_help {
        help_lines.push("-".repeat(width));
        help_lines.push(fit_line(&term::styled_bold("Keybindings:"), width));
        help_lines.push(fit_line("  j / Down    next service", width));
        help_lines.push(fit_line("  k / Up      previous service", width));
        help_lines.push(fit_line("  g           first service", width));
        help_lines.push(fit_line("  G           last service", width));
        help_lines.push(fit_line("  /           filter services", width));
        help_lines.push(fit_line("  Space       pause or follow log tail", width));
        help_lines.push(fit_line("  PgUp/PgDn   scroll log tail", width));
        help_lines.push(fit_line("  End         return to live log tail", width));
        help_lines.push(fit_line("  a           toggle selected/all logs", width));
        help_lines.push(fit_line(
            "  d/l/s       debug/logs/stats command after final state",
            width,
        ));
        help_lines.push(fit_line("  ?           toggle help", width));
        help_lines.push(fit_line("  q           quit", width));
        help_lines.push("-".repeat(width));
    }

    let footer = if model.input_mode == InputMode::Search {
        "Enter apply  Esc cancel  Ctrl-U clear  Backspace delete"
    } else if model.hold_state.is_some() {
        "q exit  d debug  l logs  s stats  ? help"
    } else {
        "q quit  ? help  / filter  Space pause  a all  PgUp/PgDn scroll  End follow"
    };
    let footer_lines = vec!["-".repeat(width), fit_line(footer, width)];
    let help_budget = height.saturating_sub(lines.len() + search_lines.len() + footer_lines.len());
    if help_lines.len() > help_budget {
        help_lines.truncate(help_budget);
    }

    let table_width = MIN_TABLE_WIDTH.min(width.saturating_sub(24));
    let log_width = width.saturating_sub(table_width + 3);
    let body_height = height
        .saturating_sub(lines.len() + search_lines.len() + help_lines.len() + footer_lines.len());
    let mut table_lines = Vec::with_capacity(body_height);
    table_lines.push(fit_line(
        "svc              step         pid    ready state restarts exit",
        table_width,
    ));
    if effective.is_empty() {
        table_lines.push(fit_line("  no services match filter", table_width));
    } else {
        for (index, service) in effective.iter().enumerate() {
            let marker = if index == model.selected_index {
                term::styled_success(">")
            } else {
                " ".to_string()
            };
            let step = service.step_name.as_deref().unwrap_or("-");
            let pid = service
                .launcher_pid
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            let ready = service.healthy.map(yes_no_short).unwrap_or("-");
            let state_raw = service_state_label(service);
            let state_styled = styled_state_marker(state_raw);
            let state_col = format!(
                "{:<width$}",
                state_styled,
                width = 5 + state_styled.len() - state_raw.len()
            );
            let restarts = restart_summary(service);
            let exit = service
                .last_exit_code
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string());
            table_lines.push(fit_line(
                &format!(
                    "{marker} {:<16} {:<12} {:<6} {:<5} {} {:<8} {:<4}",
                    truncate_cell(&service.service_name, 16),
                    truncate_cell(step, 12),
                    pid,
                    ready,
                    state_col,
                    truncate_cell(&restarts, 8),
                    exit
                ),
                table_width,
            ));
        }
    }

    let mut log_lines = Vec::with_capacity(body_height);
    let log_title = match model.log_view_mode {
        LogViewMode::Selected => selected_name.to_string(),
        LogViewMode::All => "all services".to_string(),
    };
    let scroll_note = if !model.follow_logs && model.log_scroll > 0 {
        format!(" scroll +{}", model.log_scroll)
    } else {
        String::new()
    };
    log_lines.push(fit_line(
        &format!(
            "{}: {} {}{}",
            term::styled_bold("logs"),
            log_title,
            if model.follow_logs {
                "FOLLOW"
            } else {
                "PAUSED"
            },
            scroll_note
        ),
        log_width,
    ));
    if let Some(path_line) = log_path_line(model.log_view_mode, selected.copied()) {
        log_lines.push(fit_line(&term::styled_dim(&path_line), log_width));
    }
    let visible_log_lines = visible_log_lines(
        &model.log_lines,
        body_height.saturating_sub(log_lines.len()),
        model.follow_logs,
        model.log_scroll,
    );
    if visible_log_lines.is_empty() {
        let empty = empty_log_message(model.log_view_mode, selected.copied());
        log_lines.push(fit_line(&term::styled_dim(empty), log_width));
    }
    for line in visible_log_lines {
        log_lines.push(fit_line(line, log_width));
    }

    let row_count = body_height;
    let separator = pane_separator();
    for row in 0..row_count {
        let left = table_lines.get(row).map(String::as_str).unwrap_or("");
        let right = log_lines.get(row).map(String::as_str).unwrap_or("");
        lines.push(format!(
            "{} {separator} {}",
            pad_line(left, table_width),
            pad_line(right, log_width)
        ));
    }

    lines.extend(search_lines);
    lines.extend(help_lines);
    lines.extend(footer_lines);

    lines.join("\n")
}

fn render_compact_watch_frame(
    model: &WatchModel,
    effective: &[&PsServiceRow],
    selected: Option<&PsServiceRow>,
    width: usize,
    height: usize,
) -> String {
    let scheduler = format!(
        "{} ({})",
        term::styled_scheduler_state(&model.snapshot.scheduler.state),
        hpc_compose::job::scheduler_source_label(model.snapshot.scheduler.source)
    );
    let selected_name = selected
        .map(|service| service.service_name.as_str())
        .unwrap_or("<none>");
    let mut lines = Vec::new();

    push_fit_line(
        &mut lines,
        width,
        height,
        &format!(
            "{} | job {}",
            term::styled_bold("hpc-compose watch"),
            model.snapshot.record.job_id
        ),
    );
    push_fit_line(
        &mut lines,
        width,
        height,
        &format!(
            "{} | services: {} | selected: {}",
            scheduler,
            effective.len(),
            selected_name
        ),
    );
    if let Some(progress) = &model.walltime_progress {
        push_fit_line(
            &mut lines,
            width,
            height,
            &render_walltime_bar(progress, width),
        );
    }
    if let Some(metrics) = model.metrics_line.as_deref() {
        push_fit_line(&mut lines, width, height, metrics);
    }
    if let Some(hold) = model.hold_state {
        push_fit_line(
            &mut lines,
            width,
            height,
            if hold.failed {
                "final state: failed; q exit, d debug, l logs, s stats"
            } else {
                "final state: completed; q exit, d debug, l logs, s stats"
            },
        );
    }
    if let Some(filter) = model.filter.as_deref() {
        push_fit_line(&mut lines, width, height, &format!("filter: {filter}"));
    }
    if model.input_mode == InputMode::Search {
        push_fit_line(
            &mut lines,
            width,
            height,
            &format!("filter input: {}", model.search_buffer),
        );
    }
    if model.show_help {
        push_fit_line(
            &mut lines,
            width,
            height,
            "? help | / filter | Space pause | q quit",
        );
    }

    push_fit_line(&mut lines, width, height, "services:");
    if effective.is_empty() {
        push_fit_line(&mut lines, width, height, "  no services match filter");
    } else {
        for (index, service) in effective.iter().enumerate() {
            let marker = if index == model.selected_index {
                ">"
            } else {
                " "
            };
            let ready = service.healthy.map(yes_no_short).unwrap_or("-");
            push_fit_line(
                &mut lines,
                width,
                height,
                &format!(
                    "{marker} {} {} ready={ready}",
                    service.service_name,
                    styled_state_marker(service_state_label(service))
                ),
            );
        }
    }

    push_fit_line(
        &mut lines,
        width,
        height,
        &format!(
            "logs: {} {}",
            if model.log_view_mode == LogViewMode::All {
                "all services"
            } else {
                selected_name
            },
            if model.follow_logs {
                "FOLLOW"
            } else {
                "PAUSED"
            }
        ),
    );
    if model.log_lines.is_empty() {
        push_fit_line(
            &mut lines,
            width,
            height,
            &term::styled_dim(empty_log_message(model.log_view_mode, selected)),
        );
    }
    for line in visible_log_lines(
        &model.log_lines,
        height.saturating_sub(lines.len() + 1),
        model.follow_logs,
        model.log_scroll,
    ) {
        push_fit_line(&mut lines, width, height, line);
    }
    push_fit_line(
        &mut lines,
        width,
        height,
        if model.hold_state.is_some() {
            "q exit  d debug  l logs  s stats"
        } else {
            "q quit  ? help  / filter  Space pause"
        },
    );

    lines.join("\n")
}

fn push_fit_line(lines: &mut Vec<String>, width: usize, height: usize, value: &str) {
    if lines.len() < height {
        lines.push(fit_line(value, width));
    }
}

fn hold_indicator(hold: Option<WatchHoldState>) -> String {
    match hold {
        Some(WatchHoldState { failed: true }) => {
            format!(" | {}", term::styled_error("held: failed"))
        }
        Some(WatchHoldState { failed: false }) => {
            format!(" | {}", term::styled_success("held: completed"))
        }
        None => String::new(),
    }
}

fn log_view_label(mode: LogViewMode) -> &'static str {
    match mode {
        LogViewMode::Selected => "selected",
        LogViewMode::All => "all",
    }
}

fn service_state_label(service: &PsServiceRow) -> &'static str {
    if service_matches_failure(service) {
        return "FAIL";
    }
    match service.status.as_deref() {
        Some("ready") => "OK",
        Some("running") => "RUN",
        Some("starting") => "WAIT",
        Some("exited") => {
            if service.completed_successfully == Some(true) {
                "DONE"
            } else {
                "EXIT"
            }
        }
        Some("failed") => "FAIL",
        Some("unknown") | None => "UNK",
        Some(_) => "STATE",
    }
}

fn styled_state_marker(value: &str) -> String {
    match value {
        "OK" | "RUN" | "DONE" => term::styled_success(value),
        "WAIT" => term::styled_warning(value),
        "FAIL" | "EXIT" => term::styled_error(value),
        "UNK" => term::styled_dim(value),
        _ => value.to_string(),
    }
}

fn restart_summary(service: &PsServiceRow) -> String {
    match (service.restart_count, service.max_restarts) {
        (Some(count), Some(max)) => format!("{count}/{max}"),
        (Some(count), None) => count.to_string(),
        _ => "-".to_string(),
    }
}

fn log_path_line(mode: LogViewMode, selected: Option<&PsServiceRow>) -> Option<String> {
    match mode {
        LogViewMode::All => Some("path: all tracked service logs".to_string()),
        LogViewMode::Selected => {
            selected.map(|service| format!("path: {}", service.path.display()))
        }
    }
}

fn empty_log_message(mode: LogViewMode, selected: Option<&PsServiceRow>) -> &'static str {
    match mode {
        LogViewMode::All => "<no service log lines yet>",
        LogViewMode::Selected => match selected {
            Some(service) if !service.present => "<log file missing>",
            Some(_) => "<no log lines yet>",
            None => "<no service selected>",
        },
    }
}

fn visible_log_lines(
    lines: &[String],
    capacity: usize,
    follow: bool,
    scroll_from_bottom: usize,
) -> &[String] {
    if capacity == 0 || lines.is_empty() {
        return &[];
    }
    let end = if follow {
        lines.len()
    } else {
        lines.len().saturating_sub(scroll_from_bottom)
    };
    let start = end.saturating_sub(capacity);
    &lines[start..end]
}

fn build_all_log_lines(snapshot: &PsSnapshot, lines: usize, capacity: usize) -> Vec<String> {
    let mut collected = Vec::new();
    for service in &snapshot.services {
        let prefix = format!("[{}]", service.service_name);
        for line in tail_lines(&service.path, lines).unwrap_or_default() {
            collected.push(format!("{prefix} {line}"));
        }
    }
    capped_lines(collected, capacity.max(1))
}

fn service_matches_failure(service: &PsServiceRow) -> bool {
    service.status.as_deref() == Some("failed")
        || service.last_exit_code.is_some_and(|code| code != 0)
        || service.completed_successfully == Some(false)
            && service
                .status
                .as_deref()
                .is_some_and(|status| status == "exited")
}

fn first_failed_service_index(services: &[PsServiceRow]) -> Option<usize> {
    services.iter().position(service_matches_failure)
}

fn should_hold_on_exit(policy: HoldOnExit, outcome: &WatchOutcome) -> bool {
    match policy {
        HoldOnExit::Never => false,
        HoldOnExit::Failure => matches!(outcome, WatchOutcome::Failed(_)),
        HoldOnExit::Always => matches!(
            outcome,
            WatchOutcome::Completed(_) | WatchOutcome::Failed(_)
        ),
    }
}

fn load_watch_metrics_line(
    record: &SubmissionRecord,
    scheduler: &SchedulerOptions,
) -> Option<String> {
    let snapshot = build_stats_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &StatsOptions {
            scheduler: scheduler.clone(),
            sstat_bin: "sstat".to_string(),
        },
    )
    .ok()?;
    format_watch_metrics_line(&snapshot)
}

fn format_watch_metrics_line(snapshot: &StatsSnapshot) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(failure) = &snapshot.first_failure {
        parts.push(format!(
            "first failure: {} exit={}",
            failure.service, failure.exit_code
        ));
    }
    if let Some(gpu) = snapshot
        .sampler
        .as_ref()
        .and_then(|sampler| sampler.gpu.as_ref())
        && let Some(node) = gpu.nodes.first()
    {
        let util = node
            .avg_utilization_gpu
            .map(|value| format!("{value:.0}%"))
            .unwrap_or_else(|| "-".to_string());
        let mem = match (node.memory_used_mib, node.memory_total_mib) {
            (Some(used), Some(total)) => format!("{used}/{total} MiB"),
            _ => "-".to_string(),
        };
        parts.push(format!("gpu: {} util={} mem={}", node.gpu_count, util, mem));
    }
    if snapshot.available {
        parts.push(format!("stats: {}", snapshot.source));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" | "))
    }
}

fn command_hint_for_key(
    key: WatchKey,
    record: &SubmissionRecord,
    selected_service: Option<&str>,
) -> String {
    let compose = shell_quote(&record.compose_file.display().to_string());
    let job = shell_quote(&record.job_id);
    match key {
        WatchKey::DebugHint => format!("hpc-compose debug -f {compose} --job-id {job}"),
        WatchKey::LogsHint => match selected_service {
            Some(service) => format!(
                "hpc-compose logs -f {compose} --job-id {job} --service {} --lines 200",
                shell_quote(service)
            ),
            None => format!("hpc-compose logs -f {compose} --job-id {job} --lines 200"),
        },
        WatchKey::StatsHint => format!("hpc-compose stats -f {compose} --job-id {job}"),
        _ => String::new(),
    }
}

fn shell_quote(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn pane_separator() -> &'static str {
    if term::unicode_allowed_raw() {
        "\u{2502}"
    } else {
        "|"
    }
}

fn render_model(model: &WatchModel, (width, height): (usize, usize)) -> Result<()> {
    let frame = render_watch_frame(model, width, height);
    let mut stdout = io::stdout();
    execute!(stdout, Clear(ClearType::All), MoveTo(0, 0))
        .context("failed to clear watch UI frame")?;
    write!(stdout, "{frame}").context("failed to write watch UI frame")?;
    stdout.flush().context("failed to flush watch UI frame")
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

#[cfg(test)]
fn clamp_selected_index(snapshot: &PsSnapshot, selected_index: usize) -> usize {
    if snapshot.services.is_empty() {
        0
    } else {
        selected_index.min(snapshot.services.len() - 1)
    }
}

fn clamp_selected_index_raw(services: &[&PsServiceRow], selected_index: usize) -> usize {
    if services.is_empty() {
        0
    } else {
        selected_index.min(services.len() - 1)
    }
}

fn filtered_services<'a>(
    services: &'a [PsServiceRow],
    filter: Option<&str>,
) -> Vec<&'a PsServiceRow> {
    match filter {
        Some(pattern) if !pattern.is_empty() => services
            .iter()
            .filter(|s| s.service_name.contains(pattern))
            .collect(),
        _ => services.iter().collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchKey {
    Char(char),
    Backspace,
    Clear,
    Submit,
    Cancel,
}

#[cfg(test)]
fn parse_search_keys(buffer: &mut Vec<u8>) -> Vec<SearchKey> {
    let mut keys = Vec::new();
    let mut index = 0;
    while index < buffer.len() {
        match buffer[index] {
            0x7f | 0x08 => {
                keys.push(SearchKey::Backspace);
                index += 1;
            }
            b'\n' | b'\r' => {
                keys.push(SearchKey::Submit);
                index += 1;
            }
            0x15 => {
                keys.push(SearchKey::Clear);
                index += 1;
            }
            0x1b => {
                keys.push(SearchKey::Cancel);
                let mut consume = 1;
                if buffer.len() > index + 1 && buffer[index + 1] == b'[' {
                    consume = 3.min(buffer.len() - index);
                }
                index += consume;
            }
            byte if (0x20..0x7f).contains(&byte) => {
                keys.push(SearchKey::Char(byte as char));
                index += 1;
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

fn log_capacity(height: usize) -> usize {
    height.saturating_sub(6).max(4)
}

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn render_walltime_bar(progress: &WalltimeProgress, width: usize) -> String {
    let summary = format!(
        "walltime: [{}] {}% {}",
        "{}",
        walltime_progress_percent(progress),
        format_walltime_summary(progress)
    );
    let reserved = summary.len().saturating_sub(2);
    let bar_width = width.saturating_sub(reserved).clamp(10, 24);
    let filled = if progress.total_seconds == 0 {
        bar_width
    } else {
        ((u128::from(progress.elapsed_seconds) * bar_width as u128)
            / u128::from(progress.total_seconds)) as usize
    }
    .min(bar_width);
    let pct = if progress.total_seconds == 0 {
        0
    } else {
        u128::from(progress.elapsed_seconds) * 100 / u128::from(progress.total_seconds)
    };
    let bar = if term::unicode_allowed_raw() {
        let filled_char = "\u{2588}";
        let empty_char = "\u{2591}";
        let raw_bar = format!(
            "{}{}",
            filled_char.repeat(filled),
            empty_char.repeat(bar_width - filled)
        );
        if pct >= 90 {
            term::styled_error_raw(&raw_bar)
        } else if pct >= 75 {
            term::styled_warning_raw(&raw_bar)
        } else {
            term::styled_success_raw(&raw_bar)
        }
    } else {
        format!("{}{}", "=".repeat(filled), "-".repeat(bar_width - filled))
    };
    summary.replacen("{}", &bar, 1)
}

fn terminal_size() -> (usize, usize) {
    if let Ok((cols, rows)) = terminal::size() {
        return (usize::from(cols), usize::from(rows));
    }
    let columns = std::env::var("COLUMNS").ok();
    let rows = std::env::var("LINES").ok();
    fallback_terminal_size(columns.as_deref(), rows.as_deref())
}

fn fallback_terminal_size(columns: Option<&str>, rows: Option<&str>) -> (usize, usize) {
    (
        parse_terminal_env_size(columns, DEFAULT_WIDTH),
        parse_terminal_env_size(rows, DEFAULT_HEIGHT),
    )
}

fn parse_terminal_env_size(value: Option<&str>, default: usize) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
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
            b' ' => {
                keys.push(WatchKey::TogglePause);
                index += 1;
            }
            b'a' => {
                keys.push(WatchKey::ToggleAllLogs);
                index += 1;
            }
            b'd' => {
                keys.push(WatchKey::DebugHint);
                index += 1;
            }
            b'l' => {
                keys.push(WatchKey::LogsHint);
                index += 1;
            }
            b's' => {
                keys.push(WatchKey::StatsHint);
                index += 1;
            }
            b'?' => {
                keys.push(WatchKey::Help);
                index += 1;
            }
            b'/' => {
                keys.push(WatchKey::Search);
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
            0x1b if buffer.len().saturating_sub(index) >= 4
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'5'
                && buffer[index + 3] == b'~' =>
            {
                keys.push(WatchKey::PageUp);
                index += 4;
            }
            0x1b if buffer.len().saturating_sub(index) >= 4
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'6'
                && buffer[index + 3] == b'~' =>
            {
                keys.push(WatchKey::PageDown);
                index += 4;
            }
            0x1b if buffer.len().saturating_sub(index) >= 4
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'F'
                && buffer[index + 3] == b'~' =>
            {
                keys.push(WatchKey::End);
                index += 4;
            }
            0x1b if buffer.len().saturating_sub(index) >= 3
                && buffer[index + 1] == b'['
                && buffer[index + 2] == b'F' =>
            {
                keys.push(WatchKey::End);
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

const ANSI_RESET_ALL: &str = "\x1b[0m";

fn ansi_escape_len(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'\x1b') {
        return None;
    }
    match bytes.get(start + 1).copied() {
        Some(b'[') => {
            let mut index = start + 2;
            while let Some(&byte) = bytes.get(index) {
                if (0x40..=0x7e).contains(&byte) {
                    return Some(index - start + 1);
                }
                index += 1;
            }
            Some(bytes.len() - start)
        }
        Some(b']') => {
            let mut index = start + 2;
            while let Some(&byte) = bytes.get(index) {
                if byte == 0x07 {
                    return Some(index - start + 1);
                }
                if byte == b'\x1b' && bytes.get(index + 1) == Some(&b'\\') {
                    return Some(index - start + 2);
                }
                index += 1;
            }
            Some(bytes.len() - start)
        }
        _ => None,
    }
}

fn visible_width(value: &str) -> usize {
    let bytes = value.as_bytes();
    let mut width = 0;
    let mut index = 0;
    while index < value.len() {
        if let Some(len) = ansi_escape_len(bytes, index) {
            index += len;
            continue;
        }
        let ch = value[index..]
            .chars()
            .next()
            .expect("visible_width walked a valid UTF-8 boundary");
        width += 1;
        index += ch.len_utf8();
    }
    width
}

fn truncate_cell(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let bytes = value.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    let mut visible = 0;
    while index < value.len() {
        if let Some(len) = ansi_escape_len(bytes, index) {
            out.push_str(&value[index..index + len]);
            index += len;
            continue;
        }
        if visible >= width {
            break;
        }
        let ch = value[index..]
            .chars()
            .next()
            .expect("truncate_cell walked a valid UTF-8 boundary");
        out.push(ch);
        visible += 1;
        index += ch.len_utf8();
    }
    if visible >= width && value[index..].contains("\x1b[") {
        out.push_str(ANSI_RESET_ALL);
    }
    out
}

fn fit_line(value: &str, width: usize) -> String {
    pad_line(&truncate_cell(value, width), width)
}

fn pad_line(value: &str, width: usize) -> String {
    let len = visible_width(value);
    if len >= width {
        truncate_cell(value, width)
    } else {
        format!("{value}{}", " ".repeat(width - len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output;
    use hpc_compose::job::{
        PsSnapshot, QueueDiagnostics, RequestedWalltime, SchedulerOptions, SchedulerSource,
        SchedulerStatus, SubmissionBackend, SubmissionKind, SubmissionRecord, WalltimeProgress,
        WatchOutcome, build_submission_record_with_backend, state_path_for_record,
        write_submission_record,
    };

    fn sample_snapshot() -> PsSnapshot {
        PsSnapshot {
            record: SubmissionRecord {
                schema_version: 1,
                backend: hpc_compose::job::SubmissionBackend::Slurm,
                kind: SubmissionKind::Main,
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
                service_name: None,
                command_override: None,
                requested_walltime: Some(RequestedWalltime {
                    original: "00:10:00".into(),
                    seconds: 600,
                }),
                config_snapshot_yaml: None,
                cached_artifacts: Vec::new(),
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
                    completed_successfully: Some(false),
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
                    completed_successfully: Some(false),
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

    fn sample_watch_model() -> WatchModel {
        WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: Vec::new(),
            follow_logs: true,
            log_scroll: 0,
            log_view_mode: LogViewMode::Selected,
            hold_state: None,
            metrics_line: None,
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
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
            b'j', b'k', b'g', b'G', b'\t', 0x1b, b'[', b'A', 0x1b, b'[', b'B', b'q', b'?', b'/',
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
                WatchKey::Help,
                WatchKey::Search,
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
                walltime_progress: None,
                log_lines: vec!["booting".into(), "ready".into()],
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            18,
        );
        assert!(frame.contains("hpc-compose watch"));
        assert!(frame.contains("job 12345"));
        assert!(frame.contains("logs"));
        assert!(frame.contains(">"));
        assert!(frame.contains("api"));
        assert!(frame.contains("ready"));
        assert!(frame.contains("worker"));
        assert!(frame.contains("q quit"));
        assert!(frame.lines().count() <= 18);
    }

    #[test]
    fn render_watch_frame_normal_snapshot_stays_stable() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["booting".into(), "ready".into()],
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            18,
        );
        let lines = canonical_frame_lines(&frame);

        assert_snapshot_line(
            &lines,
            0,
            "hpc-compose watch | RUNNING (squeue) | job 12345",
        );
        assert_snapshot_line(
            &lines,
            1,
            "services: 2 | selected: api | logs: selected FOLLOW",
        );
        assert!(lines[4].contains("api"));
        assert!(lines[5].contains("booting"));
        assert!(lines.last().unwrap_or(&String::new()).contains("q quit"));
    }

    #[test]
    fn env_and_terminal_helpers_cover_force_and_fallback_paths() {
        assert!(force_watch_ui_from_value(Some(OsStr::new("1"))));
        assert!(!force_watch_ui_from_value(Some(OsStr::new("0"))));
        assert!(!force_watch_ui_from_value(None));

        assert!(watch_ui_available(true, false, false));
        assert!(watch_ui_available(false, true, true));
        assert!(!watch_ui_available(false, true, false));

        assert_eq!(fallback_terminal_size(Some("101"), Some("33")), (101, 33));
        assert_eq!(
            fallback_terminal_size(Some("bad"), Some("also-bad")),
            (DEFAULT_WIDTH, DEFAULT_HEIGHT)
        );
        assert_eq!(parse_terminal_env_size(Some("72"), DEFAULT_WIDTH), 72);
        assert_eq!(
            parse_terminal_env_size(Some("not-a-number"), DEFAULT_WIDTH),
            DEFAULT_WIDTH
        );
    }

    #[test]
    fn selection_and_formatting_helpers_cover_remaining_paths() {
        let snapshot = sample_snapshot();
        assert_eq!(initial_selected_index(&snapshot, None).expect("default"), 0);
        assert_eq!(
            initial_selected_index(&snapshot, Some("worker")).expect("selected worker"),
            1
        );
        let err = initial_selected_index(&snapshot, Some("missing")).expect_err("missing service");
        assert!(err.to_string().contains("does not exist"));

        let mut empty = snapshot.clone();
        empty.services.clear();
        assert_eq!(clamp_selected_index(&empty, 5), 0);
        assert_eq!(clamp_selected_index(&snapshot, 7), 1);

        assert_eq!(log_capacity(2), 4);
        assert_eq!(log_capacity(12), 6);
        assert_eq!(yes_no_short(true), "yes");
        assert_eq!(yes_no_short(false), "no");
        assert_eq!(truncate_cell("abcdef", 3), "abc");
        assert_eq!(fit_line("abcdef", 4), "abcd");
        assert_eq!(pad_line("abc", 5), "abc  ");
        assert_eq!(
            capped_lines(vec!["a".into(), "b".into(), "c".into()], 2),
            vec!["b", "c"]
        );
    }

    #[test]
    fn ansi_aware_formatting_uses_visible_width() {
        let truncated = fit_line("\x1b[31mabcdef\x1b[39m", 4);
        assert_eq!(visible_width(&truncated), 4);
        assert!(truncated.starts_with("\x1b[31m"));
        assert!(truncated.ends_with(ANSI_RESET_ALL));
        assert!(truncated.contains("abcd"));
        assert!(!truncated.contains("abcde"));

        let padded = pad_line("\x1b[32mabc\x1b[39m", 5);
        assert_eq!(visible_width(&padded), 5);
        assert!(padded.ends_with("  "));
    }

    fn strip_ansi_for_snapshot(value: &str) -> String {
        let bytes = value.as_bytes();
        let mut out = String::new();
        let mut index = 0;
        while index < value.len() {
            if let Some(len) = ansi_escape_len(bytes, index) {
                index += len;
                continue;
            }
            let ch = value[index..]
                .chars()
                .next()
                .expect("strip_ansi_for_snapshot walked a valid UTF-8 boundary");
            out.push(ch);
            index += ch.len_utf8();
        }
        out
    }

    fn canonical_frame_lines(frame: &str) -> Vec<String> {
        strip_ansi_for_snapshot(frame)
            .replace('\u{2502}', "|")
            .lines()
            .map(|line| line.trim_end().to_string())
            .collect()
    }

    fn assert_snapshot_line(lines: &[String], index: usize, expected: &str) {
        assert_eq!(
            lines.get(index).map(String::as_str),
            Some(expected),
            "unexpected snapshot line {index}"
        );
    }

    #[test]
    fn read_new_lines_and_selected_log_buffer_cover_growth_and_reset_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let log_path = tmpdir.path().join("service.log");
        fs::write(&log_path, "one\ntwo\npart").expect("seed log");

        let mut offset = 0;
        let mut pending = String::new();
        let lines = read_new_lines(&log_path, &mut offset, &mut pending).expect("initial read");
        assert_eq!(lines, vec!["one", "two"]);
        assert_eq!(pending, "part");

        fs::write(&log_path, "reset\n").expect("truncate log");
        let lines = read_new_lines(&log_path, &mut offset, &mut pending).expect("truncated read");
        assert_eq!(lines, vec!["reset"]);
        assert!(pending.is_empty());

        let missing = tmpdir.path().join("missing.log");
        let lines = read_new_lines(&missing, &mut offset, &mut pending).expect("missing log");
        assert!(lines.is_empty());
        assert_eq!(offset, 0);
        assert!(pending.is_empty());

        fs::write(&log_path, "alpha\nbeta\ngamma\n").expect("rewrite log");
        let row = PsServiceRow {
            service_name: "api".into(),
            path: log_path.clone(),
            present: true,
            updated_at: None,
            updated_age_seconds: None,
            log_path: Some(log_path.clone()),
            step_name: Some("hpc-compose:api".into()),
            launch_index: Some(0),
            launcher_pid: Some(4242),
            healthy: Some(true),
            completed_successfully: Some(false),
            readiness_configured: Some(true),
            status: Some("ready".into()),
            failure_policy_mode: None,
            restart_count: Some(0),
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
        };

        let mut buffer = SelectedLogBuffer::seed(Some(&row), 2, 2);
        assert_eq!(buffer.lines, vec!["beta", "gamma"]);

        fs::write(&log_path, "alpha\nbeta\ngamma\ndelta\n").expect("append log");
        buffer.refresh().expect("refresh");
        assert_eq!(buffer.lines, vec!["gamma", "delta"]);

        let other_path = tmpdir.path().join("worker.log");
        fs::write(&other_path, "worker-started\n").expect("other log");
        let other = PsServiceRow {
            service_name: "worker".into(),
            path: other_path.clone(),
            present: true,
            updated_at: None,
            updated_age_seconds: None,
            log_path: Some(other_path),
            step_name: Some("hpc-compose:worker".into()),
            launch_index: Some(1),
            launcher_pid: Some(5252),
            healthy: Some(false),
            completed_successfully: Some(false),
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
        };
        buffer.reseed_if_needed(Some(&other), 5, 4);
        assert_eq!(buffer.service_name, "worker");
        assert_eq!(buffer.lines, vec!["worker-started"]);

        buffer.reseed_if_needed(None, 5, 4);
        assert_eq!(buffer.service_name, "<none>");
        assert!(buffer.lines.is_empty());
    }

    #[test]
    fn render_watch_frame_prefers_detail_then_pending_reason() {
        let mut detail_snapshot = sample_snapshot();
        detail_snapshot.scheduler.detail = Some("visible in queue".into());
        let detail_frame = render_watch_frame(
            &WatchModel {
                snapshot: detail_snapshot,
                selected_index: 1,
                walltime_progress: None,
                log_lines: vec!["tail".into()],
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            90,
            14,
        );
        assert!(detail_frame.contains("note: visible in queue"));

        let mut pending_snapshot = sample_snapshot();
        pending_snapshot.scheduler.state = "PENDING".into();
        pending_snapshot.queue_diagnostics = Some(QueueDiagnostics {
            pending_reason: Some("Resources".into()),
            eligible_time: None,
            start_time: None,
        });
        let pending_frame = render_watch_frame(
            &WatchModel {
                snapshot: pending_snapshot,
                selected_index: 0,
                walltime_progress: None,
                log_lines: Vec::new(),
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            90,
            14,
        );
        assert!(pending_frame.contains("pending reason"));
        assert!(pending_frame.contains("Resources"));
    }

    #[test]
    fn render_watch_frame_includes_walltime_bar_when_available() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: Some(WalltimeProgress {
                    original: "00:10:00".into(),
                    elapsed_seconds: 300,
                    total_seconds: 600,
                    remaining_seconds: 300,
                }),
                log_lines: Vec::new(),
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            14,
        );
        assert!(frame.contains("walltime: ["));
        assert!(frame.contains("50% 00:05:00 / 00:10:00 remaining 00:05:00"));
    }

    #[test]
    fn terminal_guard_and_run_watch_ui_cover_interactive_paths() {
        let guard = TerminalGuard::enter().expect("enter terminal guard");
        drop(guard);

        render_model(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["line".into()],
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            (90, 14),
        )
        .expect("render model");

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {}\n    command: /bin/true\nx-slurm:\n  cache_dir: {}\n",
                local_image.display(),
                tmpdir.path().join("cache").display()
            ),
        )
        .expect("compose");
        let runtime_plan = output::load_runtime_plan(&compose).expect("runtime plan");
        let script_path = tmpdir.path().join("job.local.sh");
        let record = build_submission_record_with_backend(
            &compose,
            tmpdir.path(),
            &script_path,
            &runtime_plan,
            "local-watch-ui-123",
            SubmissionBackend::Local,
        )
        .expect("record");
        write_submission_record(&record).expect("write record");

        let state_path = state_path_for_record(&record);
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent).expect("state dir");
        }
        fs::write(
            &state_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "backend": SubmissionBackend::Local,
                "job_status": "COMPLETED",
                "job_exit_code": 0,
                "supervisor_pid": serde_json::Value::Null,
                "services": []
            }))
            .expect("state json"),
        )
        .expect("write state");

        let outcome = run_watch_ui(
            &record,
            &SchedulerOptions {
                squeue_bin: "/definitely/missing-squeue".into(),
                sacct_bin: "/definitely/missing-sacct".into(),
            },
            None,
            5,
            HoldOnExit::Never,
        )
        .expect("run watch ui");
        assert!(matches!(outcome, WatchOutcome::Completed(_)));
    }

    #[test]
    fn filtered_services_narrows_by_name() {
        let snapshot = sample_snapshot();
        let all = filtered_services(&snapshot.services, None);
        assert_eq!(all.len(), 2);
        let narrowed = filtered_services(&snapshot.services, Some("api"));
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].service_name, "api");
        let none = filtered_services(&snapshot.services, Some("missing"));
        assert_eq!(none.len(), 0);
    }

    #[test]
    fn render_watch_frame_shows_help_overlay() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: Vec::new(),
                show_help: true,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            22,
        );
        assert!(frame.contains("Keybindings:"));
        assert!(frame.contains("j / Down"));
        assert!(frame.contains("q           quit"));
        assert!(frame.contains("q quit"));
        assert!(frame.lines().count() <= 22);
    }

    #[test]
    fn render_watch_frame_help_snapshot_stays_stable() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: Vec::new(),
                show_help: true,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            22,
        );
        let lines = canonical_frame_lines(&frame);

        assert!(lines.iter().any(|line| line == "Keybindings:"));
        assert!(
            lines
                .iter()
                .any(|line| line == "  /           filter services")
        );
        assert!(lines.iter().any(|line| line == "  q           quit"));
        assert!(lines.last().unwrap_or(&String::new()).contains("q quit"));
    }

    #[test]
    fn render_watch_frame_shows_filter_indicator() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: Vec::new(),
                show_help: false,
                filter: Some("api".into()),
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            14,
        );
        assert!(frame.contains("filter: api"));
    }

    #[test]
    fn render_watch_frame_filtered_snapshot_stays_stable() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: Vec::new(),
                show_help: false,
                filter: Some("api".into()),
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            100,
            14,
        );
        let lines = canonical_frame_lines(&frame);

        assert_snapshot_line(
            &lines,
            0,
            "hpc-compose watch | RUNNING (squeue) | job 12345 | filter: api",
        );
        assert_snapshot_line(
            &lines,
            1,
            "services: 1 | selected: api | logs: selected FOLLOW",
        );
        assert!(lines.iter().any(|line| line.contains("> api")));
        assert!(!lines.iter().any(|line| line.contains("worker")));
    }

    #[test]
    fn render_watch_frame_bounds_footer_search_and_help() {
        let search_frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["tail".into()],
                show_help: false,
                filter: None,
                search_buffer: "api".into(),
                input_mode: InputMode::Search,
                ..sample_watch_model()
            },
            90,
            12,
        );
        assert!(search_frame.contains("filter: api"));
        assert!(
            search_frame
                .lines()
                .last()
                .unwrap_or("")
                .contains("Enter apply")
        );
        assert!(search_frame.lines().count() <= 12);

        let help_frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["tail".into()],
                show_help: true,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            90,
            12,
        );
        assert!(help_frame.contains("Keybindings:"));
        assert!(help_frame.lines().last().unwrap_or("").contains("q quit"));
        assert!(help_frame.lines().count() <= 12);
    }

    #[test]
    fn render_watch_frame_respects_narrow_terminal_dimensions() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: Some(WalltimeProgress {
                    original: "00:10:00".into(),
                    elapsed_seconds: 300,
                    total_seconds: 600,
                    remaining_seconds: 300,
                }),
                log_lines: vec![
                    "a deliberately long log line that must not wrap the terminal".into(),
                    "ready".into(),
                ],
                show_help: true,
                filter: Some("api".into()),
                search_buffer: "api".into(),
                input_mode: InputMode::Search,
                ..sample_watch_model()
            },
            48,
            9,
        );

        let lines = frame.lines().collect::<Vec<_>>();
        assert!(lines.len() <= 9);
        assert!(lines.iter().all(|line| visible_width(line) <= 48));
        assert!(frame.contains("hpc-compose watch"));
        assert!(frame.contains("RUNNING"));
    }

    #[test]
    fn render_watch_frame_compact_snapshot_stays_stable() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["tail".into()],
                show_help: true,
                filter: Some("api".into()),
                search_buffer: "api".into(),
                input_mode: InputMode::Search,
                ..sample_watch_model()
            },
            48,
            9,
        );
        let lines = canonical_frame_lines(&frame);

        assert_snapshot_line(&lines, 0, "hpc-compose watch | job 12345");
        assert_snapshot_line(&lines, 2, "filter: api");
        assert_snapshot_line(&lines, 3, "filter input: api");
        assert_snapshot_line(&lines, 4, "? help | / filter | Space pause | q quit");
        assert_snapshot_line(&lines, 6, "> api OK ready=yes");
        assert_eq!(lines.len(), 9);
    }

    #[test]
    fn render_watch_frame_handles_tiny_terminal_without_overflow() {
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: sample_snapshot(),
                selected_index: 0,
                walltime_progress: None,
                log_lines: vec!["tail".into()],
                show_help: false,
                filter: None,
                search_buffer: String::new(),
                input_mode: InputMode::Normal,
                ..sample_watch_model()
            },
            12,
            3,
        );

        let lines = frame.lines().collect::<Vec<_>>();
        assert!(lines.len() <= 3);
        assert!(lines.iter().all(|line| visible_width(line) <= 12));
        assert!(frame.contains("hpc-compose"));
    }

    #[test]
    fn search_keys_parse_correctly() {
        let mut buf = vec![b'a', b'b', 0x7f, b'\n'];
        let keys = parse_search_keys(&mut buf);
        assert_eq!(
            keys,
            vec![
                SearchKey::Char('a'),
                SearchKey::Char('b'),
                SearchKey::Backspace,
                SearchKey::Submit,
            ]
        );

        let mut cancel_buf = vec![0x1b];
        let keys = parse_search_keys(&mut cancel_buf);
        assert_eq!(keys, vec![SearchKey::Cancel]);
    }
}
