use crate::term;

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};
use std::panic::PanicHookInfo;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use crossterm::cursor::MoveTo;
#[cfg(not(test))]
use crossterm::event::EnableMouseCapture;
use crossterm::event::{
    self, DisableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{self, Clear, ClearType};
use hpc_compose::cli::HoldOnExit;
use hpc_compose::job::{
    PsServiceRow, PsSnapshot, ReplayReport, SchedulerOptions, StatsOptions, StatsSnapshot,
    SubmissionBackend, SubmissionRecord, WalltimeProgress, WatchOutcome, build_ps_snapshot,
    build_stats_snapshot, format_walltime_summary, runtime_job_root_for_record, walltime_progress,
    walltime_progress_percent,
};

const DATA_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const INPUT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_WIDTH: usize = 120;
const DEFAULT_HEIGHT: usize = 30;
const MIN_TABLE_WIDTH: usize = 58;
const FORCE_WATCH_UI_ENV: &str = "HPC_COMPOSE_FORCE_WATCH_UI";
const METRICS_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const DATA_REFRESH_ENV: &str = "HPC_COMPOSE_WATCH_REFRESH_MS";
const METRICS_REFRESH_ENV: &str = "HPC_COMPOSE_WATCH_METRICS_REFRESH_MS";
const NOTICE_DURATION: Duration = Duration::from_secs(4);
const WATCH_MOUSE_ENV: &str = "HPC_COMPOSE_WATCH_MOUSE";

type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Sync + Send + 'static>;
type SharedPanicHook = Arc<Mutex<Option<PanicHook>>>;

/// Resolves a refresh interval with precedence: env override > settings value >
/// built-in default. Both numeric sources are clamped to `[min_ms, max_ms]`.
fn resolve_interval(
    env_name: &str,
    settings_ms: Option<u64>,
    default: Duration,
    min_ms: u64,
    max_ms: u64,
) -> Duration {
    if let Some(from_env) =
        env_refresh_interval_opt(std::env::var(env_name).ok().as_deref(), min_ms, max_ms)
    {
        return from_env;
    }
    settings_ms
        .map(|ms| Duration::from_millis(ms.clamp(min_ms, max_ms)))
        .unwrap_or(default)
}

/// Parses a clamped interval from a raw env value, or `None` when unset/invalid.
fn env_refresh_interval_opt(value: Option<&str>, min_ms: u64, max_ms: u64) -> Option<Duration> {
    value
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .map(|ms| Duration::from_millis(ms.clamp(min_ms, max_ms)))
}

/// Reads a boolean env toggle (`Some(false)` only for `"0"`).
fn env_bool(name: &str) -> Option<bool> {
    std::env::var_os(name).map(|value| value != OsStr::new("0"))
}

/// Resolved watch/replay display preferences threaded into the loops.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchPrefs {
    pub(crate) sort: ServiceSort,
    pub(crate) wrap: bool,
    pub(crate) data_refresh: Duration,
    pub(crate) metrics_refresh: Duration,
    pub(crate) mouse: bool,
}

impl Default for WatchPrefs {
    fn default() -> Self {
        Self {
            sort: ServiceSort::Spec,
            wrap: false,
            data_refresh: DATA_REFRESH_INTERVAL,
            metrics_refresh: METRICS_REFRESH_INTERVAL,
            mouse: false,
        }
    }
}

impl WatchPrefs {
    /// Resolves prefs from settings, with env vars taking precedence.
    pub(crate) fn resolve(settings: &hpc_compose::context::WatchSettings) -> Self {
        Self {
            sort: match settings.sort.as_deref() {
                Some("triage") => ServiceSort::Triage,
                _ => ServiceSort::Spec,
            },
            wrap: settings.wrap.unwrap_or(false),
            data_refresh: resolve_interval(
                DATA_REFRESH_ENV,
                settings.refresh_ms,
                DATA_REFRESH_INTERVAL,
                100,
                60_000,
            ),
            metrics_refresh: resolve_interval(
                METRICS_REFRESH_ENV,
                settings.metrics_refresh_ms,
                METRICS_REFRESH_INTERVAL,
                500,
                600_000,
            ),
            mouse: env_bool(WATCH_MOUSE_ENV)
                .or(settings.mouse)
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchKey {
    Up,
    Down,
    PageUp,
    PageDown,
    First,
    Last,
    End,
    SeekBackward,
    SeekForward,
    PreviousEvent,
    NextEvent,
    ReplayStart,
    SpeedDown,
    SpeedUp,
    Tab,
    TogglePause,
    ToggleAllLogs,
    DebugHint,
    LogsHint,
    StatsHint,
    Quit,
    Help,
    Search,
    LogSearch,
    ToggleWrap,
    CycleSort,
    Restart,
    ShowDetail,
    Yank,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InputMode {
    Normal,
    /// Typing a service-name filter.
    Search,
    /// Typing a query to find within log content.
    LogSearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogViewMode {
    Selected,
    All,
}

/// Ordering applied to the service table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServiceSort {
    /// Declaration order from the compose spec (default).
    Spec,
    /// Surface problems first: failed, then unhealthy, then the rest.
    Triage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WatchHoldState {
    pub(crate) failed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReplayWatchStatus {
    pub(crate) cursor_unix: u64,
    pub(crate) speed: f64,
    pub(crate) paused: bool,
    pub(crate) fidelity: String,
    /// Timeline bounds and event positions for the scrubber bar.
    pub(crate) start_unix: u64,
    pub(crate) end_unix: u64,
    pub(crate) event_unix: Vec<u64>,
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
    /// Active in-log search query; highlights matches in the log pane.
    pub(crate) log_query: Option<String>,
    /// When set, wrap long log lines instead of truncating them.
    pub(crate) log_wrap: bool,
    /// Ordering applied to the service table.
    pub(crate) sort_mode: ServiceSort,
    /// Transient status line (e.g. restart feedback), shown briefly.
    pub(crate) notice: Option<String>,
    /// When set, the body shows a detail panel for the selected service.
    pub(crate) show_detail: bool,
    pub(crate) replay: Option<ReplayWatchStatus>,
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

struct TerminalGuard {
    entered_terminal: bool,
    restore_armed: Arc<AtomicBool>,
    previous_hook: Option<SharedPanicHook>,
}

impl TerminalGuard {
    fn enter(mouse: bool) -> Result<Self> {
        #[cfg(test)]
        {
            let _ = mouse;
            Ok(Self::new(false))
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
            if mouse {
                execute!(stdout, EnableMouseCapture).context("failed to enable mouse capture")?;
            }
            stdout
                .flush()
                .context("failed to flush alternate-screen entry")?;
            Ok(Self::new(true))
        }
    }

    fn new(entered_terminal: bool) -> Self {
        let restore_armed = Arc::new(AtomicBool::new(true));
        let previous_hook =
            install_terminal_panic_hook(entered_terminal, Arc::clone(&restore_armed));
        Self {
            entered_terminal,
            restore_armed,
            previous_hook,
        }
    }

    #[cfg(test)]
    fn panic_restore_armed(&self) -> bool {
        self.restore_armed.load(Ordering::SeqCst)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.restore_armed.store(false, Ordering::SeqCst);
        if self.entered_terminal {
            restore_terminal_best_effort();
        }
        if let Some(previous_hook) = self.previous_hook.take() {
            restore_previous_panic_hook(previous_hook);
        }
    }
}

#[cfg(not(test))]
fn install_terminal_panic_hook(
    entered_terminal: bool,
    restore_armed: Arc<AtomicBool>,
) -> Option<SharedPanicHook> {
    let previous_hook = std::panic::take_hook();
    let previous_hook = Arc::new(Mutex::new(Some(previous_hook)));
    let hook_previous = Arc::clone(&previous_hook);
    std::panic::set_hook(Box::new(move |info| {
        if entered_terminal && restore_armed.swap(false, Ordering::SeqCst) {
            restore_terminal_best_effort();
        }
        if let Ok(guard) = hook_previous.lock()
            && let Some(previous_hook) = guard.as_ref()
        {
            previous_hook(info);
        }
    }));
    Some(previous_hook)
}

#[cfg(test)]
fn install_terminal_panic_hook(
    _entered_terminal: bool,
    _restore_armed: Arc<AtomicBool>,
) -> Option<SharedPanicHook> {
    None
}

#[cfg(not(test))]
fn restore_previous_panic_hook(previous_hook: SharedPanicHook) {
    if let Ok(mut guard) = previous_hook.lock()
        && let Some(previous_hook) = guard.take()
    {
        std::panic::set_hook(previous_hook);
    }
}

#[cfg(test)]
fn restore_previous_panic_hook(_previous_hook: SharedPanicHook) {}

fn restore_terminal_best_effort() {
    let mut stdout = io::stdout();
    // DisableMouseCapture is harmless when capture was never enabled, so it is
    // always sent here (including from the panic hook, which has no state).
    let _ = execute!(
        stdout,
        DisableMouseCapture,
        crossterm::cursor::Show,
        crossterm::terminal::LeaveAlternateScreen
    );
    let _ = terminal::disable_raw_mode();
    let _ = stdout.flush();
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
    prefs: WatchPrefs,
) -> Result<WatchOutcome> {
    let guard = TerminalGuard::enter(prefs.mouse)?;
    let mut events = TerminalEventSource;
    let result = run_watch_ui_loop(
        record,
        options,
        initial_service,
        lines,
        hold_on_exit,
        &mut events,
        prefs,
    );
    drop(guard);
    let result = result?;
    if let Some(command) = result.command_hint {
        println!("next command: {command}");
    }
    Ok(result.outcome)
}

pub(crate) fn run_replay_ui(
    report: &ReplayReport,
    initial_service: Option<&str>,
    lines: usize,
    speed: f64,
    prefs: WatchPrefs,
) -> Result<()> {
    let guard = TerminalGuard::enter(prefs.mouse)?;
    let mut events = TerminalEventSource;
    let result = run_replay_ui_loop(report, initial_service, lines, speed, &mut events, prefs);
    drop(guard);
    result.map(|_| ())
}

#[derive(Debug)]
struct WatchLoopResult {
    outcome: WatchOutcome,
    command_hint: Option<String>,
    /// Service focused when the loop exited, if any matched the active filter.
    /// Only inspected by tests; production drives behavior off `outcome`.
    #[cfg_attr(not(test), allow(dead_code))]
    selected_service: Option<String>,
}

/// Final UI state captured when a replay loop exits. Lets tests assert on the
/// state machine without a TTY; production discards it via `map(|_| ())`.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
struct ReplayLoopResult {
    selected_service: Option<String>,
    filter: Option<String>,
    log_view_mode: LogViewMode,
    log_query: Option<String>,
    log_wrap: bool,
    sort_mode: ServiceSort,
    show_detail: bool,
    playback: ReplayPlaybackState,
}

fn run_replay_ui_loop(
    report: &ReplayReport,
    initial_service: Option<&str>,
    lines: usize,
    speed: f64,
    events: &mut dyn WatchEventSource,
    prefs: WatchPrefs,
) -> Result<ReplayLoopResult> {
    if report.frames.is_empty() {
        return Ok(ReplayLoopResult {
            selected_service: None,
            filter: None,
            log_view_mode: LogViewMode::Selected,
            log_query: None,
            log_wrap: false,
            sort_mode: ServiceSort::Spec,
            show_detail: false,
            playback: ReplayPlaybackState::new(report, speed),
        });
    }
    let mut playback = ReplayPlaybackState::new(report, speed);
    let mut snapshot = report.frames[playback.frame_index].snapshot.clone();
    let initial_selected_index = initial_selected_index(&snapshot, initial_service)?;
    let mut filter: Option<String> = None;
    let mut sort_mode = prefs.sort;
    let initial_selected_service = snapshot
        .services
        .get(initial_selected_index)
        .map(|row| row.service_name.as_str());
    let mut selected_index = preserve_selected_index(
        &snapshot.services,
        filter.as_deref(),
        sort_mode,
        initial_selected_service,
        initial_selected_index,
    );
    let (_, height) = terminal_size();
    let mut log_buffer = SelectedLogBuffer::seed(
        selected_effective_service(
            &snapshot.services,
            filter.as_deref(),
            sort_mode,
            selected_index,
        ),
        lines,
        log_capacity(height),
    );
    let mut show_help = false;
    let mut input_mode = InputMode::Normal;
    let mut search_buffer = String::new();
    let mut log_scroll = 0usize;
    let mut log_view_mode = LogViewMode::Selected;
    let mut log_query: Option<String> = None;
    let mut log_wrap = prefs.wrap;
    let mut show_detail = false;
    let mut last_tick = Instant::now();
    let mut renderer = FrameRenderer::new();

    loop {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(last_tick);
        last_tick = now;
        if !playback.paused {
            let advanced = playback.cursor_unix as f64 + elapsed.as_secs_f64() * playback.speed;
            playback.cursor_unix = report.clamp_cursor(advanced.floor().max(0.0) as u64);
            playback.frame_index = report.frame_index_at_or_before(playback.cursor_unix);
            if Some(playback.cursor_unix) == report.timeline_end_unix {
                playback.paused = true;
            }
        }

        let frame = &report.frames[playback.frame_index];
        let selected_name = selected_service_name(
            &snapshot.services,
            filter.as_deref(),
            sort_mode,
            selected_index,
        );
        snapshot = frame.snapshot.clone();
        let effective = effective_services(&snapshot.services, filter.as_deref(), sort_mode);
        selected_index =
            preserve_selected_index_raw(&effective, selected_name.as_deref(), selected_index);
        let (_, height) = terminal_size();
        let resolved = effective.get(selected_index);
        let original_index = resolved.and_then(|row| {
            snapshot
                .services
                .iter()
                .position(|service| service.service_name == row.service_name)
        });
        log_buffer.reseed_if_needed(
            original_index.map(|index| &snapshot.services[index]),
            lines,
            log_capacity(height),
        );
        let all_log_lines = build_all_log_lines(&snapshot, lines, log_capacity(height));
        let displayed_log_lines = match log_view_mode {
            LogViewMode::Selected => log_buffer.lines.clone(),
            LogViewMode::All => all_log_lines.clone(),
        };

        let (frame_width, frame_height) = terminal_size();
        renderer.render(
            &render_watch_frame(
                &WatchModel {
                    snapshot: snapshot.clone(),
                    selected_index,
                    walltime_progress: None,
                    log_lines: displayed_log_lines,
                    follow_logs: false,
                    log_scroll,
                    log_view_mode,
                    hold_state: None,
                    metrics_line: frame.metrics_line.clone(),
                    show_help,
                    filter: filter.clone(),
                    search_buffer: search_buffer.clone(),
                    input_mode,
                    log_query: log_query.clone(),
                    log_wrap,
                    sort_mode,
                    notice: None,
                    show_detail,
                    replay: Some(ReplayWatchStatus {
                        cursor_unix: playback.cursor_unix,
                        speed: playback.speed,
                        paused: playback.paused,
                        fidelity: report.fidelity.clone(),
                        start_unix: report.timeline_start_unix.unwrap_or(0),
                        end_unix: report.timeline_end_unix.unwrap_or(0),
                        event_unix: report.events.iter().map(|event| event.at_unix).collect(),
                    }),
                },
                frame_width,
                frame_height,
            ),
            (frame_width, frame_height),
        )?;

        if let Some(event) = events.poll_event(INPUT_POLL_INTERVAL, input_mode)? {
            if input_mode == InputMode::Search || input_mode == InputMode::LogSearch {
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
                    SearchKey::Clear => search_buffer.clear(),
                    SearchKey::Submit => {
                        let value = (!search_buffer.is_empty()).then(|| search_buffer.clone());
                        if input_mode == InputMode::LogSearch {
                            log_query = value;
                        } else {
                            filter = value;
                            selected_index = 0;
                        }
                        input_mode = InputMode::Normal;
                    }
                    SearchKey::Cancel => {
                        search_buffer.clear();
                        input_mode = InputMode::Normal;
                    }
                }
            } else if show_detail && matches!(event, WatchInput::Search(SearchKey::Cancel)) {
                show_detail = false;
            } else if let WatchInput::Normal(key) = event {
                match key {
                    WatchKey::Quit => break,
                    WatchKey::Help => show_help = !show_help,
                    WatchKey::ShowDetail => show_detail = !show_detail,
                    WatchKey::Search => {
                        input_mode = InputMode::Search;
                        search_buffer = filter.clone().unwrap_or_default();
                    }
                    WatchKey::LogSearch => {
                        input_mode = InputMode::LogSearch;
                        search_buffer = log_query.clone().unwrap_or_default();
                    }
                    WatchKey::ToggleWrap => {
                        log_wrap = !log_wrap;
                        log_scroll = 0;
                    }
                    WatchKey::CycleSort => {
                        let current =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode)
                                .get(selected_index)
                                .map(|row| row.service_name.clone());
                        sort_mode = match sort_mode {
                            ServiceSort::Spec => ServiceSort::Triage,
                            ServiceSort::Triage => ServiceSort::Spec,
                        };
                        if let Some(name) = current {
                            let reordered = effective_services(
                                &snapshot.services,
                                filter.as_deref(),
                                sort_mode,
                            );
                            if let Some(index) =
                                reordered.iter().position(|row| row.service_name == name)
                            {
                                selected_index = index;
                            }
                        }
                    }
                    WatchKey::TogglePause
                    | WatchKey::SeekBackward
                    | WatchKey::SeekForward
                    | WatchKey::PreviousEvent
                    | WatchKey::NextEvent
                    | WatchKey::ReplayStart
                    | WatchKey::SpeedDown
                    | WatchKey::SpeedUp => {
                        playback = apply_replay_key(playback, report, key);
                    }
                    WatchKey::ToggleAllLogs => {
                        log_view_mode = match log_view_mode {
                            LogViewMode::Selected => LogViewMode::All,
                            LogViewMode::All => LogViewMode::Selected,
                        };
                        log_scroll = 0;
                    }
                    WatchKey::PageUp => {
                        log_scroll = log_scroll.saturating_add(10);
                    }
                    WatchKey::PageDown => {
                        log_scroll = log_scroll.saturating_sub(10);
                    }
                    WatchKey::End => {
                        log_scroll = 0;
                        playback = apply_replay_key(playback, report, WatchKey::Last);
                    }
                    other => {
                        let effective =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode);
                        selected_index = apply_watch_key(selected_index, effective.len(), other);
                        log_scroll = 0;
                    }
                }
            }
        }
    }

    let selected_service = effective_services(&snapshot.services, filter.as_deref(), sort_mode)
        .get(selected_index)
        .map(|row| row.service_name.clone());
    Ok(ReplayLoopResult {
        selected_service,
        filter,
        log_view_mode,
        log_query,
        log_wrap,
        sort_mode,
        show_detail,
        playback,
    })
}

fn run_watch_ui_loop(
    record: &SubmissionRecord,
    options: &SchedulerOptions,
    initial_service: Option<&str>,
    lines: usize,
    hold_on_exit: HoldOnExit,
    events: &mut dyn WatchEventSource,
    prefs: WatchPrefs,
) -> Result<WatchLoopResult> {
    let mut snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
    let data_refresh = prefs.data_refresh;
    let metrics_refresh = prefs.metrics_refresh;
    let mut filter: Option<String> = None;
    let mut sort_mode = prefs.sort;
    let initial_selected_index = initial_selected_index(&snapshot, initial_service)?;
    let initial_selected_service = snapshot
        .services
        .get(initial_selected_index)
        .map(|row| row.service_name.as_str());
    let mut selected_index = preserve_selected_index(
        &snapshot.services,
        filter.as_deref(),
        sort_mode,
        initial_selected_service,
        initial_selected_index,
    );
    let (_, height) = terminal_size();
    let mut log_buffer = SelectedLogBuffer::seed(
        selected_effective_service(
            &snapshot.services,
            filter.as_deref(),
            sort_mode,
            selected_index,
        ),
        lines,
        log_capacity(height),
    );
    let mut all_log_lines = build_all_log_lines(&snapshot, lines, log_capacity(height));
    let mut last_refresh = Instant::now();
    let mut last_metrics_refresh = Instant::now()
        .checked_sub(metrics_refresh)
        .unwrap_or_else(Instant::now);
    let mut metrics_line = None;
    let mut show_help = false;
    let mut input_mode = InputMode::Normal;
    let mut search_buffer = String::new();
    let mut follow_logs = true;
    let mut log_scroll = 0usize;
    let mut log_view_mode = LogViewMode::Selected;
    let mut log_query: Option<String> = None;
    let mut log_wrap = prefs.wrap;
    let mut notice: Option<String> = None;
    let mut notice_until: Option<Instant> = None;
    let mut show_detail = false;
    let mut terminal_outcome: Option<WatchOutcome> = None;
    let mut renderer = FrameRenderer::new();

    let (outcome, command_hint) = loop {
        if last_refresh.elapsed() >= data_refresh {
            let selected_name = selected_service_name(
                &snapshot.services,
                filter.as_deref(),
                sort_mode,
                selected_index,
            );
            snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), options)?;
            let effective = effective_services(&snapshot.services, filter.as_deref(), sort_mode);
            selected_index =
                preserve_selected_index_raw(&effective, selected_name.as_deref(), selected_index);
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
        if last_metrics_refresh.elapsed() >= metrics_refresh {
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
                && let Some(failed_service) = first_failed_service_name(&snapshot.services)
            {
                filter = None;
                selected_index = preserve_selected_index(
                    &snapshot.services,
                    filter.as_deref(),
                    sort_mode,
                    Some(failed_service),
                    selected_index,
                );
                log_buffer.reseed_if_needed(
                    selected_effective_service(
                        &snapshot.services,
                        filter.as_deref(),
                        sort_mode,
                        selected_index,
                    ),
                    lines,
                    log_capacity(terminal_size().1),
                );
            }
            if should_hold_on_exit(hold_on_exit, &outcome) {
                terminal_outcome = Some(outcome);
            } else {
                break (outcome, None);
            }
        }

        // Expire a transient notice once its display window has passed.
        if notice_until.is_some_and(|deadline| Instant::now() >= deadline) {
            notice = None;
            notice_until = None;
        }

        let displayed_log_lines = match log_view_mode {
            LogViewMode::Selected => log_buffer.lines.clone(),
            LogViewMode::All => all_log_lines.clone(),
        };

        let (frame_width, frame_height) = terminal_size();
        renderer.render(
            &render_watch_frame(
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
                    log_query: log_query.clone(),
                    log_wrap,
                    sort_mode,
                    notice: notice.clone(),
                    show_detail,
                    replay: None,
                },
                frame_width,
                frame_height,
            ),
            (frame_width, frame_height),
        )?;

        if let Some(event) = events.poll_event(INPUT_POLL_INTERVAL, input_mode)? {
            if input_mode == InputMode::Search || input_mode == InputMode::LogSearch {
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
                        let value = (!search_buffer.is_empty()).then(|| search_buffer.clone());
                        if input_mode == InputMode::LogSearch {
                            log_query = value;
                        } else {
                            filter = value;
                            selected_index = 0;
                        }
                        input_mode = InputMode::Normal;
                    }
                    SearchKey::Cancel => {
                        search_buffer.clear();
                        input_mode = InputMode::Normal;
                    }
                }
            } else if show_detail && matches!(event, WatchInput::Search(SearchKey::Cancel)) {
                show_detail = false;
            } else if let WatchInput::Normal(key) = event {
                let held_outcome = terminal_outcome.clone();
                match key {
                    WatchKey::Quit => {
                        break (
                            held_outcome.unwrap_or_else(|| {
                                WatchOutcome::Interrupted(snapshot.scheduler.clone())
                            }),
                            None,
                        );
                    }
                    WatchKey::Help => {
                        show_help = !show_help;
                    }
                    WatchKey::ShowDetail => {
                        show_detail = !show_detail;
                    }
                    WatchKey::Search => {
                        input_mode = InputMode::Search;
                        search_buffer = filter.clone().unwrap_or_default();
                    }
                    WatchKey::LogSearch => {
                        input_mode = InputMode::LogSearch;
                        search_buffer = log_query.clone().unwrap_or_default();
                    }
                    WatchKey::ToggleWrap => {
                        log_wrap = !log_wrap;
                        log_scroll = 0;
                    }
                    WatchKey::CycleSort => {
                        let current =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode)
                                .get(selected_index)
                                .map(|row| row.service_name.clone());
                        sort_mode = match sort_mode {
                            ServiceSort::Spec => ServiceSort::Triage,
                            ServiceSort::Triage => ServiceSort::Spec,
                        };
                        // Keep the same service selected across the reorder.
                        if let Some(name) = current {
                            let reordered = effective_services(
                                &snapshot.services,
                                filter.as_deref(),
                                sort_mode,
                            );
                            if let Some(index) =
                                reordered.iter().position(|row| row.service_name == name)
                            {
                                selected_index = index;
                            }
                        }
                    }
                    WatchKey::Restart => {
                        let target =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode)
                                .get(selected_index)
                                .map(|row| row.service_name.clone());
                        notice = Some(match target {
                            None => "restart: no service selected".to_string(),
                            Some(_) if !restart_supported(record) => {
                                "restart: only supported for local supervised jobs".to_string()
                            }
                            Some(service) => match request_service_restart(record, &service) {
                                Ok(_) => format!("restart requested: {service}"),
                                Err(err) => format!("restart failed: {err}"),
                            },
                        });
                        notice_until = Some(Instant::now() + NOTICE_DURATION);
                    }
                    WatchKey::Yank => {
                        let target =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode)
                                .get(selected_index)
                                .map(|row| row.service_name.clone());
                        notice = Some(match target {
                            None => "yank: no service selected".to_string(),
                            Some(service) => {
                                let command = command_hint_for_key(
                                    WatchKey::LogsHint,
                                    record,
                                    Some(service.as_str()),
                                );
                                match copy_to_clipboard(&command) {
                                    Ok(()) => format!("copied logs command for {service}"),
                                    Err(err) => format!("yank failed: {err}"),
                                }
                            }
                        });
                        notice_until = Some(Instant::now() + NOTICE_DURATION);
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
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode)
                                .get(selected_index)
                                .map(|row| row.service_name.as_str()),
                        );
                        break (
                            held_outcome.expect("held outcome checked above"),
                            Some(command),
                        );
                    }
                    other => {
                        let effective =
                            effective_services(&snapshot.services, filter.as_deref(), sort_mode);
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
    };

    let selected_service = effective_services(&snapshot.services, filter.as_deref(), sort_mode)
        .get(selected_index)
        .map(|row| row.service_name.clone());
    Ok(WatchLoopResult {
        outcome,
        command_hint,
        selected_service,
    })
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
        WatchKey::SeekBackward
        | WatchKey::SeekForward
        | WatchKey::PreviousEvent
        | WatchKey::NextEvent
        | WatchKey::ReplayStart
        | WatchKey::SpeedDown
        | WatchKey::SpeedUp
        | WatchKey::PageUp
        | WatchKey::PageDown
        | WatchKey::End
        | WatchKey::TogglePause
        | WatchKey::ToggleAllLogs
        | WatchKey::DebugHint
        | WatchKey::LogsHint
        | WatchKey::StatsHint
        | WatchKey::Quit
        | WatchKey::Help
        | WatchKey::Search
        | WatchKey::LogSearch
        | WatchKey::ToggleWrap
        | WatchKey::CycleSort
        | WatchKey::Restart
        | WatchKey::ShowDetail
        | WatchKey::Yank => selected_index,
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ReplayPlaybackState {
    pub(crate) frame_index: usize,
    pub(crate) cursor_unix: u64,
    pub(crate) paused: bool,
    pub(crate) speed: f64,
}

impl ReplayPlaybackState {
    pub(crate) fn new(report: &ReplayReport, speed: f64) -> Self {
        let cursor_unix = report.timeline_start_unix.unwrap_or(0);
        Self {
            frame_index: 0,
            cursor_unix,
            paused: false,
            speed,
        }
    }
}

pub(crate) fn apply_replay_key(
    mut state: ReplayPlaybackState,
    report: &ReplayReport,
    key: WatchKey,
) -> ReplayPlaybackState {
    if report.frames.is_empty() {
        return state;
    }
    match key {
        WatchKey::TogglePause => state.paused = !state.paused,
        WatchKey::SeekBackward => {
            state.cursor_unix = report.clamp_cursor(state.cursor_unix.saturating_sub(5));
            state.frame_index = report.frame_index_at_or_before(state.cursor_unix);
            state.paused = true;
        }
        WatchKey::SeekForward => {
            state.cursor_unix = report.clamp_cursor(state.cursor_unix.saturating_add(5));
            state.frame_index = report.frame_index_at_or_before(state.cursor_unix);
            state.paused = true;
        }
        WatchKey::PreviousEvent => {
            state.frame_index = state.frame_index.saturating_sub(1);
            state.cursor_unix = report.frames[state.frame_index].cursor_unix;
            state.paused = true;
        }
        WatchKey::NextEvent => {
            state.frame_index = (state.frame_index + 1).min(report.frames.len() - 1);
            state.cursor_unix = report.frames[state.frame_index].cursor_unix;
            state.paused = true;
        }
        WatchKey::ReplayStart => {
            state.frame_index = 0;
            state.cursor_unix = report.frames[0].cursor_unix;
            state.paused = true;
        }
        WatchKey::Last | WatchKey::End => {
            state.frame_index = report.frames.len() - 1;
            state.cursor_unix = report.frames[state.frame_index].cursor_unix;
            state.paused = true;
        }
        WatchKey::SpeedDown => state.speed = previous_replay_speed(state.speed),
        WatchKey::SpeedUp => state.speed = next_replay_speed(state.speed),
        _ => {}
    }
    state
}

fn previous_replay_speed(current: f64) -> f64 {
    const SPEEDS: [f64; 3] = [1.0, 10.0, 100.0];
    SPEEDS
        .iter()
        .rev()
        .copied()
        .find(|speed| *speed < current)
        .unwrap_or(SPEEDS[0])
}

fn next_replay_speed(current: f64) -> f64 {
    const SPEEDS: [f64; 3] = [1.0, 10.0, 100.0];
    SPEEDS
        .iter()
        .copied()
        .find(|speed| *speed > current)
        .unwrap_or(SPEEDS[SPEEDS.len() - 1])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WatchInput {
    Normal(WatchKey),
    Search(SearchKey),
}

/// Source of user input for the watch and replay loops.
///
/// Production code polls the real terminal through crossterm. Tests inject a
/// scripted source so the loops' state transitions (navigation, search,
/// scrolling, replay playback) can be exercised deterministically without a
/// TTY.
trait WatchEventSource {
    /// Returns the next input event, waiting up to `timeout` for one.
    ///
    /// `Ok(None)` means no event arrived within the timeout, which lets the
    /// loop tick its time-based refreshes.
    fn poll_event(&mut self, timeout: Duration, mode: InputMode) -> Result<Option<WatchInput>>;
}

/// Live terminal event source backed by crossterm.
struct TerminalEventSource;

impl WatchEventSource for TerminalEventSource {
    fn poll_event(&mut self, timeout: Duration, mode: InputMode) -> Result<Option<WatchInput>> {
        read_watch_event(timeout, mode)
    }
}

fn read_watch_event(timeout: Duration, mode: InputMode) -> Result<Option<WatchInput>> {
    if !event::poll(timeout).context("failed to poll watch UI input")? {
        return Ok(None);
    }
    match event::read().context("failed to read watch UI input")? {
        Event::Key(key) => Ok(map_key_event(key, mode)),
        Event::Mouse(mouse) => Ok(map_mouse_event(mouse.kind)),
        _ => Ok(None),
    }
}

/// Maps a mouse event to a watch action. Only the scroll wheel is wired (to the
/// log pane); other mouse events are ignored. Mouse events only arrive when
/// capture is opted in (see [`watch_mouse_enabled`]).
fn map_mouse_event(kind: MouseEventKind) -> Option<WatchInput> {
    match kind {
        MouseEventKind::ScrollUp => Some(WatchInput::Normal(WatchKey::PageUp)),
        MouseEventKind::ScrollDown => Some(WatchInput::Normal(WatchKey::PageDown)),
        _ => None,
    }
}

fn map_key_event(key: KeyEvent, mode: InputMode) -> Option<WatchInput> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return Some(WatchInput::Normal(WatchKey::Quit));
    }
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('u')) {
        return Some(WatchInput::Search(SearchKey::Clear));
    }

    // In text-entry modes (service filter, in-log search) every printable key
    // is query text; only a few keys act as controls. This is what lets a
    // query contain letters like `a`/`s`/`q` that are action keys in normal
    // mode.
    if mode != InputMode::Normal {
        return match key.code {
            KeyCode::Enter => Some(WatchInput::Search(SearchKey::Submit)),
            KeyCode::Esc => Some(WatchInput::Search(SearchKey::Cancel)),
            KeyCode::Backspace => Some(WatchInput::Search(SearchKey::Backspace)),
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                Some(WatchInput::Search(SearchKey::Char(ch)))
            }
            _ => None,
        };
    }

    match key.code {
        KeyCode::Char('q') => Some(WatchInput::Normal(WatchKey::Quit)),
        KeyCode::Char('j') | KeyCode::Down => Some(WatchInput::Normal(WatchKey::Down)),
        KeyCode::Char('k') | KeyCode::Up => Some(WatchInput::Normal(WatchKey::Up)),
        KeyCode::Char('g') => Some(WatchInput::Normal(WatchKey::First)),
        KeyCode::Home => Some(WatchInput::Normal(WatchKey::ReplayStart)),
        KeyCode::Char('G') => Some(WatchInput::Normal(WatchKey::Last)),
        KeyCode::Left => Some(WatchInput::Normal(WatchKey::SeekBackward)),
        KeyCode::Right => Some(WatchInput::Normal(WatchKey::SeekForward)),
        KeyCode::Char('[') => Some(WatchInput::Normal(WatchKey::PreviousEvent)),
        KeyCode::Char(']') => Some(WatchInput::Normal(WatchKey::NextEvent)),
        KeyCode::Char('-') => Some(WatchInput::Normal(WatchKey::SpeedDown)),
        KeyCode::Char('+') | KeyCode::Char('=') => Some(WatchInput::Normal(WatchKey::SpeedUp)),
        KeyCode::Tab => Some(WatchInput::Normal(WatchKey::Tab)),
        KeyCode::PageUp => Some(WatchInput::Normal(WatchKey::PageUp)),
        KeyCode::PageDown => Some(WatchInput::Normal(WatchKey::PageDown)),
        KeyCode::End => Some(WatchInput::Normal(WatchKey::End)),
        KeyCode::Char(' ') => Some(WatchInput::Normal(WatchKey::TogglePause)),
        KeyCode::Char('a') => Some(WatchInput::Normal(WatchKey::ToggleAllLogs)),
        KeyCode::Char('d') => Some(WatchInput::Normal(WatchKey::DebugHint)),
        KeyCode::Char('l') => Some(WatchInput::Normal(WatchKey::LogsHint)),
        KeyCode::Char('s') => Some(WatchInput::Normal(WatchKey::StatsHint)),
        KeyCode::Char('f') => Some(WatchInput::Normal(WatchKey::LogSearch)),
        KeyCode::Char('w') => Some(WatchInput::Normal(WatchKey::ToggleWrap)),
        KeyCode::Char('o') => Some(WatchInput::Normal(WatchKey::CycleSort)),
        KeyCode::Char('r') => Some(WatchInput::Normal(WatchKey::Restart)),
        KeyCode::Char('y') => Some(WatchInput::Normal(WatchKey::Yank)),
        KeyCode::Char('?') => Some(WatchInput::Normal(WatchKey::Help)),
        KeyCode::Char('/') => Some(WatchInput::Normal(WatchKey::Search)),
        KeyCode::Enter => Some(WatchInput::Normal(WatchKey::ShowDetail)),
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
    let effective = effective_services(
        &model.snapshot.services,
        model.filter.as_deref(),
        model.sort_mode,
    );
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

    let title_line = if let Some(replay) = &model.replay {
        format!(
            "{} | {} | job {}{}",
            term::styled_bold("hpc-compose replay"),
            replay_header_status(replay),
            model.snapshot.record.job_id,
            filter_indicator
        )
    } else {
        format!(
            "{} | {} | job {}{}{}",
            term::styled_bold("hpc-compose watch"),
            scheduler,
            model.snapshot.record.job_id,
            filter_indicator,
            hold_indicator(model.hold_state)
        )
    };

    let mut lines = vec![
        fit_line(&title_line, width),
        fit_line(
            &format!(
                "services: {} | selected: {} | logs: {} {}{}",
                effective.len(),
                selected_name,
                log_view_label(model.log_view_mode),
                if model.follow_logs {
                    "FOLLOW"
                } else {
                    "PAUSED"
                },
                match model.sort_mode {
                    ServiceSort::Triage => " | sort: triage",
                    ServiceSort::Spec => "",
                }
            ),
            width,
        ),
    ];
    if let Some(progress) = &model.walltime_progress {
        lines.push(fit_line(&render_walltime_bar(progress, width), width));
    }
    if let Some(replay) = &model.replay {
        lines.push(fit_line(&render_replay_scrubber(replay, width), width));
    }
    if let Some(metrics) = model.metrics_line.as_deref() {
        lines.push(fit_line(metrics, width));
    }
    if let Some(notice) = model.notice.as_deref() {
        lines.push(fit_line(&term::styled_warning(notice), width));
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
    if model.input_mode == InputMode::Search || model.input_mode == InputMode::LogSearch {
        let prompt = if model.input_mode == InputMode::LogSearch {
            "find"
        } else {
            "filter"
        };
        search_lines.push("-".repeat(width));
        search_lines.push(fit_line(
            &format!("{prompt}: {}", model.search_buffer),
            width,
        ));
    }

    let mut help_lines = Vec::new();
    if model.show_help {
        help_lines.push("-".repeat(width));
        help_lines.push(fit_line(&term::styled_bold("Keybindings:"), width));
        help_lines.push(fit_line("  j / Down    next service", width));
        help_lines.push(fit_line("  k / Up      previous service", width));
        help_lines.push(fit_line("  g           first service", width));
        help_lines.push(fit_line("  G           last service", width));
        help_lines.push(fit_line("  /           filter services by name", width));
        help_lines.push(fit_line("  f           find in logs", width));
        if model.replay.is_some() {
            help_lines.push(fit_line("  Space       pause or play replay", width));
            help_lines.push(fit_line("  +/-         change replay speed", width));
            help_lines.push(fit_line("  Left/Right  seek replay by 5 seconds", width));
            help_lines.push(fit_line(
                "  [ / ]       previous or next replay event",
                width,
            ));
            help_lines.push(fit_line("  Home/End    first or final replay frame", width));
        } else {
            help_lines.push(fit_line("  Space       pause or follow log tail", width));
        }
        help_lines.push(fit_line("  PgUp/PgDn   scroll log tail", width));
        if model.replay.is_none() {
            help_lines.push(fit_line("  End         return to live log tail", width));
        }
        help_lines.push(fit_line("  a           toggle selected/all logs", width));
        help_lines.push(fit_line("  w           toggle log line wrap", width));
        help_lines.push(fit_line(
            "  o           cycle service sort (spec/triage)",
            width,
        ));
        if model.replay.is_none() {
            help_lines.push(fit_line("  r           restart selected service", width));
        }
        help_lines.push(fit_line("  Enter       service detail panel", width));
        help_lines.push(fit_line(
            "  y           yank logs command to clipboard",
            width,
        ));
        help_lines.push(fit_line(
            "  d/l/s       debug/logs/stats command after final state",
            width,
        ));
        help_lines.push(fit_line("  ?           toggle help", width));
        help_lines.push(fit_line("  q           quit", width));
        help_lines.push("-".repeat(width));
    }

    let footer = if model.input_mode == InputMode::Search
        || model.input_mode == InputMode::LogSearch
    {
        "Enter apply  Esc cancel  Ctrl-U clear  Backspace delete"
    } else if model.replay.is_some() {
        "q quit  Space play/pause  +/- speed  Left/Right seek  [/] event  f find  o sort"
    } else if model.hold_state.is_some() {
        "q exit  d debug  l logs  s stats  ? help"
    } else if model.show_detail {
        "Enter/Esc back  j/k change service  y yank  q quit"
    } else {
        "q quit  ? help  Enter detail  / filter  f find  a all  w wrap  o sort  r restart  y yank"
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
    let wrap_note = if model.log_wrap { " WRAP" } else { "" };
    let search_note = match model.log_query.as_deref() {
        Some(query) if !query.is_empty() => {
            format!(" /{query} ({})", count_log_matches(&model.log_lines, query))
        }
        _ => String::new(),
    };
    log_lines.push(fit_line(
        &format!(
            "{}: {} {}{}{}{}",
            term::styled_bold("logs"),
            log_title,
            if model.follow_logs {
                "FOLLOW"
            } else {
                "PAUSED"
            },
            wrap_note,
            scroll_note,
            search_note
        ),
        log_width,
    ));
    if let Some(path_line) = log_path_line(model.log_view_mode, selected.copied()) {
        log_lines.push(fit_line(&term::styled_dim(&path_line), log_width));
    }
    let displayed = expand_log_lines(&model.log_lines, log_width, model.log_wrap);
    let visible = visible_log_lines(
        &displayed,
        body_height.saturating_sub(log_lines.len()),
        model.follow_logs,
        model.log_scroll,
    );
    if visible.is_empty() {
        let empty = empty_log_message(model.log_view_mode, selected.copied());
        log_lines.push(fit_line(&term::styled_dim(empty), log_width));
    }
    for line in visible {
        log_lines.push(fit_line(
            &style_log_row(line, model.log_query.as_deref()),
            log_width,
        ));
    }

    let row_count = body_height;
    if let Some(service) = selected.filter(|_| model.show_detail) {
        let detail = render_service_detail(service, width, row_count);
        for row in 0..row_count {
            lines.push(
                detail
                    .get(row)
                    .cloned()
                    .unwrap_or_else(|| pad_line("", width)),
            );
        }
    } else {
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
        &match &model.replay {
            Some(replay) => format!(
                "{} | {} | job {}",
                term::styled_bold("hpc-compose replay"),
                replay_header_status(replay),
                model.snapshot.record.job_id
            ),
            None => format!(
                "{} | job {}",
                term::styled_bold("hpc-compose watch"),
                model.snapshot.record.job_id
            ),
        },
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
    if let Some(notice) = model.notice.as_deref() {
        push_fit_line(&mut lines, width, height, &term::styled_warning(notice));
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
    if model.input_mode == InputMode::Search || model.input_mode == InputMode::LogSearch {
        let prompt = if model.input_mode == InputMode::LogSearch {
            "find input"
        } else {
            "filter input"
        };
        push_fit_line(
            &mut lines,
            width,
            height,
            &format!("{prompt}: {}", model.search_buffer),
        );
    }
    if model.show_help {
        push_fit_line(
            &mut lines,
            width,
            height,
            "? help | / filter | f find | w wrap | o sort | q quit",
        );
    }

    if let Some(detail_service) = selected.filter(|_| model.show_detail) {
        let budget = height.saturating_sub(lines.len() + 1);
        for line in render_service_detail(detail_service, width, budget) {
            push_fit_line(&mut lines, width, height, &line);
        }
    } else {
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

        let compact_search_note = match model.log_query.as_deref() {
            Some(query) if !query.is_empty() => {
                format!(" /{query} ({})", count_log_matches(&model.log_lines, query))
            }
            _ => String::new(),
        };
        push_fit_line(
            &mut lines,
            width,
            height,
            &format!(
                "logs: {} {}{}{}",
                if model.log_view_mode == LogViewMode::All {
                    "all services"
                } else {
                    selected_name
                },
                if model.follow_logs {
                    "FOLLOW"
                } else {
                    "PAUSED"
                },
                if model.log_wrap { " WRAP" } else { "" },
                compact_search_note
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
        let compact_displayed = expand_log_lines(&model.log_lines, width, model.log_wrap);
        for line in visible_log_lines(
            &compact_displayed,
            height.saturating_sub(lines.len() + 1),
            model.follow_logs,
            model.log_scroll,
        ) {
            push_fit_line(
                &mut lines,
                width,
                height,
                &style_log_row(line, model.log_query.as_deref()),
            );
        }
    }
    push_fit_line(
        &mut lines,
        width,
        height,
        if model.replay.is_some() {
            "q quit  Space play/pause  +/- speed  [/] event"
        } else if model.hold_state.is_some() {
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

fn replay_header_status(replay: &ReplayWatchStatus) -> String {
    let playback = if replay.paused { "PAUSED" } else { "PLAY" };
    format!(
        "t={} | speed={} | {} | {}",
        replay.cursor_unix,
        replay_speed_label(replay.speed),
        playback,
        replay.fidelity
    )
}

fn replay_speed_label(speed: f64) -> String {
    if (speed.fract()).abs() < f64::EPSILON {
        format!("{speed:.0}x")
    } else {
        format!("{speed:.2}x")
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

/// Severity inferred from a log line, used only for color highlighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogSeverity {
    Error,
    Warn,
}

/// Heuristically classifies a log line by severity. Level tokens are matched as
/// whole words so substrings like `errored` or `forewarn` don't trip it.
fn log_severity(line: &str) -> Option<LogSeverity> {
    let lower = line.to_ascii_lowercase();
    if ["error", "fatal", "panic"]
        .iter()
        .any(|word| contains_word(&lower, word))
    {
        Some(LogSeverity::Error)
    } else if ["warn", "warning"]
        .iter()
        .any(|word| contains_word(&lower, word))
    {
        Some(LogSeverity::Warn)
    } else {
        None
    }
}

/// Returns true if `needle` starts a word in `haystack` (the preceding
/// character is non-alphanumeric). `haystack` is assumed already lowercased.
///
/// Only the prefix boundary is checked, so inflected forms like `panicked`,
/// `errored`, and `warnings` are still detected while embedded matches such as
/// `terror` (containing `error`) are rejected.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        if before_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Wraps each case-insensitive `query` occurrence in `row` with highlight
/// styling. Returns `None` when the row contains no match. ASCII-lowercasing
/// preserves byte offsets, so the spans map back onto the original `row`.
fn highlight_matches(row: &str, query: &str) -> Option<String> {
    if query.is_empty() {
        return None;
    }
    let row_lower = row.to_ascii_lowercase();
    let query_lower = query.to_ascii_lowercase();
    if !row_lower.contains(&query_lower) {
        return None;
    }
    let mut out = String::with_capacity(row.len());
    let mut from = 0;
    while let Some(offset) = row_lower[from..].find(&query_lower) {
        let start = from + offset;
        let end = start + query_lower.len();
        out.push_str(&row[from..start]);
        out.push_str(&term::styled_highlight_raw(&row[start..end]));
        from = end;
    }
    out.push_str(&row[from..]);
    Some(out)
}

/// Styles a single visible log row. An active search match takes precedence
/// (keeping nested escape sequences out of the picture); otherwise the row is
/// colored by inferred severity.
fn style_log_row(row: &str, query: Option<&str>) -> String {
    if let Some(query) = query.filter(|query| !query.is_empty())
        && let Some(highlighted) = highlight_matches(row, query)
    {
        return highlighted;
    }
    match log_severity(row) {
        Some(LogSeverity::Error) => term::styled_error_raw(row),
        Some(LogSeverity::Warn) => term::styled_warning_raw(row),
        None => row.to_string(),
    }
}

/// Expands raw log lines for display, wrapping each to `width` when `wrap` is
/// set. Log lines are plain text, so character count equals visible width.
fn expand_log_lines(lines: &[String], width: usize, wrap: bool) -> Vec<String> {
    if !wrap || width == 0 {
        return lines.to_vec();
    }
    let mut out = Vec::with_capacity(lines.len());
    for line in lines {
        if line.is_empty() {
            out.push(String::new());
            continue;
        }
        let chars: Vec<char> = line.chars().collect();
        for chunk in chars.chunks(width) {
            out.push(chunk.iter().collect());
        }
    }
    out
}

/// Counts log lines containing `query` (case-insensitive).
fn count_log_matches(lines: &[String], query: &str) -> usize {
    if query.is_empty() {
        return 0;
    }
    let query_lower = query.to_ascii_lowercase();
    lines
        .iter()
        .filter(|line| line.to_ascii_lowercase().contains(&query_lower))
        .count()
}

/// Filters services by name and applies the requested ordering. Selection and
/// rendering both go through this so `selected_index` always lines up with what
/// is shown.
fn effective_services<'a>(
    services: &'a [PsServiceRow],
    filter: Option<&str>,
    sort: ServiceSort,
) -> Vec<&'a PsServiceRow> {
    let mut effective = filtered_services(services, filter);
    if sort == ServiceSort::Triage {
        // Stable sort keeps spec order within each triage rank.
        effective.sort_by_key(|service| service_triage_rank(service));
    }
    effective
}

fn selected_service_name(
    services: &[PsServiceRow],
    filter: Option<&str>,
    sort: ServiceSort,
    selected_index: usize,
) -> Option<String> {
    effective_services(services, filter, sort)
        .get(selected_index)
        .map(|row| row.service_name.clone())
}

fn selected_effective_service<'a>(
    services: &'a [PsServiceRow],
    filter: Option<&str>,
    sort: ServiceSort,
    selected_index: usize,
) -> Option<&'a PsServiceRow> {
    effective_services(services, filter, sort)
        .get(selected_index)
        .copied()
}

fn preserve_selected_index(
    services: &[PsServiceRow],
    filter: Option<&str>,
    sort: ServiceSort,
    selected_service: Option<&str>,
    fallback_index: usize,
) -> usize {
    let effective = effective_services(services, filter, sort);
    preserve_selected_index_raw(&effective, selected_service, fallback_index)
}

fn preserve_selected_index_raw(
    services: &[&PsServiceRow],
    selected_service: Option<&str>,
    fallback_index: usize,
) -> usize {
    if let Some(name) = selected_service
        && let Some(index) = services.iter().position(|row| row.service_name == name)
    {
        return index;
    }
    clamp_selected_index_raw(services, fallback_index)
}

/// Triage rank: failures first, then unhealthy, then everything else.
fn service_triage_rank(service: &PsServiceRow) -> u8 {
    if service_matches_failure(service) {
        0
    } else if service.healthy == Some(false) {
        1
    } else {
        2
    }
}

/// True when the record's backend runs a consumer for dev-control restart
/// requests (the local Pyxis/Enroot supervisor). Slurm batch jobs do not.
fn restart_supported(record: &SubmissionRecord) -> bool {
    record.backend == SubmissionBackend::Local
}

/// Writes a dev-control restart request for `service`, the same file-based
/// mechanism `hpc-compose dev` uses for file-watch reloads. Returns the request
/// path on success.
fn request_service_restart(record: &SubmissionRecord, service: &str) -> Result<PathBuf> {
    let request_dir = runtime_job_root_for_record(record)
        .join("dev-control")
        .join("restart");
    fs::create_dir_all(&request_dir)
        .with_context(|| format!("failed to create {}", request_dir.display()))?;
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = request_dir.join(format!("restart-{}-{millis}.request", std::process::id()));
    fs::write(&path, format!("{service}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
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

fn first_failed_service_name(services: &[PsServiceRow]) -> Option<&str> {
    services
        .iter()
        .find(|service| service_matches_failure(service))
        .map(|service| service.service_name.as_str())
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
            accounting: false,
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

/// Standard base64 encoding with padding. Inlined to avoid a dependency for the
/// single OSC 52 use site.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 63) as usize] as char);
        out.push(TABLE[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Builds an OSC 52 "set clipboard" escape sequence. Modern terminals (and tmux
/// with `set-clipboard on`) honor this even over SSH, so it needs no host-side
/// clipboard tool.
fn osc52_sequence(text: &str) -> String {
    format!("\u{1b}]52;c;{}\u{7}", base64_encode(text.as_bytes()))
}

/// Copies `text` to the system clipboard via OSC 52. The escape is out-of-band
/// (no cursor movement or visible output), so it is safe to emit mid-frame.
fn copy_to_clipboard(text: &str) -> Result<()> {
    let mut stdout = io::stdout();
    write!(stdout, "{}", osc52_sequence(text)).context("failed to write clipboard sequence")?;
    stdout.flush().context("failed to flush clipboard sequence")
}

fn pane_separator() -> &'static str {
    if term::unicode_allowed_raw() {
        "\u{2502}"
    } else {
        "|"
    }
}

/// Incremental terminal renderer.
///
/// The watch and replay loops repaint on every input-poll tick (~10 Hz), but
/// the rendered frame is usually identical between ticks. A naive
/// `Clear(All)` + full repaint each tick causes visible flicker and, over SSH
/// (the common HPC case), streams a full screen of bytes continuously for a
/// static view. This renderer keeps the previously drawn rows and rewrites
/// only the rows that changed, skipping the write entirely when nothing
/// changed. Every frame row is padded to the full width (see `fit_line`), so
/// overwriting a row fully covers its prior content without a screen clear.
struct FrameRenderer {
    previous_lines: Vec<String>,
    last_size: Option<(usize, usize)>,
    needs_full_redraw: bool,
}

impl FrameRenderer {
    fn new() -> Self {
        Self {
            previous_lines: Vec::new(),
            last_size: None,
            needs_full_redraw: true,
        }
    }

    fn render(&mut self, frame: &str, size: (usize, usize)) -> Result<()> {
        // A resize invalidates every cached row (and the terminal may reflow),
        // so fall back to a full repaint for that frame.
        if self.last_size != Some(size) {
            self.needs_full_redraw = true;
            self.last_size = Some(size);
        }

        let lines: Vec<&str> = frame.split('\n').collect();
        let mut stdout = io::stdout();
        if self.needs_full_redraw {
            execute!(stdout, Clear(ClearType::All)).context("failed to clear watch UI frame")?;
        }

        let mut wrote = self.needs_full_redraw;
        for (row, line) in lines.iter().enumerate() {
            let unchanged = !self.needs_full_redraw
                && self.previous_lines.get(row).map(String::as_str) == Some(*line);
            if unchanged {
                continue;
            }
            execute!(stdout, MoveTo(0, row as u16), Clear(ClearType::CurrentLine))
                .context("failed to position watch UI cursor")?;
            write!(stdout, "{line}").context("failed to write watch UI frame")?;
            wrote = true;
        }

        // Wipe rows left over from a previously taller frame (terminal shrank).
        if self.previous_lines.len() > lines.len() {
            execute!(
                stdout,
                MoveTo(0, lines.len() as u16),
                Clear(ClearType::FromCursorDown)
            )
            .context("failed to clear stale watch UI rows")?;
            wrote = true;
        }

        if wrote {
            stdout.flush().context("failed to flush watch UI frame")?;
        }
        self.previous_lines = lines.into_iter().map(str::to_string).collect();
        self.needs_full_redraw = false;
        Ok(())
    }
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

/// Renders a one-line replay timeline scrubber: a track with event ticks and
/// the playback cursor positioned between the timeline start and end.
fn render_replay_scrubber(replay: &ReplayWatchStatus, width: usize) -> String {
    let span = replay.end_unix.saturating_sub(replay.start_unix);
    let elapsed = replay.cursor_unix.saturating_sub(replay.start_unix);
    let label = format!("timeline {elapsed}s/{span}s ");
    let bar_width = width.saturating_sub(visible_width(&label) + 2).clamp(8, 48);
    let position = |unix: u64| -> usize {
        if span == 0 {
            0
        } else {
            ((u128::from(unix.saturating_sub(replay.start_unix)) * (bar_width - 1) as u128)
                / u128::from(span)) as usize
        }
        .min(bar_width - 1)
    };
    let (track, tick, head) = if term::unicode_allowed_raw() {
        ('\u{2500}', '\u{2506}', '\u{25cf}')
    } else {
        ('-', '|', '#')
    };
    let mut cells = vec![track; bar_width];
    for event in &replay.event_unix {
        cells[position(*event)] = tick;
    }
    cells[position(replay.cursor_unix)] = head;
    let bar: String = cells.into_iter().collect();
    format!("{label}[{bar}]")
}

fn format_duration_secs(secs: u64) -> String {
    let (minutes, seconds) = (secs / 60, secs % 60);
    if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// Builds the detail-panel lines for one service: the fields the compact table
/// cannot show. Lines are fit to `width`; at most `height` rows are returned.
fn render_service_detail(service: &PsServiceRow, width: usize, height: usize) -> Vec<String> {
    let yes_no = |value: Option<bool>| value.map(yes_no_short).unwrap_or("-").to_string();
    let text = |value: &Option<String>| value.clone().unwrap_or_else(|| "-".to_string());
    let num = |value: Option<i64>| {
        value
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string())
    };

    let mut rows: Vec<String> = Vec::new();
    rows.push(term::styled_bold(&format!(
        "[ {} ]  {}",
        service.service_name,
        service_state_label(service)
    )));
    rows.push(format!("step        {}", text(&service.step_name)));
    rows.push(format!(
        "pid         {}    ready {}",
        num(service.launcher_pid.map(i64::from)),
        yes_no(service.healthy)
    ));
    rows.push(format!("status      {}", text(&service.status)));
    rows.push(format!(
        "exit        {}    completed {}",
        num(service.last_exit_code.map(i64::from)),
        yes_no(service.completed_successfully)
    ));
    rows.push(format!(
        "placement   {}   nodes {}   ntasks {} ({}/node)",
        text(&service.placement_mode),
        num(service.nodes.map(i64::from)),
        num(service.ntasks.map(i64::from)),
        num(service.ntasks_per_node.map(i64::from))
    ));
    rows.push(format!("nodelist    {}", text(&service.nodelist)));
    rows.push(format!(
        "policy      {}   ready-cfg {}",
        text(&service.failure_policy_mode),
        yes_no(service.readiness_configured)
    ));
    rows.push(format!(
        "restarts    {}/{}   window {}s ({} max, {} failed)",
        num(service.restart_count.map(i64::from)),
        num(service.max_restarts.map(i64::from)),
        num(service.window_seconds.map(|v| v as i64)),
        num(service.max_restarts_in_window.map(i64::from)),
        num(service.restart_failures_in_window.map(i64::from))
    ));
    if let Some(duration) = service.duration_seconds {
        rows.push(format!("duration    {}", format_duration_secs(duration)));
    }
    if let Some(assertions) = &service.assertions {
        let summary = if !assertions.failures.is_empty() {
            format!(
                "{} failed: {}",
                assertions.failures.len(),
                assertions.failures.join("; ")
            )
        } else {
            assertions.status.clone().unwrap_or_else(|| {
                if assertions.configured {
                    "configured".to_string()
                } else {
                    "-".to_string()
                }
            })
        };
        rows.push(format!("assertions  {summary}"));
    }
    rows.push(format!("log         {}", service.path.display()));
    rows.push(String::new());
    rows.push(term::styled_dim("Esc/Enter back   j/k change service"));

    rows.into_iter()
        .take(height)
        .map(|line| fit_line(&line, width))
        .collect()
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
        PsSnapshot, QueueDiagnostics, ReplayArtifactPaths, ReplayEvent, ReplayEventKind,
        ReplayFrame, ReplayReport, ReplayServiceFrame, RequestedWalltime, SchedulerOptions,
        SchedulerSource, SchedulerStatus, SubmissionBackend, SubmissionKind, SubmissionRecord,
        WalltimeProgress, WatchOutcome, build_submission_record_with_backend,
        state_path_for_record, write_submission_record,
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
                slurm_array: None,
                sweep: None,
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
                    started_at: None,
                    finished_at: None,
                    duration_seconds: None,
                    assertions: None,
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
                    started_at: None,
                    finished_at: None,
                    duration_seconds: None,
                    assertions: None,
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
            log_query: None,
            log_wrap: false,
            sort_mode: ServiceSort::Spec,
            notice: None,
            show_detail: false,
            replay: None,
        }
    }

    fn sample_replay_report() -> ReplayReport {
        let snapshot = sample_snapshot();
        let events = vec![
            ReplayEvent {
                at_unix: 100,
                attempt: None,
                kind: ReplayEventKind::ServiceStart,
                service: Some("api".into()),
                exit_code: None,
                detail: Some("started".into()),
            },
            ReplayEvent {
                at_unix: 110,
                attempt: None,
                kind: ReplayEventKind::ServiceExit,
                service: Some("api".into()),
                exit_code: Some(7),
                detail: Some("node=n1".into()),
            },
        ];
        let frames = events
            .iter()
            .enumerate()
            .map(|(index, event)| ReplayFrame {
                cursor_unix: event.at_unix,
                event_index: index,
                event: event.clone(),
                services: vec![ReplayServiceFrame {
                    service_name: "api".into(),
                    status: if index == 0 {
                        "running".into()
                    } else {
                        "failed".into()
                    },
                    started_at: Some(100),
                    finished_at: (index == 1).then_some(110),
                    last_exit_code: (index == 1).then_some(7),
                    restart_count: Some(0),
                }],
                metrics_line: (index == 1).then_some("gpu: 1 util=90% mem=4/8 MiB".into()),
                fidelity_note: Some("best-effort replay from existing tracked artifacts".into()),
                snapshot: {
                    let mut snapshot = snapshot.clone();
                    snapshot.scheduler.state = if index == 0 {
                        "RUNNING".into()
                    } else {
                        "FAILED".into()
                    };
                    snapshot.scheduler.failed = index == 1;
                    snapshot.scheduler.terminal = index == 1;
                    snapshot.scheduler.detail =
                        Some("best-effort replay from existing tracked artifacts".into());
                    snapshot.services[0].status = Some(if index == 0 {
                        "running".into()
                    } else {
                        "failed".into()
                    });
                    snapshot.services[0].last_exit_code = (index == 1).then_some(7);
                    snapshot
                },
            })
            .collect::<Vec<_>>();
        ReplayReport {
            job_id: "12345".into(),
            record: snapshot.record.clone(),
            fidelity: "best-effort".into(),
            notes: vec!["best-effort".into()],
            artifacts: ReplayArtifactPaths::default(),
            events,
            frames,
            timeline_start_unix: Some(100),
            timeline_end_unix: Some(110),
        }
    }

    /// Deterministic event source that replays a scripted sequence of inputs,
    /// then quits so the watch and replay loops always terminate.
    struct ScriptedEvents {
        events: std::collections::VecDeque<WatchInput>,
    }

    impl ScriptedEvents {
        fn new(events: impl IntoIterator<Item = WatchInput>) -> Self {
            Self {
                events: events.into_iter().collect(),
            }
        }
    }

    impl WatchEventSource for ScriptedEvents {
        fn poll_event(
            &mut self,
            _timeout: Duration,
            _mode: InputMode,
        ) -> Result<Option<WatchInput>> {
            Ok(Some(
                self.events
                    .pop_front()
                    .unwrap_or(WatchInput::Normal(WatchKey::Quit)),
            ))
        }
    }

    fn normal(key: WatchKey) -> WatchInput {
        WatchInput::Normal(key)
    }

    #[test]
    fn frame_renderer_tracks_rows_and_handles_resize() {
        let mut renderer = FrameRenderer::new();
        renderer.render("a\nb\nc", (3, 3)).expect("initial paint");
        assert_eq!(renderer.previous_lines, vec!["a", "b", "c"]);

        // Identical frame: nothing to rewrite, cached rows preserved.
        renderer.render("a\nb\nc", (3, 3)).expect("identical paint");
        assert_eq!(renderer.previous_lines.len(), 3);

        // Shorter frame at the same size exercises the trailing-clear branch.
        renderer.render("a\nb", (3, 3)).expect("shorter paint");
        assert_eq!(renderer.previous_lines, vec!["a", "b"]);

        // Taller frame grows the cached rows.
        renderer.render("a\nb\nc\nd", (3, 3)).expect("taller paint");
        assert_eq!(renderer.previous_lines.len(), 4);

        // A size change forces a full repaint and updates the tracked size.
        renderer.render("x\ny", (2, 2)).expect("resized paint");
        assert_eq!(renderer.last_size, Some((2, 2)));
        assert_eq!(renderer.previous_lines, vec!["x", "y"]);
    }

    fn search(key: SearchKey) -> WatchInput {
        WatchInput::Search(key)
    }

    #[test]
    fn replay_loop_navigation_selects_service_via_injected_events() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([normal(WatchKey::Down), normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.selected_service.as_deref(), Some("worker"));
    }

    #[test]
    fn replay_loop_initial_triage_sort_preserves_service_selection() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(
            &report,
            Some("api"),
            5,
            1.0,
            &mut events,
            WatchPrefs {
                sort: ServiceSort::Triage,
                ..WatchPrefs::default()
            },
        )
        .expect("replay loop runs");
        assert_eq!(result.sort_mode, ServiceSort::Triage);
        assert_eq!(result.selected_service.as_deref(), Some("api"));
    }

    #[test]
    fn replay_loop_filter_narrows_to_matching_service() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([
            normal(WatchKey::Search),
            search(SearchKey::Char('w')),
            search(SearchKey::Char('o')),
            search(SearchKey::Submit),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.filter.as_deref(), Some("wo"));
        assert_eq!(result.selected_service.as_deref(), Some("worker"));
    }

    #[test]
    fn replay_loop_search_cancel_restores_unfiltered_view() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([
            normal(WatchKey::Search),
            search(SearchKey::Char('z')),
            search(SearchKey::Cancel),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert!(result.filter.is_none());
        assert_eq!(result.selected_service.as_deref(), Some("api"));
    }

    #[test]
    fn replay_loop_speed_and_pause_keys_update_playback() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([
            normal(WatchKey::SpeedUp),
            normal(WatchKey::TogglePause),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.playback.speed, 10.0);
        assert!(result.playback.paused);
    }

    #[test]
    fn replay_loop_event_step_advances_cursor_and_pauses() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([normal(WatchKey::NextEvent), normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.playback.frame_index, 1);
        assert_eq!(result.playback.cursor_unix, 110);
        assert!(result.playback.paused);
    }

    #[test]
    fn replay_loop_toggle_all_logs_changes_view_mode() {
        let report = sample_replay_report();
        let mut events =
            ScriptedEvents::new([normal(WatchKey::ToggleAllLogs), normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.log_view_mode, LogViewMode::All);
    }

    #[test]
    fn replay_loop_empty_report_returns_without_reading_events() {
        let mut report = sample_replay_report();
        report.frames.clear();
        let mut events = ScriptedEvents::new([normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert!(result.selected_service.is_none());
    }

    #[test]
    fn replay_loop_log_search_sets_query_without_touching_filter() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([
            normal(WatchKey::LogSearch),
            search(SearchKey::Char('e')),
            search(SearchKey::Char('r')),
            search(SearchKey::Char('r')),
            search(SearchKey::Submit),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.log_query.as_deref(), Some("err"));
        assert!(result.filter.is_none());
    }

    #[test]
    fn replay_loop_toggle_wrap_flips_state() {
        let report = sample_replay_report();
        let mut events =
            ScriptedEvents::new([normal(WatchKey::ToggleWrap), normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert!(result.log_wrap);
    }

    #[test]
    fn replay_loop_cycle_sort_switches_and_preserves_selection() {
        let report = sample_replay_report();
        // Select `worker` (unhealthy), then switch to triage order. `worker`
        // moves to the front but stays selected.
        let mut events = ScriptedEvents::new([
            normal(WatchKey::Down),
            normal(WatchKey::CycleSort),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert_eq!(result.sort_mode, ServiceSort::Triage);
        assert_eq!(result.selected_service.as_deref(), Some("worker"));
    }

    #[test]
    fn log_severity_classifies_levels_by_word() {
        assert_eq!(log_severity("[ERROR] boom"), Some(LogSeverity::Error));
        assert_eq!(log_severity("level=warn retrying"), Some(LogSeverity::Warn));
        assert_eq!(
            log_severity("thread 'main' panicked"),
            Some(LogSeverity::Error)
        );
        assert_eq!(log_severity("all systems nominal"), None);
        // Inflected forms are detected via the prefix boundary.
        assert_eq!(
            log_severity("the build errored earlier"),
            Some(LogSeverity::Error)
        );
        // ...but embedded matches like `terror` (contains `error`) are rejected.
        assert_eq!(log_severity("terror is not a level"), None);
    }

    #[test]
    fn highlight_and_count_track_query_matches() {
        let lines = [
            "api ok".to_string(),
            "API ready".to_string(),
            "db idle".to_string(),
        ];
        assert_eq!(count_log_matches(&lines, "api"), 2);
        assert_eq!(count_log_matches(&lines, ""), 0);
        assert!(highlight_matches("nothing here", "zzz").is_none());
        // Highlighting preserves the visible text (only adds styling).
        let highlighted = highlight_matches("hello WORLD", "world").expect("match present");
        assert_eq!(strip_ansi_for_snapshot(&highlighted), "hello WORLD");
    }

    #[test]
    fn env_refresh_interval_opt_parses_and_clamps() {
        assert_eq!(env_refresh_interval_opt(None, 100, 60_000), None);
        assert_eq!(env_refresh_interval_opt(Some("nope"), 100, 60_000), None);
        // Below the floor and above the ceiling are clamped.
        assert_eq!(
            env_refresh_interval_opt(Some("10"), 100, 60_000),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            env_refresh_interval_opt(Some("999999"), 100, 60_000),
            Some(Duration::from_millis(60_000))
        );
        assert_eq!(
            env_refresh_interval_opt(Some(" 2500 "), 100, 60_000),
            Some(Duration::from_millis(2500))
        );
    }

    #[test]
    fn watch_prefs_resolve_reads_settings() {
        use hpc_compose::context::WatchSettings;
        let prefs = WatchPrefs::resolve(&WatchSettings {
            sort: Some("triage".into()),
            wrap: Some(true),
            refresh_ms: Some(250),
            metrics_refresh_ms: Some(2000),
            mouse: Some(true),
        });
        assert_eq!(prefs.sort, ServiceSort::Triage);
        assert!(prefs.wrap);
        assert_eq!(prefs.data_refresh, Duration::from_millis(250));
        assert_eq!(prefs.metrics_refresh, Duration::from_millis(2000));
        assert!(prefs.mouse);
        // Defaults when unset.
        let defaults = WatchPrefs::resolve(&WatchSettings::default());
        assert_eq!(defaults.sort, ServiceSort::Spec);
        assert!(!defaults.wrap);
        assert_eq!(defaults.data_refresh, DATA_REFRESH_INTERVAL);
    }

    #[test]
    fn expand_log_lines_wraps_only_when_enabled() {
        let lines = vec!["abcdef".to_string(), "gh".to_string()];
        assert_eq!(expand_log_lines(&lines, 3, false), lines);
        assert_eq!(
            expand_log_lines(&lines, 3, true),
            vec!["abc".to_string(), "def".to_string(), "gh".to_string()]
        );
    }

    #[test]
    fn replay_scrubber_renders_label_cursor_and_handles_zero_span() {
        let replay = ReplayWatchStatus {
            cursor_unix: 105,
            speed: 1.0,
            paused: false,
            fidelity: "best-effort".into(),
            start_unix: 100,
            end_unix: 110,
            event_unix: vec![100, 110],
        };
        let bar = render_replay_scrubber(&replay, 80);
        assert!(bar.contains("timeline 5s/10s"));
        let visible = strip_ansi_for_snapshot(&bar);
        // Cursor head is drawn (ASCII `#` or unicode ●).
        assert!(visible.contains('#') || visible.contains('\u{25cf}'));
        // A zero-span timeline must not panic.
        let zero = ReplayWatchStatus {
            cursor_unix: 100,
            start_unix: 100,
            end_unix: 100,
            event_unix: vec![100],
            ..replay
        };
        let _ = render_replay_scrubber(&zero, 80);
    }

    #[test]
    fn render_service_detail_surfaces_table_omitted_fields() {
        let snapshot = sample_snapshot();
        let detail = render_service_detail(&snapshot.services[0], 80, 40).join("\n");
        let visible = strip_ansi_for_snapshot(&detail);
        assert!(visible.contains("[ api ]"));
        assert!(visible.contains("pid         4242"));
        assert!(visible.contains("placement   primary"));
        assert!(visible.contains("nodelist    node001"));
        assert!(visible.contains("restarts    1/3"));
        assert!(visible.contains("Esc/Enter back"));
    }

    #[test]
    fn replay_loop_enter_toggles_detail_panel() {
        let report = sample_replay_report();
        let mut events =
            ScriptedEvents::new([normal(WatchKey::ShowDetail), normal(WatchKey::Quit)]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert!(result.show_detail);
    }

    #[test]
    fn replay_loop_escape_closes_detail_panel() {
        let report = sample_replay_report();
        let mut events = ScriptedEvents::new([
            normal(WatchKey::ShowDetail),
            search(SearchKey::Cancel),
            normal(WatchKey::Quit),
        ]);
        let result = run_replay_ui_loop(&report, None, 5, 1.0, &mut events, WatchPrefs::default())
            .expect("replay loop runs");
        assert!(!result.show_detail);
    }

    #[test]
    fn render_watch_frame_detail_panel_replaces_body() {
        let model = WatchModel {
            show_detail: true,
            ..sample_watch_model()
        };
        let frame = render_watch_frame(&model, 100, 24);
        let visible = strip_ansi_for_snapshot(&frame);
        // The detail panel is shown instead of the two-pane table/log body.
        assert!(visible.contains("nodelist    node001"));
        assert!(!visible.contains("svc              step"));
    }

    #[test]
    fn base64_encode_matches_rfc4648_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn osc52_sequence_wraps_base64_payload() {
        assert_eq!(osc52_sequence("hi"), "\u{1b}]52;c;aGk=\u{7}");
    }

    #[test]
    fn effective_services_triage_orders_problems_first() {
        let snapshot = sample_snapshot();
        let names = |services: &[&PsServiceRow]| {
            services
                .iter()
                .map(|s| s.service_name.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(&effective_services(
                &snapshot.services,
                None,
                ServiceSort::Spec
            )),
            vec!["api".to_string(), "worker".to_string()]
        );
        // `worker` is unhealthy, so triage order surfaces it first.
        assert_eq!(
            names(&effective_services(
                &snapshot.services,
                None,
                ServiceSort::Triage
            )),
            vec!["worker".to_string(), "api".to_string()]
        );
    }

    #[test]
    fn preserve_selected_index_keeps_service_across_triage_sort() {
        let snapshot = sample_snapshot();
        let selected_index = preserve_selected_index(
            &snapshot.services,
            None,
            ServiceSort::Triage,
            Some("api"),
            0,
        );
        let selected = selected_effective_service(
            &snapshot.services,
            None,
            ServiceSort::Triage,
            selected_index,
        )
        .expect("selected service");
        assert_eq!(selected.service_name, "api");
    }

    #[test]
    fn restart_supported_gates_on_local_backend() {
        let mut record = sample_snapshot().record;
        record.backend = SubmissionBackend::Local;
        assert!(restart_supported(&record));
        record.backend = SubmissionBackend::Slurm;
        assert!(!restart_supported(&record));
    }

    #[test]
    fn request_service_restart_writes_named_request() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let mut record = sample_snapshot().record;
        record.submit_dir = tmpdir.path().to_path_buf();
        let path = request_service_restart(&record, "api").expect("write request");
        assert!(path.exists());
        assert_eq!(
            std::fs::read_to_string(&path).expect("read request").trim(),
            "api"
        );
        let display = path.to_string_lossy();
        assert!(display.contains("dev-control"));
        assert!(display.ends_with(".request"));
    }

    #[test]
    fn watch_loop_restart_writes_request_for_local_job() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {img}\n    command: /bin/true\nx-slurm:\n  cache_dir: {cache}\n",
                img = local_image.display(),
                cache = tmpdir.path().join("cache").display()
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
            "local-watch-restart-123",
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

        let options = SchedulerOptions {
            squeue_bin: "/definitely/missing-squeue".into(),
            sacct_bin: "/definitely/missing-sacct".into(),
        };
        // HoldOnExit::Always keeps the completed job open so `r` is processed.
        let mut events = ScriptedEvents::new([normal(WatchKey::Restart), normal(WatchKey::Quit)]);
        run_watch_ui_loop(
            &record,
            &options,
            None,
            5,
            HoldOnExit::Always,
            &mut events,
            WatchPrefs::default(),
        )
        .expect("watch loop runs");

        let restart_dir = runtime_job_root_for_record(&record)
            .join("dev-control")
            .join("restart");
        let requests: Vec<_> = fs::read_dir(&restart_dir)
            .expect("restart dir exists")
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(requests.len(), 1, "exactly one restart request written");
    }

    #[test]
    fn watch_loop_navigates_services_via_injected_events() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let local_image = tmpdir.path().join("local.sqsh");
        fs::write(&local_image, "sqsh").expect("local image");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            format!(
                "name: demo\nservices:\n  api:\n    image: {img}\n    command: /bin/true\n  worker:\n    image: {img}\n    command: /bin/true\nx-slurm:\n  cache_dir: {cache}\n",
                img = local_image.display(),
                cache = tmpdir.path().join("cache").display()
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
            "local-watch-nav-123",
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

        let options = SchedulerOptions {
            squeue_bin: "/definitely/missing-squeue".into(),
            sacct_bin: "/definitely/missing-sacct".into(),
        };

        // Resolve the service ordering the snapshot will present so the
        // assertion is independent of spec iteration order.
        let snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), &options)
            .expect("snapshot");
        assert!(
            snapshot.services.len() >= 2,
            "fixture must expose at least two services"
        );
        let second_service = snapshot.services[1].service_name.clone();

        // HoldOnExit::Always keeps the completed job's UI open so the down-key
        // is processed before the quit.
        let mut events = ScriptedEvents::new([normal(WatchKey::Down), normal(WatchKey::Quit)]);
        let result = run_watch_ui_loop(
            &record,
            &options,
            None,
            5,
            HoldOnExit::Always,
            &mut events,
            WatchPrefs::default(),
        )
        .expect("watch loop runs");

        assert!(matches!(result.outcome, WatchOutcome::Completed(_)));
        assert_eq!(
            result.selected_service.as_deref(),
            Some(second_service.as_str())
        );
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
    fn render_watch_frame_shows_replay_status_and_controls() {
        let report = sample_replay_report();
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: report.frames[1].snapshot.clone(),
                metrics_line: report.frames[1].metrics_line.clone(),
                replay: Some(ReplayWatchStatus {
                    cursor_unix: 110,
                    speed: 10.0,
                    paused: true,
                    fidelity: "best-effort".into(),
                    start_unix: 100,
                    end_unix: 110,
                    event_unix: vec![100, 110],
                }),
                ..sample_watch_model()
            },
            110,
            22,
        );
        let stripped = strip_ansi_for_snapshot(&frame);
        assert!(stripped.contains("hpc-compose replay"));
        assert!(stripped.contains("t=110 | speed=10x | PAUSED | best-effort"));
        assert!(stripped.contains("gpu: 1 util=90% mem=4/8 MiB"));
        assert!(stripped.contains("Space play/pause"));
        assert!(stripped.contains("[/] event"));
    }

    #[test]
    fn render_compact_watch_frame_shows_replay_header() {
        let report = sample_replay_report();
        let frame = render_watch_frame(
            &WatchModel {
                snapshot: report.frames[0].snapshot.clone(),
                replay: Some(ReplayWatchStatus {
                    cursor_unix: 100,
                    speed: 1.0,
                    paused: false,
                    fidelity: "best-effort".into(),
                    start_unix: 100,
                    end_unix: 110,
                    event_unix: vec![100, 110],
                }),
                ..sample_watch_model()
            },
            60,
            10,
        );
        let stripped = strip_ansi_for_snapshot(&frame);
        assert!(stripped.contains("hpc-compose replay"));
        assert!(stripped.contains("speed=1x"));
        assert!(stripped.contains("Space play/pause"));
    }

    #[test]
    fn replay_key_navigation_updates_playback_state() {
        let report = sample_replay_report();
        let state = ReplayPlaybackState::new(&report, 1.0);
        let paused = apply_replay_key(state, &report, WatchKey::TogglePause);
        assert!(paused.paused);
        let next = apply_replay_key(paused, &report, WatchKey::NextEvent);
        assert_eq!(next.frame_index, 1);
        assert_eq!(next.cursor_unix, 110);
        let faster = apply_replay_key(next, &report, WatchKey::SpeedUp);
        assert_eq!(faster.speed, 10.0);
        let slower = apply_replay_key(faster, &report, WatchKey::SpeedDown);
        assert_eq!(slower.speed, 1.0);
        let first = apply_replay_key(next, &report, WatchKey::ReplayStart);
        assert_eq!(first.frame_index, 0);
        let final_state = apply_replay_key(first, &report, WatchKey::End);
        assert_eq!(final_state.frame_index, 1);
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
    fn ctrl_c_maps_to_quit() {
        assert_eq!(
            map_key_event(
                KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
                InputMode::Normal
            ),
            Some(WatchInput::Normal(WatchKey::Quit))
        );
        assert_eq!(
            map_key_event(
                KeyEvent::new(KeyCode::Char('u'), KeyModifiers::CONTROL),
                InputMode::Normal
            ),
            Some(WatchInput::Search(SearchKey::Clear))
        );
    }

    #[test]
    fn map_key_event_is_mode_aware_for_text_entry() {
        // `q` quits in normal mode but is plain text while typing a query.
        assert_eq!(
            map_key_event(
                KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
                InputMode::Normal
            ),
            Some(WatchInput::Normal(WatchKey::Quit))
        );
        for mode in [InputMode::Search, InputMode::LogSearch] {
            assert_eq!(
                map_key_event(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE), mode),
                Some(WatchInput::Search(SearchKey::Char('q')))
            );
            assert_eq!(
                map_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), mode),
                Some(WatchInput::Search(SearchKey::Submit))
            );
        }
    }

    #[test]
    fn map_mouse_event_scrolls_log_pane() {
        assert_eq!(
            map_mouse_event(MouseEventKind::ScrollUp),
            Some(WatchInput::Normal(WatchKey::PageUp))
        );
        assert_eq!(
            map_mouse_event(MouseEventKind::ScrollDown),
            Some(WatchInput::Normal(WatchKey::PageDown))
        );
        assert_eq!(map_mouse_event(MouseEventKind::Moved), None);
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
            started_at: None,
            finished_at: None,
            duration_seconds: None,
            assertions: None,
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
            started_at: None,
            finished_at: None,
            duration_seconds: None,
            assertions: None,
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
        let guard = TerminalGuard::enter(false).expect("enter terminal guard");
        assert!(guard.panic_restore_armed());
        drop(guard);

        let model = WatchModel {
            snapshot: sample_snapshot(),
            selected_index: 0,
            walltime_progress: None,
            log_lines: vec!["line".into()],
            show_help: false,
            filter: None,
            search_buffer: String::new(),
            input_mode: InputMode::Normal,
            ..sample_watch_model()
        };
        let mut renderer = FrameRenderer::new();
        renderer
            .render(&render_watch_frame(&model, 90, 14), (90, 14))
            .expect("render frame");
        // A second identical render exercises the no-change diff path.
        renderer
            .render(&render_watch_frame(&model, 90, 14), (90, 14))
            .expect("render frame again");
        // A different size forces the resize/full-repaint path.
        renderer
            .render(&render_watch_frame(&model, 70, 10), (70, 10))
            .expect("render frame resized");

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
            WatchPrefs::default(),
        )
        .expect("run watch ui");
        assert!(matches!(outcome, WatchOutcome::Completed(_)));
    }

    #[test]
    fn should_hold_on_exit_matches_policy_and_terminal_outcome() {
        fn status(state: &str, failed: bool) -> SchedulerStatus {
            SchedulerStatus {
                state: state.into(),
                source: SchedulerSource::Sacct,
                terminal: true,
                failed,
                detail: None,
            }
        }

        let completed = WatchOutcome::Completed(status("COMPLETED", false));
        let failed = WatchOutcome::Failed(status("FAILED", true));
        let unknown = WatchOutcome::Unknown(status("unknown", false));
        let interrupted = WatchOutcome::Interrupted(status("RUNNING", false));

        for outcome in [&completed, &failed, &unknown, &interrupted] {
            assert!(!should_hold_on_exit(HoldOnExit::Never, outcome));
        }

        assert!(!should_hold_on_exit(HoldOnExit::Failure, &completed));
        assert!(should_hold_on_exit(HoldOnExit::Failure, &failed));
        assert!(!should_hold_on_exit(HoldOnExit::Failure, &unknown));
        assert!(!should_hold_on_exit(HoldOnExit::Failure, &interrupted));

        assert!(should_hold_on_exit(HoldOnExit::Always, &completed));
        assert!(should_hold_on_exit(HoldOnExit::Always, &failed));
        assert!(!should_hold_on_exit(HoldOnExit::Always, &unknown));
        assert!(!should_hold_on_exit(HoldOnExit::Always, &interrupted));
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
            28,
        );
        assert!(frame.contains("Keybindings:"));
        assert!(frame.contains("j / Down"));
        assert!(frame.contains("f           find in logs"));
        assert!(frame.contains("w           toggle log line wrap"));
        assert!(frame.contains("o           cycle service sort"));
        assert!(frame.contains("q           quit"));
        assert!(frame.contains("q quit"));
        assert!(frame.lines().count() <= 28);
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
            28,
        );
        let lines = canonical_frame_lines(&frame);

        assert!(lines.iter().any(|line| line == "Keybindings:"));
        assert!(
            lines
                .iter()
                .any(|line| line == "  /           filter services by name")
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
        assert_snapshot_line(
            &lines,
            4,
            "? help | / filter | f find | w wrap | o sort | q",
        );
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
