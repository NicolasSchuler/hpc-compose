use crate::term;

use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};
use std::panic::PanicHookInfo;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::JoinHandle;
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
    GpuNodeSummary, PsServiceRow, PsSnapshot, ReplayReport, SchedulerOptions, SchedulerStatus,
    StatsOptions, StatsSnapshot, SubmissionBackend, SubmissionRecord, WalltimeProgress,
    WatchOutcome, build_ps_snapshot, build_stats_snapshot_with_status, format_walltime_summary,
    runtime_job_root_for_record, walltime_progress, walltime_progress_percent,
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
/// How often the background probe worker wakes to check its refresh timers and
/// the shutdown flag between probes. Small enough that quit is observed
/// promptly, large enough to avoid a busy-loop.
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Upper bound the UI thread waits for the probe worker to finish on shutdown
/// before detaching it. Kept short so a worker parked in a ~10s sstat/squeue
/// timeout never delays terminal restore.
const WORKER_JOIN_TIMEOUT: Duration = Duration::from_millis(200);

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

struct WatchFrameModel<'a> {
    snapshot: &'a PsSnapshot,
    selected_index: usize,
    walltime_progress: Option<&'a WalltimeProgress>,
    log_lines: &'a [String],
    follow_logs: bool,
    log_scroll: usize,
    log_view_mode: LogViewMode,
    hold_state: Option<WatchHoldState>,
    metrics_line: Option<&'a str>,
    show_help: bool,
    filter: Option<&'a str>,
    search_buffer: &'a str,
    input_mode: InputMode,
    log_query: Option<&'a str>,
    log_wrap: bool,
    sort_mode: ServiceSort,
    notice: Option<&'a str>,
    show_detail: bool,
    replay: Option<&'a ReplayWatchStatus>,
}

impl<'a> From<&'a WatchModel> for WatchFrameModel<'a> {
    fn from(model: &'a WatchModel) -> Self {
        Self {
            snapshot: &model.snapshot,
            selected_index: model.selected_index,
            walltime_progress: model.walltime_progress.as_ref(),
            log_lines: &model.log_lines,
            follow_logs: model.follow_logs,
            log_scroll: model.log_scroll,
            log_view_mode: model.log_view_mode,
            hold_state: model.hold_state,
            metrics_line: model.metrics_line.as_deref(),
            show_help: model.show_help,
            filter: model.filter.as_deref(),
            search_buffer: &model.search_buffer,
            input_mode: model.input_mode,
            log_query: model.log_query.as_deref(),
            log_wrap: model.log_wrap,
            sort_mode: model.sort_mode,
            notice: model.notice.as_deref(),
            show_detail: model.show_detail,
            replay: model.replay.as_ref(),
        }
    }
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

/// Armed while a `TerminalGuard` holds the terminal in raw / alternate-screen
/// mode. The panic hook, `Drop`, and the SIGTERM/SIGHUP handlers each try to
/// claim the restore with `swap(false)`; the first to win performs it, so the
/// terminal is restored exactly once regardless of which path fires. This is a
/// process-global rather than a per-guard flag because the C signal handler
/// cannot capture guard state, and watch/replay never nest guards.
static TERMINAL_RESTORE_ARMED: AtomicBool = AtomicBool::new(false);

struct TerminalGuard {
    entered_terminal: bool,
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
        // Arm the shared restore guard before installing any handler so a panic
        // or signal that fires immediately still finds the flag set.
        TERMINAL_RESTORE_ARMED.store(true, Ordering::SeqCst);
        let previous_hook = install_terminal_panic_hook(entered_terminal);
        if entered_terminal {
            signal_restore::install();
        }
        Self {
            entered_terminal,
            previous_hook,
        }
    }

    #[cfg(test)]
    fn panic_restore_armed(&self) -> bool {
        TERMINAL_RESTORE_ARMED.load(Ordering::SeqCst)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        // Claim the restore first so our still-installed signal handler cannot
        // repeat it; then neutralize the handlers now that no guard is live.
        let claimed = TERMINAL_RESTORE_ARMED.swap(false, Ordering::SeqCst);
        if self.entered_terminal && claimed {
            restore_terminal_best_effort();
        }
        if self.entered_terminal {
            signal_restore::remove();
        }
        if let Some(previous_hook) = self.previous_hook.take() {
            restore_previous_panic_hook(previous_hook);
        }
    }
}

#[cfg(not(test))]
fn install_terminal_panic_hook(entered_terminal: bool) -> Option<SharedPanicHook> {
    let previous_hook = std::panic::take_hook();
    let previous_hook = Arc::new(Mutex::new(Some(previous_hook)));
    let hook_previous = Arc::clone(&previous_hook);
    std::panic::set_hook(Box::new(move |info| {
        if entered_terminal && TERMINAL_RESTORE_ARMED.swap(false, Ordering::SeqCst) {
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
fn install_terminal_panic_hook(_entered_terminal: bool) -> Option<SharedPanicHook> {
    None
}

/// SIGTERM/SIGHUP handling that restores the terminal before the process dies.
///
/// An external `kill` or a terminal-close (SIGHUP) takes the default
/// termination action, so neither `Drop` nor the panic hook runs and the
/// terminal is left in raw + alternate-screen mode. While a `TerminalGuard`
/// holds the terminal we install a handler that performs the same best-effort
/// restore, then resets the disposition to default and re-raises the signal so
/// the exit status still reflects the signal. `restore_terminal_best_effort`
/// only writes escape sequences and calls `tcsetattr`, the accepted pattern for
/// this class of TUI signal handler. libc is used directly (already a direct
/// dependency, and the mechanism `dev.rs` uses) so no new dependency is added.
#[cfg(all(unix, not(test)))]
mod signal_restore {
    use super::{Ordering, TERMINAL_RESTORE_ARMED, restore_terminal_best_effort};
    use std::sync::atomic::AtomicUsize;

    // Previous dispositions, restored when the guard drops so normal exit does
    // not leave our handler installed for the next command.
    static PREV_SIGTERM: AtomicUsize = AtomicUsize::new(0);
    static PREV_SIGHUP: AtomicUsize = AtomicUsize::new(0);

    extern "C" fn handle_terminal_signal(signum: libc::c_int) {
        // Whoever claims the flag owns the single restore; Drop/panic are gated
        // on the same flag so this cannot double-restore.
        if TERMINAL_RESTORE_ARMED.swap(false, Ordering::SeqCst) {
            restore_terminal_best_effort();
        }
        // Preserve exit-status semantics: default the disposition and re-raise.
        unsafe {
            libc::signal(signum, libc::SIG_DFL);
            libc::raise(signum);
        }
    }

    pub(super) fn install() {
        let handler = handle_terminal_signal as *const () as usize;
        unsafe {
            PREV_SIGTERM.store(libc::signal(libc::SIGTERM, handler), Ordering::SeqCst);
            PREV_SIGHUP.store(libc::signal(libc::SIGHUP, handler), Ordering::SeqCst);
        }
    }

    pub(super) fn remove() {
        unsafe {
            libc::signal(libc::SIGTERM, PREV_SIGTERM.load(Ordering::SeqCst));
            libc::signal(libc::SIGHUP, PREV_SIGHUP.load(Ordering::SeqCst));
        }
    }
}

/// Non-Unix / test builds have no signal restore; `Drop` and the panic hook
/// remain the only restore paths.
#[cfg(not(all(unix, not(test))))]
mod signal_restore {
    pub(super) fn install() {}
    pub(super) fn remove() {}
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
    if let Err(err) = execute!(
        stdout,
        DisableMouseCapture,
        crossterm::cursor::Show,
        crossterm::terminal::LeaveAlternateScreen
    ) {
        eprintln!("warning: failed to restore alternate-screen terminal state: {err}");
    }
    if let Err(err) = terminal::disable_raw_mode() {
        eprintln!("warning: failed to disable raw terminal mode: {err}");
    }
    if let Err(err) = stdout.flush() {
        eprintln!("warning: failed to flush terminal restore output: {err}");
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
        let (frame_width, frame_height) = terminal_size();
        let displayed_log_lines = match log_view_mode {
            LogViewMode::Selected => log_buffer.lines.as_slice(),
            LogViewMode::All => all_log_lines.as_slice(),
        };
        let replay_status = ReplayWatchStatus {
            cursor_unix: playback.cursor_unix,
            speed: playback.speed,
            paused: playback.paused,
            fidelity: report.fidelity.clone(),
            start_unix: report.timeline_start_unix.unwrap_or(0),
            end_unix: report.timeline_end_unix.unwrap_or(0),
            event_unix: report.events.iter().map(|event| event.at_unix).collect(),
        };
        let rendered = render_watch_frame_model(
            &WatchFrameModel {
                snapshot: &snapshot,
                selected_index,
                walltime_progress: None,
                log_lines: displayed_log_lines,
                follow_logs: false,
                log_scroll,
                log_view_mode,
                hold_state: None,
                metrics_line: frame.metrics_line.as_deref(),
                show_help,
                filter: filter.as_deref(),
                search_buffer: &search_buffer,
                input_mode,
                log_query: log_query.as_deref(),
                log_wrap,
                sort_mode,
                notice: None,
                show_detail,
                replay: Some(&replay_status),
            },
            frame_width,
            frame_height,
        );
        renderer.render(&rendered, (frame_width, frame_height))?;

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

/// Returns whether the live walltime progress changed since the last render.
///
/// The watch loop wakes far more often (~10x/sec) than the walltime bar
/// advances (once per second), so this gate lets idle wake-ups skip rebuilding
/// the frame while still repainting on each real progress tick.
fn walltime_changed(
    previous: &Option<WalltimeProgress>,
    current: &Option<WalltimeProgress>,
) -> bool {
    previous != current
}

/// A refresh produced by the background probe worker and handed to the UI thread
/// over an [`mpsc`] channel. Keeping data and metrics as separate variants lets
/// the two run on their own cadence while the UI applies whichever is freshest.
enum WatchWorkerMsg {
    /// A new `ps`-style snapshot (squeue/sacct + service rows). Carries the fetch
    /// `Result` so a probe failure propagates to the UI thread exactly as the old
    /// inline `build_ps_snapshot(...)?` did. Boxed to keep the enum small since a
    /// `PsSnapshot` dwarfs the metrics variant.
    Data(Result<Box<PsSnapshot>>),
    /// A refreshed metrics line (sstat + sampler summary), or `None` when nothing
    /// is available. Errors are swallowed here just as the old inline path did.
    Metrics(Option<String>),
}

/// The freshest data/metrics drained from the worker channel in one UI tick.
struct DrainedWorker {
    /// The last data message seen this tick, if any.
    data: Option<Result<Box<PsSnapshot>>>,
    /// The last metrics message seen this tick, if any.
    metrics: Option<Option<String>>,
}

/// Collapses every message currently queued from the worker down to the freshest
/// of each kind (last write wins). The UI thread applies only the newest, so a
/// backlog of stale snapshots never causes redundant reseeding or a visible
/// rewind — it jumps straight to the latest state.
fn drain_worker_messages<I>(messages: I) -> DrainedWorker
where
    I: IntoIterator<Item = WatchWorkerMsg>,
{
    let mut data = None;
    let mut metrics = None;
    for message in messages {
        match message {
            WatchWorkerMsg::Data(snapshot) => data = Some(snapshot),
            WatchWorkerMsg::Metrics(line) => metrics = Some(line),
        }
    }
    DrainedWorker { data, metrics }
}

/// Applies a freshly drained metrics line to the current value, reporting whether
/// it changed (and therefore whether a repaint is warranted). Mirrors the old
/// inline `if refreshed != metrics_line` gate so idle metrics ticks stay quiet.
fn apply_worker_metrics(metrics_line: &mut Option<String>, incoming: Option<String>) -> bool {
    if *metrics_line != incoming {
        *metrics_line = incoming;
        true
    } else {
        false
    }
}

/// Owns the background probe worker and guarantees it is torn down when the watch
/// loop exits by any path (quit key, terminal state, error, panic). On drop it
/// signals shutdown then waits a bounded time for the worker to finish; if the
/// worker is parked mid-probe (a scheduler probe can block up to ~10s), it
/// detaches rather than joining so quit — and the terminal restore that follows —
/// is never held hostage to a hung sstat/squeue call.
struct WatchWorker {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for WatchWorker {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let Some(handle) = self.handle.take() else {
            return;
        };
        let deadline = Instant::now() + WORKER_JOIN_TIMEOUT;
        while !handle.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if handle.is_finished() {
            let _ = handle.join();
        }
        // Otherwise detach: dropping `handle` here lets the worker keep running
        // until its in-flight probe returns and observes `shutdown`, so a stuck
        // sstat/squeue never blocks exit.
    }
}

/// Background probe loop: fetches `ps` snapshots on the data cadence and metrics
/// lines on the metrics cadence, sending each over `tx`. Runs off the UI thread
/// so keystrokes and redraw never wait on a scheduler probe.
fn run_watch_worker(
    record: SubmissionRecord,
    options: SchedulerOptions,
    data_refresh: Duration,
    metrics_refresh: Duration,
    initial_scheduler: SchedulerStatus,
    tx: mpsc::Sender<WatchWorkerMsg>,
    shutdown: Arc<AtomicBool>,
) {
    let stats_options = StatsOptions {
        scheduler: options.clone(),
        sstat_bin: "sstat".to_string(),
        accounting: false,
    };
    // The UI thread already fetched an initial snapshot synchronously, so start
    // the data timer now. Seed the metrics timer in the past so the first metrics
    // line is produced immediately, mirroring the old inline cadence.
    let mut last_data = Instant::now();
    let mut last_metrics = Instant::now()
        .checked_sub(metrics_refresh)
        .unwrap_or_else(Instant::now);
    // Reuse the scheduler status the data probe already fetched for the metrics
    // snapshot so the metrics path never re-runs squeue/sacct (the old
    // `build_stats_snapshot` did). Seeded from the UI thread's initial snapshot.
    let mut last_scheduler = initial_scheduler;
    while !shutdown.load(Ordering::Relaxed) {
        if last_data.elapsed() >= data_refresh {
            let snapshot = build_ps_snapshot(&record.compose_file, Some(&record.job_id), &options);
            if let Ok(snapshot) = &snapshot {
                last_scheduler = snapshot.scheduler.clone();
            }
            if tx
                .send(WatchWorkerMsg::Data(snapshot.map(Box::new)))
                .is_err()
            {
                return; // UI thread dropped the receiver; nothing left to do.
            }
            last_data = Instant::now();
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
        }
        if last_metrics.elapsed() >= metrics_refresh {
            let metrics =
                load_watch_metrics_line(&record, &stats_options, Some(last_scheduler.clone()));
            if tx.send(WatchWorkerMsg::Metrics(metrics)).is_err() {
                return;
            }
            last_metrics = Instant::now();
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
        }
        std::thread::sleep(WORKER_POLL_INTERVAL);
    }
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
    let mut metrics_line = None;

    // Move the periodic scheduler probes (squeue/sacct for data, sstat for
    // metrics) onto a background worker so the UI thread never blocks on IO. The
    // worker sends refreshes over `worker_rx`; the UI drains them non-blockingly
    // each iteration. `worker_rx` is declared before `_worker` so the guard's
    // drop (which joins/detaches the thread) runs before the receiver is dropped.
    let (worker_tx, worker_rx) = mpsc::channel::<WatchWorkerMsg>();
    let worker_shutdown = Arc::new(AtomicBool::new(false));
    let _worker = WatchWorker {
        shutdown: Arc::clone(&worker_shutdown),
        handle: Some({
            let record = record.clone();
            let options = options.clone();
            let initial_scheduler = snapshot.scheduler.clone();
            std::thread::Builder::new()
                .name("hpc-watch-probe".to_string())
                .spawn(move || {
                    run_watch_worker(
                        record,
                        options,
                        data_refresh,
                        metrics_refresh,
                        initial_scheduler,
                        worker_tx,
                        worker_shutdown,
                    )
                })
                .context("failed to spawn watch probe worker")?
        }),
    };
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
    // Redraw gate: the loop wakes every `INPUT_POLL_INTERVAL` (~100ms) but the
    // frame only changes on a data/metrics refresh, a key/mouse/resize event, a
    // notice change, or the per-second walltime tick. Skipping the snapshot
    // clone + frame formatting on idle wake-ups avoids ~10x/sec of redundant
    // work. The first iteration always renders.
    let mut dirty = true;
    let mut last_walltime: Option<WalltimeProgress> = None;
    let mut last_render_size: Option<(usize, usize)> = None;

    let (outcome, command_hint) = loop {
        // Drain everything the worker queued since the last iteration, keeping
        // only the freshest data/metrics. This never blocks: `try_iter` yields
        // just the already-delivered messages so keystrokes and redraw are never
        // held up by an in-flight probe.
        let drained = drain_worker_messages(worker_rx.try_iter());
        if let Some(result) = drained.data {
            // A probe failure propagates exactly as the old inline
            // `build_ps_snapshot(...)?` did; the worker guard's drop still tears
            // the thread down on this early return.
            let new_snapshot = result?;
            dirty = true;
            let selected_name = selected_service_name(
                &snapshot.services,
                filter.as_deref(),
                sort_mode,
                selected_index,
            );
            snapshot = *new_snapshot;
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
        }
        if let Some(refreshed) = drained.metrics
            && apply_worker_metrics(&mut metrics_line, refreshed)
        {
            dirty = true;
        }
        let walltime_progress = walltime_progress(
            &snapshot.record,
            &snapshot.scheduler,
            snapshot.queue_diagnostics.as_ref(),
            current_unix_timestamp(),
        );
        // The walltime bar advances once per second; treat any change to it as a
        // reason to repaint even when no other input arrived.
        if walltime_changed(&last_walltime, &walltime_progress) {
            dirty = true;
            last_walltime = walltime_progress.clone();
        }
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
            // Reaching a terminal state changes the header/footer/selection, so
            // force a repaint regardless of any other trigger this iteration.
            dirty = true;
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
            dirty = true;
        }

        // A real terminal resize (no input event) must still trigger a repaint.
        let frame_size = terminal_size();
        if last_render_size != Some(frame_size) {
            dirty = true;
        }

        if dirty {
            let (frame_width, frame_height) = frame_size;
            let displayed_log_lines = match log_view_mode {
                LogViewMode::Selected => log_buffer.lines.as_slice(),
                LogViewMode::All => all_log_lines.as_slice(),
            };
            let hold_state = terminal_outcome.as_ref().map(|outcome| WatchHoldState {
                failed: matches!(outcome, WatchOutcome::Failed(_)),
            });
            let rendered = render_watch_frame_model(
                &WatchFrameModel {
                    snapshot: &snapshot,
                    selected_index,
                    walltime_progress: walltime_progress.as_ref(),
                    log_lines: displayed_log_lines,
                    follow_logs,
                    log_scroll,
                    log_view_mode,
                    hold_state,
                    metrics_line: metrics_line.as_deref(),
                    show_help,
                    filter: filter.as_deref(),
                    search_buffer: &search_buffer,
                    input_mode,
                    log_query: log_query.as_deref(),
                    log_wrap,
                    sort_mode,
                    notice: notice.as_deref(),
                    show_detail,
                    replay: None,
                },
                frame_width,
                frame_height,
            );
            renderer.render(&rendered, (frame_width, frame_height))?;
            last_render_size = Some(frame_size);
            dirty = false;
        }

        if let Some(event) = events.poll_event(INPUT_POLL_INTERVAL, input_mode)? {
            // Any handled input may change the view, selection, scroll, filter,
            // or notice; conservatively repaint on the next iteration.
            dirty = true;
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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn render_watch_frame(model: &WatchModel, width: usize, height: usize) -> String {
    render_watch_frame_model(&WatchFrameModel::from(model), width, height)
}

fn render_watch_frame_model(model: &WatchFrameModel<'_>, width: usize, height: usize) -> String {
    let width = width.max(1);
    let height = height.max(1);
    let effective = effective_services(&model.snapshot.services, model.filter, model.sort_mode);
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
        .map(|f| format!(" | {}", term::styled_warning(&format!("filter: {f}"))))
        .unwrap_or_default();

    let title_line = if let Some(replay) = model.replay {
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
    if let Some(progress) = model.walltime_progress {
        lines.push(fit_line(&render_walltime_bar(progress, width), width));
    }
    if let Some(replay) = model.replay {
        lines.push(fit_line(&render_replay_scrubber(replay, width), width));
    }
    if let Some(metrics) = model.metrics_line {
        lines.push(fit_line(metrics, width));
    }
    if let Some(notice) = model.notice {
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
    let search_note = match model.log_query {
        Some(query) if !query.is_empty() => {
            format!(" /{query} ({})", count_log_matches(model.log_lines, query))
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
    let displayed = expand_log_lines(model.log_lines, log_width, model.log_wrap);
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
        log_lines.push(fit_line(&style_log_row(line, model.log_query), log_width));
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
    model: &WatchFrameModel<'_>,
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
        &match model.replay {
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
    if let Some(progress) = model.walltime_progress {
        push_fit_line(
            &mut lines,
            width,
            height,
            &render_walltime_bar(progress, width),
        );
    }
    if let Some(metrics) = model.metrics_line {
        push_fit_line(&mut lines, width, height, metrics);
    }
    if let Some(notice) = model.notice {
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
    if let Some(filter) = model.filter {
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

        let compact_search_note = match model.log_query {
            Some(query) if !query.is_empty() => {
                format!(" /{query} ({})", count_log_matches(model.log_lines, query))
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
        let compact_displayed = expand_log_lines(model.log_lines, width, model.log_wrap);
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
                &style_log_row(line, model.log_query),
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
    crate::secure_io::write_atomic(&path, format!("{service}\n").as_bytes(), false)
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

/// Loads the compact watch metrics line, reusing an already-probed scheduler
/// status so the metrics path never re-runs squeue/sacct (only sstat + the
/// sampler are read here). `prefetched` carries the status the data probe just
/// fetched; passing `None` falls back to probing, matching the old behavior.
fn load_watch_metrics_line(
    record: &SubmissionRecord,
    stats_options: &StatsOptions,
    prefetched: Option<SchedulerStatus>,
) -> Option<String> {
    let snapshot = build_stats_snapshot_with_status(
        &record.compose_file,
        Some(&record.job_id),
        stats_options,
        prefetched,
    )
    .ok()?;
    format_watch_metrics_line(&snapshot)
}

/// Formats the compact per-node GPU summary for the watch metrics line, e.g.
/// `gpu: 4 util=72% mem=2100/40960 MiB power=185W`. Power is shown only when the
/// sampler reported it (aggregated draw across the node's devices).
fn format_gpu_metrics(node: &GpuNodeSummary) -> String {
    let util = node
        .avg_utilization_gpu
        .map(|value| format!("{value:.0}%"))
        .unwrap_or_else(|| "-".to_string());
    let mem = match (node.memory_used_mib, node.memory_total_mib) {
        (Some(used), Some(total)) => format!("{used}/{total} MiB"),
        _ => "-".to_string(),
    };
    let mut line = format!("gpu: {} util={} mem={}", node.gpu_count, util, mem);
    if let Some(power) = node.power_draw_w {
        line.push_str(&format!(" power={power:.0}W"));
    }
    line
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
        parts.push(format_gpu_metrics(node));
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
    if lines == 0 {
        return Ok(Vec::new());
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to open {}", path.display()));
        }
    };
    let file_len = file
        .metadata()
        .with_context(|| format!("failed to read metadata for {}", path.display()))?
        .len();
    if file_len == 0 {
        return Ok(Vec::new());
    }

    const TAIL_CHUNK_SIZE: u64 = 16 * 1024;
    let mut position = file_len;
    let mut newline_count = 0usize;
    let mut chunks = Vec::new();
    while position > 0 && newline_count <= lines {
        let read_len = position.min(TAIL_CHUNK_SIZE) as usize;
        position -= read_len as u64;
        let mut chunk = vec![0_u8; read_len];
        file.seek(SeekFrom::Start(position))
            .with_context(|| format!("failed to seek {}", path.display()))?;
        file.read_exact(&mut chunk)
            .with_context(|| format!("failed to read {}", path.display()))?;
        newline_count += chunk.iter().filter(|byte| **byte == b'\n').count();
        chunks.push(chunk);
    }

    let total_len = chunks.iter().map(Vec::len).sum();
    let mut bytes = Vec::with_capacity(total_len);
    for chunk in chunks.iter().rev() {
        bytes.extend_from_slice(chunk);
    }
    let raw = String::from_utf8_lossy(&bytes);
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
mod tests;
