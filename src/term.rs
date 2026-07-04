use std::io::{self, IsTerminal};
use std::sync::atomic::{AtomicU8, Ordering};

use owo_colors::OwoColorize;

static COLOR_POLICY: AtomicU8 = AtomicU8::new(ColorPolicy::Always as u8);

/// Controls when styled terminal output should be emitted.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, clap::ValueEnum)]
pub enum ColorPolicy {
    /// Enable color only when the destination stream is a terminal.
    Auto,
    /// Always emit ANSI styling sequences.
    Always,
    /// Never emit ANSI styling sequences.
    Never,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OutputStream {
    Stdout,
    Stderr,
}

pub(crate) fn init_color(policy: ColorPolicy) {
    COLOR_POLICY.store(policy as u8, Ordering::Relaxed);
}

fn current_color_policy() -> ColorPolicy {
    match COLOR_POLICY.load(Ordering::Relaxed) {
        value if value == ColorPolicy::Auto as u8 => ColorPolicy::Auto,
        value if value == ColorPolicy::Never as u8 => ColorPolicy::Never,
        _ => ColorPolicy::Always,
    }
}

fn colors_enabled() -> bool {
    colors_enabled_for(OutputStream::Stdout)
}

fn stderr_colors_enabled() -> bool {
    colors_enabled_for(OutputStream::Stderr)
}

fn colors_enabled_for(stream: OutputStream) -> bool {
    match current_color_policy() {
        ColorPolicy::Always => true,
        ColorPolicy::Never => false,
        ColorPolicy::Auto => auto_detect_color(stream),
    }
}

fn auto_detect_color(stream: OutputStream) -> bool {
    auto_detect_color_with_terminal_state(
        stream,
        std::env::var_os("NO_COLOR").is_some(),
        clicolor_force_active(std::env::var_os("CLICOLOR_FORCE")),
        clicolor_disabled(std::env::var_os("CLICOLOR")),
        std::env::var("TERM").ok().as_deref(),
        io::stdout().is_terminal(),
        io::stderr().is_terminal(),
    )
}

/// `CLICOLOR_FORCE` is "active" when it is set to any value other than `0`,
/// per the CLICOLORS informal spec (<https://bixense.com/clicolors/>).
fn clicolor_force_active(value: Option<std::ffi::OsString>) -> bool {
    matches!(value, Some(v) if v != "0")
}

/// `CLICOLOR=0` explicitly disables color. Any other value (or unset) leaves
/// the auto/tty behavior intact, per the CLICOLORS informal spec.
fn clicolor_disabled(value: Option<std::ffi::OsString>) -> bool {
    matches!(value, Some(v) if v == "0")
}

/// Resolve color for the `auto` policy, honoring `NO_COLOR`, the CLICOLORS
/// spec (`CLICOLOR` / `CLICOLOR_FORCE`), `TERM=dumb`, and tty state.
///
/// Precedence (highest first). `--color always|never` is resolved by the
/// caller before this function is reached, so the explicit flag always wins:
/// 1. `NO_COLOR` set (any value) -> disabled. We let `NO_COLOR` win over
///    `CLICOLOR_FORCE`: the CLICOLORS spec itself is silent on `NO_COLOR`
///    (it predates it), and the no-color.org convention plus most
///    implementations treat a color-*off* signal as dominant when it conflicts
///    with a color-*on* one.
/// 2. `CLICOLOR_FORCE` != 0 -> forced on, even when not a tty and even under
///    `TERM=dumb`.
/// 3. `TERM=dumb` -> disabled.
/// 4. Not a terminal -> disabled.
/// 5. `CLICOLOR=0` -> disabled.
/// 6. Otherwise (a tty, `CLICOLOR` unset or non-zero) -> enabled.
fn auto_detect_color_with_terminal_state(
    stream: OutputStream,
    no_color: bool,
    clicolor_force: bool,
    clicolor_off: bool,
    term: Option<&str>,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> bool {
    if no_color {
        return false;
    }
    if clicolor_force {
        return true;
    }
    if term == Some("dumb") {
        return false;
    }
    let is_terminal = match stream {
        OutputStream::Stdout => stdout_is_terminal,
        OutputStream::Stderr => stderr_is_terminal,
    };
    if !is_terminal {
        return false;
    }
    !clicolor_off
}

macro_rules! styled_fn {
    ($name:ident, $method:ident) => {
        pub(crate) fn $name(text: &str) -> String {
            if colors_enabled() {
                text.$method().to_string()
            } else {
                text.to_string()
            }
        }
    };
}

styled_fn!(styled_success, green);
styled_fn!(styled_warning, yellow);
styled_fn!(styled_error, red);
styled_fn!(styled_dim, dimmed);
styled_fn!(styled_bold, bold);

pub(crate) fn styled_label(label: &str, value: &str) -> String {
    if colors_enabled() {
        format!("{}: {}", label.bold(), value)
    } else {
        format!("{}: {}", label, value)
    }
}

pub(crate) fn styled_state_done_stderr() -> String {
    if stderr_colors_enabled() {
        "done".green().bold().to_string()
    } else {
        "done".to_string()
    }
}

pub(crate) fn styled_state_run_stderr() -> String {
    if stderr_colors_enabled() {
        "run".yellow().bold().to_string()
    } else {
        "run".to_string()
    }
}

pub(crate) fn styled_state_fail_stderr() -> String {
    if stderr_colors_enabled() {
        "fail".red().bold().to_string()
    } else {
        "fail".to_string()
    }
}

pub(crate) fn styled_state_checked_stderr() -> String {
    if stderr_colors_enabled() {
        "checked".cyan().bold().to_string()
    } else {
        "checked".to_string()
    }
}

pub(crate) fn styled_scheduler_state(state: &str) -> String {
    if !colors_enabled() {
        return state.to_string();
    }
    match state.to_uppercase().as_str() {
        s if s.contains("RUNNING") => state.green().to_string(),
        s if s.contains("COMPLETED") => state.cyan().to_string(),
        s if s.contains("PENDING") => state.yellow().to_string(),
        s if s.contains("FAILED")
            | s.contains("TIMEOUT")
            | s.contains("CANCELLED")
            | s.contains("NODE_FAIL") =>
        {
            state.red().to_string()
        }
        _ => state.to_string(),
    }
}

pub(crate) fn styled_service_status(status: &str) -> String {
    if !colors_enabled() {
        return status.to_string();
    }
    match status {
        "ready" | "running" => status.green().to_string(),
        "starting" => status.yellow().to_string(),
        "failed" | "exited" => status.red().to_string(),
        "unknown" => status.dimmed().to_string(),
        _ => status.to_string(),
    }
}

pub(crate) fn styled_action_ok() -> String {
    if colors_enabled() {
        symbol_ok().green().bold().to_string()
    } else {
        symbol_ok().to_string()
    }
}

pub(crate) fn styled_action_build() -> String {
    if colors_enabled() {
        symbol_build().yellow().bold().to_string()
    } else {
        symbol_build().to_string()
    }
}

pub(crate) fn styled_action_reuse() -> String {
    if colors_enabled() {
        symbol_reuse().cyan().bold().to_string()
    } else {
        symbol_reuse().to_string()
    }
}

pub(crate) fn symbol_ok() -> &'static str {
    if unicode_allowed() {
        "\u{2713} OK"
    } else {
        "OK"
    }
}

#[allow(dead_code)]
pub(crate) fn symbol_fail() -> &'static str {
    if unicode_allowed() {
        "\u{2717} FAIL"
    } else {
        "FAIL"
    }
}

#[allow(dead_code)]
pub(crate) fn symbol_run() -> &'static str {
    if unicode_allowed() {
        "\u{25cf} RUN"
    } else {
        "RUN"
    }
}

#[allow(dead_code)]
pub(crate) fn symbol_pending() -> &'static str {
    if unicode_allowed() {
        "\u{25d0} PEND"
    } else {
        "PEND"
    }
}

fn symbol_build() -> &'static str {
    if unicode_allowed() {
        "\u{25cf} BUILD"
    } else {
        "BUILD"
    }
}

fn symbol_reuse() -> &'static str {
    if unicode_allowed() {
        "\u{25d0} REUSE"
    } else {
        "REUSE"
    }
}

fn unicode_allowed() -> bool {
    if colors_enabled() {
        unicode_allowed_raw()
    } else {
        false
    }
}

pub(crate) fn unicode_allowed_raw() -> bool {
    std::env::var("LANG")
        .or_else(|_| std::env::var("LC_ALL"))
        .is_ok_and(|v| v.contains("UTF-8") || v.contains("utf8") || v.contains("utf-8"))
}

pub(crate) fn styled_success_raw(text: &str) -> String {
    if colors_enabled() {
        text.green().to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn styled_warning_raw(text: &str) -> String {
    if colors_enabled() {
        text.yellow().to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn styled_error_raw(text: &str) -> String {
    if colors_enabled() {
        text.red().to_string()
    } else {
        text.to_string()
    }
}

/// Highlights a substring (e.g. a search match) using reverse video, which
/// stands out regardless of the surrounding foreground color.
pub(crate) fn styled_highlight_raw(text: &str) -> String {
    if colors_enabled() {
        text.reversed().to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn styled_section_header(text: &str) -> String {
    if colors_enabled() {
        text.bold().to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn styled_note(text: &str) -> String {
    if colors_enabled() {
        text.dimmed().to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn styled_service_log_prefix(service: &str) -> String {
    if !colors_enabled() {
        return format!("[{service}]");
    }
    let color_index = simple_hash(service) as usize % 7;
    match color_index {
        0 => format!("[{}]", service.cyan()),
        1 => format!("[{}]", service.magenta()),
        2 => format!("[{}]", service.blue()),
        3 => format!("[{}]", service.green()),
        4 => format!("[{}]", service.yellow()),
        5 => format!("[{}]", service.red()),
        _ => format!("[{}]", service.purple()),
    }
}

fn simple_hash(s: &str) -> u32 {
    let mut hash: u32 = 5381;
    for byte in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u32);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    fn color_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("color test lock")
    }

    fn with_color_policy<T>(policy: ColorPolicy, action: impl FnOnce() -> T) -> T {
        let _guard = color_test_lock();
        let previous = current_color_policy();
        init_color(policy);
        let result = action();
        init_color(previous);
        result
    }

    /// Convenience wrapper for the common case in these tests: no `NO_COLOR`,
    /// no CLICOLOR overrides.
    fn detect(
        stream: OutputStream,
        term: Option<&str>,
        stdout_tty: bool,
        stderr_tty: bool,
    ) -> bool {
        auto_detect_color_with_terminal_state(
            stream, false, false, false, term, stdout_tty, stderr_tty,
        )
    }

    #[test]
    fn auto_color_detection_tracks_each_stream_independently() {
        assert!(detect(
            OutputStream::Stdout,
            Some("xterm-256color"),
            true,
            false,
        ));
        assert!(!detect(
            OutputStream::Stdout,
            Some("xterm-256color"),
            false,
            true,
        ));
        assert!(detect(
            OutputStream::Stderr,
            Some("xterm-256color"),
            false,
            true,
        ));
    }

    #[test]
    fn auto_color_detection_respects_no_color_and_dumb_term() {
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            true,
            false,
            false,
            Some("xterm-256color"),
            true,
            true,
        ));
        assert!(!detect(OutputStream::Stderr, Some("dumb"), true, true,));
    }

    #[test]
    fn clicolor_env_parsing_follows_spec() {
        // CLICOLOR_FORCE active for any non-"0" value; inactive when unset or "0".
        assert!(clicolor_force_active(Some("1".into())));
        assert!(clicolor_force_active(Some("yes".into())));
        assert!(!clicolor_force_active(Some("0".into())));
        assert!(!clicolor_force_active(None));

        // CLICOLOR disables only for the exact value "0".
        assert!(clicolor_disabled(Some("0".into())));
        assert!(!clicolor_disabled(Some("1".into())));
        assert!(!clicolor_disabled(None));
    }

    #[test]
    fn clicolor_force_enables_color_when_not_a_tty() {
        // CLICOLOR_FORCE forces color on even without a terminal...
        assert!(auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            true, // CLICOLOR_FORCE
            false,
            Some("xterm-256color"),
            false, // not a tty
            false,
        ));
        // ...and even under TERM=dumb.
        assert!(auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            true,
            false,
            Some("dumb"),
            false,
            false,
        ));
    }

    #[test]
    fn clicolor_zero_disables_color_on_a_tty() {
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            false,
            true, // CLICOLOR=0
            Some("xterm-256color"),
            true, // is a tty
            true,
        ));
    }

    /// Pins the full precedence order documented on
    /// `auto_detect_color_with_terminal_state`.
    #[test]
    fn color_precedence_order_is_pinned() {
        // 1. NO_COLOR wins over CLICOLOR_FORCE.
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            true, // NO_COLOR
            true, // CLICOLOR_FORCE
            false,
            Some("xterm-256color"),
            true,
            true,
        ));
        // 2. CLICOLOR_FORCE wins over TERM=dumb, not-a-tty, and CLICOLOR=0.
        assert!(auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            true, // CLICOLOR_FORCE
            true, // CLICOLOR=0 (ignored under force)
            Some("dumb"),
            false, // not a tty
            false,
        ));
        // 3. TERM=dumb wins over a live tty and CLICOLOR (non-force).
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            false,
            false,
            Some("dumb"),
            true, // tty, but dumb
            true,
        ));
        // 4. not-a-tty wins over CLICOLOR=0 being absent (still off).
        assert!(!detect(
            OutputStream::Stdout,
            Some("xterm-256color"),
            false, // not a tty
            false,
        ));
        // 5. CLICOLOR=0 disables an otherwise-colorable tty.
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            false,
            true, // CLICOLOR=0
            Some("xterm-256color"),
            true,
            true,
        ));
        // 6. Default: tty with no overrides -> color on.
        assert!(detect(
            OutputStream::Stdout,
            Some("xterm-256color"),
            true,
            true,
        ));
    }

    #[test]
    fn styling_helpers_respect_always_and_never_policies() {
        with_color_policy(ColorPolicy::Always, || {
            assert!(styled_success("ok").contains("\u{1b}["));
            assert!(styled_warning("warn").contains("\u{1b}["));
            assert!(styled_error("err").contains("\u{1b}["));
            assert!(styled_dim("dim").contains("\u{1b}["));
            assert!(styled_bold("bold").contains("\u{1b}["));
            assert!(styled_label("key", "value").contains("\u{1b}["));
            assert!(styled_state_done_stderr().contains("\u{1b}["));
            assert!(styled_state_run_stderr().contains("\u{1b}["));
            assert!(styled_state_fail_stderr().contains("\u{1b}["));
            assert!(styled_scheduler_state("RUNNING").contains("\u{1b}["));
            assert!(styled_scheduler_state("FAILED").contains("\u{1b}["));
            assert!(styled_service_status("ready").contains("\u{1b}["));
            assert!(styled_service_status("failed").contains("\u{1b}["));
            assert!(styled_action_ok().contains("\u{1b}["));
            assert!(styled_action_build().contains("\u{1b}["));
            assert!(styled_action_reuse().contains("\u{1b}["));
            assert!(styled_section_header("Header").contains("\u{1b}["));
            assert!(styled_note("note").contains("\u{1b}["));
        });

        with_color_policy(ColorPolicy::Never, || {
            assert_eq!(styled_success("ok"), "ok");
            assert_eq!(styled_warning("warn"), "warn");
            assert_eq!(styled_error("err"), "err");
            assert_eq!(styled_dim("dim"), "dim");
            assert_eq!(styled_bold("bold"), "bold");
            assert_eq!(styled_label("key", "value"), "key: value");
            assert_eq!(styled_state_done_stderr(), "done");
            assert_eq!(styled_state_run_stderr(), "run");
            assert_eq!(styled_state_fail_stderr(), "fail");
            assert_eq!(styled_scheduler_state("RUNNING"), "RUNNING");
            assert_eq!(styled_service_status("ready"), "ready");
            assert_eq!(styled_action_ok(), "OK");
            assert_eq!(styled_action_build(), "BUILD");
            assert_eq!(styled_action_reuse(), "REUSE");
            assert_eq!(styled_section_header("Header"), "Header");
            assert_eq!(styled_note("note"), "note");
        });
    }

    #[test]
    fn styling_helpers_cover_unmatched_states_without_recoloring() {
        with_color_policy(ColorPolicy::Always, || {
            assert_eq!(styled_scheduler_state("STAGING"), "STAGING");
            assert_eq!(styled_service_status("custom"), "custom");
        });
    }
}
