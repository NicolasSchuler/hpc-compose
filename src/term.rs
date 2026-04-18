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
        std::env::var("TERM").ok().as_deref(),
        io::stdout().is_terminal(),
        io::stderr().is_terminal(),
    )
}

fn auto_detect_color_with_terminal_state(
    stream: OutputStream,
    no_color: bool,
    term: Option<&str>,
    stdout_is_terminal: bool,
    stderr_is_terminal: bool,
) -> bool {
    if no_color || term == Some("dumb") {
        return false;
    }
    match stream {
        OutputStream::Stdout => stdout_is_terminal,
        OutputStream::Stderr => stderr_is_terminal,
    }
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
        "OK".green().bold().to_string()
    } else {
        "OK".to_string()
    }
}

pub(crate) fn styled_action_build() -> String {
    if colors_enabled() {
        "BUILD".yellow().bold().to_string()
    } else {
        "BUILD".to_string()
    }
}

pub(crate) fn styled_action_reuse() -> String {
    if colors_enabled() {
        "REUSE".cyan().bold().to_string()
    } else {
        "REUSE".to_string()
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

    #[test]
    fn auto_color_detection_tracks_each_stream_independently() {
        assert!(auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            Some("xterm-256color"),
            true,
            false,
        ));
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stdout,
            false,
            Some("xterm-256color"),
            false,
            true,
        ));
        assert!(auto_detect_color_with_terminal_state(
            OutputStream::Stderr,
            false,
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
            Some("xterm-256color"),
            true,
            true,
        ));
        assert!(!auto_detect_color_with_terminal_state(
            OutputStream::Stderr,
            false,
            Some("dumb"),
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
