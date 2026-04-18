use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

use crate::term;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressMode {
    Hidden,
    Plain,
    Spinner,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ProgressReporter {
    mode: ProgressMode,
}

impl ProgressReporter {
    #[must_use]
    pub(crate) fn new(enabled: bool) -> Self {
        let mode = progress_mode(enabled, io::stderr().is_terminal());
        Self { mode }
    }

    #[must_use]
    pub(crate) fn start(self, message: impl Into<String>) -> ProgressStep {
        ProgressStep::new(self.mode, message.into())
    }

    pub(crate) fn run_result<T, E>(
        self,
        message: impl Into<String>,
        operation: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let step = self.start(message);
        match operation() {
            Ok(value) => {
                step.finish();
                Ok(value)
            }
            Err(err) => {
                step.fail();
                Err(err)
            }
        }
    }
}

fn progress_mode(enabled: bool, stderr_is_terminal: bool) -> ProgressMode {
    if !enabled {
        ProgressMode::Hidden
    } else if stderr_is_terminal {
        ProgressMode::Spinner
    } else {
        ProgressMode::Plain
    }
}

pub(crate) struct ProgressStep {
    mode: ProgressMode,
    message: String,
    started_at: Instant,
    pb: Option<ProgressBar>,
    finished: bool,
}

impl ProgressStep {
    fn new(mode: ProgressMode, message: String) -> Self {
        let pb = match mode {
            ProgressMode::Hidden => None,
            ProgressMode::Plain => {
                write_plain_start(&message);
                None
            }
            ProgressMode::Spinner => {
                let pb = ProgressBar::new_spinner();
                pb.set_style(
                    ProgressStyle::with_template("{spinner} {msg}")
                        .unwrap_or_else(|_| ProgressStyle::default_spinner()),
                );
                pb.set_message(message.clone());
                pb.enable_steady_tick(Duration::from_millis(100));
                Some(pb)
            }
        };

        Self {
            mode,
            message,
            started_at: Instant::now(),
            pb,
            finished: false,
        }
    }

    pub(crate) fn finish(mut self) {
        self.complete(true);
    }

    pub(crate) fn fail(mut self) {
        self.complete(false);
    }

    fn complete(&mut self, success: bool) {
        if self.finished {
            return;
        }

        if let Some(pb) = self.pb.take() {
            pb.finish_and_clear();
        }

        let elapsed = format_elapsed(self.started_at.elapsed());

        match self.mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain => {
                write_plain_complete(success, &self.message, &elapsed);
            }
            ProgressMode::Spinner => {
                write_spinner_complete(success, &self.message, &elapsed);
            }
        }

        self.finished = true;
    }
}

impl Drop for ProgressStep {
    fn drop(&mut self) {
        if let Some(pb) = self.pb.take() {
            pb.finish_and_clear();
        }
    }
}

fn write_plain_start(message: &str) {
    let state = term::styled_state_run_stderr();
    let mut stderr = io::stderr();
    let _ = writeln!(stderr, "[{state}] {message}");
    let _ = stderr.flush();
}

fn write_plain_complete(success: bool, message: &str, elapsed: &str) {
    let state = if success {
        term::styled_state_done_stderr()
    } else {
        term::styled_state_fail_stderr()
    };
    let mut stderr = io::stderr();
    let _ = writeln!(stderr, "[{state}] {message} ({elapsed})");
    let _ = stderr.flush();
}

fn write_spinner_complete(success: bool, message: &str, elapsed: &str) {
    let state = if success {
        term::styled_state_done_stderr()
    } else {
        term::styled_state_fail_stderr()
    };
    let mut stderr = io::stderr();
    let _ = writeln!(stderr, "[{state}] {message} ({elapsed})");
    let _ = stderr.flush();
}

fn format_elapsed(duration: Duration) -> String {
    if duration.as_secs() >= 60 {
        let total_seconds = duration.as_secs();
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        format!("{minutes}m{seconds:02}s")
    } else if duration.as_secs() >= 1 {
        format!("{:.1}s", duration.as_secs_f64())
    } else {
        format!("{}ms", duration.as_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_mode_covers_hidden_plain_and_spinner() {
        assert_eq!(progress_mode(false, false), ProgressMode::Hidden);
        assert_eq!(progress_mode(true, false), ProgressMode::Plain);
        assert_eq!(progress_mode(true, true), ProgressMode::Spinner);
    }

    #[test]
    fn progress_reporter_run_result_covers_success_and_failure() {
        let ok = ProgressReporter {
            mode: ProgressMode::Hidden,
        }
        .run_result("ok", || -> Result<_, &'static str> { Ok(7) })
        .expect("success result");
        assert_eq!(ok, 7);

        let err = ProgressReporter {
            mode: ProgressMode::Hidden,
        }
        .run_result("err", || -> Result<(), &'static str> { Err("boom") })
        .expect_err("failure result");
        assert_eq!(err, "boom");
    }

    #[test]
    fn progress_step_helpers_cover_plain_and_spinner_paths() {
        let plain = ProgressStep::new(ProgressMode::Plain, "plain".into());
        assert!(plain.pb.is_none());
        assert!(!plain.finished);
        plain.finish();

        let spinner = ProgressStep::new(ProgressMode::Spinner, "spinner".into());
        assert!(spinner.pb.is_some());
        spinner.fail();

        let hidden = ProgressStep::new(ProgressMode::Hidden, "hidden".into());
        hidden.finish();
    }

    #[test]
    fn format_elapsed_covers_subsecond_seconds_and_minutes() {
        assert_eq!(format_elapsed(Duration::from_millis(345)), "345ms");
        assert_eq!(format_elapsed(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_elapsed(Duration::from_secs(125)), "2m05s");
    }
}
