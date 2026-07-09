use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, IsTerminal, Write};
use std::time::{Duration, Instant};

use hpc_compose::prepare::{ArtifactAction, PrepareReporter, PrepareSummary};
use hpc_compose::runtime_plan::RuntimePlan;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

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

    pub(crate) fn run_checked_result<T, E>(
        self,
        message: impl Into<String>,
        operation: impl FnOnce() -> Result<T, E>,
        is_failure: impl FnOnce(&T) -> bool,
    ) -> Result<T, E> {
        let step = self.start(message);
        match operation() {
            Ok(value) => {
                if is_failure(&value) {
                    step.fail();
                } else {
                    step.checked();
                }
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

    pub(crate) fn checked(mut self) {
        self.complete_checked();
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

    fn complete_checked(&mut self) {
        if self.finished {
            return;
        }

        if let Some(pb) = self.pb.take() {
            pb.finish_and_clear();
        }

        let elapsed = format_elapsed(self.started_at.elapsed());
        match self.mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain | ProgressMode::Spinner => {
                let state = term::styled_state_checked_stderr();
                let mut stderr = io::stderr();
                let _ = writeln!(stderr, "[{state}] {} ({elapsed})", self.message);
                let _ = stderr.flush();
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

/// Live per-service progress for the prepare step. Owns the whole prepare
/// display (header line + one bar per service) and acts as the [`PrepareReporter`]
/// the library streams sub-progress into. On a non-terminal/quiet target it
/// falls back to plain `[run]`/`[done]` lines (plus coarse phase transitions),
/// so CI logs and `--format json` stay clean.
pub(crate) struct PrepareProgress {
    mode: ProgressMode,
    #[allow(dead_code)]
    multi: Option<MultiProgress>,
    header: Option<ProgressBar>,
    bars: RefCell<HashMap<String, ServiceBar>>,
}

struct ServiceBar {
    name: String,
    bar: ProgressBar,
    phase: String,
    line: String,
    bytes: u64,
    phase_started: Instant,
}

impl ServiceBar {
    fn render(&self) {
        self.bar.set_message(service_bar_message(
            &self.name,
            &self.phase,
            &self.line,
            self.bytes,
            self.phase_started.elapsed(),
        ));
    }
}

/// Builds a per-service progress bar message. Split out as a pure function so the
/// phase/output/bytes/elapsed formatting can be unit-tested without a live
/// indicatif bar. `elapsed` is the time spent in the current phase, surfaced live
/// during streaming so a long-running import/extract does not look stuck.
fn service_bar_message(
    name: &str,
    phase: &str,
    line: &str,
    bytes: u64,
    elapsed: Duration,
) -> String {
    let mut message = if phase.is_empty() {
        format!("{name} ...")
    } else {
        format!("{name}: {phase}")
    };
    if !line.is_empty() {
        message.push_str(" — ");
        message.push_str(line);
    }
    if bytes > 0 {
        message.push_str(&format!(" ({} written)", humanize_bytes(bytes)));
    }
    if !phase.is_empty() {
        message.push_str(&format!(" [{}]", format_elapsed(elapsed)));
    }
    message
}

impl PrepareProgress {
    pub(crate) fn new(plan: &RuntimePlan, enabled: bool) -> Self {
        let mut mode = progress_mode(enabled, io::stderr().is_terminal());
        // In verbose mode the library streams raw tool output straight through, so
        // demote the spinner to plain phase lines to avoid fighting that output.
        if mode == ProgressMode::Spinner && hpc_compose::prepare::prepare_verbose_enabled() {
            mode = ProgressMode::Plain;
        }
        if mode != ProgressMode::Spinner {
            return Self {
                mode,
                multi: None,
                header: None,
                bars: RefCell::new(HashMap::new()),
            };
        }
        let multi = MultiProgress::new();
        let header_style = ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner());
        let header = multi.add(ProgressBar::new_spinner());
        header.set_style(header_style);
        let style = ProgressStyle::with_template("    {wide_msg}")
            .unwrap_or_else(|_| ProgressStyle::default_bar());
        let mut bars = HashMap::new();
        for svc in &plan.ordered_services {
            let bar = multi.add(ProgressBar::new_spinner());
            bar.set_style(style.clone());
            bar.set_message(format!("{} ...", svc.name));
            bar.enable_steady_tick(Duration::from_millis(120));
            bars.insert(
                svc.name.clone(),
                ServiceBar {
                    name: svc.name.clone(),
                    bar,
                    phase: String::new(),
                    line: String::new(),
                    bytes: 0,
                    phase_started: Instant::now(),
                },
            );
        }
        Self {
            mode,
            multi: Some(multi),
            header: Some(header),
            bars: RefCell::new(bars),
        }
    }

    /// Runs the prepare operation, owning the surrounding `[run]`/`[done]`
    /// timing line so it does not contend with the per-service bars.
    pub(crate) fn run<T, E>(
        &self,
        message: impl AsRef<str>,
        operation: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let message = message.as_ref();
        let started = Instant::now();
        match self.mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain => write_plain_start(message),
            ProgressMode::Spinner => {
                if let Some(header) = &self.header {
                    header.set_message(message.to_string());
                    header.enable_steady_tick(Duration::from_millis(120));
                }
            }
        }
        let result = operation();
        let elapsed = format_elapsed(started.elapsed());
        if let Some(header) = &self.header {
            header.finish_and_clear();
        }
        match self.mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain => write_plain_complete(result.is_ok(), message, &elapsed),
            ProgressMode::Spinner => {
                // Clear the live service bars before the raw completion write, so
                // it is not appended onto a still-active bar (which corrupts
                // indicatif's line accounting and erases the completion line).
                for service_bar in self.bars.borrow().values() {
                    service_bar.bar.finish_and_clear();
                }
                write_spinner_complete(result.is_ok(), message, &elapsed);
            }
        }
        result
    }

    pub(crate) fn finish_from_summary(&self, summary: &PrepareSummary) {
        let bars = self.bars.borrow();
        for svc in &summary.services {
            if let Some(service_bar) = bars.get(&svc.service_name) {
                let action_label = match svc.runtime_image.action {
                    ArtifactAction::Present => "present",
                    ArtifactAction::Reused => "reused",
                    ArtifactAction::Built => "built",
                };
                service_bar
                    .bar
                    .set_message(format!("{} {}", svc.service_name, action_label));
                service_bar.bar.finish_and_clear();
            }
        }
    }
}

impl Drop for PrepareProgress {
    fn drop(&mut self) {
        for service_bar in self.bars.borrow().values() {
            service_bar.bar.finish_and_clear();
        }
        if let Some(header) = &self.header {
            header.finish_and_clear();
        }
    }
}

impl PrepareReporter for PrepareProgress {
    fn step_started(&self, service: &str, phase: &str) {
        match self.mode {
            ProgressMode::Spinner => {
                if let Some(service_bar) = self.bars.borrow_mut().get_mut(service) {
                    service_bar.phase = phase.to_string();
                    service_bar.line.clear();
                    service_bar.bytes = 0;
                    service_bar.phase_started = Instant::now();
                    service_bar.render();
                }
            }
            ProgressMode::Plain => {
                let mut stderr = io::stderr();
                let _ = writeln!(stderr, "    {service}: {phase}");
                let _ = stderr.flush();
            }
            ProgressMode::Hidden => {}
        }
    }

    fn step_output(&self, service: &str, line: &str) {
        if self.mode != ProgressMode::Spinner {
            return;
        }
        if let Some(service_bar) = self.bars.borrow_mut().get_mut(service) {
            service_bar.line = truncate_line(line);
            service_bar.render();
        }
    }

    fn step_bytes(&self, service: &str, bytes: u64) {
        if self.mode != ProgressMode::Spinner {
            return;
        }
        if let Some(service_bar) = self.bars.borrow_mut().get_mut(service) {
            service_bar.bytes = bytes;
            service_bar.render();
        }
    }
}

fn truncate_line(line: &str) -> String {
    const MAX: usize = 100;
    if line.chars().count() <= MAX {
        return line.to_string();
    }
    let truncated: String = line.chars().take(MAX - 1).collect();
    format!("{truncated}…")
}

fn humanize_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
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

    #[test]
    fn service_bar_message_shows_phase_output_bytes_and_live_elapsed() {
        // Idle bar before any phase: no elapsed, no trailing noise.
        assert_eq!(
            service_bar_message("trainer", "", "", 0, Duration::from_secs(3)),
            "trainer ..."
        );
        // Active phase shows the phase and a live [elapsed] so a quiet import or
        // extract does not look stuck.
        assert_eq!(
            service_bar_message(
                "trainer",
                "importing pytorch",
                "",
                0,
                Duration::from_secs(42)
            ),
            "trainer: importing pytorch [42.0s]"
        );
        // Streaming output line and bytes written are both surfaced, with elapsed last.
        assert_eq!(
            service_bar_message(
                "trainer",
                "importing pytorch",
                "Parsing layer 3/12",
                512 * 1024 * 1024,
                Duration::from_secs(90),
            ),
            "trainer: importing pytorch — Parsing layer 3/12 (512.0 MiB written) [1m30s]"
        );
    }
}
