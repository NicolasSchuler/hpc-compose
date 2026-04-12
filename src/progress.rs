use std::io::{self, IsTerminal, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressMode {
    Hidden,
    Plain,
    Spinner,
}

/// Small stderr progress reporter for long-running CLI phases.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProgressReporter {
    mode: ProgressMode,
}

impl ProgressReporter {
    /// Builds a reporter that stays quiet unless progress output is desired.
    #[must_use]
    pub(crate) fn new(enabled: bool) -> Self {
        let mode = if !enabled {
            ProgressMode::Hidden
        } else if io::stderr().is_terminal() {
            ProgressMode::Spinner
        } else {
            ProgressMode::Plain
        };
        Self { mode }
    }

    /// Starts a progress step that the caller will finish explicitly.
    #[must_use]
    pub(crate) fn start(self, message: impl Into<String>) -> ProgressStep {
        ProgressStep::new(self.mode, message.into())
    }

    /// Runs a fallible operation while emitting phase progress.
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

/// One in-flight progress phase.
pub(crate) struct ProgressStep {
    mode: ProgressMode,
    message: String,
    started_at: Instant,
    spinner: Option<SpinnerHandle>,
    finished: bool,
}

impl ProgressStep {
    fn new(mode: ProgressMode, message: String) -> Self {
        match mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain => write_plain_line("run", &message, None),
            ProgressMode::Spinner => {}
        }

        let spinner = matches!(mode, ProgressMode::Spinner).then(|| SpinnerHandle::new(&message));

        Self {
            mode,
            message,
            started_at: Instant::now(),
            spinner,
            finished: false,
        }
    }

    /// Marks the step as completed.
    pub(crate) fn finish(mut self) {
        self.complete("done");
    }

    /// Marks the step as failed.
    pub(crate) fn fail(mut self) {
        self.complete("fail");
    }

    fn complete(&mut self, state: &'static str) {
        if self.finished {
            return;
        }

        if let Some(spinner) = self.spinner.take() {
            spinner.stop();
        }

        let elapsed = format_elapsed(self.started_at.elapsed());
        match self.mode {
            ProgressMode::Hidden => {}
            ProgressMode::Plain => write_plain_line(state, &self.message, Some(&elapsed)),
            ProgressMode::Spinner => write_spinner_line(state, &self.message, &elapsed),
        }

        self.finished = true;
    }
}

impl Drop for ProgressStep {
    fn drop(&mut self) {
        if let Some(spinner) = self.spinner.take() {
            spinner.stop();
        }
    }
}

struct SpinnerHandle {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl SpinnerHandle {
    fn new(message: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::clone(&stop);
        let message = message.to_string();
        let handle = thread::spawn(move || {
            const FRAMES: [&str; 4] = ["-", "\\", "|", "/"];
            let mut frame_index = 0usize;
            while !stop_signal.load(Ordering::Relaxed) {
                let mut stderr = io::stderr();
                let _ = write!(
                    stderr,
                    "\r\x1b[2K[{}] {}",
                    FRAMES[frame_index % FRAMES.len()],
                    message
                );
                let _ = stderr.flush();
                frame_index += 1;
                thread::sleep(Duration::from_millis(100));
            }
        });

        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn write_plain_line(state: &str, message: &str, elapsed: Option<&str>) {
    let mut stderr = io::stderr();
    let _ = match elapsed {
        Some(elapsed) => writeln!(stderr, "[{state}] {message} ({elapsed})"),
        None => writeln!(stderr, "[{state}] {message}"),
    };
    let _ = stderr.flush();
}

fn write_spinner_line(state: &str, message: &str, elapsed: &str) {
    let mut stderr = io::stderr();
    let _ = write!(stderr, "\r\x1b[2K");
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
