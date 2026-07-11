//! Bounded execution and executable discovery for short diagnostic probes.

use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Limits applied to one short-lived diagnostic command.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProbeOptions {
    pub(crate) timeout: Duration,
    /// Maximum bytes retained from each output stream. Excess bytes are still
    /// drained so a verbose child cannot block on a full pipe.
    pub(crate) max_output_bytes: usize,
}

impl Default for ProbeOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
        }
    }
}

/// Captured result of a diagnostic command.
#[derive(Debug)]
pub(crate) struct ProbeOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

/// Failure to launch or finish a diagnostic command.
#[derive(Debug)]
pub(crate) enum ProbeError {
    Unavailable {
        command_name: String,
        binary: String,
        source: io::Error,
    },
    TimedOut {
        command_name: String,
        binary: String,
        timeout: Duration,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        stdout_truncated: bool,
        stderr_truncated: bool,
    },
    OutputLimitExceeded {
        command_name: String,
        binary: String,
        max_output_bytes: usize,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        stdout_truncated: bool,
        stderr_truncated: bool,
    },
    PostSpawnIo {
        command_name: String,
        binary: String,
        operation: &'static str,
        source: io::Error,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        stdout_truncated: bool,
        stderr_truncated: bool,
    },
    Io(io::Error),
}

impl ProbeError {
    pub(crate) fn detail(&self) -> String {
        match self {
            Self::Unavailable {
                command_name,
                binary,
                source,
            } => command_unavailable_detail(command_name, binary, source),
            Self::TimedOut {
                command_name,
                binary,
                timeout,
                ..
            } => format!(
                "{command_name} timed out after {:.1}s at '{binary}'",
                timeout.as_secs_f64()
            ),
            Self::OutputLimitExceeded {
                command_name,
                binary,
                max_output_bytes,
                stdout_truncated,
                stderr_truncated,
                ..
            } => {
                let streams = match (*stdout_truncated, *stderr_truncated) {
                    (true, true) => "stdout and stderr",
                    (true, false) => "stdout",
                    (false, true) => "stderr",
                    (false, false) => "output",
                };
                format!(
                    "{command_name} exceeded the {max_output_bytes}-byte {streams} capture limit at '{binary}'"
                )
            }
            Self::PostSpawnIo {
                command_name,
                binary,
                operation,
                source,
                ..
            } => format!("{command_name} failed while {operation} at '{binary}': {source}"),
            Self::Io(err) => err.to_string(),
        }
    }

    pub(crate) fn captured_stdout(&self) -> &[u8] {
        match self {
            Self::TimedOut { stdout, .. }
            | Self::OutputLimitExceeded { stdout, .. }
            | Self::PostSpawnIo { stdout, .. } => stdout,
            _ => &[],
        }
    }

    pub(crate) fn captured_stderr(&self) -> &[u8] {
        match self {
            Self::TimedOut { stderr, .. }
            | Self::OutputLimitExceeded { stderr, .. }
            | Self::PostSpawnIo { stderr, .. } => stderr,
            _ => &[],
        }
    }

    pub(crate) fn captured_output_truncated(&self) -> (bool, bool) {
        match self {
            Self::TimedOut {
                stdout_truncated,
                stderr_truncated,
                ..
            }
            | Self::OutputLimitExceeded {
                stdout_truncated,
                stderr_truncated,
                ..
            }
            | Self::PostSpawnIo {
                stdout_truncated,
                stderr_truncated,
                ..
            } => (*stdout_truncated, *stderr_truncated),
            _ => (false, false),
        }
    }
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail())
    }
}

impl std::error::Error for ProbeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable { source, .. }
            | Self::PostSpawnIo { source, .. }
            | Self::Io(source) => Some(source),
            Self::TimedOut { .. } | Self::OutputLimitExceeded { .. } => None,
        }
    }
}

/// Runs a short diagnostic command with bounded retained output.
pub(crate) fn run(
    command: &mut Command,
    command_name: &str,
    options: ProbeOptions,
) -> Result<ProbeOutput, ProbeError> {
    run_with_poller(
        command,
        command_name,
        options,
        std::process::Child::try_wait,
    )
}

fn run_with_poller(
    command: &mut Command,
    command_name: &str,
    options: ProbeOptions,
    mut poll_child: impl FnMut(&mut std::process::Child) -> io::Result<Option<ExitStatus>>,
) -> Result<ProbeOutput, ProbeError> {
    let binary = command.get_program().to_string_lossy().into_owned();
    configure_process_group(command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|source| {
        if command_unavailable_error(&source) {
            ProbeError::Unavailable {
                command_name: command_name.to_string(),
                binary: binary.clone(),
                source,
            }
        } else {
            ProbeError::Io(source)
        }
    })?;

    let stdout_handle = child
        .stdout
        .take()
        .map(|pipe| read_pipe_thread(pipe, options.max_output_bytes));
    let stderr_handle = child
        .stderr
        .take()
        .map(|pipe| read_pipe_thread(pipe, options.max_output_bytes));
    let started = Instant::now();
    let status = loop {
        match poll_child(&mut child) {
            Ok(Some(status)) => break status,
            Err(source) => {
                terminate_process_group(&mut child);
                let _ = child.wait();
                let (stdout, stderr) = finish_timed_out_pipe_readers(stdout_handle, stderr_handle);
                return Err(ProbeError::PostSpawnIo {
                    command_name: command_name.to_string(),
                    binary,
                    operation: "polling child status",
                    source,
                    stdout: stdout.bytes,
                    stderr: stderr.bytes,
                    stdout_truncated: stdout.truncated,
                    stderr_truncated: stderr.truncated,
                });
            }
            Ok(None) if started.elapsed() >= options.timeout => {
                terminate_process_group(&mut child);
                let _ = child.wait();
                let (stdout, stderr) = finish_timed_out_pipe_readers(stdout_handle, stderr_handle);
                return Err(ProbeError::TimedOut {
                    command_name: command_name.to_string(),
                    binary,
                    timeout: options.timeout,
                    stdout: stdout.bytes,
                    stderr: stderr.bytes,
                    stdout_truncated: stdout.truncated,
                    stderr_truncated: stderr.truncated,
                });
            }
            Ok(None) => std::thread::sleep(POLL_INTERVAL),
        }
    };

    while !pipes_finished(&stdout_handle, &stderr_handle) {
        if started.elapsed() >= options.timeout {
            terminate_process_group(&mut child);
            let _ = child.wait();
            let (stdout, stderr) = finish_timed_out_pipe_readers(stdout_handle, stderr_handle);
            return Err(ProbeError::TimedOut {
                command_name: command_name.to_string(),
                binary,
                timeout: options.timeout,
                stdout: stdout.bytes,
                stderr: stderr.bytes,
                stdout_truncated: stdout.truncated,
                stderr_truncated: stderr.truncated,
            });
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    let mut stdout_capture = join_pipe(stdout_handle, "stdout");
    let mut stderr_capture = join_pipe(stderr_handle, "stderr");
    if let Some(source) = stdout_capture
        .error
        .take()
        .or_else(|| stderr_capture.error.take())
    {
        terminate_process_group(&mut child);
        let _ = child.wait();
        return Err(ProbeError::PostSpawnIo {
            command_name: command_name.to_string(),
            binary,
            operation: "capturing child output",
            source,
            stdout: stdout_capture.output.bytes,
            stderr: stderr_capture.output.bytes,
            stdout_truncated: stdout_capture.output.truncated,
            stderr_truncated: stderr_capture.output.truncated,
        });
    }
    let stdout = stdout_capture.output;
    let stderr = stderr_capture.output;
    if stdout.truncated || stderr.truncated {
        return Err(ProbeError::OutputLimitExceeded {
            command_name: command_name.to_string(),
            binary,
            max_output_bytes: options.max_output_bytes,
            stdout_truncated: stdout.truncated,
            stderr_truncated: stderr.truncated,
            stdout: stdout.bytes,
            stderr: stderr.bytes,
        });
    }
    Ok(ProbeOutput {
        status,
        stdout: stdout.bytes,
        stderr: stderr.bytes,
    })
}

fn pipes_finished<T, U>(
    stdout: &Option<std::thread::JoinHandle<T>>,
    stderr: &Option<std::thread::JoinHandle<U>>,
) -> bool {
    stdout
        .as_ref()
        .is_none_or(std::thread::JoinHandle::is_finished)
        && stderr
            .as_ref()
            .is_none_or(std::thread::JoinHandle::is_finished)
}

/// Completes timeout cleanup without letting inherited pipe handles defeat the
/// caller's deadline. Unix kills the probe's whole process group, so readers
/// should promptly observe EOF and are joined. Other platforms can only kill
/// the direct child with the standard library; a descendant may still own the
/// pipe handles, so dropping the join handles intentionally detaches those
/// readers until the descendant exits rather than blocking the probe caller.
#[cfg(unix)]
fn finish_timed_out_pipe_readers(
    stdout: Option<std::thread::JoinHandle<PipeCapture>>,
    stderr: Option<std::thread::JoinHandle<PipeCapture>>,
) -> (BoundedBytes, BoundedBytes) {
    (
        join_pipe(stdout, "stdout").output,
        join_pipe(stderr, "stderr").output,
    )
}

#[cfg(not(unix))]
fn finish_timed_out_pipe_readers(
    stdout: Option<std::thread::JoinHandle<PipeCapture>>,
    stderr: Option<std::thread::JoinHandle<PipeCapture>>,
) -> (BoundedBytes, BoundedBytes) {
    drop(stdout);
    drop(stderr);
    (BoundedBytes::empty(), BoundedBytes::empty())
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_group(child: &mut std::process::Child) {
    let process_group = -(child.id() as i32);
    // SAFETY: the child was placed in a new process group whose id is its pid;
    // sending SIGKILL to the negative id targets only that probe group.
    let killed = unsafe { libc::kill(process_group, libc::SIGKILL) } == 0;
    if !killed {
        let _ = child.kill();
    }
}

#[cfg(not(unix))]
fn terminate_process_group(child: &mut std::process::Child) {
    let _ = child.kill();
}

/// Builds and runs a short diagnostic command with the default limits.
pub(crate) fn capture(
    binary: impl AsRef<OsStr>,
    args: &[&str],
    command_name: &str,
) -> Result<ProbeOutput, ProbeError> {
    let mut command = Command::new(binary);
    command.args(args);
    run(&mut command, command_name, ProbeOptions::default())
}

struct BoundedBytes {
    bytes: Vec<u8>,
    truncated: bool,
}

struct PipeCapture {
    output: BoundedBytes,
    error: Option<io::Error>,
}

impl PipeCapture {
    fn empty() -> Self {
        Self {
            output: BoundedBytes::empty(),
            error: None,
        }
    }
}

impl BoundedBytes {
    fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
        }
    }
}

fn read_pipe_thread(
    mut pipe: impl Read + Send + 'static,
    max_output_bytes: usize,
) -> std::thread::JoinHandle<PipeCapture> {
    std::thread::spawn(move || {
        let mut bytes = Vec::with_capacity(max_output_bytes.min(16 * 1024));
        let mut truncated = false;
        let mut error = None;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = match pipe.read(&mut buffer) {
                Ok(read) => read,
                Err(source) => {
                    error = Some(source);
                    break;
                }
            };
            if read == 0 {
                break;
            }
            let remaining = max_output_bytes.saturating_sub(bytes.len());
            let retained = remaining.min(read);
            bytes.extend_from_slice(&buffer[..retained]);
            truncated |= retained < read;
        }
        PipeCapture {
            output: BoundedBytes { bytes, truncated },
            error,
        }
    })
}

fn join_pipe(
    handle: Option<std::thread::JoinHandle<PipeCapture>>,
    stream: &'static str,
) -> PipeCapture {
    match handle {
        Some(handle) => match handle.join() {
            Ok(mut capture) => {
                if let Some(source) = capture.error.take() {
                    capture.error = Some(io::Error::new(
                        source.kind(),
                        format!("failed to read probe {stream}: {source}"),
                    ));
                }
                capture
            }
            Err(_) => PipeCapture {
                output: BoundedBytes::empty(),
                error: Some(io::Error::other(format!(
                    "probe {stream} reader thread panicked"
                ))),
            },
        },
        None => PipeCapture::empty(),
    }
}

pub(crate) fn command_unavailable_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
    )
}

pub(crate) fn command_unavailable_detail(
    command_name: &str,
    binary: &str,
    err: &io::Error,
) -> String {
    match err.kind() {
        io::ErrorKind::NotFound => format!("{command_name} not available at '{binary}' ({err})"),
        io::ErrorKind::PermissionDenied => {
            format!("{command_name} not executable at '{binary}' ({err})")
        }
        _ => format!("{command_name} unavailable at '{binary}' ({err})"),
    }
}

/// Why a requested executable could not be resolved.
#[derive(Debug)]
pub(crate) enum ExecutableError {
    NotFound(String),
    NotAFile(PathBuf),
    NotExecutable(PathBuf),
    Metadata { path: PathBuf, source: io::Error },
}

impl fmt::Display for ExecutableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(binary) => write!(formatter, "executable '{binary}' was not found"),
            Self::NotAFile(path) => write!(formatter, "'{}' is not a file", path.display()),
            Self::NotExecutable(path) => {
                write!(formatter, "'{}' is not executable", path.display())
            }
            Self::Metadata { path, source } => {
                write!(
                    formatter,
                    "failed to inspect '{}': {source}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for ExecutableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Metadata { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Resolves an explicit path or a command name on `PATH` to an executable file.
pub(crate) fn resolve_executable(binary: &str) -> Result<PathBuf, ExecutableError> {
    if explicit_path(binary) {
        return validate_executable(PathBuf::from(binary));
    }

    let Some(path_var) = std::env::var_os("PATH") else {
        return Err(ExecutableError::NotFound(binary.to_string()));
    };
    let mut unsuitable = None;
    for dir in std::env::split_paths(&path_var) {
        match validate_executable(dir.join(binary)) {
            Ok(path) => return Ok(path),
            Err(ExecutableError::NotFound(_)) => {}
            Err(err) => {
                unsuitable.get_or_insert(err);
            }
        }
    }
    Err(unsuitable.unwrap_or_else(|| ExecutableError::NotFound(binary.to_string())))
}

fn explicit_path(binary: &str) -> bool {
    let path = Path::new(binary);
    path.is_absolute() || binary.contains('/') || binary.contains('\\')
}

fn validate_executable(path: PathBuf) -> Result<PathBuf, ExecutableError> {
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(ExecutableError::NotFound(path.display().to_string()));
        }
        Err(source) => return Err(ExecutableError::Metadata { path, source }),
    };
    if !metadata.is_file() {
        return Err(ExecutableError::NotAFile(path));
    }
    if !metadata_is_executable(&metadata) {
        return Err(ExecutableError::NotExecutable(path));
    }
    Ok(path)
}

#[cfg(unix)]
fn metadata_is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn metadata_is_executable(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn write_script(dir: &Path, name: &str, body: &str, executable: bool) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = dir.join(name);
        fs::write(&path, body).expect("script");
        let mode = if executable { 0o755 } else { 0o644 };
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("permissions");
        path
    }

    #[cfg(unix)]
    fn run_after_child_ready(
        command: &mut Command,
        command_name: &str,
        options: ProbeOptions,
        ready_path: &Path,
    ) -> (Result<ProbeOutput, ProbeError>, Option<Instant>) {
        let readiness_deadline = Instant::now() + Duration::from_secs(10);
        let mut ready_at = None;
        let result = run_with_poller(command, command_name, options, |child| {
            if ready_at.is_none() {
                while !ready_path.exists() {
                    if let Some(status) = child.try_wait()? {
                        return Ok(Some(status));
                    }
                    if Instant::now() >= readiness_deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "probe fixture did not publish output readiness",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                ready_at = Some(Instant::now());
            }
            child.try_wait()
        });
        (result, ready_at)
    }

    #[cfg(unix)]
    #[test]
    fn probe_captures_success_and_failure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let success = write_script(
            temp.path(),
            "success",
            "#!/bin/sh\nprintf 'hello'\nprintf 'note' >&2\n",
            true,
        );
        let mut command = Command::new(success);
        let output = run(&mut command, "success", ProbeOptions::default()).expect("success");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"hello");
        assert_eq!(output.stderr, b"note");

        let failure = write_script(temp.path(), "failure", "#!/bin/sh\nexit 7\n", true);
        let mut command = Command::new(failure);
        let output = run(&mut command, "failure", ProbeOptions::default()).expect("capture");
        assert_eq!(output.status.code(), Some(7));
    }

    #[cfg(unix)]
    #[test]
    fn probe_timeout_returns_promptly() {
        let temp = tempfile::tempdir().expect("tempdir");
        let published = temp.path().join("published");
        let script = write_script(
            temp.path(),
            "slow",
            &format!(
                "#!/bin/sh\nprintf '123;cluster\\n'\nprintf 'accepted\\n' >&2\n: > {}\nsleep 10\n",
                crate::shell_quote::quote(&published.display().to_string())
            ),
            true,
        );
        let mut command = Command::new(script);
        let (result, ready_at) = run_after_child_ready(
            &mut command,
            "slow",
            ProbeOptions {
                timeout: Duration::from_millis(50),
                ..ProbeOptions::default()
            },
            &published,
        );
        let error = result.expect_err("timeout");
        assert!(matches!(error, ProbeError::TimedOut { .. }));
        assert_eq!(error.captured_stdout(), b"123;cluster\n");
        assert_eq!(error.captured_stderr(), b"accepted\n");
        assert_eq!(error.captured_output_truncated(), (false, false));
        assert!(
            ready_at.expect("child readiness").elapsed() < Duration::from_secs(1),
            "probe did not return promptly after its child published readiness"
        );
    }

    #[cfg(unix)]
    #[test]
    fn probe_timeout_marks_bounded_partial_output_as_truncated() {
        let temp = tempfile::tempdir().expect("tempdir");
        let published = temp.path().join("published");
        let script = write_script(
            temp.path(),
            "verbose-timeout",
            &format!(
                "#!/bin/sh\nprintf '123456'\nprintf 'abcdef' >&2\n: > {}\nsleep 10\n",
                crate::shell_quote::quote(&published.display().to_string())
            ),
            true,
        );
        let mut command = Command::new(script);
        let (result, _ready_at) = run_after_child_ready(
            &mut command,
            "verbose timeout",
            ProbeOptions {
                timeout: Duration::from_millis(50),
                max_output_bytes: 3,
            },
            &published,
        );
        let error = result.expect_err("timeout");

        assert!(
            matches!(error, ProbeError::TimedOut { .. }),
            "unexpected error: {error}"
        );
        assert_eq!(error.captured_stdout(), b"123");
        assert_eq!(error.captured_stderr(), b"abc");
        assert_eq!(error.captured_output_truncated(), (true, true));
    }

    #[cfg(unix)]
    #[test]
    fn output_limit_error_retains_bounded_output_for_cleanup_protocols() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = write_script(
            temp.path(),
            "verbose-success",
            "#!/bin/sh\nprintf '999;cluster\\nextra'\nprintf 'stderr-data' >&2\n",
            true,
        );
        let mut command = Command::new(script);
        let error = run(
            &mut command,
            "verbose success",
            ProbeOptions {
                timeout: Duration::from_secs(10),
                max_output_bytes: 12,
            },
        )
        .expect_err("bounded output overflow");

        assert!(matches!(error, ProbeError::OutputLimitExceeded { .. }));
        assert_eq!(error.captured_stdout(), b"999;cluster\n");
        assert_eq!(error.captured_stderr(), b"stderr-data");
        assert_eq!(error.captured_output_truncated(), (true, false));
    }

    #[cfg(unix)]
    #[test]
    fn injected_post_spawn_poll_error_cleans_process_group_and_keeps_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let published = temp.path().join("published");
        let descendant_survived = temp.path().join("descendant-survived");
        let script = write_script(
            temp.path(),
            "poll-error",
            &format!(
                "#!/bin/sh\nprintf '777;cluster\\n'\n(sleep 1; : > {}) &\n: > {}\nsleep 10\n",
                crate::shell_quote::quote(&descendant_survived.display().to_string()),
                crate::shell_quote::quote(&published.display().to_string())
            ),
            true,
        );
        let mut command = Command::new(script);
        let mut ready_at = None;
        let error = run_with_poller(
            &mut command,
            "injected poll error",
            ProbeOptions {
                timeout: Duration::from_secs(10),
                max_output_bytes: 64,
            },
            |_| {
                if published.exists() {
                    ready_at = Some(Instant::now());
                    Err(io::Error::other("injected child-status poll failure"))
                } else {
                    Ok(None)
                }
            },
        )
        .expect_err("injected post-spawn failure");

        assert!(matches!(error, ProbeError::PostSpawnIo { .. }));
        assert_eq!(error.captured_stdout(), b"777;cluster\n");
        assert!(
            ready_at.expect("child readiness").elapsed() < Duration::from_secs(1),
            "post-spawn cleanup did not return promptly after child readiness"
        );
        std::thread::sleep(Duration::from_millis(1_200));
        assert!(
            !descendant_survived.exists(),
            "post-spawn I/O failure left a probe descendant alive"
        );
    }

    #[test]
    fn pipe_read_error_retains_bytes_read_before_failure() {
        struct PartialThenError {
            emitted: bool,
        }

        impl Read for PartialThenError {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                if self.emitted {
                    return Err(io::Error::other("injected pipe read failure"));
                }
                self.emitted = true;
                let bytes = b"888;cluster\n";
                buffer[..bytes.len()].copy_from_slice(bytes);
                Ok(bytes.len())
            }
        }

        let capture = join_pipe(
            Some(read_pipe_thread(PartialThenError { emitted: false }, 64)),
            "stdout",
        );
        assert_eq!(capture.output.bytes, b"888;cluster\n");
        assert!(capture.error.is_some());
    }

    #[test]
    fn pipe_reader_join_failure_becomes_captured_io_error() {
        let reader = std::thread::spawn(|| -> PipeCapture {
            panic!("injected pipe reader panic");
        });
        let capture = join_pipe(Some(reader), "stderr");
        assert!(capture.output.bytes.is_empty());
        assert!(
            capture
                .error
                .expect("join failure")
                .to_string()
                .contains("reader thread panicked")
        );
    }

    #[cfg(unix)]
    #[test]
    fn probe_timeout_kills_background_descendant_holding_pipes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let published = temp.path().join("published");
        let script = write_script(
            temp.path(),
            "background",
            &format!(
                "#!/bin/sh\nsleep 2 &\n: > {}\nexit 0\n",
                crate::shell_quote::quote(&published.display().to_string())
            ),
            true,
        );
        let mut command = Command::new(script);
        let (result, ready_at) = run_after_child_ready(
            &mut command,
            "background",
            ProbeOptions {
                timeout: Duration::from_millis(50),
                ..ProbeOptions::default()
            },
            &published,
        );
        let error = result.expect_err("background process must not extend the probe indefinitely");
        assert!(matches!(error, ProbeError::TimedOut { .. }));
        assert!(ready_at.expect("child readiness").elapsed() < Duration::from_secs(1));
    }

    #[cfg(not(unix))]
    #[test]
    fn timeout_cleanup_detaches_pipe_readers_that_cannot_reach_eof() {
        fn blocked_reader() -> std::thread::JoinHandle<PipeCapture> {
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_secs(2));
                PipeCapture::empty()
            })
        }

        let started = Instant::now();
        let captured =
            finish_timed_out_pipe_readers(Some(blocked_reader()), Some(blocked_reader()));
        assert!(captured.0.bytes.is_empty());
        assert!(captured.1.bytes.is_empty());
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[cfg(unix)]
    #[test]
    fn probe_drains_but_only_retains_bounded_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = write_script(
            temp.path(),
            "verbose",
            "#!/bin/sh\ni=0; while [ $i -lt 10000 ]; do printf x; printf y >&2; i=$((i+1)); done\n",
            true,
        );
        let mut command = Command::new(script);
        let error = run(
            &mut command,
            "verbose",
            ProbeOptions {
                max_output_bytes: 128,
                ..ProbeOptions::default()
            },
        )
        .expect_err("oversized output");
        assert!(matches!(
            error,
            ProbeError::OutputLimitExceeded {
                stdout_truncated: true,
                stderr_truncated: true,
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn executable_lookup_rejects_directory_and_non_executable_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        assert!(matches!(
            resolve_executable(temp.path().to_str().expect("path")),
            Err(ExecutableError::NotAFile(_))
        ));
        let file = write_script(temp.path(), "plain", "#!/bin/sh\n", false);
        assert!(matches!(
            resolve_executable(file.to_str().expect("path")),
            Err(ExecutableError::NotExecutable(_))
        ));
    }
}
