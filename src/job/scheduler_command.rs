use std::error::Error;
use std::fmt;
use std::process::{Command, Output};
use std::time::Duration;

use crate::process_probe::{self, ProbeError, ProbeOptions};

const DEFAULT_SCHEDULER_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const SCHEDULER_COMMAND_TIMEOUT_ENV: &str = "HPC_COMPOSE_SCHEDULER_COMMAND_TIMEOUT_MS";

#[derive(Debug)]
pub(crate) struct SchedulerCommandUnavailable {
    detail: String,
}

impl SchedulerCommandUnavailable {
    fn new(detail: String) -> Self {
        Self { detail }
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for SchedulerCommandUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for SchedulerCommandUnavailable {}

#[derive(Debug)]
pub(super) enum SchedulerCommandError {
    Unavailable(SchedulerCommandUnavailable),
    Io(std::io::Error),
}

impl SchedulerCommandError {
    pub(super) fn unavailable_detail(self) -> Option<String> {
        match self {
            Self::Unavailable(err) => Some(err.detail().to_string()),
            Self::Io(_) => None,
        }
    }

    pub(super) fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Unavailable(err) => err.into(),
            Self::Io(err) => err.into(),
        }
    }
}

fn scheduler_command_timeout() -> Duration {
    std::env::var(SCHEDULER_COMMAND_TIMEOUT_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_SCHEDULER_COMMAND_TIMEOUT)
}

pub(super) fn run_scheduler_command(
    command: &mut Command,
    command_name: &str,
    binary: &str,
) -> std::result::Result<Output, SchedulerCommandError> {
    run_scheduler_command_with_timeout(command, command_name, binary, scheduler_command_timeout())
}

fn run_scheduler_command_with_timeout(
    command: &mut Command,
    command_name: &str,
    _binary: &str,
    timeout: Duration,
) -> std::result::Result<Output, SchedulerCommandError> {
    let output = process_probe::run(
        command,
        command_name,
        ProbeOptions {
            timeout,
            ..ProbeOptions::default()
        },
    )
    .map_err(|err| match err {
        ProbeError::Unavailable { .. } | ProbeError::TimedOut { .. } => {
            SchedulerCommandError::Unavailable(SchedulerCommandUnavailable::new(err.detail()))
        }
        err @ ProbeError::OutputLimitExceeded { .. } => SchedulerCommandError::Io(
            std::io::Error::new(std::io::ErrorKind::InvalidData, err.detail()),
        ),
        err @ ProbeError::PostSpawnIo { .. } => {
            SchedulerCommandError::Io(std::io::Error::other(err.detail()))
        }
        ProbeError::Io(err) => SchedulerCommandError::Io(err),
    })?;
    Ok(Output {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

pub(crate) fn command_unavailable_error(err: &std::io::Error) -> bool {
    process_probe::command_unavailable_error(err)
}

pub(crate) fn command_unavailable_detail(
    command_name: &str,
    binary: &str,
    err: &std::io::Error,
) -> String {
    process_probe::command_unavailable_detail(command_name, binary, err)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::*;

    #[cfg(unix)]
    fn write_fake_probe(tmpdir: &Path, name: &str, stdout: &str) -> PathBuf {
        write_fake_script(
            tmpdir,
            name,
            &format!("#!/bin/sh\ncat <<'EOF'\n{stdout}\nEOF\n"),
        )
    }

    #[cfg(unix)]
    fn write_fake_script(tmpdir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let path = tmpdir.join(name);
        fs::write(&path, body).expect("fake probe");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
        path
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_command_reads_full_output() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let printer = write_fake_probe(tmpdir.path(), "printy-squeue", "JOBID STATE\n42 RUNNING");
        let binary = printer.to_string_lossy().to_string();
        let mut command = Command::new(&printer);

        let output = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_secs(5),
        )
        .expect("fake command should succeed");

        assert!(output.status.success());
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("JOBID STATE"));
        assert!(stdout.contains("42 RUNNING"));
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_command_timeout_reports_unavailable() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let sleeper = write_fake_script(tmpdir.path(), "sleepy-squeue", "#!/bin/sh\nsleep 5\n");
        let binary = sleeper.to_string_lossy().to_string();
        let mut command = Command::new(&sleeper);

        let err = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_millis(50),
        )
        .expect_err("sleeping command should time out");

        match err {
            SchedulerCommandError::Unavailable(err) => {
                assert!(err.detail().contains("squeue timed out"));
                assert!(err.detail().contains(&binary));
            }
            SchedulerCommandError::Io(err) => panic!("expected timeout detail, got {err}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_command_missing_binary_preserves_unavailable_variant_and_detail() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("missing-squeue");
        let binary = missing.to_string_lossy().to_string();
        let mut command = Command::new(&missing);

        let err = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_secs(1),
        )
        .expect_err("missing command should be unavailable");

        match err {
            SchedulerCommandError::Unavailable(err) => {
                assert!(
                    err.detail()
                        .starts_with(&format!("squeue not available at '{binary}' ("))
                );
                assert!(err.detail().ends_with(')'));
                assert_eq!(err.to_string(), err.detail());
                assert!(err.source().is_none());
            }
            SchedulerCommandError::Io(err) => panic!("expected unavailable detail, got {err}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_command_permission_denied_preserves_unavailable_variant_and_detail() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let binary_path = tmpdir.path().join("non-executable-squeue");
        fs::write(&binary_path, "#!/bin/sh\nexit 0\n").expect("write fixture");
        fs::set_permissions(&binary_path, fs::Permissions::from_mode(0o644))
            .expect("fixture permissions");
        let binary = binary_path.to_string_lossy().to_string();
        let mut command = Command::new(&binary_path);

        let err = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_secs(1),
        )
        .expect_err("non-executable command should be unavailable");

        match err {
            SchedulerCommandError::Unavailable(err) => {
                assert!(
                    err.detail()
                        .starts_with(&format!("squeue not executable at '{binary}' ("))
                );
                assert!(err.detail().ends_with(')'));
            }
            SchedulerCommandError::Io(err) => panic!("expected unavailable detail, got {err}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_command_output_limit_preserves_io_variant_kind_and_detail() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let printer = write_fake_script(
            tmpdir.path(),
            "verbose-squeue",
            "#!/bin/sh\nhead -c 1048577 /dev/zero\n",
        );
        let binary = printer.to_string_lossy().to_string();
        let mut command = Command::new(&printer);

        let err = run_scheduler_command_with_timeout(
            &mut command,
            "squeue",
            &binary,
            Duration::from_secs(5),
        )
        .expect_err("oversized output should fail");

        match err {
            SchedulerCommandError::Io(err) => {
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
                assert_eq!(
                    err.to_string(),
                    format!("squeue exceeded the 1048576-byte stdout capture limit at '{binary}'")
                );
            }
            SchedulerCommandError::Unavailable(err) => {
                panic!("expected output-limit IO error, got {err}")
            }
        }
    }

    #[test]
    fn scheduler_command_anyhow_conversion_preserves_downcast_types() {
        let unavailable = SchedulerCommandError::Unavailable(SchedulerCommandUnavailable::new(
            "squeue unavailable".to_string(),
        ))
        .into_anyhow();
        assert_eq!(
            unavailable
                .downcast_ref::<SchedulerCommandUnavailable>()
                .map(SchedulerCommandUnavailable::detail),
            Some("squeue unavailable")
        );

        let io = SchedulerCommandError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken scheduler pipe",
        ))
        .into_anyhow();
        let io = io
            .downcast_ref::<std::io::Error>()
            .expect("IO conversion should preserve the concrete error");
        assert_eq!(io.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(io.to_string(), "broken scheduler pipe");
    }
}
