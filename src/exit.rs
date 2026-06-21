//! Process exit-code propagation for direct-execution commands.
//!
//! Commands that exec a child process on the user's behalf (`run -- ...`,
//! `alloc -- ...`, `shell`, `notebook`) should surface the child's real exit
//! status rather than collapsing every failure to `1`, so that scripts and CI
//! can distinguish, say, a test runner's exit code `2` from `5`.
//!
//! [`ExitCodeError`] carries that status through the normal `anyhow::Error`
//! channel. The binary entrypoint downcasts it and exits with the preserved
//! code instead of rendering a diagnostic.

use thiserror::Error;

/// An error carrying a specific process exit code to propagate to the caller's
/// shell. Construct it at a site where a child process exited nonzero and that
/// status is meaningful to the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("command exited with status {0}")]
pub struct ExitCodeError(pub i32);

impl ExitCodeError {
    /// Returns the carried exit code.
    #[must_use]
    pub fn code(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_round_trips_the_carried_status() {
        assert_eq!(ExitCodeError(0).code(), 0);
        assert_eq!(ExitCodeError(1).code(), 1);
        assert_eq!(ExitCodeError(2).code(), 2);
        assert_eq!(ExitCodeError(137).code(), 137);
        assert_eq!(ExitCodeError(-1).code(), -1);
    }

    #[test]
    fn display_reports_the_status() {
        assert_eq!(ExitCodeError(5).to_string(), "command exited with status 5");
    }

    #[test]
    fn downcasts_through_anyhow_preserving_the_code() {
        // main.rs downcasts the error carrier off the anyhow channel; this
        // pins that the code survives the boxing round-trip.
        let err: anyhow::Error = ExitCodeError(2).into();
        let recovered = err
            .downcast_ref::<ExitCodeError>()
            .expect("ExitCodeError survives anyhow round-trip");
        assert_eq!(recovered.code(), 2);
    }

    #[test]
    fn is_copy_eq_and_distinct_by_code() {
        let err = ExitCodeError(42);
        let copied = err; // Copy
        assert_eq!(err, copied);
        assert_ne!(ExitCodeError(1), ExitCodeError(2));
    }
}
