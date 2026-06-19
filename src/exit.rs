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

use std::error::Error;
use std::fmt;

/// An error carrying a specific process exit code to propagate to the caller's
/// shell. Construct it at a site where a child process exited nonzero and that
/// status is meaningful to the user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitCodeError(pub i32);

impl ExitCodeError {
    /// Returns the carried exit code.
    #[must_use]
    pub fn code(self) -> i32 {
        self.0
    }
}

impl fmt::Display for ExitCodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "command exited with status {}", self.0)
    }
}

impl Error for ExitCodeError {}
