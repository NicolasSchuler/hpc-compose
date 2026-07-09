//! The first-party exit-code catalog and the typed error layer that drives it.
//!
//! `hpc-compose` maps its own failures onto a small, stable set of process exit
//! codes so that scripts and CI can distinguish "spec invalid" from "cluster
//! unreachable" from "lint findings present" without scraping stderr. The set is
//! deliberately minimal because **every code is a contract forever**:
//!
//! | Code | Meaning                                         |
//! |------|-------------------------------------------------|
//! | 0    | success                                         |
//! | 1    | generic / unexpected failure                    |
//! | 2    | usage or spec validation error                  |
//! | 3    | preflight / environment not ready (unreachable) |
//! | 4    | lint findings present                           |
//! | *n*  | a spawned child process's status, propagated    |
//!
//! Codes 1-4 are what `hpc-compose` emits for *its own* failures. Direct-execution
//! commands (`run`, `alloc`, `shell`, `notebook`, `reach`, `exec`) instead exec a
//! child on the user's behalf; that child's exit status is surfaced verbatim via
//! [`ExitCodeError`] and may coincide with a reserved code, exactly as `env(1)`,
//! `timeout(1)`, and shells behave.
//!
//! ## How a failure gets its code
//!
//! Command code never calls `process::exit`; `main.rs` is the only exit site. A
//! failing command returns an [`anyhow::Error`], and `main.rs` calls
//! [`exit_code_for`] to derive the code by inspecting the error chain:
//!
//! - an [`ExitCodeError`] carries a child's status (pass-through);
//! - a [`crate::spec_error::SpecError`] or generic spec-validation carrier
//!   means the spec is invalid (code 2);
//! - a [`UsageError`] means a command-level flag or argument combination is invalid → code 2;
//! - a [`LintFindingsError`] means lint findings failed the gate → code 4;
//! - an [`EnvironmentError`] means a preflight/reachability check failed → code 3;
//! - anything else is an uncategorized failure → code 1.
//!
//! Clap already exits with code 2 for parse-level usage errors (unknown flags,
//! missing arguments) before a command ever runs, which is why code 2 also means
//! "usage": the two spellings of a usage error share one code.

use thiserror::Error;

/// The first-party exit-code catalog: the codes `hpc-compose` returns for its
/// own failures. Child pass-through statuses are carried separately by
/// [`ExitCodeError`]; see the module docs for the full contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// The command completed successfully (`0`).
    Success,
    /// A generic or unexpected failure with no more specific category (`1`).
    General,
    /// A usage error (bad flags/argument combination) or an invalid spec (`2`).
    Usage,
    /// A preflight or environment readiness failure, including an unreachable
    /// cluster (`3`).
    Environment,
    /// Lint findings are present and failed the configured gate (`4`).
    Lint,
}

impl ExitCode {
    /// Returns the numeric process exit code for this category.
    #[must_use]
    pub fn code(self) -> i32 {
        match self {
            ExitCode::Success => 0,
            ExitCode::General => 1,
            ExitCode::Usage => 2,
            ExitCode::Environment => 3,
            ExitCode::Lint => 4,
        }
    }
}

/// An error carrying a specific process exit code to propagate to the caller's
/// shell. Construct it at a site where a child process exited nonzero and that
/// status is meaningful to the user (`run`/`alloc`/`shell`/`notebook`/`reach`/
/// `exec`). It takes precedence over the [`ExitCode`] catalog: a real child
/// status is surfaced verbatim rather than remapped onto a reserved code.
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

/// A preflight or environment-readiness failure: the cluster is unreachable, a
/// probe failed, or a required part of the environment is not ready. Classified
/// as [`ExitCode::Environment`] (code 3). Construct it in place of a generic
/// `bail!` at the site that determines the environment is not usable.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct EnvironmentError(pub String);

impl EnvironmentError {
    /// Builds an environment-readiness error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// A semantic command-line usage error discovered after Clap parsing. Classified
/// as [`ExitCode::Usage`] (code 2), matching parser-level unknown flag / missing
/// argument failures. Use this for value-dependent or context-dependent
/// argument combinations that cannot be expressed cleanly in Clap metadata.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct UsageError(pub String);

impl UsageError {
    /// Builds a command usage error from a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Signals that `lint` found findings that fail the configured gate (errors, or
/// warnings without `--allow-warnings`). Classified as [`ExitCode::Lint`]
/// (code 4). The message matches the historical `lint` failure text so CI logs
/// are unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "lint found {warning_count} warning(s) and {error_count} error(s); pass --allow-warnings to allow warnings"
)]
pub struct LintFindingsError {
    /// The number of warning-level findings.
    pub warning_count: usize,
    /// The number of error-level findings.
    pub error_count: usize,
}

impl LintFindingsError {
    /// Builds a lint-findings error from the failing counts.
    #[must_use]
    pub fn new(warning_count: usize, error_count: usize) -> Self {
        Self {
            warning_count,
            error_count,
        }
    }
}

/// Categorizes a first-party failure into the [`ExitCode`] catalog by walking
/// the error chain. This ignores [`ExitCodeError`] child pass-through, which
/// [`exit_code_for`] handles first; call [`exit_code_for`] for the final code.
#[must_use]
pub fn classify(error: &anyhow::Error) -> ExitCode {
    // An invalid spec is a usage/validation error. Specialized failures use
    // SpecError; generic parsing and validation failures are marked at the
    // ComposeSpec load boundary. Checked before Environment so a spec error
    // surfaced from `preflight`/`doctor` is reported as 2, not 3.
    if error
        .downcast_ref::<crate::spec_error::SpecError>()
        .is_some()
        || error
            .downcast_ref::<crate::spec_error::SpecValidationError>()
            .is_some()
    {
        return ExitCode::Usage;
    }
    if error.downcast_ref::<UsageError>().is_some() {
        return ExitCode::Usage;
    }
    if error.downcast_ref::<LintFindingsError>().is_some() {
        return ExitCode::Lint;
    }
    if error.downcast_ref::<EnvironmentError>().is_some() {
        return ExitCode::Environment;
    }
    ExitCode::General
}

/// Derives the process exit code for a failed command. A child process's status
/// ([`ExitCodeError`]) takes precedence and is surfaced verbatim (remapping a
/// child's `0` to `1` so a "failure" never exits `0`); otherwise the error is
/// [`classify`]ed into the catalog.
#[must_use]
pub fn exit_code_for(error: &anyhow::Error) -> i32 {
    if let Some(child) = error.downcast_ref::<ExitCodeError>() {
        let code = child.code();
        // A child that "failed" but reported 0 must not exit 0, or callers would
        // read the run as successful.
        return if code == 0 { 1 } else { code };
    }
    classify(error).code()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec_error::{SpecError, SpecValidationError};

    #[test]
    fn catalog_codes_are_stable() {
        assert_eq!(ExitCode::Success.code(), 0);
        assert_eq!(ExitCode::General.code(), 1);
        assert_eq!(ExitCode::Usage.code(), 2);
        assert_eq!(ExitCode::Environment.code(), 3);
        assert_eq!(ExitCode::Lint.code(), 4);
    }

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

    #[test]
    fn environment_error_carries_its_message() {
        let err = EnvironmentError::new("login node unreachable");
        assert_eq!(err.to_string(), "login node unreachable");
    }

    #[test]
    fn lint_findings_error_matches_historical_message() {
        let err = LintFindingsError::new(3, 1);
        assert_eq!(
            err.to_string(),
            "lint found 3 warning(s) and 1 error(s); pass --allow-warnings to allow warnings"
        );
    }

    #[test]
    fn classify_maps_spec_error_to_usage() {
        let err: anyhow::Error = SpecError::MissingServices.into();
        assert_eq!(classify(&err), ExitCode::Usage);
        assert_eq!(exit_code_for(&err), 2);
    }

    #[test]
    fn classify_finds_spec_error_through_context() {
        // Commands wrap load failures with context; the code must still resolve.
        let err = anyhow::Error::from(SpecError::MissingServices).context("while loading the spec");
        assert_eq!(classify(&err), ExitCode::Usage);
        assert_eq!(exit_code_for(&err), 2);
    }

    #[test]
    fn classify_maps_generic_spec_validation_carrier_to_usage() {
        let err: anyhow::Error =
            SpecValidationError::new(anyhow::anyhow!("invalid semantic value")).into();
        assert_eq!(classify(&err), ExitCode::Usage);
        assert_eq!(exit_code_for(&err), 2);
    }

    #[test]
    fn classify_maps_lint_findings_to_lint() {
        let err: anyhow::Error = LintFindingsError::new(1, 0).into();
        assert_eq!(classify(&err), ExitCode::Lint);
        assert_eq!(exit_code_for(&err), 4);
    }

    #[test]
    fn classify_maps_environment_error_to_environment() {
        let err: anyhow::Error = EnvironmentError::new("cluster unreachable").into();
        assert_eq!(classify(&err), ExitCode::Environment);
        assert_eq!(exit_code_for(&err), 3);
    }

    #[test]
    fn classify_defaults_uncategorized_to_general() {
        let err = anyhow::anyhow!("something unexpected happened");
        assert_eq!(classify(&err), ExitCode::General);
        assert_eq!(exit_code_for(&err), 1);
    }

    #[test]
    fn exit_code_for_prefers_child_status_over_catalog() {
        // A child status wins even when it collides with a reserved code.
        let err: anyhow::Error = ExitCodeError(3).into();
        assert_eq!(exit_code_for(&err), 3);
        let err: anyhow::Error = ExitCodeError(137).into();
        assert_eq!(exit_code_for(&err), 137);
    }

    #[test]
    fn exit_code_for_remaps_child_zero_to_one() {
        let err: anyhow::Error = ExitCodeError(0).into();
        assert_eq!(exit_code_for(&err), 1);
    }
}
