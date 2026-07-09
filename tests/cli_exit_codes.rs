//! End-to-end coverage for the stable exit-code catalog (see
//! `docs/src/exit-codes.md` and `hpc_compose::exit`). Each test pins one code so
//! a regression that collapses a category back onto the generic `1` is caught.
//!
//! Codes covered elsewhere by their natural home:
//! - `3` (environment): `cli_doctor_readiness.rs` (a failing readiness probe).
//! - `4` (lint findings): `cli_spec.rs` (the opinionated-findings lint test).
//! - child pass-through: `cli_exec.rs` (a child's status is surfaced verbatim).

mod support;

use support::*;

/// An invalid spec is a validation error: code 2.
#[test]
fn validate_on_invalid_spec_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    // No `services:` / `steps:` mapping -> SpecError::MissingServices.
    let compose = write_compose(tmpdir.path(), "compose.yaml", "x-slurm:\n  mem: 256M\n");

    let output = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "an invalid spec should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

#[test]
fn validate_on_malformed_yaml_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "services:\n  app:\n    image: [unterminated\n",
    );

    let output = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "malformed YAML should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

#[test]
fn validate_on_generic_semantic_spec_error_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "services:\n  app:\n    image: alpine:latest\nx-slurm:\n  watchdog:\n    grace_period_seconds: 0\n",
    );

    let output = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );

    assert_failure(&output);
    assert!(stderr_text(&output).contains("grace_period_seconds must be at least 1"));
    assert_eq!(
        output.status.code(),
        Some(2),
        "semantic spec validation errors should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

/// A missing spec file is a validation error (SpecError::SpecFileNotFound): code 2.
#[test]
fn validate_on_missing_spec_file_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing = tmpdir.path().join("does-not-exist.yaml");

    let output = run_cli(
        tmpdir.path(),
        &["validate", "-f", missing.to_str().expect("path")],
    );

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a missing spec file should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

/// A parse-level usage error (unknown flag) is reported by clap, which exits 2.
#[test]
fn unknown_flag_is_a_usage_error_exit_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(tmpdir.path(), &["validate", "--no-such-flag"]);

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unknown flag should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

/// Semantic argument-combination errors now use the same stable usage exit code
/// as parser-level clap errors.
#[test]
fn semantic_argument_combination_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(tmpdir.path(), &["cache", "prune"]);

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a semantic argument-combination error should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}

#[test]
fn semantic_value_usage_error_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(
        tmpdir.path(),
        &["when", "--free-nodes", "0", "--partition", "gpu"],
    );

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a semantic argument value error should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
    assert!(stderr_text(&output).contains("--free-nodes must be greater than zero"));
}

/// Clap-level argument relationships also exit 2.
#[test]
fn clap_argument_relationship_exits_2() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    // `up --format` requires `--detach` or `--dry-run`; clap catches this before
    // any spec or cluster resolution, so no compose file is needed.
    let output = run_cli(tmpdir.path(), &["up", "--format", "json"]);

    assert_failure(&output);
    assert_eq!(
        output.status.code(),
        Some(2),
        "a clap argument relationship error should exit 2\nstdout:\n{}\nstderr:\n{}",
        stdout_text(&output),
        stderr_text(&output),
    );
}
