//! CLI argument-validation guards.
//!
//! `src/cli/commands.rs` is ~3000 lines of clap derive definitions that
//! llvm-cov cannot instrument and the coverage gate excludes, yet the
//! `conflicts_with`/`requires` rules on them are real, user-facing input
//! validation. A dropped rule (e.g. silently allowing `--rightsize` together
//! with `--dependencies`) would be invisible to coverage. These tests pin the
//! parse-time rejections so such a regression fails loudly.
//!
//! All of these fire during argument parsing, before any file access, so no
//! compose fixture or fake tooling is required.

mod support;

use support::*;

fn arg_error(args: &[&str]) -> String {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(tmp.path(), args);
    assert_failure(&output);
    stderr_text(&output)
}

#[test]
fn inspect_rejects_verbose_with_rightsize() {
    let stderr = arg_error(&["inspect", "--verbose", "--rightsize"]);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got: {stderr}"
    );
}

#[test]
fn inspect_rejects_tree_with_dependencies() {
    let stderr = arg_error(&["inspect", "--tree", "--dependencies"]);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got: {stderr}"
    );
}

#[test]
fn inspect_rejects_rightsize_with_dependencies() {
    let stderr = arg_error(&["inspect", "--rightsize", "--dependencies"]);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got: {stderr}"
    );
}

#[test]
fn inspect_rejects_dependencies_format_without_dependencies() {
    let stderr = arg_error(&["inspect", "--dependencies-format", "dot"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}

#[test]
fn inspect_rejects_job_id_without_rightsize() {
    let stderr = arg_error(&["inspect", "--job-id", "12345"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}
