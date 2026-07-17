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

use crate::support::*;

fn arg_error(args: &[&str]) -> String {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(tmp.path(), args);
    assert_failure(&output);
    stderr_text(&output)
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

#[test]
fn diff_rejects_against_spec_with_positional_job_ids() {
    let stderr = arg_error(&["diff", "11111", "22222", "--against-spec"]);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got: {stderr}"
    );
}

#[test]
fn diff_rejects_against_spec_with_jobs_matrix() {
    let stderr = arg_error(&["diff", "--against-spec", "--jobs", "1,2,3"]);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got: {stderr}"
    );
}

#[test]
fn diff_rejects_job_id_without_against_spec() {
    let stderr = arg_error(&["diff", "--job-id", "12345"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}

#[test]
fn diff_rejects_fail_on_change_without_against_spec() {
    let stderr = arg_error(&["diff", "--fail-on-change"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}

#[test]
fn up_format_requires_detach_or_dry_run() {
    let stderr = arg_error(&["up", "--format", "json"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}

#[test]
fn up_watch_queue_conflicts_with_detach_dry_run_and_local() {
    for args in [
        ["up", "--watch-queue", "--detach"].as_slice(),
        ["up", "--watch-queue", "--dry-run"].as_slice(),
        ["up", "--watch-queue", "--local"].as_slice(),
    ] {
        let stderr = arg_error(args);
        assert!(
            stderr.contains("cannot be used with"),
            "expected a clap conflict error for {args:?}, got: {stderr}"
        );
    }
}

#[test]
fn up_queue_warn_after_requires_watch_queue() {
    let stderr = arg_error(&["up", "--queue-warn-after", "5m"]);
    assert!(
        stderr.to_lowercase().contains("required"),
        "expected a clap requires error, got: {stderr}"
    );
}

// The following pin runtime (dispatch-time) `bail!` cross-flag guards in
// `src/commands/mod.rs`, which are not clap-native but still fire before any
// file access, so they need no fixture either.

#[test]
fn when_after_job_condition_requires_after_job() {
    let stderr = arg_error(&["when", "--after-job-condition", "afterok"]);
    assert!(
        stderr.contains("when --after-job-condition requires --after-job"),
        "expected the after-job-condition requires guard, got: {stderr}"
    );
}

#[test]
fn when_rejects_unknown_after_job_condition_value() {
    let stderr = arg_error(&[
        "when",
        "--after-job",
        "123",
        "--after-job-condition",
        "badcond",
    ]);
    assert!(
        stderr.contains("unknown --after-job-condition 'badcond'"),
        "expected an after-job-condition value error, got: {stderr}"
    );
}
