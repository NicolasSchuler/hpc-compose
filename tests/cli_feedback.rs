mod support;

use serde_json::Value;
use support::*;

#[test]
fn feedback_text_prints_local_report_and_issue_url() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(tmpdir.path(), &["feedback", "--kind", "bug"]);

    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("hpc-compose feedback (bug)"));
    assert!(stdout.contains("Issue link: https://github.com/"));
    assert!(stdout.contains("Local report:"));
    assert!(stdout.contains("No telemetry was sent"));
    assert!(stderr_text(&output).is_empty());
}

#[test]
fn feedback_json_has_schema_version_and_no_telemetry() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(
        tmpdir.path(),
        &["feedback", "--kind", "feature", "--format", "json"],
    );

    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("feedback json");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["kind"], "feature");
    assert_eq!(value["report"]["package"], "hpc-compose");
    assert_eq!(value["telemetry_sent"], false);
    assert!(
        value["issue_url"]
            .as_str()
            .expect("issue url")
            .contains("template=feature_request.yml")
    );
    assert!(stderr_text(&output).is_empty());
}
