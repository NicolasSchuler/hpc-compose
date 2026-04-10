mod support;

use std::fs;

use hpc_compose::job::{build_submission_record, latest_record_path_for, write_submission_record};
use serde_json::Value;
use support::*;

fn write_record(
    compose: &std::path::Path,
    submit_dir: &std::path::Path,
    job_id: &str,
    submitted_at: u64,
) {
    fs::create_dir_all(submit_dir).expect("submit dir");
    let plan = runtime_plan(compose);
    let mut record = build_submission_record(
        compose,
        submit_dir,
        &submit_dir.join(format!("{job_id}.sbatch")),
        &plan,
        job_id,
    )
    .expect("record");
    record.submitted_at = submitted_at;
    write_submission_record(&record).expect("write record");
}

#[test]
fn jobs_list_scans_repo_tree_and_recovers_latest_when_pointer_is_missing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let root = fs::canonicalize(tmpdir.path()).expect("canonical root");

    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project_a = tmpdir.path().join("project-a");
    let project_b = tmpdir.path().join("project-b");
    fs::create_dir_all(&project_a).expect("project a");
    fs::create_dir_all(&project_b).expect("project b");

    let compose_a = write_prepare_compose(&project_a, &cache_dir);
    let compose_b = write_prepare_compose(&project_b, &cache_dir);
    let submit_a = tmpdir.path().join("submit-a");
    let submit_b = tmpdir.path().join("submit-b");

    write_record(&compose_a, &submit_a, "11111", 10);
    write_record(&compose_a, &submit_a, "22222", 20);
    write_record(&compose_b, &submit_b, "33333", 30);

    fs::remove_file(latest_record_path_for(&compose_a)).expect("remove latest");
    let runtime_dir = submit_a.join(".hpc-compose/22222/logs");
    fs::create_dir_all(&runtime_dir).expect("runtime dir");
    fs::write(runtime_dir.join("app.log"), "hello\n").expect("runtime log");

    let text = run_cli(tmpdir.path(), &["jobs", "list"]);
    assert_success(&text);
    let text_stdout = stdout_text(&text);
    assert!(text_stdout.contains("* 22222"));
    assert!(text_stdout.contains("* 33333"));

    let json = run_cli(tmpdir.path(), &["jobs", "list", "--format", "json"]);
    assert_success(&json);
    let payload: Value = serde_json::from_str(&stdout_text(&json)).expect("jobs json");
    assert_eq!(
        payload["scan_root"],
        Value::from(root.display().to_string())
    );
    let jobs = payload["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 3);

    let job_11111 = jobs
        .iter()
        .find(|job| job["job_id"] == "11111")
        .expect("job 11111");
    let job_22222 = jobs
        .iter()
        .find(|job| job["job_id"] == "22222")
        .expect("job 22222");
    let job_33333 = jobs
        .iter()
        .find(|job| job["job_id"] == "33333")
        .expect("job 33333");

    assert_eq!(job_11111["is_latest"], Value::from(false));
    assert_eq!(job_22222["is_latest"], Value::from(true));
    assert_eq!(job_33333["is_latest"], Value::from(true));
    assert_eq!(job_22222["runtime_job_root_present"], Value::from(true));
    assert_eq!(job_11111["runtime_job_root_present"], Value::from(false));
    assert_eq!(job_22222["disk_usage_bytes"], Value::Null);
}

#[test]
fn jobs_list_reports_disk_usage_in_json_when_requested() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");

    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let submit_dir = tmpdir.path().join("submit");

    write_record(&compose, &submit_dir, "44444", 40);
    let runtime_dir = submit_dir.join(".hpc-compose/44444/logs");
    fs::create_dir_all(&runtime_dir).expect("runtime dir");
    fs::write(runtime_dir.join("app.log"), "hello world\n").expect("runtime log");

    let json = run_cli(
        &project,
        &["jobs", "list", "--disk-usage", "--format", "json"],
    );
    assert_success(&json);
    let payload: Value = serde_json::from_str(&stdout_text(&json)).expect("jobs json");
    let jobs = payload["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["job_id"], Value::from("44444"));
    assert!(jobs[0]["disk_usage_bytes"].as_u64().unwrap_or(0) > 0);
}

#[test]
fn jobs_list_reports_disk_usage_in_text_when_requested() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");

    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let submit_dir = tmpdir.path().join("submit");

    write_record(&compose, &submit_dir, "55555", 50);
    let runtime_dir = submit_dir.join(".hpc-compose/55555/logs");
    fs::create_dir_all(&runtime_dir).expect("runtime dir");
    fs::write(runtime_dir.join("app.log"), "hello world\n").expect("runtime log");

    let text = run_cli(&project, &["jobs", "list", "--disk-usage"]);
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains("scan root:"));
    assert!(stdout.contains("* 55555"));
    assert!(stdout.contains("runtime=runtime"));
    assert!(stdout.contains("size="));
}
