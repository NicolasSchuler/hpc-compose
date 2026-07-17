use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

use crate::support::*;
use serde_json::Value;

fn write_compose(dir: &std::path::Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("compose.yaml");
    fs::write(&path, body).expect("write compose");
    path
}

#[test]
fn doctor_uses_resolved_plan_cache_dir_when_file_is_provided() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("project-cache");
    let compose = write_compose(
        tmpdir.path(),
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: python:3.12-slim
    command: python -V
"#,
            cache_dir.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );

    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("doctor json");
    let messages = payload["passed_checks"]
        .as_array()
        .expect("passed checks")
        .iter()
        .filter_map(|item| item["message"].as_str())
        .collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .any(|message| message.contains(&cache_dir.display().to_string())),
        "expected doctor cache check to use resolved plan cache dir {}\nstdout:\n{}",
        cache_dir.display(),
        stdout_text(&output)
    );
}

#[test]
fn deprecated_doctor_cluster_report_ignores_file_argument() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing = tmpdir.path().join("missing-compose.yaml");

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "-f",
            missing.to_str().expect("path"),
            "--cluster-report",
            "--cluster-report-out",
            "-",
            "--format",
            "json",
        ],
    );

    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("doctor json");
    assert_eq!(payload["schema_version"], Value::from(1));
    assert_eq!(payload["wrote"], Value::from(false));
    assert_eq!(payload["path"], Value::Null);
}

#[test]
fn doctor_readiness_explains_probe_variants_and_infers_single_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        r#"
name: readiness-demo
services:
  api:
    image: python:3.12-slim
    command: python -m http.server 8000
    readiness:
      type: http
      url: http://127.0.0.1:8000/health
      status_code: 204
      timeout_seconds: 7
"#,
    );

    let text = run_cli(
        tmpdir.path(),
        &["doctor", "readiness", "-f", compose.to_str().expect("path")],
    );
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains("readiness service: api"));
    assert!(stdout.contains("type: http"));
    assert!(stdout.contains("generated behavior: wait_for_http"));

    let json = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "readiness",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&json);
    let payload: Value = serde_json::from_str(&stdout_text(&json)).expect("json");
    assert_eq!(payload["ok"], Value::from(true));
    assert_eq!(payload["service"], Value::from("api"));
    assert_eq!(payload["type"], Value::from("http"));
    assert_eq!(payload["mode"], Value::from("explain"));
    assert_eq!(payload["ran"], Value::from(false));
    assert_eq!(payload["timeout_seconds"], Value::from(7));
}

#[test]
fn doctor_readiness_requires_service_when_multiple_services_have_readiness() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        r#"
name: readiness-demo
services:
  api:
    image: python:3.12-slim
    command: python -m http.server 8000
    readiness:
      type: tcp
      port: 8000
  worker:
    image: python:3.12-slim
    command: python worker.py
    readiness:
      type: sleep
      seconds: 1
"#,
    );

    let output = run_cli(
        tmpdir.path(),
        &["doctor", "readiness", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("multiple readiness services"));
}

#[test]
fn doctor_readiness_runs_tcp_probe() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("tcp listener");
    let port = listener.local_addr().expect("addr").port();
    let handle = thread::spawn(move || {
        let _ = listener.accept();
    });
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        &format!(
            r#"
name: readiness-demo
services:
  api:
    image: python:3.12-slim
    command: python -m http.server {port}
    readiness:
      type: tcp
      port: {port}
      timeout_seconds: 2
"#
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "readiness",
            "-f",
            compose.to_str().expect("path"),
            "--run",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    handle.join().expect("join listener");
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ran"], Value::from(true));
    assert_eq!(payload["passed"], Value::from(true));
}

#[test]
fn doctor_readiness_runs_http_probe_and_reports_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("http listener");
    let port = listener.local_addr().expect("addr").port();
    let handle = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buffer = [0_u8; 512];
            let _ = stream.read(&mut buffer);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        }
    });
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        &format!(
            r#"
name: readiness-demo
services:
  api:
    image: python:3.12-slim
    command: python -m http.server {port}
    readiness:
      type: http
      url: http://127.0.0.1:{port}/health
      status_code: 204
      timeout_seconds: 1
"#
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "readiness",
            "-f",
            compose.to_str().expect("path"),
            "--run",
            "--format",
            "json",
        ],
    );
    assert_failure(&output);
    // A failed readiness probe is an environment failure: code 3.
    assert_eq!(
        output.status.code(),
        Some(3),
        "a failed readiness probe should exit 3\nstderr:\n{}",
        stderr_text(&output),
    );
    handle.join().expect("join listener");
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("json");
    assert_eq!(payload["ran"], Value::from(true));
    assert_eq!(payload["passed"], Value::from(false));
    assert!(stderr_text(&output).contains("readiness probe failed"));
}

#[test]
fn doctor_readiness_runs_log_probe_with_explicit_log_file() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log_file = tmpdir.path().join("api.log");
    fs::write(&log_file, "booted\nready\n").expect("write log");
    let compose = write_compose(
        tmpdir.path(),
        r#"
name: readiness-demo
services:
  api:
    image: python:3.12-slim
    command: python api.py
    readiness:
      type: log
      pattern: ready
      timeout_seconds: 1
"#,
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "doctor",
            "readiness",
            "-f",
            compose.to_str().expect("path"),
            "--run",
            "--log-file",
            log_file.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("result: passed"));
}
