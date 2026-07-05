mod support;

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use support::*;

const WS_NAME: &str = "wstest";

/// Fake ws_* tool fixture: workspaces are directories under `root`, so
/// ws_find hit/miss follows directory existence, ws_allocate creates the
/// directory (counting invocations), ws_extend records its arguments,
/// ws_release removes the directory (counting invocations), and ws_list
/// emits a realistic hpc-workspace block per directory.
struct WsFixture {
    root: PathBuf,
    find: PathBuf,
    allocate: PathBuf,
    extend: PathBuf,
    release: PathBuf,
    list: PathBuf,
    allocate_count: PathBuf,
    allocate_args: PathBuf,
    extend_args: PathBuf,
    release_count: PathBuf,
}

impl WsFixture {
    fn workspace_dir(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn tool_args(&self) -> Vec<String> {
        vec![
            "--ws-find-bin".to_string(),
            self.find.display().to_string(),
            "--ws-allocate-bin".to_string(),
            self.allocate.display().to_string(),
            "--ws-extend-bin".to_string(),
            self.extend.display().to_string(),
            "--ws-release-bin".to_string(),
            self.release.display().to_string(),
            "--ws-list-bin".to_string(),
            self.list.display().to_string(),
        ]
    }

    fn allocate_invocations(&self) -> u32 {
        read_count(&self.allocate_count)
    }

    fn release_invocations(&self) -> u32 {
        read_count(&self.release_count)
    }
}

fn read_count(path: &Path) -> u32 {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(0)
}

fn write_ws_tools(tmpdir: &Path) -> WsFixture {
    let root = tmpdir.join("ws-root");
    fs::create_dir_all(&root).expect("ws root");

    let find = tmpdir.join("ws_find");
    write_script(
        &find,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
dir="{root}/$1"
if [[ -d "$dir" ]]; then
  echo "$dir"
  exit 0
fi
exit 1
"#,
            root = root.display()
        ),
    );

    let allocate_count = tmpdir.join("ws-allocate-count");
    let allocate_args = tmpdir.join("ws-allocate-args");
    let allocate = tmpdir.join("ws_allocate");
    write_script(
        &allocate,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
count_file="{count}"
n=0
if [[ -f "$count_file" ]]; then
  n="$(cat "$count_file")"
fi
echo "$((n + 1))" > "$count_file"
echo "$@" >> "{args}"
mkdir -p "{root}/$1"
echo "Info: creating workspace."
"#,
            count = allocate_count.display(),
            args = allocate_args.display(),
            root = root.display()
        ),
    );

    let extend_args = tmpdir.join("ws-extend-args");
    let extend = tmpdir.join("ws_extend");
    write_script(
        &extend,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
echo "$@" >> "{args}"
echo "Info: extending workspace."
"#,
            args = extend_args.display()
        ),
    );

    let release_count = tmpdir.join("ws-release-count");
    let release = tmpdir.join("ws_release");
    write_script(
        &release,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
count_file="{count}"
n=0
if [[ -f "$count_file" ]]; then
  n="$(cat "$count_file")"
fi
echo "$((n + 1))" > "$count_file"
rm -rf "{root}/$1"
echo "Info: releasing workspace."
"#,
            count = release_count.display(),
            root = root.display()
        ),
    );

    let list = tmpdir.join("ws_list");
    write_script(
        &list,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
for dir in "{root}"/*/; do
  [[ -d "$dir" ]] || continue
  name="$(basename "$dir")"
  echo "id: $name"
  echo "     workspace directory  : ${{dir%/}}"
  echo "     remaining time       : 29 days 23 hours"
  echo "     creation time        : Thu Jul  3 10:00:00 2026"
  echo "     expiration date      : Mon Aug  3 10:00:00 2026"
  echo "     available extensions : 3"
done
"#,
            root = root.display()
        ),
    );

    WsFixture {
        root,
        find,
        allocate,
        extend,
        release,
        list,
        allocate_count,
        allocate_args,
        extend_args,
        release_count,
    }
}

fn write_workspace_settings(root: &Path) {
    write_workspace_settings_named(root, WS_NAME);
}

fn write_workspace_settings_named(root: &Path, name: &str) {
    fs::create_dir_all(root.join(".hpc-compose")).expect("settings dir");
    fs::write(
        root.join(".hpc-compose/settings.toml"),
        format!("version = 1\n\n[defaults.workspace]\nname = \"{name}\"\nduration_days = 5\n"),
    )
    .expect("settings");
}

fn run_workspace(cwd: &Path, fixture: &WsFixture, args: &[&str]) -> std::process::Output {
    let mut full: Vec<String> = vec!["workspace".to_string()];
    full.extend(args.iter().map(|arg| (*arg).to_string()));
    full.extend(fixture.tool_args());
    let refs: Vec<&str> = full.iter().map(String::as_str).collect();
    run_cli(cwd, &refs)
}

fn state_file_text(root: &Path) -> String {
    fs::read_to_string(root.join(".hpc-compose/workspace-state.toml")).expect("state file")
}

#[test]
fn workspace_status_reports_existing_workspace_text_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("pre-existing workspace");

    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_success(&status);
    let stdout = stdout_text(&status);
    assert!(
        stdout.contains(&format!("workspace: {WS_NAME}")),
        "got: {stdout}"
    );
    assert!(
        stdout.contains(&fixture.workspace_dir(WS_NAME).display().to_string()),
        "got: {stdout}"
    );
    assert!(stdout.contains("29 days 23 hours"), "got: {stdout}");
    assert!(stdout.contains("available extensions: 3"), "got: {stdout}");

    let status_json = run_workspace(tmpdir.path(), &fixture, &["status", "--format", "json"]);
    assert_success(&status_json);
    let payload: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status JSON");
    assert_eq!(payload["schema_version"], Value::from(1));
    assert_eq!(payload["name"], Value::from(WS_NAME));
    assert_eq!(payload["exists"], Value::from(true));
    assert_eq!(
        payload["path"],
        Value::from(fixture.workspace_dir(WS_NAME).display().to_string())
    );
    assert_eq!(payload["extensions_remaining"], Value::from(3));
    assert_eq!(
        payload["remaining_display"],
        Value::from("29 days 23 hours")
    );
    assert!(
        payload["expiry_epoch"].as_u64().expect("expiry_epoch") > 0,
        "expiry_epoch should be computed from the remaining time"
    );

    // The read refreshed the persisted per-profile state file.
    let state = state_file_text(tmpdir.path());
    assert!(state.contains("version = 1"), "got: {state}");
    assert!(state.contains("[profiles.default]"), "got: {state}");
    assert!(state.contains(WS_NAME), "got: {state}");
    assert!(state.contains("expiry_epoch"), "got: {state}");
}

#[test]
fn workspace_status_without_configuration_fails_with_settings_hint() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let fixture = write_ws_tools(tmpdir.path());

    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_failure(&status);
    let stderr = stderr_text(&status);
    assert!(
        stderr.contains("no workspace is configured"),
        "got: {stderr}"
    );
    assert!(stderr.contains("settings.toml"), "got: {stderr}");
    assert!(stderr.contains("workspace"), "got: {stderr}");
}

#[test]
fn workspace_status_reports_missing_workspace_with_allocate_hint() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());

    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_success(&status);
    let stdout = stdout_text(&status);
    assert!(stdout.contains("does not exist yet"), "got: {stdout}");
    assert!(stdout.contains("workspace allocate"), "got: {stdout}");

    let status_json = run_workspace(tmpdir.path(), &fixture, &["status", "--format", "json"]);
    assert_success(&status_json);
    let payload: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status JSON");
    assert_eq!(payload["exists"], Value::from(false));
    assert_eq!(payload["path"], Value::Null);
}

#[test]
fn workspace_allocate_is_idempotent_and_only_allocates_once() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());

    let first = run_workspace(tmpdir.path(), &fixture, &["allocate"]);
    assert_success(&first);
    let stdout = stdout_text(&first);
    assert!(stdout.contains("allocated workspace"), "got: {stdout}");
    assert!(fixture.workspace_dir(WS_NAME).is_dir());
    assert_eq!(fixture.allocate_invocations(), 1);
    // Settings duration_days (5) flows into ws_allocate when no flag is set.
    let allocate_args = fs::read_to_string(&fixture.allocate_args).expect("allocate args");
    assert_eq!(allocate_args.trim(), format!("{WS_NAME} 5"));

    let second = run_workspace(tmpdir.path(), &fixture, &["allocate", "--format", "json"]);
    assert_success(&second);
    let payload: Value = serde_json::from_str(&stdout_text(&second)).expect("allocate JSON");
    assert_eq!(payload["already_allocated"], Value::from(true));
    assert_eq!(payload["duration_days"], Value::Null);
    assert_eq!(
        fixture.allocate_invocations(),
        1,
        "second allocate must not invoke ws_allocate again"
    );
}

#[test]
fn workspace_extend_passes_days_and_refreshes_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");

    let extend = run_workspace(
        tmpdir.path(),
        &fixture,
        &["extend", "--days", "7", "--format", "json"],
    );
    assert_success(&extend);
    let payload: Value = serde_json::from_str(&stdout_text(&extend)).expect("extend JSON");
    assert_eq!(payload["days"], Value::from(7));
    assert_eq!(payload["extensions_remaining"], Value::from(3));
    let extend_args = fs::read_to_string(&fixture.extend_args).expect("extend args");
    assert_eq!(extend_args.trim(), format!("{WS_NAME} 7"));
    assert!(state_file_text(tmpdir.path()).contains(WS_NAME));

    // Extending a missing workspace fails with an allocate hint instead of
    // invoking ws_extend.
    fs::remove_dir_all(fixture.workspace_dir(WS_NAME)).expect("remove workspace");
    let missing = run_workspace(tmpdir.path(), &fixture, &["extend", "--days", "7"]);
    assert_failure(&missing);
    assert!(
        stderr_text(&missing).contains("workspace allocate"),
        "got: {}",
        stderr_text(&missing)
    );
}

#[test]
fn workspace_release_requires_confirmation_when_not_a_terminal() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");

    let release = run_workspace(tmpdir.path(), &fixture, &["release"]);
    assert_failure(&release);
    let stderr = stderr_text(&release);
    assert!(stderr.contains("requires --yes"), "got: {stderr}");
    assert!(stderr.contains("release workspace"), "got: {stderr}");
    assert_eq!(fixture.release_invocations(), 0);
    assert!(fixture.workspace_dir(WS_NAME).is_dir());
}

#[test]
fn workspace_release_refuses_while_tracked_records_live_under_it() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    let workspace_dir = fixture.workspace_dir(WS_NAME);
    fs::create_dir_all(&workspace_dir).expect("workspace");

    // A tracked record whose cache_dir lies under the workspace path.
    let jobs_dir = tmpdir.path().join(".hpc-compose/jobs");
    fs::create_dir_all(&jobs_dir).expect("jobs dir");
    let record = serde_json::json!({
        "schema_version": 3,
        "backend": "slurm",
        "kind": "main",
        "job_id": "424242",
        "submitted_at": 0,
        "compose_file": tmpdir.path().join("compose.yaml"),
        "submit_dir": tmpdir.path(),
        "script_path": tmpdir.path().join("run.sbatch"),
        "cache_dir": workspace_dir.join("hpc-compose-cache"),
        "batch_log": tmpdir.path().join("logs/x.out"),
        "service_logs": {}
    });
    fs::write(
        jobs_dir.join("424242.json"),
        serde_json::to_vec_pretty(&record).expect("record json"),
    )
    .expect("record");

    let release = run_workspace(tmpdir.path(), &fixture, &["release", "--yes"]);
    assert_failure(&release);
    let stderr = stderr_text(&release);
    assert!(stderr.contains("refusing to release"), "got: {stderr}");
    assert!(stderr.contains("424242"), "got: {stderr}");
    assert!(stderr.contains("down"), "got: {stderr}");
    assert_eq!(fixture.release_invocations(), 0);
    assert!(workspace_dir.is_dir(), "workspace must survive the refusal");
}

#[test]
fn workspace_release_with_yes_releases_and_clears_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");

    // Populate the state file first so release provably clears the entry.
    assert_success(&run_workspace(tmpdir.path(), &fixture, &["status"]));
    assert!(state_file_text(tmpdir.path()).contains(WS_NAME));

    let release = run_workspace(
        tmpdir.path(),
        &fixture,
        &["release", "--yes", "--format", "json"],
    );
    assert_success(&release);
    let payload: Value = serde_json::from_str(&stdout_text(&release)).expect("release JSON");
    assert_eq!(payload["released"], Value::from(true));
    assert_eq!(fixture.release_invocations(), 1);
    assert!(!fixture.workspace_dir(WS_NAME).exists());
    assert!(
        !state_file_text(tmpdir.path()).contains(WS_NAME),
        "state entry must be cleared after release"
    );

    // Releasing again is an idempotent no-op that never re-invokes ws_release.
    let again = run_workspace(tmpdir.path(), &fixture, &["release", "--yes"]);
    assert_success(&again);
    assert!(
        stdout_text(&again).contains("nothing to release"),
        "got: {}",
        stdout_text(&again)
    );
    assert_eq!(fixture.release_invocations(), 1);
}

#[test]
fn workspace_status_survives_non_ascii_ws_list_output() {
    // Regression: a ws_list line with a multi-byte character crossing byte
    // offset 3 used to panic the parser (exit 101) instead of being skipped.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");
    write_script(
        &fixture.list,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
echo "a€rie: non-ascii noise"
echo "id: {WS_NAME}"
echo "     remaining time       : 3 days"
"#
        ),
    );

    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_success(&status);
    let stdout = stdout_text(&status);
    assert!(stdout.contains("3 days"), "got: {stdout}");
}

#[test]
fn workspace_status_recovers_from_corrupt_or_future_version_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");
    let state_path = tmpdir.path().join(".hpc-compose/workspace-state.toml");

    // Corrupt state: status must not fail — it warns and rebuilds the file.
    fs::write(&state_path, "not [valid toml").expect("corrupt state");
    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_success(&status);
    assert!(
        stderr_text(&status).contains("regenerable cache"),
        "got: {}",
        stderr_text(&status)
    );
    let state = state_file_text(tmpdir.path());
    assert!(state.contains("version = 1"), "got: {state}");
    assert!(state.contains(WS_NAME), "got: {state}");

    // Unknown (future) schema version: same fallback + canonical rewrite.
    fs::write(&state_path, "version = 2\n").expect("future version state");
    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_success(&status);
    let state = state_file_text(tmpdir.path());
    assert!(state.contains("version = 1"), "got: {state}");
    assert!(state.contains(WS_NAME), "got: {state}");
}

#[test]
fn workspace_release_with_yes_succeeds_despite_future_version_state() {
    // Regression: a stale state file must never fail `release` after
    // ws_release already removed the workspace.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());
    fs::create_dir_all(fixture.workspace_dir(WS_NAME)).expect("workspace");
    let state_path = tmpdir.path().join(".hpc-compose/workspace-state.toml");
    fs::write(
        &state_path,
        format!(
            "version = 2\n\n[profiles.default]\nname = \"{WS_NAME}\"\npath = \"/stale\"\nlast_checked = 0\n"
        ),
    )
    .expect("future version state");

    let release = run_workspace(tmpdir.path(), &fixture, &["release", "--yes"]);
    assert_success(&release);
    assert_eq!(fixture.release_invocations(), 1);
    assert!(!fixture.workspace_dir(WS_NAME).exists());
    let state = state_file_text(tmpdir.path());
    assert!(state.contains("version = 1"), "got: {state}");
    assert!(
        !state.contains(WS_NAME),
        "stale entry must be cleared: {state}"
    );
}

#[test]
fn workspace_rejects_leading_dash_name() {
    // A leading-dash name would be passed to the ws_* tools as a flag.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings_named(tmpdir.path(), "--delete-everything");
    let fixture = write_ws_tools(tmpdir.path());

    let status = run_workspace(tmpdir.path(), &fixture, &["status"]);
    assert_failure(&status);
    let stderr = stderr_text(&status);
    assert!(stderr.contains("must not start with"), "got: {stderr}");
    assert!(stderr.contains("settings"), "got: {stderr}");
}

#[test]
fn workspace_allocate_rejects_zero_duration() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_workspace_settings(tmpdir.path());
    let fixture = write_ws_tools(tmpdir.path());

    let allocate = run_workspace(
        tmpdir.path(),
        &fixture,
        &["allocate", "--duration-days", "0"],
    );
    assert_failure(&allocate);
    assert!(
        stderr_text(&allocate).contains("at least 1 day"),
        "got: {}",
        stderr_text(&allocate)
    );
    assert_eq!(
        fixture.allocate_invocations(),
        0,
        "ws_allocate must not run with a zero duration"
    );
}
