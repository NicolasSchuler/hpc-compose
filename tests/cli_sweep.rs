mod support;

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use support::*;

fn write_sweep_compose(root: &Path, cache_dir: &Path, values: &[&str]) -> PathBuf {
    let values = values.join(", ");
    let compose = root.join("train.yaml");
    fs::write(
        &compose,
        format!(
            r#"
name: sweep-train
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [{}]
  matrix: full
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}"]
"#,
            cache_dir.display(),
            values
        ),
    )
    .expect("write sweep compose");
    compose
}

fn write_incrementing_sbatch(tmpdir: &Path, start: u32) -> PathBuf {
    let path = tmpdir.join("sbatch-incrementing");
    let counter = tmpdir.join("sbatch-counter");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
counter="{}"
if [[ -f "$counter" ]]; then
  next="$(cat "$counter")"
else
  next="{}"
fi
echo "$((next + 1))" > "$counter"
echo "Submitted batch job $next"
"#,
            counter.display(),
            start
        ),
    );
    path
}

fn write_failing_second_sbatch(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-fails-second");
    let counter = tmpdir.join("sbatch-fail-counter");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
counter="{}"
if [[ -f "$counter" ]]; then
  next="$(cat "$counter")"
else
  next="1"
fi
echo "$((next + 1))" > "$counter"
if [[ "$next" == "1" ]]; then
  echo "Submitted batch job 11111"
  exit 0
fi
echo "submit boom" >&2
exit 1
"#,
            counter.display()
        ),
    );
    path
}

#[test]
fn sweep_dry_run_expands_trials_without_writing_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001", "0.01"]);

    let plan = run_cli(
        tmpdir.path(),
        &["plan", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&plan);
    assert!(stderr_text(&plan).contains("missing variable 'lr'"));

    let dry_run = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--dry-run",
            "--no-preflight",
            "--skip-prepare",
        ],
    );
    assert_success(&dry_run);
    let stdout = stdout_text(&dry_run);
    assert!(stdout.contains("dry run: no scripts written"));
    assert!(stdout.contains("t000"));
    assert!(stdout.contains("lr=0.001"));
    assert!(!tmpdir.path().join(".hpc-compose/sweeps").exists());

    let dry_run_json = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--dry-run",
            "--no-preflight",
            "--skip-prepare",
            "--format",
            "json",
        ],
    );
    assert_success(&dry_run_json);
    let payload: Value =
        serde_json::from_str(&stdout_text(&dry_run_json)).expect("dry-run JSON output");
    assert_eq!(payload["dry_run"], Value::from(true));
    assert_eq!(
        payload["manifest"]["trials"][0]["trial_id"],
        Value::from("t000")
    );
    assert_eq!(
        payload["manifest"]["trials"][1]["variables"]["lr"],
        Value::from("0.01")
    );
    assert!(!tmpdir.path().join(".hpc-compose/sweeps").exists());
}

#[test]
fn sweep_submit_persists_manifest_and_sweep_trial_records() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001", "0.01"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 11111);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let latest_manifest = tmpdir.path().join(".hpc-compose/sweeps/latest.json");
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(&latest_manifest).expect("read latest sweep manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["trials"].as_array().expect("trials").len(), 2);
    assert_eq!(manifest["trials"][0]["job_id"], Value::from("11111"));
    assert_eq!(manifest["trials"][1]["job_id"], Value::from("11112"));

    let record_path = tmpdir.path().join(".hpc-compose/jobs/11111.json");
    let record: Value =
        serde_json::from_str(&fs::read_to_string(record_path).expect("read record"))
            .expect("record json");
    assert_eq!(record["kind"], Value::from("sweep_trial"));
    assert_eq!(record["sweep"]["trial_id"], Value::from("t000"));
    assert_eq!(record["sweep"]["variables"]["lr"], Value::from("0.001"));
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
}

#[test]
fn sweep_submit_stops_and_persists_submit_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001", "0.01", "0.1"]);
    let sbatch = write_failing_second_sbatch(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    assert!(stderr_text(&submit).contains("sweep trial t001 failed"));

    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/sweeps/latest.json"))
            .expect("read manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["trials"][0]["job_id"], Value::from("11111"));
    assert!(
        manifest["trials"][1]["submit_error"]
            .as_str()
            .unwrap_or_default()
            .contains("sbatch failed")
    );
    assert_eq!(manifest["trials"][2]["job_id"], Value::Null);
}

#[test]
fn sweep_submit_enforces_and_overrides_fanout_guard() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let values = (0..101).map(|index| index.to_string()).collect::<Vec<_>>();
    let value_refs = values.iter().map(String::as_str).collect::<Vec<_>>();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &value_refs);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 12000);

    let guarded = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&guarded);
    assert!(stderr_text(&guarded).contains("above the limit of 100"));
    assert!(!tmpdir.path().join("sbatch-counter").exists());

    let permitted = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--max-trials",
            "101",
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&permitted);
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/sweeps/latest.json"))
            .expect("read manifest"),
    )
    .expect("manifest json");
    assert_eq!(manifest["trials"].as_array().expect("trials").len(), 101);
    assert_eq!(manifest["trials"][100]["trial_id"], Value::from("t100"));
}

#[test]
fn sweep_status_aggregates_scheduler_and_submit_states() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(
        tmpdir.path(),
        cache.path(),
        &["0.001", "0.01", "0.1", "1.0"],
    );
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 11111);
    let submit = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let squeue = tmpdir.path().join("squeue-by-job");
    write_script(
        &squeue,
        r#"#!/bin/bash
set -euo pipefail
job=""
format_string=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-j" ]]; then job="$arg"; fi
  if [[ "$prev" == "-o" || "$prev" == "--format" ]]; then format_string="$arg"; fi
  prev="$arg"
done
case "$job" in
  11112) [[ "$format_string" == *"%T|%r|%S"* ]] && echo "RUNNING|N/A|N/A" || echo "RUNNING" ;;
  11113) [[ "$format_string" == *"%T|%r|%S"* ]] && echo "PENDING|Resources|N/A" || echo "PENDING" ;;
  *) exit 0 ;;
esac
"#,
    );
    let sacct = tmpdir.path().join("sacct-by-job");
    write_script(
        &sacct,
        r#"#!/bin/bash
set -euo pipefail
job=""
format_string=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-j" || "$prev" == "--jobs" ]]; then job="$arg"; fi
  if [[ "$prev" == "--format" ]]; then format_string="$arg"; fi
  prev="$arg"
done
case "$job" in
  11111) [[ "$format_string" == *"State,Eligible,Start,Reason"* ]] && echo "COMPLETED|Unknown|Unknown|None" || echo "COMPLETED" ;;
  11114) [[ "$format_string" == *"State,Eligible,Start,Reason"* ]] && echo "FAILED|Unknown|Unknown|None" || echo "FAILED" ;;
  *) exit 0 ;;
esac
"#,
    );

    let status = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    assert_eq!(payload["summary"]["completed"], Value::from(1));
    assert_eq!(payload["summary"]["running"], Value::from(1));
    assert_eq!(payload["summary"]["pending"], Value::from(1));
    assert_eq!(payload["summary"]["failed"], Value::from(1));
}
