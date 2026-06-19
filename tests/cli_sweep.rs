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

#[test]
fn sweep_list_reports_persisted_manifests_without_scheduler() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 21000);

    let first_compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001"]);
    let first = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            first_compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&first);
    let first_payload: Value = serde_json::from_str(&stdout_text(&first)).expect("first json");
    let first_sweep_id = first_payload["manifest"]["sweep_id"]
        .as_str()
        .expect("first sweep id")
        .to_string();

    let second_compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.01", "0.1"]);
    let second = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            second_compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&second);
    let second_payload: Value = serde_json::from_str(&stdout_text(&second)).expect("second json");
    let second_sweep_id = second_payload["manifest"]["sweep_id"]
        .as_str()
        .expect("second sweep id")
        .to_string();

    let list = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "list",
            "-f",
            second_compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&list);
    let payload: Value = serde_json::from_str(&stdout_text(&list)).expect("list json");
    let sweeps = payload["sweeps"].as_array().expect("sweeps array");
    assert_eq!(sweeps.len(), 2);
    assert!(sweeps.windows(2).all(|pair| {
        pair[0]["submitted_at"].as_u64().unwrap_or_default()
            >= pair[1]["submitted_at"].as_u64().unwrap_or_default()
    }));
    assert!(sweeps.iter().any(|sweep| {
        sweep["sweep_id"] == first_sweep_id.as_str()
            && sweep["trials"].as_array().expect("trials").len() == 1
    }));
    assert!(sweeps.iter().any(|sweep| {
        sweep["sweep_id"] == second_sweep_id.as_str()
            && sweep["trials"].as_array().expect("trials").len() == 2
    }));

    let text = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "list",
            "-f",
            second_compose.to_str().expect("path"),
        ],
    );
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains(&first_sweep_id));
    assert!(stdout.contains(&second_sweep_id));
}

#[test]
fn sweep_list_ignores_corrupt_manifest_dirs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 21500);

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
            "--format",
            "json",
        ],
    );
    assert_success(&submit);
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/sweeps/corrupt")).expect("corrupt dir");
    fs::write(
        tmpdir
            .path()
            .join(".hpc-compose/sweeps/corrupt/manifest.json"),
        "{not json",
    )
    .expect("corrupt manifest");

    let list = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "list",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&list);
    let payload: Value = serde_json::from_str(&stdout_text(&list)).expect("list json");
    let sweeps = payload["sweeps"].as_array().expect("sweeps array");
    assert_eq!(sweeps.len(), 1);
    assert_ne!(sweeps[0]["sweep_id"], Value::from("corrupt"));
}

#[test]
fn sweep_status_specific_sweep_id_uses_requested_manifest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 22000);

    let first_compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001"]);
    let first = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            first_compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&first);
    let first_payload: Value = serde_json::from_str(&stdout_text(&first)).expect("first json");
    let first_sweep_id = first_payload["manifest"]["sweep_id"]
        .as_str()
        .expect("first sweep id")
        .to_string();

    let second_compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.01"]);
    let second = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            second_compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&second);

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "status",
            "-f",
            second_compose.to_str().expect("path"),
            "--sweep-id",
            &first_sweep_id,
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
    assert_eq!(payload["sweep_id"], Value::from(first_sweep_id.as_str()));
    assert_eq!(
        payload["trials"][0]["variables"]["lr"],
        Value::from("0.001")
    );
}

#[test]
fn sweep_submit_rejects_array_before_sbatch_in_dry_run_and_submit() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "array-sweep.yaml",
        &format!(
            r#"
name: array-sweep
x-slurm:
  cache_dir: {}
  array: 0-3
sweep:
  parameters:
    lr: [0.001]
  matrix: full
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}"]
"#,
            cache.path().display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    for dry_run in [true, false] {
        let mut args = vec![
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ];
        if dry_run {
            args.push("--dry-run");
        }
        let output = run_cli(tmpdir.path(), &args);
        assert_failure(&output);
        assert!(stderr_text(&output).contains("sweep submit does not support x-slurm.array"));
        assert!(!sbatch_log.exists());
    }
}

#[test]
fn sweep_random_matrix_cli_persists_seed_and_trial_vars() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "random-sweep.yaml",
        &format!(
            r#"
name: random-sweep
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [0.001, 0.01, 0.1]
    batch: [16, 32]
  matrix:
    random: 3
    seed: stable-seed
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}", "--batch", "${{batch}}"]
"#,
            cache.path().display()
        ),
    );
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 23000);
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
            "--format",
            "json",
        ],
    );
    assert_success(&submit);
    let payload: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let manifest = &payload["manifest"];
    assert_eq!(manifest["matrix"], Value::from("random"));
    assert_eq!(manifest["seed"], Value::from("stable-seed"));
    let trials = manifest["trials"].as_array().expect("trials");
    assert_eq!(trials.len(), 3);
    for trial in trials {
        assert!(trial["variables"]["lr"].as_str().is_some());
        assert!(trial["variables"]["batch"].as_str().is_some());
        let script = fs::read_to_string(trial["script_path"].as_str().expect("script path"))
            .expect("trial script");
        assert!(script.contains("--lr"));
        assert!(script.contains("--batch"));
    }
}

#[test]
fn sweep_random_matrix_without_seed_persists_sweep_id_as_seed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "random-no-seed.yaml",
        &format!(
            r#"
name: random-no-seed
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [0.001, 0.01, 0.1]
    batch: [16, 32]
  matrix:
    random: 2
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}", "--batch", "${{batch}}"]
"#,
            cache.path().display()
        ),
    );
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 23500);

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
            "--format",
            "json",
        ],
    );
    assert_success(&submit);
    let payload: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let manifest = &payload["manifest"];
    assert_eq!(manifest["matrix"], Value::from("random"));
    assert_eq!(manifest["seed"], manifest["sweep_id"]);
    assert_eq!(manifest["trials"].as_array().expect("trials").len(), 2);
}

#[test]
fn sweep_status_reports_submit_failed_and_unsubmitted_trials() {
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

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
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
            write_fake_squeue(tmpdir.path(), &squeue_state)
                .to_str()
                .expect("path"),
            "--sacct-bin",
            write_fake_sacct(tmpdir.path(), &sacct_state)
                .to_str()
                .expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    assert_eq!(payload["summary"]["completed"], Value::from(1));
    assert_eq!(payload["summary"]["submit_failed"], Value::from(1));
    assert_eq!(payload["summary"]["unknown"], Value::from(1));
    assert_eq!(payload["trials"][1]["status"], Value::from("submit_failed"));
    assert!(
        payload["trials"][1]["submit_error"]
            .as_str()
            .unwrap_or_default()
            .contains("sbatch failed")
    );
    assert_eq!(payload["trials"][2]["status"], Value::from("unknown"));
    assert!(
        payload["trials"][2]["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("no recorded job id")
    );
}

#[test]
fn sweep_status_missing_trial_record_is_missing_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.001", "0.01"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 24000);

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
            "--format",
            "json",
        ],
    );
    assert_success(&submit);
    let payload: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let missing_job_id = payload["manifest"]["trials"][1]["job_id"]
        .as_str()
        .expect("second job id");
    fs::remove_file(
        tmpdir
            .path()
            .join(".hpc-compose/jobs")
            .join(format!("{missing_job_id}.json")),
    )
    .expect("remove trial record");

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
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
            write_fake_squeue(tmpdir.path(), &squeue_state)
                .to_str()
                .expect("path"),
            "--sacct-bin",
            write_fake_sacct(tmpdir.path(), &sacct_state)
                .to_str()
                .expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    assert_eq!(payload["summary"]["completed"], Value::from(1));
    assert_eq!(payload["summary"]["missing_tracking"], Value::from(1));
    assert_eq!(
        payload["trials"][1]["status"],
        Value::from("missing_tracking")
    );
    assert!(
        payload["trials"][1]["detail"]
            .as_str()
            .unwrap_or_default()
            .contains(missing_job_id)
    );
}

#[test]
fn sweep_random_matrix_respects_max_trials_before_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "random-guard.yaml",
        &format!(
            r#"
name: random-guard
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [0.001, 0.01, 0.1]
  matrix:
    random: 3
    seed: stable-seed
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}"]
"#,
            cache.path().display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let output = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--max-trials",
            "2",
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("above the limit of 2"));
    assert!(!sbatch_log.exists());
    assert!(!tmpdir.path().join(".hpc-compose/sweeps").exists());
}

fn write_objective_sweep_compose(root: &Path, cache_dir: &Path) -> PathBuf {
    let compose = root.join("train-obj.yaml");
    fs::write(
        &compose,
        format!(
            r#"
name: sweep-obj-train
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [0.001, 0.01]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'final loss=([0-9.]+)'
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}"]
"#,
            cache_dir.display()
        ),
    )
    .expect("write objective sweep compose");
    compose
}

#[test]
fn sweep_objective_omitted_group_does_not_serialize_zero() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_objective_sweep_compose(tmpdir.path(), cache.path());

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
            "--format",
            "json",
        ],
    );
    assert_success(&dry_run);
    let payload: Value = serde_json::from_str(&stdout_text(&dry_run)).expect("dry-run JSON output");
    assert_eq!(
        payload["manifest"]["objective"]["direction"],
        Value::from("minimize")
    );
    assert_eq!(
        payload["manifest"]["objective"]["group"],
        Value::Null,
        "default capture group should be schema-defaulted, not serialized as zero"
    );
}

#[test]
fn sweep_objective_rejects_zero_group() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = tmpdir.path().join("train-obj-zero-group.yaml");
    fs::write(
        &compose,
        format!(
            r#"
name: sweep-obj-bad
x-slurm:
  cache_dir: {}
  time: "00:01:00"
sweep:
  parameters:
    lr: [0.001]
  matrix: full
  objective:
    direction: minimize
    log_pattern: 'final loss=([0-9.]+)'
    group: 0
services:
  trainer:
    image: docker://python:3.11
    command: ["python", "train.py", "--lr", "${{lr}}"]
"#,
            cache.path().display()
        ),
    )
    .expect("write bad objective sweep compose");

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
            "--format",
            "json",
        ],
    );
    assert_failure(&dry_run);
    assert!(
        stderr_text(&dry_run).contains("sweep.objective.group must be at least 1"),
        "expected objective group validation failure:\n{}",
        stderr_text(&dry_run)
    );
}

fn completed_squeue_sacct(dir: &Path) -> (PathBuf, PathBuf) {
    let squeue = dir.join("squeue-completed");
    write_script(
        &squeue,
        r#"#!/bin/bash
set -euo pipefail
echo "COMPLETED"
"#,
    );
    let sacct = dir.join("sacct-completed");
    write_script(
        &sacct,
        r#"#!/bin/bash
set -euo pipefail
format_string=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "--format" ]]; then format_string="$arg"; fi
  prev="$arg"
done
echo "COMPLETED|Unknown|Unknown|None"
"#,
    );
    (squeue, sacct)
}

#[test]
fn sweep_observe_parses_log_objectives_and_ranks_best() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_objective_sweep_compose(tmpdir.path(), cache.path());
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 20000);
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

    // Write an objective line into each trial's tracked service log.
    let manifest_path = tmpdir.path().join(".hpc-compose/sweeps/latest.json");
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).expect("manifest"))
            .expect("manifest json");
    let job_ids = manifest["trials"]
        .as_array()
        .expect("trials")
        .iter()
        .map(|t| t["job_id"].as_str().expect("job id").to_string())
        .collect::<Vec<_>>();
    assert_eq!(job_ids.len(), 2);
    // job 20000 -> loss 0.5, job 20001 -> loss 0.1 (lower is better).
    let losses = ["0.5", "0.1"];
    for (job_id, loss) in job_ids.iter().zip(losses.iter()) {
        let record_path = tmpdir
            .path()
            .join(format!(".hpc-compose/jobs/{job_id}.json"));
        let record: Value =
            serde_json::from_str(&fs::read_to_string(&record_path).expect("record"))
                .expect("record json");
        let log_path = PathBuf::from(
            record["service_logs"]["trainer"]
                .as_str()
                .expect("trainer log path"),
        );
        fs::create_dir_all(log_path.parent().expect("log parent")).expect("log dir");
        fs::write(&log_path, format!("epoch done\nfinal loss={loss}\n")).expect("write log");
    }

    let (squeue, sacct) = completed_squeue_sacct(tmpdir.path());
    let observe = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "observe",
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
    assert_success(&observe);
    let payload: Value = serde_json::from_str(&stdout_text(&observe)).expect("observe json");
    assert_eq!(payload["objective_configured"], Value::from(true));
    // best objective must be the lower loss (0.1).
    assert_eq!(payload["best_objective"], Value::from("0.1"));
    let best = payload["best_trial"].as_str().expect("best trial");
    let best_loss = payload["trials"]
        .as_array()
        .expect("trials")
        .iter()
        .find(|t| t["trial_id"].as_str() == Some(best))
        .and_then(|t| t["objective"].as_str())
        .expect("best objective value");
    assert_eq!(best_loss, "0.1");

    // The persisted manifest must carry the best trial.
    let manifest_after: Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).expect("manifest after"))
            .expect("manifest json after");
    assert_eq!(manifest_after["best_trial"], payload["best_trial"]);
}

#[test]
fn sweep_observe_without_objective_is_a_noop_warning() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    // Reuse the non-objective sweep compose helper.
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.01"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 30000);
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
    let manifest_path = tmpdir.path().join(".hpc-compose/sweeps/latest.json");
    let manifest_before = fs::read_to_string(&manifest_path).expect("manifest before observe");

    let (squeue, sacct) = completed_squeue_sacct(tmpdir.path());
    let observe = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "observe",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&observe);
    assert!(stdout_text(&observe).contains("no sweep.objective configured"));
    let manifest_after = fs::read_to_string(&manifest_path).expect("manifest after observe");
    assert_eq!(
        manifest_after, manifest_before,
        "observe without sweep.objective should not mutate the manifest"
    );
}

#[test]
fn sweep_stop_requires_yes_flag() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.01"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 40000);
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

    let (squeue, sacct) = completed_squeue_sacct(tmpdir.path());
    let stop = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "stop",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--scancel-bin",
            "scancel",
        ],
    );
    assert_failure(&stop);
    assert!(stderr_text(&stop).contains("--yes not set"));
}

#[test]
fn sweep_stop_with_yes_records_stop_on_manifest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache = safe_cache_dir();
    let compose = write_sweep_compose(tmpdir.path(), cache.path(), &["0.01"]);
    let sbatch = write_incrementing_sbatch(tmpdir.path(), 50000);
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

    let scancel = tmpdir.path().join("scancel-log");
    write_script(
        &scancel,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
echo "$@" >> '{}'
"#,
            tmpdir.path().join("scancel-calls.log").display()
        ),
    );
    let (squeue, sacct) = completed_squeue_sacct(tmpdir.path());
    let stop = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "stop",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--reason",
            "objective threshold met",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&stop);
    let stop_payload: Value = serde_json::from_str(&stdout_text(&stop)).expect("sweep stop json");
    assert_eq!(stop_payload["cancelled_count"], Value::from(0));
    assert_eq!(stop_payload["skipped_count"], Value::from(1));
    assert_eq!(
        stop_payload["stop_reason"],
        Value::from("objective threshold met")
    );
    // All trials were reported COMPLETED, so none get cancelled; the manifest
    // still records the stop.
    let manifest_path = tmpdir.path().join(".hpc-compose/sweeps/latest.json");
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(&manifest_path).expect("manifest"))
            .expect("manifest json");
    assert!(manifest["stopped_at"].as_u64().is_some());
    assert_eq!(
        manifest["stop_reason"],
        Value::from("objective threshold met")
    );
}
