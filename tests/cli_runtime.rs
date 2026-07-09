mod support;

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use hpc_compose::job::{
    GitProvenance, JobProvenance, SWEEP_MANIFEST_SCHEMA_VERSION, SubmissionBackend, SubmissionKind,
    SubmissionRecord, SubmissionRecordBuildOptions, SweepManifest, SweepManifestTrial,
    SweepTrialMetadata, artifact_manifest_path_for_record, artifact_payload_dir_for_record,
    build_submission_record, build_submission_record_with_backend_and_options,
    build_submission_record_with_options, latest_record_path_for, load_submission_record,
    state_path_for_record, sweep_manifest_path_for, write_submission_record, write_sweep_manifest,
};
use hpc_compose::render::log_file_name_for_service;
use hpc_compose::rendezvous::{RendezvousRegisterRequest, build_record, register};
use serde_json::Value;
use support::*;

fn write_failing_tool(tmpdir: &Path, name: &str, log_path: &Path) -> PathBuf {
    let path = tmpdir.join(name);
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
printf '%s\n' "$0 $*" >> '{}'
exit 42
"#,
            log_path.display()
        ),
    );
    path
}

#[cfg(target_os = "linux")]
fn wait_for_service_assertion_status(
    cwd: &Path,
    compose: &Path,
    job_id: &str,
    service_name: &str,
    expected: &str,
) -> Value {
    let mut last = Value::Null;
    for _ in 0..80 {
        let status = run_cli(
            cwd,
            &[
                "status",
                "-f",
                compose.to_str().expect("path"),
                "--job-id",
                job_id,
                "--format",
                "json",
            ],
        );
        assert_success(&status);
        last = serde_json::from_str(&stdout_text(&status)).expect("status json");
        let observed = last["services"]
            .as_array()
            .and_then(|services| {
                services
                    .iter()
                    .find(|service| service["service_name"].as_str() == Some(service_name))
            })
            .and_then(|service| service["assertions"]["status"].as_str());
        if observed == Some(expected) {
            return last;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("assertion status did not become {expected}: {last:#}");
}

#[test]
fn up_command_runs_end_to_end_with_fake_tools() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("submit.sbatch");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_stdout = stdout_text(&submit);
    let submit_stderr = stderr_text(&submit);
    assert!(submit_stderr.contains("[run] Running preflight checks"));
    assert!(submit_stderr.contains("[done] Preparing runtime artifacts"));
    assert!(submit_stderr.contains("[done] Rendering submission script"));
    assert!(submit_stderr.contains("[done] Submitting job to Slurm"));
    assert!(submit_stdout.contains("Submitted batch job 12345"));
    assert!(submit_stdout.contains("rendered script:"));
    assert!(submit_stdout.contains("log  service 'app':"));
    assert!(script_out.exists());
    assert!(plan.ordered_services[0].runtime_image.exists());
}

#[test]
fn up_print_endpoints_surfaces_readiness_endpoints_in_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().display();
    let compose = tmpdir.path().join("endpoints.yaml");
    fs::write(
        &compose,
        format!(
            r#"name: endpoints-test
x-slurm:
  cache_dir: {cache_dir}
  time: "00:10:00"
services:
  api:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: tcp
      port: 8000
  dash:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: http
      url: "http://127.0.0.1:6006"
  worker:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: sleep
      seconds: 1
"#
        ),
    )
    .expect("write compose");

    // With --print-endpoints: TCP/HTTP endpoints + next_commands are populated;
    // Sleep readiness is excluded. Dry-run keeps it static (no scheduler).
    let out = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--dry-run",
            "--format",
            "json",
            "--print-endpoints",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&out);
    let json: Value = serde_json::from_str(&stdout_text(&out)).expect("up json");
    let endpoints = json["endpoints"].as_array().expect("endpoints array");
    let names: Vec<&str> = endpoints
        .iter()
        .filter_map(|e| e["service"].as_str())
        .collect();
    assert!(names.contains(&"api"), "tcp endpoint present: {json:#}");
    assert!(names.contains(&"dash"), "http endpoint present: {json:#}");
    assert!(
        !names.contains(&"worker"),
        "sleep readiness excluded: {json:#}"
    );
    let api = endpoints
        .iter()
        .find(|e| e["service"] == "api")
        .expect("api endpoint");
    assert_eq!(api["port"], 8000);
    let dash = endpoints
        .iter()
        .find(|e| e["service"] == "dash")
        .expect("dash endpoint");
    assert_eq!(dash["port"], 6006);
    assert_eq!(dash["host"], "127.0.0.1");
    assert_eq!(dash["url"], "http://127.0.0.1:6006");
    assert!(
        json["next_commands"]
            .as_array()
            .is_some_and(|c| !c.is_empty()),
        "next_commands non-empty: {json:#}"
    );
    assert_eq!(json["submitted"], false);
    assert!(json["job_id"].is_null());

    // Without --print-endpoints: keys are omitted (default JSON shape unchanged).
    let plain = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--dry-run",
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&plain);
    let plain_json: Value = serde_json::from_str(&stdout_text(&plain)).expect("plain json");
    assert!(
        plain_json.get("endpoints").is_none(),
        "endpoints omitted by default: {plain_json:#}"
    );
    assert!(
        plain_json.get("next_commands").is_none(),
        "next_commands omitted by default: {plain_json:#}"
    );
}

#[test]
fn alloc_exports_hpc_compose_environment_inside_salloc() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: alloc-demo
x-slurm:
  cache_dir: {}
  job_name: alloc-demo
  nodes: 2
  time: "00:10:00"
  submit_args:
    - --reservation=dev
services:
  app:
    image: {}
    command: ["echo", "ok"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let salloc_log = tmpdir.path().join("salloc.args");
    let salloc = tmpdir.path().join("salloc");
    write_script(
        &salloc,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" > '{}'
while [[ $# -gt 0 && "$1" == --* ]]; do
  shift
done
export SLURM_JOB_ID=777
export SLURM_JOB_NODELIST='node01,node02'
export SLURM_JOB_NUM_NODES=2
export SLURM_SUBMIT_DIR="$PWD"
PATH="{}:$PATH"
exec "$@"
"#,
            salloc_log.display(),
            tmpdir.path().display()
        ),
    );
    let scontrol = write_fake_scontrol(tmpdir.path());
    let env_log = tmpdir.path().join("alloc.env");
    let output = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--scontrol-bin",
            scontrol.to_str().expect("path"),
            "--",
            "bash",
            "-lc",
            &format!(
                "printf '%s|%s|%s|%s|%s\\n' \"$HPC_COMPOSE_ALLOCATION\" \"$HPC_COMPOSE_COMPOSE_FILE\" \"$HPC_COMPOSE_CACHE_DIR\" \"$HPC_COMPOSE_NODE_COUNT\" \"$HPC_COMPOSE_PRIMARY_NODE\" > '{}'",
                env_log.display()
            ),
        ],
    );
    assert_success(&output);
    let args = fs::read_to_string(salloc_log).expect("salloc args");
    assert!(args.contains("--job-name=alloc-demo"));
    assert!(args.contains("--nodes=2"));
    assert!(args.contains("--time=00:10:00"));
    assert!(args.contains("--reservation=dev"));
    let env = fs::read_to_string(env_log).expect("alloc env");
    assert!(env.contains("1|"));
    assert!(env.contains(compose.to_str().expect("compose")));
    assert!(env.contains(cache_dir.to_str().expect("cache")));
    assert!(env.contains("|2|node01"));
}

#[test]
fn alloc_rejects_array_and_scheduler_dependency_before_salloc() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let salloc_called = tmpdir.path().join("salloc.called");
    let salloc = tmpdir.path().join("salloc");
    write_script(
        &salloc,
        &format!(
            "#!/bin/bash\nset -euo pipefail\ntouch '{}'\nexit 0\n",
            salloc_called.display()
        ),
    );

    let array_compose = write_compose(
        tmpdir.path(),
        "array.yaml",
        &format!(
            r#"
x-slurm:
  array: 0-3
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let array = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            array_compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&array);
    assert!(stderr_text(&array).contains("alloc does not support x-slurm.array"));
    assert!(!salloc_called.exists());

    let dependency_compose = write_compose(
        tmpdir.path(),
        "dependency.yaml",
        &format!(
            r#"
x-slurm:
  after_job: "12345"
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let dependency = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            dependency_compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&dependency);
    assert!(stderr_text(&dependency).contains("alloc does not support Slurm job dependencies"));
    assert!(!salloc_called.exists());
}

#[test]
fn alloc_rejects_singleton_dependency_before_salloc() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let salloc_called = tmpdir.path().join("salloc.called");
    let salloc = tmpdir.path().join("salloc-singleton");
    write_script(
        &salloc,
        &format!(
            "#!/bin/bash\nset -euo pipefail\ntouch '{}'\nexit 0\n",
            salloc_called.display()
        ),
    );
    let compose = write_compose(
        tmpdir.path(),
        "singleton.yaml",
        &format!(
            r#"
x-slurm:
  dependency: singleton
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "alloc",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--salloc-bin",
            salloc.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("alloc does not support Slurm job dependencies"));
    assert!(!salloc_called.exists());
}

#[test]
fn run_service_inside_allocation_uses_srun_without_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: run-alloc
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: ["echo", "base"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let srun_log = tmpdir.path().join("srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch_called = tmpdir.path().join("sbatch.called");
    let sbatch = tmpdir.path().join("sbatch");
    write_script(
        &sbatch,
        &format!(
            "#!/bin/bash\nset -euo pipefail\ntouch '{}'\necho 'Submitted batch job 999'\n",
            sbatch_called.display()
        ),
    );
    let mut path_entries = vec![tmpdir.path().to_path_buf()];
    let bash_path = test_bash_path();
    if let Some(bash_dir) = bash_path.parent() {
        path_entries.push(bash_dir.to_path_buf());
    }
    if let Some(existing_path) = std::env::var_os("PATH") {
        path_entries.extend(std::env::split_paths(&existing_path));
    }
    let path = std::env::join_paths(path_entries)
        .expect("test PATH")
        .to_string_lossy()
        .into_owned();
    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "app",
            "--",
            "echo",
            "hello",
        ],
        &[
            ("HPC_COMPOSE_ALLOCATION", "1"),
            ("SLURM_JOB_ID", "777"),
            ("SLURM_JOB_NODELIST", "node01"),
            ("SLURM_SUBMIT_DIR", tmpdir.path().to_str().expect("tmp")),
            ("PATH", &path),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("using active Slurm allocation 777"));
    assert!(
        !sbatch_called.exists(),
        "sbatch should not be called inside alloc"
    );
    assert!(
        fs::read_to_string(srun_log)
            .expect("srun log")
            .contains("hpc-compose:app")
    );
    let record = load_submission_record(&compose, Some("777")).expect("run record");
    assert_eq!(record.kind, SubmissionKind::Run);
    assert_eq!(record.job_id, "777");
}

#[test]
fn status_array_merges_squeue_and_sacct_rows() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: array-demo
x-slurm:
  cache_dir: {}
  array: "7-8"
services:
  app:
    image: {}
    command: ["echo", "task"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record_with_options(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("array.sbatch"),
        &plan,
        "12345",
        &SubmissionRecordBuildOptions {
            slurm_array: Some("7-8".into()),
            ..SubmissionRecordBuildOptions::default()
        },
    )
    .expect("record");
    write_submission_record(&record).expect("write record");
    let squeue = tmpdir.path().join("squeue-array");
    write_script(
        &squeue,
        r#"#!/bin/bash
set -euo pipefail
if [[ "$*" == *"--array"* ]]; then
  printf '12345_7|RUNNING|00:01:00|node01\n12345_8|PENDING|00:00:00|(Priority)\n'
else
  printf 'RUNNING|None|2026-04-06T10:05:00\n'
fi
"#,
    );
    let sacct = tmpdir.path().join("sacct-array");
    write_script(
        &sacct,
        r#"#!/bin/bash
set -euo pipefail
if [[ "$*" == *"--array"* ]]; then
  printf '12345_7|COMPLETED|0:0|60\n12345_8|FAILED|1:0|30\n'
else
  printf 'RUNNING|2026-04-06T10:00:00|2026-04-06T10:05:00|None\n'
fi
"#,
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--array",
            "--job-id",
            "12345_7",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let json: Value = serde_json::from_str(&stdout_text(&output)).expect("status json");
    assert_eq!(json["array"]["parent_job_id"], Value::from("12345"));
    assert_eq!(json["array"]["filtered_task_id"], Value::from(7));
    assert_eq!(json["array"]["tasks"].as_array().expect("tasks").len(), 1);
    assert_eq!(json["array"]["tasks"][0]["state"], Value::from("RUNNING"));
    assert_eq!(json["array"]["tasks"][0]["source"], Value::from("squeue"));
}

#[test]
fn status_and_stats_degrade_when_scheduler_commands_are_missing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: missing-tools
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: ["echo", "task"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");
    let missing_squeue = tmpdir.path().join("missing-squeue");
    let missing_sacct = tmpdir.path().join("missing-sacct");
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            missing_squeue.to_str().expect("path"),
            "--sacct-bin",
            missing_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    assert!(stdout_text(&status).contains("not available"));

    let running_squeue = tmpdir.path().join("squeue-running");
    write_script(
        &running_squeue,
        "#!/bin/bash\nset -euo pipefail\nprintf 'RUNNING|None|2026-04-06T10:05:00\\n'\n",
    );
    let missing_sstat = tmpdir.path().join("missing-sstat");
    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            running_squeue.to_str().expect("path"),
            "--sacct-bin",
            missing_sacct.to_str().expect("path"),
            "--sstat-bin",
            missing_sstat.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    assert!(stdout_text(&stats).contains("sstat not available"));
}

#[test]
fn status_and_stats_surface_idle_resource_watchdog_warning() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: watchdog-demo
x-slurm:
  cache_dir: {}
  metrics:
    interval_seconds: 60
  watchdog:
    grace_period_seconds: 1
    gpu:
      window_seconds: 120
      compute_below_pct: 2
      memory_resident_above_pct: 20
    cpu:
      window_seconds: 120
      compute_below_pct: 5
      memory_resident_above_pct: 20
services:
  app:
    image: {}
    command: ["python", "train.py"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite old record");

    let metrics_dir = tmpdir.path().join(".hpc-compose/12345/metrics");
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    fs::write(
        metrics_dir.join("meta.json"),
        r#"{
  "interval_seconds": 60,
  "collectors": [
    {"name":"gpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-10T10:01:00Z"},
    {"name":"cpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-10T10:01:00Z"},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-10T10:01:00Z"}
  ]
}"#,
    )
    .expect("meta");
    fs::write(
        metrics_dir.join("gpu.jsonl"),
        "\
{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"index\":\"0\",\"uuid\":\"GPU-0\",\"utilization_gpu\":\"0\",\"utilization_memory\":\"99\",\"memory_used_mib\":\"30000\",\"memory_total_mib\":\"40000\"}\n\
{\"sampled_at\":\"2026-04-10T10:01:00Z\",\"index\":\"0\",\"uuid\":\"GPU-0\",\"utilization_gpu\":\"0\",\"utilization_memory\":\"99\",\"memory_used_mib\":\"30000\",\"memory_total_mib\":\"40000\"}\n",
    )
    .expect("gpu metrics");
    fs::write(
        metrics_dir.join("cpu.jsonl"),
        "\
{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"node\":\"node01\",\"cpu_util_pct\":50.0,\"core_count\":8,\"loadavg_1m\":4.0}\n\
{\"sampled_at\":\"2026-04-10T10:01:00Z\",\"node\":\"node01\",\"cpu_util_pct\":50.0,\"core_count\":8,\"loadavg_1m\":4.0}\n",
    )
    .expect("cpu metrics");
    fs::write(
        metrics_dir.join("slurm.jsonl"),
        "\
{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:30\",\"ave_rss\":\"4000M\",\"max_rss\":\"4000M\",\"alloc_tres\":\"cpu=8,mem=64G,gres/gpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:30\"}\n\
{\"sampled_at\":\"2026-04-10T10:01:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:30\",\"ave_rss\":\"4000M\",\"max_rss\":\"4000M\",\"alloc_tres\":\"cpu=8,mem=64G,gres/gpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:30\"}\n",
    )
    .expect("slurm metrics");

    let squeue_state = tmpdir.path().join("watchdog-squeue.state");
    fs::write(
        &squeue_state,
        "STATE=RUNNING\nREASON=None\nSTART=2026-04-10T09:59:00\n",
    )
    .expect("squeue state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct_state = tmpdir.path().join("watchdog-sacct.state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("watchdog-sstat.output");
    fs::write(&sstat_output, "").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let status = run_cli(
        tmpdir.path(),
        &[
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
    let status_json: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    assert_eq!(status_json["watchdog"]["status"], Value::from("warning"));
    assert!(
        status_json["watchdog"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("resident VRAM"))
    );
    let gpu = status_json["watchdog"]["observations"]
        .as_array()
        .expect("observations")
        .iter()
        .find(|item| item["resource"] == "gpu")
        .expect("gpu observation");
    assert_eq!(gpu["classification"], Value::from("resident_idle"));
    assert_eq!(gpu["memory_signal"], Value::from("gpu_memory_used_total"));

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let stats_stdout = stdout_text(&stats);
    assert!(stats_stdout.contains("Watchdog:"));
    assert!(stats_stdout.contains("gpu: resident_idle"));
    assert!(stats_stdout.contains("resident VRAM"));
}

#[test]
fn concurrent_up_invocations_for_same_spec_fail_fast_on_lock() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: lock-demo
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: ["echo", "ok"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let sbatch = tmpdir.path().join("sbatch-slow");
    // Genuine wall-clock wait: the first `up` must still hold the submission
    // lock while the second `up` races for it, so sbatch stalls long enough for
    // the concurrent attempt below to observe the lock as held.
    write_script(
        &sbatch,
        "#!/bin/bash\nset -euo pipefail\nsleep 2\necho 'Submitted batch job 12345'\n",
    );
    let first = Command::new(bin_path())
        .current_dir(tmpdir.path())
        .args([
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn first up");
    let lock_dir = tmpdir.path().join(".hpc-compose/locks");
    for _ in 0..50 {
        if lock_dir
            .read_dir()
            .ok()
            .and_then(|mut entries| entries.next())
            .is_some()
        {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let second = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&second);
    assert!(stderr_text(&second).contains("another hpc-compose up appears to be running"));
    let first_output = first.wait_with_output().expect("first output");
    assert_success(&first_output);
}

#[test]
fn submit_passes_burst_buffer_directives_to_sbatch_script() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r##"
x-slurm:
  cache_dir: {}
  burst_buffer:
    directives:
      - "#BB create_persistent name=data capacity=100G"
      - "#DW jobdw capacity=10GB access_mode=striped type=scratch"
services:
  app:
    image: {}
    command: /bin/true
"##,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = tmpdir.path().join("sbatch-burst-buffer");
    write_script(
        &sbatch,
        r#"#!/bin/bash
set -euo pipefail
script_path="${!#}"
grep -F '#BB create_persistent name=data capacity=100G' "$script_path" >/dev/null
grep -F '#DW jobdw capacity=10GB access_mode=striped type=scratch' "$script_path" >/dev/null
echo "Submitted batch job 12345"
"#,
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let out = stdout_text(&submit);
    assert!(out.contains("Submitted batch job 12345"));
    // The human summary box surfaces parameterized next-step hints, with `pull`
    // suggested before the destructive `down` so results are collected first.
    assert!(out.contains("Next:"), "next-step hints shown: {out}");
    assert!(
        out.contains("hpc-compose pull --job-id 12345"),
        "pull hint parameterized with the job id: {out}"
    );
}

#[test]
fn submit_runtime_stages_through_shared_scratch_and_cleans_up_on_success() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let input = tmpdir.path().join("input.txt");
    fs::write(&input, "payload-from-stage-in\n").expect("input");
    let scratch_base = tmpdir.path().join("scratch");
    let stage_out = tmpdir.path().join("outputs/result.txt");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
  scratch:
    scope: shared
    base: {}
    mount: /scratch
    cleanup: on_success
  stage_in:
    - from: {}
      to: /scratch/output/result.txt
      mode: copy
  stage_out:
    - from: /scratch/output/result.txt
      to: {}
      when: always
      mode: copy
services:
  app:
    image: {}
    command: /bin/true
"#,
            cache_dir.display(),
            scratch_base.display(),
            input.display(),
            stage_out.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert_eq!(
        fs::read_to_string(&stage_out).expect("stage out"),
        "payload-from-stage-in\n"
    );
    assert!(!scratch_base.join("12345").exists());
}

#[test]
fn up_renders_cluster_side_huggingface_stage_in_step() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
  stage_in:
    - to: /models/llama
      hf:
        repo: meta-llama/Llama-3.1-8B
        revision: 0e9e39f249a16976918f6564b8830bc894c89659
        kind: model
services:
  app:
    image: {}
    command: /bin/true
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--huggingface-cli-bin",
            "/opt/hf/huggingface-cli",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let script = fs::read_to_string(&script_out).expect("rendered script");
    // The download runs inside the allocation with the overridden CLI, pinned
    // revision, into a temp dir that is atomically renamed into the CAS path.
    assert!(
        script.contains(
            "'/opt/hf/huggingface-cli' download 'meta-llama/Llama-3.1-8B' --revision '0e9e39f249a16976918f6564b8830bc894c89659' --local-dir \"$hf_tmp\""
        ),
        "expected guarded cluster-side huggingface-cli download; got:\n{script}"
    );
    assert!(
        script.contains("mv \"$hf_tmp\" \"$HF_STAGE_TARGET\""),
        "atomic rename into the CAS dir; got:\n{script}"
    );
    assert!(
        script.contains(&format!("{}/models/", cache_dir.display())),
        "downloads into the content-addressed cache path"
    );
    assert!(script.contains("stage_in_huggingface_artifacts"));
    assert!(script.contains(".hpc-compose-hf-complete"));
    // Never mounts the hf:// URI and never inlines the token.
    assert!(!script.contains("hf://"), "no literal hf:// in the script");
    assert!(
        !script.contains("HF_TOKEN"),
        "HF_TOKEN must never be inlined"
    );
}

#[test]
fn status_reports_malformed_submission_record_without_panicking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let latest = latest_record_path_for(&compose);
    fs::create_dir_all(latest.parent().expect("metadata dir")).expect("metadata dir");
    fs::write(&latest, "{ definitely not valid json\n").expect("malformed record");

    let status = run_cli(
        tmpdir.path(),
        &["status", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&status);
    let stderr = stderr_text(&status);
    assert!(stderr.contains("failed to parse"));
    assert!(!stderr.to_lowercase().contains("panicked"));
}

#[test]
fn submit_runtime_loads_top_level_and_service_modules() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
x-env:
  modules:
    purge: true
    load:
      - gcc/13
services:
  app:
    image: {}
    command: /bin/true
    x-env:
      modules:
        load:
          - cuda/12.4
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let module_log = tmpdir.path().join("module.log");
    let _module = write_fake_module(tmpdir.path(), &module_log);
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let calls = fs::read_to_string(module_log).expect("module log");
    assert!(calls.contains("purge"));
    assert!(calls.contains("load gcc/13"));
    assert!(calls.contains("load cuda/12.4"));
}

#[test]
fn up_command_submits_watches_and_propagates_terminal_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &squeue_state,
        &sacct_state,
        "COMPLETED",
        "ready",
        0,
    );
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let up = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&up);
    let stdout = stdout_text(&up);
    assert!(stdout.contains("Submitted batch job 12345"));
    assert!(stdout.contains("watching job 12345"));
    assert!(stdout.contains("[app] ready"));
    assert!(stdout.contains("COMPLETED"));
    assert!(tmpdir.path().join(".hpc-compose/latest.json").exists());

    fs::write(&squeue_state, "NONE\n").expect("reset squeue state");
    fs::write(&sacct_state, "NONE\n").expect("reset sacct state");
    let failed_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &squeue_state,
        &sacct_state,
        "FAILED",
        "boom",
        0,
    );
    let failed = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            failed_sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    let failed_text = format!("{}{}", stdout_text(&failed), stderr_text(&failed));
    assert!(failed_text.contains("[app] boom"));
    assert!(failed_text.contains("FAILED"));
}

#[test]
fn submit_skip_prepare_reuses_existing_artifact() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(
        !stdout_text(&submit).contains("BUILD") || !stdout_text(&submit).contains("service 'app'")
    );
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
}

fn submit_sif_backend_runs_prepare_render_submit_with_fake_runtime(backend: &str) {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
runtime:
  backend: {backend}
x-slurm:
  cache_dir: {}
services:
  app:
    image: docker://alpine:3.19
    command: /bin/true
    x-runtime:
      prepare:
        commands:
          - echo prepared
"#,
            cache_dir.display()
        ),
    );
    let runtime_log = tmpdir.path().join(format!("{backend}.log"));
    let runtime = write_fake_sif_runtime(tmpdir.path(), backend, &runtime_log);
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join(format!("{backend}.sbatch"));

    let mut args = vec![
        "up",
        "--detach",
        "-f",
        compose.to_str().expect("path"),
        "--no-preflight",
        "--sbatch-bin",
        sbatch.to_str().expect("path"),
        "--script-out",
        script_out.to_str().expect("path"),
    ];
    match backend {
        "apptainer" => {
            args.push("--apptainer-bin");
            args.push(runtime.to_str().expect("path"));
        }
        "singularity" => {
            args.push("--singularity-bin");
            args.push(runtime.to_str().expect("path"));
        }
        _ => unreachable!("unexpected backend"),
    }

    let submit = run_cli(tmpdir.path(), &args);
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
    let runtime_calls = fs::read_to_string(&runtime_log).expect("runtime log");
    assert!(runtime_calls.contains("build --force"));
    assert!(runtime_calls.contains("exec --writable"));
    assert!(script_out.exists());
    let rendered = fs::read_to_string(&script_out).expect("rendered script");
    assert!(rendered.contains(runtime.to_str().expect("path")));
    assert!(rendered.contains(&format!(
        "local -a runtime_cmd=('{}' 'exec')",
        runtime.display()
    )));
    assert!(tmpdir.path().join(".hpc-compose/latest.json").exists());
}

#[test]
fn submit_apptainer_backend_runs_prepare_render_submit_with_fake_runtime() {
    submit_sif_backend_runs_prepare_render_submit_with_fake_runtime("apptainer");
}

#[test]
fn submit_singularity_backend_runs_prepare_render_submit_with_fake_runtime() {
    submit_sif_backend_runs_prepare_render_submit_with_fake_runtime("singularity");
}

#[test]
fn submit_restart_on_failure_restarts_once_and_status_reports_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
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
    let app = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "app")
        .expect("app service");
    assert_eq!(app["failure_policy_mode"], "restart_on_failure");
    assert_eq!(app["restart_count"], 1);
    assert_eq!(app["max_restarts"], 3);
    assert_eq!(app["window_seconds"], 60);
    assert_eq!(app["max_restarts_in_window"], 3);
    assert_eq!(app["restart_failures_in_window"], 1);
    assert_eq!(app["last_exit_code"], 0);
}

#[test]
fn submit_ignore_policy_allows_job_success_with_failed_sidecar() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  main:
    image: {}
    command: /bin/true
  sidecar:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: ignore
"#,
            local_image.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
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
    let sidecar = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "sidecar")
        .expect("sidecar service");
    assert_eq!(sidecar["failure_policy_mode"], "ignore");
    assert_eq!(sidecar["last_exit_code"], 42);
}

#[test]
fn submit_restart_on_failure_exhausted_retries_fails_job() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  flaky:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 1
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let combined = format!("{}\n{}", stdout_text(&submit), stderr_text(&submit));
    assert!(combined.contains("after 1/1 restarts"));
}

#[test]
fn submit_restart_on_failure_window_limit_blocks_crash_loop() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  loopy:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 5
        backoff_seconds: 1
        window_seconds: 60
        max_restarts_in_window: 2
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy_plan(
        tmpdir.path(),
        "hpc-compose:loopy",
        &[(41, 0), (41, 0), (41, 0)],
    );
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let combined = format!("{}\n{}", stdout_text(&submit), stderr_text(&submit));
    assert!(
        combined.contains("2/2 restart-triggering exits"),
        "combined:\n{combined}"
    );
}

#[test]
fn submit_restart_on_failure_window_ages_out_failures() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  spaced:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 5
        backoff_seconds: 1
        window_seconds: 2
        max_restarts_in_window: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy_plan(
        tmpdir.path(),
        "hpc-compose:spaced",
        &[(51, 0), (52, 2), (0, 2)],
    );
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
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
    let spaced = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "spaced")
        .expect("spaced service");
    assert_eq!(spaced["restart_count"], 2);
    assert_eq!(spaced["max_restarts"], 5);
    assert_eq!(spaced["window_seconds"], 2);
    assert_eq!(spaced["max_restarts_in_window"], 1);
    assert_eq!(spaced["restart_failures_in_window"], 0);
    assert_eq!(spaced["last_exit_code"], 0);
}

#[test]
fn submit_succeeds_when_tracking_metadata_cannot_be_written() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_root = tmpdir.path().join("readonly-compose");
    fs::create_dir_all(&compose_root).expect("compose root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&compose_root, &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let mut perms = fs::metadata(&compose_root).expect("meta").permissions();
    perms.set_mode(0o555);
    fs::set_permissions(&compose_root, perms).expect("chmod readonly");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    let mut restore = fs::metadata(&compose_root).expect("meta").permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&compose_root, restore).expect("chmod restore");

    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
    assert!(stdout_text(&submit).contains("tracking metadata could not be written"));
    assert!(
        stderr_text(&submit)
            .contains("warning: job submitted, but failed to write tracking metadata")
    );
    assert!(!compose_root.join(".hpc-compose/latest.json").exists());
}

#[test]
fn status_and_logs_commands_use_submission_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let metadata = tmpdir.path().join(".hpc-compose/latest.json");
    assert!(metadata.exists());

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "alpha\nbeta\n").expect("log");
    let batch_log = tmpdir
        .path()
        .join(".hpc-compose/logs/hpc-compose-12345.out");
    fs::create_dir_all(batch_log.parent().expect("batch log dir")).expect("batch log dir");
    fs::write(&batch_log, "batch-line\n").expect("batch log");
    let record = load_submission_record(&compose, Some("12345")).expect("record");
    let state_path = state_path_for_record(&record);
    fs::create_dir_all(state_path.parent().expect("state parent")).expect("state dir");
    fs::write(&state_path, r#"{"job_status":"COMPLETED","services":[]}"#).expect("state json");

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let status_stdout = stdout_text(&status);
    assert!(status_stdout.contains("job id: 12345"));
    assert!(status_stdout.contains("Scheduler:"));
    assert!(status_stdout.contains("  state: COMPLETED (sacct)"));
    assert!(status_stdout.contains("Runtime:"));
    assert!(status_stdout.contains("  compose file:"));
    assert!(status_stdout.contains("  batch log:"));
    assert!(status_stdout.contains("  log  service 'app':"));
    assert!(!status_stdout.contains("pending reason:"));
    assert!(!status_stdout.contains("eligible time:"));
    assert!(!status_stdout.contains("start time:"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_json);
    let value: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status json");
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    assert_eq!(value["scheduler"]["state"], Value::from("COMPLETED"));
    assert!(value.get("verification").is_none());
    assert!(value.get("queue_diagnostics").is_none());
    let batch_log_value = value["record"]["batch_log"].as_str().unwrap_or_default();
    assert!(batch_log_value.contains("/.hpc-compose/logs/"));
    assert!(batch_log_value.ends_with("hpc-compose-12345.out"));

    let status_verify_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--verify",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_verify_json);
    let value: Value =
        serde_json::from_str(&stdout_text(&status_verify_json)).expect("status verify json");
    assert_eq!(value["verification"]["ok"], Value::from(true));
    assert_eq!(value["verification"]["errors"], Value::from(0));
    assert_eq!(value["verification"]["warnings"], Value::from(0));
    assert!(
        value["verification"]["checks"]
            .as_array()
            .expect("checks")
            .iter()
            .any(|check| check["id"] == "state-json-health" && check["status"] == "passed")
    );

    let logs = run_cli(
        tmpdir.path(),
        &[
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--lines",
            "1",
        ],
    );
    assert_success(&logs);
    assert!(stdout_text(&logs).contains("[app] beta"));
}

#[test]
fn logs_command_filters_by_grep_and_coarse_since() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
services:
  api:
    image: python:3.11-slim
    command: python -c 'print("api")'
  worker:
    image: python:3.11-slim
    command: python -c 'print("worker")'
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    fs::write(
        log_dir.join(log_file_name_for_service("api")),
        "api ok\napi error\n",
    )
    .expect("api log");
    // Genuine wall-clock wait: `--since 1s` filters on file mtime (whole-second
    // resolution), so the api log must be aged >1s before the worker log is
    // written for the `--since` assertion below to distinguish them.
    thread::sleep(Duration::from_millis(2200));
    fs::write(
        log_dir.join(log_file_name_for_service("worker")),
        "worker error\nworker ok\n",
    )
    .expect("worker log");

    let grep = run_cli(
        tmpdir.path(),
        &[
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--grep",
            "error",
            "--lines",
            "10",
        ],
    );
    assert_success(&grep);
    let grep_stdout = stdout_text(&grep);
    assert!(grep_stdout.contains("[api] api error"));
    assert!(grep_stdout.contains("[worker] worker error"));
    assert!(!grep_stdout.contains("api ok"));
    assert!(!grep_stdout.contains("worker ok"));

    let since = run_cli(
        tmpdir.path(),
        &[
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--since",
            "1s",
            "--lines",
            "10",
        ],
    );
    assert_success(&since);
    let since_stdout = stdout_text(&since);
    assert!(!since_stdout.contains("api error"));
    assert!(since_stdout.contains("worker error"));

    let invalid = run_cli(
        tmpdir.path(),
        &["logs", "-f", compose.to_str().expect("path"), "--grep", "["],
    );
    assert_failure(&invalid);
    assert!(stderr_text(&invalid).contains("invalid --grep pattern"));
}

#[test]
fn status_uses_expected_scheduler_query_arguments() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue_log = tmpdir.path().join("squeue.argv");
    let sacct_log = tmpdir.path().join("sacct.argv");
    let squeue = write_fake_squeue_with_argv_log(tmpdir.path(), &squeue_state, &squeue_log);
    let sacct = write_fake_sacct_with_argv_log(tmpdir.path(), &sacct_state, &sacct_log);

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);

    let squeue_argv = fs::read_to_string(squeue_log).expect("squeue argv");
    assert!(squeue_argv.contains("12345"));
    assert!(squeue_argv.contains("--format") || squeue_argv.contains("-o"));
    assert!(squeue_argv.contains("%T|%r|%S") || squeue_argv.contains("%T"));
    let sacct_argv = fs::read_to_string(sacct_log).expect("sacct argv");
    assert!(sacct_argv.contains("12345"));
    assert!(sacct_argv.contains("--format"));
    assert!(sacct_argv.contains("State"));
}

#[test]
fn status_reports_pending_queue_diagnostics_in_text_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("pending-squeue.state");
    let sacct_state = tmpdir.path().join("pending-sacct.state");
    fs::write(
        &squeue_state,
        "STATE=PENDING\nREASON=Priority\nSTART=2026-04-07T12:34:56\n",
    )
    .expect("squeue state");
    fs::write(
        &sacct_state,
        "STATE=PENDING\nELIGIBLE=2026-04-07T10:00:00\nSTART=Unknown\nREASON=Priority\n",
    )
    .expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let status_stdout = stdout_text(&status);
    assert!(status_stdout.contains("  state: PENDING (squeue)"));
    assert!(status_stdout.contains("  pending reason: Priority"));
    assert!(status_stdout.contains("  eligible time: 2026-04-07T10:00:00"));
    assert!(status_stdout.contains("  start time: 2026-04-07T12:34:56"));
    assert!(status_stdout.contains("Runtime:"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
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
    assert_success(&status_json);
    let value: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status json");
    assert_eq!(value["scheduler"]["state"], Value::from("PENDING"));
    assert_eq!(
        value["queue_diagnostics"]["pending_reason"],
        Value::from("Priority")
    );
    assert_eq!(
        value["queue_diagnostics"]["eligible_time"],
        Value::from("2026-04-07T10:00:00")
    );
    assert_eq!(
        value["queue_diagnostics"]["start_time"],
        Value::from("2026-04-07T12:34:56")
    );
    assert_eq!(value["record"]["job_id"], "12345");

    let status_verify_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--verify",
            "--format",
            "json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_verify_json);
    let value: Value =
        serde_json::from_str(&stdout_text(&status_verify_json)).expect("status verify json");
    assert_eq!(value["verification"]["ok"], Value::from(true));
    assert_eq!(value["verification"]["warnings"], Value::from(0));
    let checks = value["verification"]["checks"].as_array().expect("checks");
    for check_id in ["state-json-health", "checkpoint-history", "log-presence"] {
        assert!(
            checks
                .iter()
                .any(|check| check["id"] == check_id && check["status"] == "skipped"),
            "expected {check_id} to be skipped: {checks:#?}"
        );
    }
}

#[test]
fn submit_cancel_and_watch_conflict_support_json_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    assert_eq!(submit_json["backend"], Value::from("slurm"));
    assert_eq!(submit_json["launched"], Value::from(false));
    assert_eq!(submit_json["submitted"], Value::from(true));
    assert_eq!(submit_json["job_id"], Value::from("12345"));
    assert_eq!(submit_json["tracking_persisted"], Value::from(true));
    assert!(
        submit_json["tracked_metadata_path"]
            .as_str()
            .unwrap_or_default()
            .ends_with(".hpc-compose/latest.json")
    );

    let conflict = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&conflict);
    assert!(stderr_text(&conflict).contains("<--detach|--dry-run>"));

    let cancel = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--format",
            "json",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel);
    let cancel_json: Value = serde_json::from_str(&stdout_text(&cancel)).expect("cancel json");
    assert_eq!(cancel_json["job_id"], Value::from("12345"));
    assert_eq!(cancel_json["cancelled"], Value::from(true));
    assert_eq!(
        fs::read_to_string(&scancel_log)
            .expect("scancel log")
            .trim(),
        "12345"
    );
}

#[test]
fn ps_command_reports_service_runtime_state_in_text_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("ps-squeue.state");
    let sacct_state = tmpdir.path().join("ps-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "booting\nready\n").expect("log");
    fs::write(
        tmpdir.path().join(".hpc-compose/12345/state.json"),
        format!(
            r#"{{
  "services": [
    {{
      "service_name": "app",
      "step_name": "hpc-compose:app",
      "log_path": "{}",
      "launch_index": 0,
      "launcher_pid": 4242,
      "healthy": true,
      "readiness_configured": true,
      "failure_policy_mode": "restart_on_failure",
      "restart_count": 1,
      "max_restarts": 3
    }}
  ]
}}"#,
            log_path.display()
        ),
    )
    .expect("state");

    let ps = run_cli(
        tmpdir.path(),
        &[
            "ps",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&ps);
    let ps_stdout = stdout_text(&ps);
    assert!(ps_stdout.contains("service"));
    assert!(ps_stdout.contains("step"));
    assert!(ps_stdout.contains("pid"));
    assert!(ps_stdout.contains("ready"));
    assert!(ps_stdout.contains("status"));
    assert!(ps_stdout.contains("restarts"));
    assert!(ps_stdout.contains("last_exit"));
    assert!(ps_stdout.contains("log"));
    assert!(ps_stdout.contains("app"));
    assert!(ps_stdout.contains("hpc-compose:app"));
    assert!(ps_stdout.contains("ready"));

    let ps_json = run_cli(
        tmpdir.path(),
        &[
            "ps",
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
    assert_success(&ps_json);
    let value: Value = serde_json::from_str(&stdout_text(&ps_json)).expect("ps json");
    let app = value["services"].as_array().expect("services")[0].clone();
    assert_eq!(app["step_name"], Value::from("hpc-compose:app"));
    assert_eq!(app["launcher_pid"], Value::from(4242));
    assert_eq!(app["healthy"], Value::from(true));
    assert_eq!(app["restart_count"], Value::from(1));
    assert_eq!(app["status"], Value::from("ready"));
}

#[test]
fn watch_command_falls_back_to_line_mode_on_non_tty() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let squeue_state = tmpdir.path().join("watch-command-squeue.state");
    let sacct_state = tmpdir.path().join("watch-command-sacct.state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &squeue_state,
        &sacct_state,
        "COMPLETED",
        "ready",
        1,
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let watch = run_cli(
        tmpdir.path(),
        &[
            "watch",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--lines",
            "1",
            "--watch-mode",
            "line",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&watch);
    let stdout = stdout_text(&watch);
    assert!(stdout.contains("watching job 12345"));
    assert!(stdout.contains("[app] ready"));
    assert!(stdout.contains("scheduler state: COMPLETED (sacct)"));
}

fn write_replay_fixture(tmpdir: &tempfile::TempDir) -> (std::path::PathBuf, SubmissionRecord) {
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let mut record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("replay.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    record.submitted_at = 100;
    write_submission_record(&record).expect("write record");
    let job_root = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(job_root.join("logs")).expect("logs");
    fs::write(job_root.join("logs/app.log"), "booting\nboom\n").expect("log");
    fs::write(
        job_root.join("state.json"),
        r#"{"services":[{"service_name":"app","started_at":101,"finished_at":120,"last_exit_code":7,"step_name":"hpc-compose:app"}]}"#,
    )
    .expect("state");
    fs::create_dir_all(job_root.join("service-exits")).expect("service exits");
    fs::write(
        job_root.join("service-exits/app.jsonl"),
        "{\"service\":\"app\",\"exit_code\":7,\"at_unix\":120,\"node\":\"n1\"}\n",
    )
    .expect("exit marker");
    fs::create_dir_all(job_root.join("metrics")).expect("metrics");
    fs::write(
        job_root.join("metrics/gpu.jsonl"),
        "{\"sampled_at\":\"1970-01-01T00:01:50Z\",\"utilization_gpu\":\"90\",\"memory_used_mib\":\"4\",\"memory_total_mib\":\"8\"}\n",
    )
    .expect("gpu metrics");
    fs::write(
        job_root.join("metrics/slurm.jsonl"),
        "{\"sampled_at\":\"1970-01-01T00:01:50Z\",\"step_id\":\"12345.0\"}\n",
    )
    .expect("slurm metrics");
    (compose, record)
}

#[test]
fn replay_command_reports_json_timeline_and_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["fidelity"], Value::from("best-effort"));
    assert!(
        value["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|event| event["kind"] == "service_exit" && event["exit_code"] == 7)
    );
    assert!(
        value["frames"]
            .as_array()
            .expect("frames")
            .iter()
            .any(|frame| frame["metrics_line"]
                .as_str()
                .is_some_and(|line| line.contains("gpu: 1")))
    );
    assert!(
        value["artifacts"]["metrics_dirs"][0]
            .as_str()
            .unwrap_or_default()
            .ends_with("/.hpc-compose/12345/metrics")
    );
}

#[test]
fn replay_line_mode_prints_stable_summary() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--watch-mode",
            "line",
        ],
    );
    assert_success(&replay);
    let stdout = stdout_text(&replay);
    assert!(stdout.contains("hpc-compose replay | job 12345 | best-effort"));
    assert!(stdout.contains("service_exit service=app exit=7"));
    assert!(stdout.contains("metrics: gpu: 1"));
}

#[test]
fn replay_explicit_tui_mode_fails_when_ui_cannot_start() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--watch-mode",
            "tui",
        ],
    );
    assert_failure(&replay);
    assert!(stdout_text(&replay).trim().is_empty());
    assert!(stderr_text(&replay).contains("--watch-mode tui"));
}

#[test]
fn replay_missing_metrics_succeeds_with_fidelity_note() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    fs::remove_dir_all(tmpdir.path().join(".hpc-compose/12345/metrics")).expect("remove metrics");

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .is_some_and(|note| note.contains("no metrics directory")))
    );
}

#[test]
fn replay_missing_state_still_uses_exit_markers() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    fs::remove_file(tmpdir.path().join(".hpc-compose/12345/state.json")).expect("remove state");

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    assert!(
        value["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|event| event["kind"] == "service_exit")
    );
}

#[test]
fn replay_rejects_missing_service_and_invalid_speed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);

    let missing = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "missing",
            "--format",
            "json",
        ],
    );
    assert_failure(&missing);
    assert!(stderr_text(&missing).contains("service 'missing' does not exist"));

    for speed in ["0", "-1", "NaN"] {
        let invalid = run_cli(
            tmpdir.path(),
            &[
                "replay",
                "-f",
                compose.to_str().expect("path"),
                "--speed",
                speed,
                "--watch-mode",
                "line",
            ],
        );
        assert_failure(&invalid);
        assert!(stderr_text(&invalid).contains("positive finite"));
    }
}

#[test]
fn replay_resume_attempts_are_reported_in_attempt_order() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    let job_root = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(job_root.join("attempts/1/service-exits")).expect("attempt 1");
    fs::create_dir_all(job_root.join("attempts/2/service-exits")).expect("attempt 2");
    fs::write(
        job_root.join("attempts/1/state.json"),
        r#"{"attempt":1,"services":[{"service_name":"app","started_at":101,"finished_at":110,"last_exit_code":41}]}"#,
    )
    .expect("attempt 1 state");
    fs::write(
        job_root.join("attempts/2/state.json"),
        r#"{"attempt":2,"services":[{"service_name":"app","started_at":201,"finished_at":220,"last_exit_code":0}]}"#,
    )
    .expect("attempt 2 state");

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    let starts = value["events"]
        .as_array()
        .expect("events")
        .iter()
        .filter(|event| event["kind"] == "attempt_start")
        .map(|event| event["attempt"].as_u64().expect("attempt"))
        .collect::<Vec<_>>();
    assert_eq!(starts, vec![1, 2]);
}

#[test]
fn checkpoints_command_reports_attempt_and_requeue_history_in_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    let job_root = tmpdir.path().join(".hpc-compose/12345");
    for (attempt, started, finished, exit) in [(0u32, 100u64, 110u64, 41i32), (1, 200, 230, 0)] {
        fs::create_dir_all(job_root.join(format!("attempts/{attempt}"))).expect("attempt dir");
        fs::write(
            job_root.join(format!("attempts/{attempt}/state.json")),
            format!(
                r#"{{"attempt":{attempt},"is_resume":{is_resume},"job_exit_code":{exit},"services":[{{"service_name":"app","started_at":{started},"finished_at":{finished},"last_exit_code":{exit},"restart_count":0}}]}}"#,
                is_resume = attempt > 0
            ),
        )
        .expect("attempt state");
    }

    let checkpoints = run_cli(
        tmpdir.path(),
        &[
            "checkpoints",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&checkpoints);
    let value: Value = serde_json::from_str(&stdout_text(&checkpoints)).expect("checkpoints json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["attempts"], Value::from(2));
    assert_eq!(value["requeues"], Value::from(1));
    assert_eq!(value["current_attempt"], Value::from(1));
    assert_eq!(value["resume_configured"], Value::from(true));
    assert_eq!(value["is_resume"], Value::from(true));
    let entries = value["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["attempt"], Value::from(0));
    assert_eq!(entries[0]["duration_seconds"], Value::from(10));
    assert_eq!(entries[1]["attempt"], Value::from(1));
    assert_eq!(entries[1]["job_exit_code"], Value::from(0));
    assert!(value["degraded"].as_array().expect("degraded").is_empty());
}

#[test]
fn checkpoints_command_degrades_gracefully_without_attempts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    // write_replay_fixture leaves a single top-level state.json and no attempts/
    // directory; checkpoints should report a single attempt and zero requeues.
    let checkpoints = run_cli(
        tmpdir.path(),
        &[
            "checkpoints",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&checkpoints);
    let value: Value = serde_json::from_str(&stdout_text(&checkpoints)).expect("checkpoints json");
    assert_eq!(value["attempts"], Value::from(1));
    assert_eq!(value["requeues"], Value::from(0));
    assert_eq!(value["current_attempt"], Value::Null);
    assert_eq!(value["resume_configured"], Value::from(false));
    assert_eq!(value["entries"].as_array().expect("entries").len(), 1);

    // Text output stays coherent too.
    let text = run_cli(
        tmpdir.path(),
        &["checkpoints", "-f", compose.to_str().expect("path")],
    );
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains("hpc-compose checkpoints | job 12345"));
    assert!(stdout.contains("attempts: 1 | requeues: 0"));
}

#[test]
fn replay_service_filter_excludes_other_service_events_and_frames() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, mut record) = write_replay_fixture(&tmpdir);
    let job_root = tmpdir.path().join(".hpc-compose/12345");
    let sidecar_log = job_root.join("logs/sidecar.log");
    fs::write(&sidecar_log, "sidecar boot\n").expect("sidecar log");
    record.service_logs.insert("sidecar".into(), sidecar_log);
    write_submission_record(&record).expect("rewrite record");
    fs::write(
        job_root.join("state.json"),
        r#"{"services":[{"service_name":"app","started_at":101,"finished_at":120,"last_exit_code":7,"step_name":"hpc-compose:app"},{"service_name":"sidecar","started_at":102,"finished_at":130,"last_exit_code":0,"step_name":"hpc-compose:sidecar"}]}"#,
    )
    .expect("state");
    fs::write(
        job_root.join("service-exits/sidecar.jsonl"),
        "{\"service\":\"sidecar\",\"exit_code\":0,\"at_unix\":130,\"node\":\"n1\"}\n",
    )
    .expect("sidecar exit marker");

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    assert!(
        value["events"]
            .as_array()
            .expect("events")
            .iter()
            .all(|event| event["service"].as_str() != Some("sidecar"))
    );
    assert!(
        value["frames"]
            .as_array()
            .expect("frames")
            .iter()
            .all(|frame| frame["services"]
                .as_array()
                .expect("services")
                .iter()
                .all(|service| service["service_name"].as_str() == Some("app")))
    );
}

#[test]
fn replay_malformed_metric_rows_report_notes_without_losing_exit_timeline() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let (compose, _record) = write_replay_fixture(&tmpdir);
    let job_root = tmpdir.path().join(".hpc-compose/12345");
    fs::write(
        job_root.join("metrics/gpu.jsonl"),
        "not json\n{\"sampled_at\":\"not-a-time\",\"utilization_gpu\":\"20\"}\n{\"sampled_at\":\"1970-01-01T00:02:00Z\",\"utilization_gpu\":\"70\",\"memory_used_mib\":\"2\",\"memory_total_mib\":\"4\"}\n",
    )
    .expect("gpu metrics");

    let replay = run_cli(
        tmpdir.path(),
        &[
            "replay",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&replay);
    let value: Value = serde_json::from_str(&stdout_text(&replay)).expect("replay json");
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .is_some_and(|note| note.contains("gpu.jsonl line 1")))
    );
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note.as_str().is_some_and(
                |note| note.contains("could not parse metrics timestamp 'not-a-time'")
            ))
    );
    assert!(
        value["events"]
            .as_array()
            .expect("events")
            .iter()
            .any(|event| event["kind"] == "service_exit" && event["exit_code"] == 7)
    );
    assert!(
        value["frames"]
            .as_array()
            .expect("frames")
            .iter()
            .any(|frame| frame["metrics_line"]
                .as_str()
                .is_some_and(|line| line.contains("gpu: 1")))
    );
}

#[test]
fn submit_watch_falls_back_when_ui_initialization_fails() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let squeue_state = tmpdir.path().join("submit-watch-squeue.state");
    let sacct_state = tmpdir.path().join("submit-watch-sacct.state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &squeue_state,
        &sacct_state,
        "COMPLETED",
        "ready",
        1,
    );

    let submit = run_cli_with_env(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
        &[("HPC_COMPOSE_FORCE_WATCH_UI", "1")],
    );
    assert_success(&submit);
    let stdout = stdout_text(&submit);
    assert!(stdout.contains("watching job 12345"));
    assert!(stdout.contains("[app] ready"));
    assert!(stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(stderr_text(&submit).contains("falling back to line mode"));
}

#[test]
fn stats_command_reports_live_step_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-squeue.state");
    let sacct_state = tmpdir.path().join("stats-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.batch|1|00:00:01|1M|1M|cpu=00:00:01
12345.0|1|00:00:10|512M|1G|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
12345.extern|1|00:00:01|1M|1M|cpu=00:00:01
12345.1|2|00:00:20|256M|512M|cpu=00:00:20
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let stdout = stdout_text(&stats);
    assert!(stdout.contains("job id: 12345"));
    assert!(stdout.contains("stats source: sstat"));
    assert!(stdout.contains("step: 12345.0"));
    assert!(stdout.contains("step: 12345.1"));
    assert!(stdout.contains("gpu util: 65"));
    assert!(!stdout.contains("12345.batch"));
    assert!(!stdout.contains("12345.extern"));

    let stats_json = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_json);
    let value: Value = serde_json::from_str(&stdout_text(&stats_json)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["available"], Value::from(true));
    assert_eq!(value["source"], Value::from("sstat"));
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    let steps = value["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["gpu_util"], Value::from("65"));
    assert_eq!(steps[0]["gpu_mem"], Value::from("1024M"));
    // gpu_count is an allocation figure (sacct AllocTRES), not an sstat usage
    // field, so a sstat-sourced step leaves it unset.
    assert_eq!(steps[0]["gpu_count"], Value::Null);
}

#[test]
fn metrics_probe_command_reports_json_without_requiring_native_capabilities() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");

    let output = run_cli(
        tmpdir.path(),
        &[
            "metrics-probe",
            "--duration-seconds",
            "0",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("metrics probe json");

    assert_eq!(value["schema_version"], Value::from(1));
    assert!(value["capabilities"]["perf_event_open"]["available"].is_boolean());
    assert!(value["capabilities"]["nvml"]["available"].is_boolean());
    assert!(value["capabilities"]["tracepoints"]["selected_tracepoints"].is_array());
    assert!(value["measurements"].is_object());
    assert!(value["recommendation"].is_string());
}

#[test]
fn stats_command_reports_sacct_accounting_outputs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("accounting-squeue.state");
    let sacct_state = tmpdir.path().join("accounting-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("accounting.output");
    fs::write(
        &accounting_output,
        "\
12345|demo|COMPLETED|0:0|120|4|240|00:04:00|cpu=4,mem=8G,gres/gpu=2|cpu=4,mem=8G|512M|cpu=00:04:00,mem=512M|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:02:00
12345.0|app|COMPLETED|0:0|100|4|200|00:03:20|cpu=4,mem=8G,gres/gpu=2|cpu=4,mem=8G|1G|cpu=00:03:20,mem=1G|1|acct|normal|gpu|2026-01-01T00:00:10|2026-01-01T00:01:50
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("accounting-sstat.output");
    fs::write(&sstat_output, "").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let text = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--accounting",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&text);
    let text_stdout = stdout_text(&text);
    assert!(text_stdout.contains("accounting source: sacct"));
    assert!(text_stdout.contains("allocated cpu hours: 0.133333"));
    assert!(text_stdout.contains("allocated gpu hours: 0.066667"));
    assert!(text_stdout.contains("max rss bytes: 1073741824"));

    let json = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--accounting",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("stats json");
    assert_eq!(value["accounting"]["available"], Value::from(true));
    assert_eq!(value["accounting"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(
        value["accounting"]["summary"]["memory_basis"],
        Value::from("allocation_tres")
    );

    let csv = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--accounting",
            "--format",
            "csv",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&csv);
    let csv_stdout = stdout_text(&csv);
    assert!(csv_stdout.contains("job_id,accounting_available,accounting_reason"));
    assert!(csv_stdout.contains("\"12345\",\"true\""));

    let jsonl = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--accounting",
            "--format",
            "jsonl",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&jsonl);
    let jsonl_stdout = stdout_text(&jsonl);
    assert!(jsonl_stdout.contains("\"record_type\":\"accounting_summary\""));
    assert!(jsonl_stdout.contains("\"record_type\":\"accounting_row\""));
}

#[test]
fn stats_accounting_handles_empty_and_failed_sacct() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("accounting-empty-squeue.state");
    let sacct_state = tmpdir.path().join("accounting-empty-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let empty_accounting = tmpdir.path().join("accounting-empty.output");
    fs::write(&empty_accounting, "").expect("empty accounting");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &empty_accounting);
    let sstat_output = tmpdir.path().join("accounting-empty-sstat.output");
    fs::write(&sstat_output, "").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let empty = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--accounting",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&empty);
    let empty_json: Value = serde_json::from_str(&stdout_text(&empty)).expect("empty json");
    assert_eq!(empty_json["accounting"]["available"], Value::from(false));
    assert!(
        empty_json["accounting"]["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no accounting rows")
    );

    let failed_sacct = write_fake_sacct_failure(tmpdir.path());
    let failed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--accounting",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            failed_sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("sacct accounting query failed"));
}

#[test]
fn stats_accounting_empty_rows_keep_json_shape_stable() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("accounting-empty-shape-squeue.state");
    let sacct_state = tmpdir.path().join("accounting-empty-shape-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let accounting_output = tmpdir.path().join("accounting-empty-shape.output");
    fs::write(&accounting_output, "").expect("empty accounting");
    let sstat_output = tmpdir.path().join("accounting-empty-shape-sstat.output");
    fs::write(&sstat_output, "").expect("empty sstat");

    let output = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--accounting",
            "--format",
            "json",
            "--sstat-bin",
            write_fake_sstat(tmpdir.path(), &sstat_output)
                .to_str()
                .expect("path"),
            "--squeue-bin",
            write_fake_squeue(tmpdir.path(), &squeue_state)
                .to_str()
                .expect("path"),
            "--sacct-bin",
            write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output)
                .to_str()
                .expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("stats json");
    assert_eq!(payload["accounting"]["available"], Value::from(false));
    assert_eq!(payload["accounting"]["source"], Value::from("sacct"));
    assert_eq!(payload["accounting"]["summary"], Value::Null);
    assert_eq!(
        payload["accounting"]["rows"].as_array().map(Vec::len),
        Some(0)
    );
    assert!(
        payload["accounting"]["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no accounting rows")
    );
}

#[test]
fn stats_malformed_sacct_accounting_reports_line_number() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-bad-accounting-squeue.state");
    let sacct_state = tmpdir.path().join("stats-bad-accounting-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("stats-bad-accounting.output");
    fs::write(
        &accounting_output,
        "\
JobIDRaw|JobName|State|ExitCode|ElapsedRaw|AllocCPUS|CPUTimeRAW|TotalCPU|AllocTRES|ReqTRES|MaxRSS|TRESUsageInTot|NNodes|Account|QOS|Partition|Start|End
12345|too|short
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("stats-bad-accounting-sstat.output");
    fs::write(&sstat_output, "").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--accounting",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&stats);
    let stderr = stderr_text(&stats);
    assert!(stderr.contains("malformed sacct accounting output on line 2"));
    assert!(stderr.contains("expected 18 fields"));
}

#[test]
fn stats_command_supports_jsonl_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-jsonl-squeue.state");
    let sacct_state = tmpdir.path().join("stats-jsonl-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("stats-jsonl.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:00:10|512M|1G|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
12345.1|2|00:00:20|256M|512M|cpu=00:00:20
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let stats_jsonl = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "jsonl",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_jsonl);

    let stdout = stdout_text(&stats_jsonl);
    let records = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("jsonl record"))
        .collect::<Vec<_>>();
    let summary = records
        .iter()
        .find(|record| record["record_type"] == "summary")
        .expect("summary record");
    assert_eq!(summary["job_id"], Value::from("12345"));
    assert_eq!(summary["stats_source"], Value::from("sstat"));
    let steps = records
        .iter()
        .filter(|record| record["record_type"] == "step")
        .collect::<Vec<_>>();
    assert_eq!(steps.len(), 2, "{stdout}");
    assert_eq!(steps[0]["step"]["step_id"], Value::from("12345.0"));
    assert_eq!(steps[1]["step"]["step_id"], Value::from("12345.1"));
    assert!(
        records.iter().all(|record| matches!(
            record["record_type"].as_str(),
            Some("summary" | "note" | "step")
        )),
        "{stdout}"
    );
}

#[test]
fn stats_command_supports_csv_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-csv-squeue.state");
    let sacct_state = tmpdir.path().join("stats-csv-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("stats-csv.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:00:10|512M|1G|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let stats_csv = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "csv",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_csv);
    let stdout = stdout_text(&stats_csv);
    assert!(stdout.contains("job_id,scheduler_state,scheduler_source,stats_source"));
    assert!(stdout.contains("\"12345\",\"RUNNING\",\"squeue\",\"sstat\""));
}

#[test]
fn stats_command_prefers_sampler_metrics_when_present() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_metrics_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let squeue_state = tmpdir.path().join("sampler-squeue.state");
    let sacct_state = tmpdir.path().join("sampler-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sampler-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:00:11|512M|1G|cpu=00:00:11
",
    )
    .expect("sstat output");
    let _runtime_sstat = write_fake_sstat(tmpdir.path(), &sstat_output);
    let stats_sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let gpu_output = tmpdir.path().join("nvidia-smi-gpu.output");
    fs::write(
        &gpu_output,
        "0, GPU-aaaa, NVIDIA H100, 91, 77, 4096, 8192, 55, 220, 300\n",
    )
    .expect("gpu output");
    let gpu_processes = tmpdir.path().join("nvidia-smi-proc.output");
    fs::write(&gpu_processes, "GPU-aaaa, 4242, python, 2048\n").expect("gpu proc output");
    let _nvidia_smi = write_fake_nvidia_smi(tmpdir.path(), &gpu_output, &gpu_processes);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["source"], Value::from("sampler"));
    assert_eq!(
        value["sampler"]["gpu"]["gpus"][0]["utilization_gpu"],
        Value::from("91")
    );
    assert_eq!(
        value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
    assert_eq!(value["steps"][0]["step_id"], Value::from("12345.0"));
    assert_eq!(value["steps"][0]["ave_cpu"], Value::from("00:00:11"));
    assert!(
        value["metrics_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/.hpc-compose/12345/metrics")
    );

    let stats_explicit = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--format",
            "json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_explicit);
    let explicit_value: Value =
        serde_json::from_str(&stdout_text(&stats_explicit)).expect("explicit stats json");
    assert_eq!(explicit_value["source"], Value::from("sampler"));
    assert_eq!(explicit_value["record"]["job_id"], Value::from("12345"));
    assert_eq!(
        explicit_value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
}

#[test]
fn inspect_rightsize_reports_conservative_suggestions() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-rightsize.yaml",
        &format!(
            r#"
name: rightsize-demo
x-slurm:
  job_name: rightsize-demo
  time: "02:00:00"
  mem: 64G
  gpus: 8
  cache_dir: {}
  metrics:
    interval_seconds: 60
services:
  training:
    image: python:3.11-slim
    command: python train.py
    x-slurm:
      cpus_per_task: 8
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let metrics_dir = tmpdir.path().join(".hpc-compose/12345/metrics");
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    let mut gpu_rows = String::new();
    for sampled_at in [
        "2026-04-10T10:00:00Z",
        "2026-04-10T10:01:00Z",
        "2026-04-10T10:02:00Z",
    ] {
        for index in 0..8 {
            let active = index < 3;
            gpu_rows.push_str(&format!(
                "{{\"sampled_at\":\"{sampled_at}\",\"node\":\"node01\",\"index\":\"{index}\",\"uuid\":\"GPU-{index}\",\"utilization_gpu\":\"{}\",\"memory_used_mib\":\"{}\"}}\n",
                if active { 70 } else { 0 },
                if active { 4096 } else { 64 }
            ));
        }
    }
    fs::write(metrics_dir.join("gpu.jsonl"), gpu_rows).expect("gpu metrics");
    fs::write(
        metrics_dir.join("gpu_processes.jsonl"),
        "{\"sampled_at\":\"2026-04-10T10:02:00Z\",\"gpu_uuid\":\"GPU-2\",\"pid\":\"42\",\"process_name\":\"python\",\"used_memory_mib\":\"4096\"}\n",
    )
    .expect("gpu processes");

    let squeue_state = tmpdir.path().join("rightsize-squeue.state");
    let sacct_state = tmpdir.path().join("rightsize-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("rightsize-accounting.output");
    fs::write(
        &accounting_output,
        "\
12345|rightsize-demo|COMPLETED|0:0|3600|8|28800|08:00:00|cpu=8,mem=64G,gres/gpu=8|cpu=8,mem=64G,gres/gpu=8|12300M|cpu=08:00:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T01:00:00
12345.0|hpc-compose:training|COMPLETED|0:0|3600|8|11520|03:12:00|cpu=8,mem=64G,gres/gpu=8|cpu=8,mem=64G,gres/gpu=8|12300M|cpu=03:12:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T01:00:00
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("rightsize-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:20:00|12000M|12300M|cpu=00:20:00,gres/gpuutil=70,gres/gpumem=4096M
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let text = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "--rightsize",
            "-f",
            compose.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains("rightsize status: complete"));
    assert!(stdout.contains("consider x-slurm.mem: 16G"));
    assert!(stdout.contains("consider services.training.x-slurm.cpus_per_task: 4"));
    assert!(stdout.contains("consider x-slurm.gpus: 4"));
    assert!(stdout.contains("consider x-slurm.time: 01:15:00"));

    let json = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "--rightsize",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("rightsize json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["complete"], Value::from(true));
    let recommendations = value["recommendations"]
        .as_array()
        .expect("recommendations");
    assert!(
        recommendations
            .iter()
            .any(|item| item["target_path"] == "x-slurm.mem" && item["suggested"] == "16G")
    );
    assert!(recommendations.iter().any(|item| {
        item["target_path"] == "services.training.x-slurm.cpus_per_task" && item["suggested"] == "4"
    }));
}

#[test]
fn score_command_reports_efficiency_for_tracked_job_id_without_file() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-score.yaml",
        &format!(
            r#"
name: score-demo
x-slurm:
  job_name: score-demo
  time: "00:03:00"
  mem: 64G
  gpus: 2
  cache_dir: {}
  metrics:
    interval_seconds: 60
services:
  training:
    image: python:3.11-slim
    command: python train.py
    x-slurm:
      cpus_per_task: 4
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let metrics_dir = tmpdir.path().join(".hpc-compose/12345/metrics");
    fs::create_dir_all(&metrics_dir).expect("metrics dir");
    fs::write(
        metrics_dir.join("meta.json"),
        r#"{
  "interval_seconds": 60,
  "collectors": [
    {"name":"gpu","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-10T10:02:00Z"},
    {"name":"slurm","enabled":true,"available":true,"note":null,"last_sampled_at":"2026-04-10T10:02:00Z"}
  ]
}"#,
    )
    .expect("meta");
    let mut gpu_rows = String::new();
    for (sampled_at, util, power) in [
        ("2026-04-10T10:00:00Z", 80, 210),
        ("2026-04-10T10:01:00Z", 80, 210),
        ("2026-04-10T10:02:00Z", 0, 40),
    ] {
        for index in 0..2 {
            gpu_rows.push_str(&format!(
                "{{\"sampled_at\":\"{sampled_at}\",\"node\":\"node01\",\"index\":\"{index}\",\"uuid\":\"GPU-{index}\",\"utilization_gpu\":\"{util}\",\"memory_used_mib\":\"{}\",\"memory_total_mib\":\"40960\",\"power_draw_w\":\"{power}\",\"power_limit_w\":\"300\"}}\n",
                if util > 0 { 8192 } else { 64 }
            ));
        }
    }
    fs::write(metrics_dir.join("gpu.jsonl"), gpu_rows).expect("gpu metrics");
    fs::write(
        metrics_dir.join("gpu_processes.jsonl"),
        "{\"sampled_at\":\"2026-04-10T10:01:00Z\",\"gpu_uuid\":\"GPU-0\",\"pid\":\"42\",\"process_name\":\"python\",\"used_memory_mib\":\"8192\"}\n",
    )
    .expect("gpu processes");
    fs::write(
        metrics_dir.join("slurm.jsonl"),
        "\
{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:40\",\"ave_rss\":\"15000M\",\"max_rss\":\"16000M\",\"alloc_tres\":\"cpu=4,mem=64G,gres/gpu=2\",\"tres_usage_in_ave\":\"cpu=00:00:40,gres/gpuutil=80,gres/gpumem=8192M\"}\n\
{\"sampled_at\":\"2026-04-10T10:01:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:40\",\"ave_rss\":\"15000M\",\"max_rss\":\"16000M\",\"alloc_tres\":\"cpu=4,mem=64G,gres/gpu=2\",\"tres_usage_in_ave\":\"cpu=00:00:40,gres/gpuutil=80,gres/gpumem=8192M\"}\n\
{\"sampled_at\":\"2026-04-10T10:02:00Z\",\"step_id\":\"12345.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:00\",\"ave_rss\":\"0\",\"max_rss\":\"0\",\"alloc_tres\":\"cpu=4,mem=64G,gres/gpu=2\",\"tres_usage_in_ave\":\"cpu=00:00:00,gres/gpuutil=0,gres/gpumem=0M\"}\n",
    )
    .expect("slurm metrics");

    let squeue_state = tmpdir.path().join("score-squeue.state");
    let sacct_state = tmpdir.path().join("score-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("score-accounting.output");
    fs::write(
        &accounting_output,
        "\
12345|score-demo|COMPLETED|0:0|180|4|720|00:12:00|cpu=4,mem=64G,gres/gpu=2|cpu=4,mem=64G,gres/gpu=2|16000M|cpu=00:12:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:03:00
12345.0|hpc-compose:training|COMPLETED|0:0|180|4|480|00:08:00|cpu=4,mem=64G,gres/gpu=2|cpu=4,mem=64G,gres/gpu=2|16000M|cpu=00:08:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:03:00
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("score-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:02:00|15000M|16000M|cpu=00:02:00,gres/gpuutil=80,gres/gpumem=8192M
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let text = run_cli(
        tmpdir.path(),
        &[
            "score",
            "12345",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&text);
    let stdout = stdout_text(&text);
    assert!(stdout.contains("EFFICIENCY SCORE:"));
    assert!(stdout.contains("GPU Util"));
    assert!(stdout.contains("Energy:"));
    assert!(stdout.contains("Tip:"));

    let json = run_cli(
        tmpdir.path(),
        &[
            "score",
            "12345",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("score json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["complete"], Value::from(true));
    assert_eq!(value["energy_basis"], Value::from("sampler_power_draw+pue"));
    assert!(value["score"].as_u64().is_some_and(|score| score > 0));
    assert!(value["components"].as_array().is_some_and(|components| {
        components
            .iter()
            .any(|component| component["name"] == "gpu_utilization")
    }));
}

#[test]
fn score_command_uses_tdp_fallback_without_gpu_sampler_history() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-score-fallback.yaml",
        &format!(
            r#"
name: score-fallback
x-slurm:
  job_name: score-fallback
  time: "00:10:00"
  mem: 8G
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    command: python app.py
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let squeue_state = tmpdir.path().join("score-fallback-squeue.state");
    let sacct_state = tmpdir.path().join("score-fallback-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("score-fallback-accounting.output");
    fs::write(
        &accounting_output,
        "\
12345|score-fallback|COMPLETED|0:0|300|1|300|00:05:00|cpu=1,mem=8G|cpu=1,mem=8G|2G|cpu=00:05:00|1|acct|normal|cpu|2026-01-01T00:00:00|2026-01-01T00:05:00
12345.0|hpc-compose:app|COMPLETED|0:0|300|1|300|00:05:00|cpu=1,mem=8G|cpu=1,mem=8G|2G|cpu=00:05:00|1|acct|normal|cpu|2026-01-01T00:00:00|2026-01-01T00:05:00
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("score-fallback-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
12345.0|1|00:05:00|2G|2G|cpu=00:05:00
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let json = run_cli(
        tmpdir.path(),
        &[
            "score",
            "12345",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("score json");
    assert_eq!(value["energy_basis"], Value::from("configured_tdp+pue"));
    assert!(
        value["energy_kwh"]
            .as_f64()
            .is_some_and(|energy| energy > 0.0)
    );
    assert!(value["components"].as_array().is_some_and(|components| {
        components.iter().any(|component| {
            component["name"] == "gpu_utilization" && component["available"] == false
        })
    }));
}

#[test]
fn score_reports_low_confidence_with_accounting_only() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-score-accounting.yaml",
        &format!(
            r#"
name: accounting-only
x-slurm:
  job_name: accounting-only
  time: "00:05:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    command: python app.py
"#,
            cache_dir.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("score-accounting.sbatch"),
        &plan,
        "24680",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    let squeue_state = tmpdir.path().join("score-accounting-squeue.state");
    let sacct_state = tmpdir.path().join("score-accounting-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let accounting_output = tmpdir.path().join("score-accounting.output");
    fs::write(
        &accounting_output,
        "\
24680|accounting-only|COMPLETED|0:0|300|2|600|00:10:00|cpu=2|cpu=2||cpu=00:10:00|1|acct|normal|cpu|2026-01-01T00:00:00|2026-01-01T00:05:00
",
    )
    .expect("accounting output");
    let sacct = write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output);
    let sstat_output = tmpdir.path().join("score-accounting-sstat.output");
    fs::write(&sstat_output, "").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let score = run_cli(
        tmpdir.path(),
        &[
            "score",
            "24680",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&score);
    let value: Value = serde_json::from_str(&stdout_text(&score)).expect("score json");
    assert_eq!(value["complete"], Value::from(true));
    assert_eq!(value["confidence"], Value::from("low"));
    assert_eq!(value["sources"], serde_json::json!(["sacct"]));
    assert!(value["components"].as_array().is_some_and(|components| {
        components
            .iter()
            .filter(|component| component["available"] == true)
            .all(|component| component["name"] == "energy_budget_utilization")
    }));
}

#[test]
fn score_invalid_resource_options_fail_before_scheduler_queries() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-score-invalid.yaml",
        &format!(
            r#"
name: score-invalid
x-slurm:
  job_name: score-invalid
  time: "00:05:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    command: python app.py
"#,
            cache_dir.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("score-invalid.sbatch"),
        &plan,
        "98765",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    let tool_log = tmpdir.path().join("score-invalid-tools.log");
    let squeue = tmpdir.path().join("score-invalid-squeue");
    let sacct = tmpdir.path().join("score-invalid-sacct");
    let sstat = tmpdir.path().join("score-invalid-sstat");
    for (path, name) in [(&squeue, "squeue"), (&sacct, "sacct"), (&sstat, "sstat")] {
        write_script(
            path,
            &format!(
                "#!/bin/bash\nset -euo pipefail\nprintf '{}:%s\\n' \"$*\" >> '{}'\nexit 0\n",
                name,
                tool_log.display()
            ),
        );
    }

    for (option_args, expected) in [
        (vec!["--pue", "0"], "score --pue must be greater than 0"),
        (
            vec!["--gpu-tdp-w=-1"],
            "score --gpu-tdp-w must be non-negative",
        ),
        (
            vec!["--cpu-watts-per-core=-1"],
            "score --cpu-watts-per-core must be non-negative",
        ),
    ] {
        let _ = fs::remove_file(&tool_log);
        let mut args = vec![
            "score",
            "98765",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
        ];
        args.extend(option_args);
        let score = run_cli(tmpdir.path(), &args);
        assert_failure(&score);
        assert!(
            stderr_text(&score).contains(expected),
            "stderr did not contain {expected:?}:\n{}",
            stderr_text(&score)
        );
        assert!(!tool_log.exists(), "scheduler tools should not be queried");
    }
}

#[test]
fn rightsize_running_job_is_provisional_and_incomplete() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "compose-rightsize-running.yaml",
        &format!(
            r#"
name: rightsize-running
x-slurm:
  job_name: rightsize-running
  time: "01:00:00"
  mem: 8G
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    command: python app.py
    x-slurm:
      cpus_per_task: 4
"#,
            cache_dir.display()
        ),
    );
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("rightsize-running.sbatch"),
        &plan,
        "13579",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    let squeue_state = tmpdir.path().join("rightsize-running-squeue.state");
    let sacct_state = tmpdir.path().join("rightsize-running-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("rightsize-running-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|TRESUsageInAve
13579.0|1|00:10:00|1024M|2G|cpu=00:10:00
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let rightsize = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "--rightsize",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&rightsize);
    let value: Value = serde_json::from_str(&stdout_text(&rightsize)).expect("rightsize json");
    assert_eq!(value["job_id"], Value::from("13579"));
    assert_eq!(value["scheduler_state"], Value::from("RUNNING"));
    assert_eq!(value["complete"], Value::from(false));
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .is_some_and(|note| note.contains("provisional")))
    );
}

#[test]
fn inspect_rightsize_requires_tracked_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_metrics_compose(tmpdir.path(), &cache_dir);

    let rightsize = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "--rightsize",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&rightsize);
    assert!(stderr_text(&rightsize).contains("requires tracked submission metadata"));
}

#[test]
fn stats_command_supports_explicit_job_id_without_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-explicit-squeue.state");
    let sacct_state = tmpdir.path().join("stats-explicit-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat-explicit.output");
    fs::write(&sstat_output, "67890.0|1|00:00:02|64M|128M|cpu=00:00:02\n").expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let stats_text = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_text);
    assert_eq!(
        stdout_text(&stats_text)
            .matches("GPU accounting metrics are unavailable")
            .count(),
        1
    );

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("67890"));
    assert_eq!(value["available"], Value::from(true));
    assert!(value["record"].is_null());
}

#[test]
fn stats_command_reports_unavailable_for_pending_and_completed_jobs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sstat_output = tmpdir.path().join("sstat-empty.output");
    fs::write(&sstat_output, "").expect("empty sstat");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let pending_squeue_state = tmpdir.path().join("pending-squeue.state");
    let pending_sacct_state = tmpdir.path().join("pending-sacct.state");
    fs::write(&pending_squeue_state, "PENDING\n").expect("pending squeue");
    fs::write(&pending_sacct_state, "NONE\n").expect("pending sacct");
    let pending_squeue = write_fake_squeue(tmpdir.path(), &pending_squeue_state);
    let pending_sacct = write_fake_sacct(tmpdir.path(), &pending_sacct_state);
    let pending = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            pending_squeue.to_str().expect("path"),
            "--sacct-bin",
            pending_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&pending);
    let pending_value: Value = serde_json::from_str(&stdout_text(&pending)).expect("pending json");
    assert_eq!(pending_value["available"], Value::from(false));
    assert!(
        pending_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("not running yet")
    );

    let completed_squeue_state = tmpdir.path().join("completed-squeue.state");
    let completed_sacct_state = tmpdir.path().join("completed-sacct.state");
    fs::write(&completed_squeue_state, "NONE\n").expect("completed squeue");
    fs::write(&completed_sacct_state, "COMPLETED\n").expect("completed sacct");
    let completed_squeue = write_fake_squeue(tmpdir.path(), &completed_squeue_state);
    let completed_sacct = write_fake_sacct(tmpdir.path(), &completed_sacct_state);
    let completed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--format",
            "json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            completed_squeue.to_str().expect("path"),
            "--sacct-bin",
            completed_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&completed);
    let completed_value: Value =
        serde_json::from_str(&stdout_text(&completed)).expect("completed json");
    assert_eq!(completed_value["available"], Value::from(false));
    assert!(
        completed_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no longer running")
    );
}

#[test]
fn stats_command_surfaces_sstat_failures_and_malformed_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-fail-squeue.state");
    let sacct_state = tmpdir.path().join("stats-fail-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let failed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--sstat-bin",
            sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("sstat failed for job 42"));
    assert!(stderr_text(&failed).contains("job accounting unavailable"));

    let malformed_output = tmpdir.path().join("sstat-malformed.output");
    fs::write(&malformed_output, "12345.0|1|00:00:01\n").expect("malformed output");
    let sstat_bad = write_fake_sstat(tmpdir.path(), &malformed_output);
    let malformed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--sstat-bin",
            sstat_bad.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&malformed);
    assert!(stderr_text(&malformed).contains("malformed sstat output"));
}

#[test]
fn cancel_uses_tracked_or_explicit_job_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let cancel_latest = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_latest);
    assert!(stdout_text(&cancel_latest).contains("cancelled job: 12345"));

    let cancel_explicit = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_explicit);
    assert!(stdout_text(&cancel_explicit).contains("cancelled job: 67890"));

    let log = fs::read_to_string(scancel_log).expect("scancel log");
    assert!(log.contains("12345"));
    assert!(log.contains("67890"));
}

#[test]
fn cancel_reports_missing_record_and_scancel_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let missing = run_cli(
        tmpdir.path(),
        &["cancel", "-f", compose.to_str().expect("path"), "--yes"],
    );
    assert_failure(&missing);
    assert!(stderr_text(&missing).contains("no tracked submission metadata exists"));

    let scancel_log = tmpdir.path().join("scancel-fail.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, false);
    let failed = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("scancel failed for job 42"));
    assert!(stderr_text(&failed).contains("permission denied"));
}

#[test]
fn down_command_cancels_and_removes_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(tmpdir.path().join(".hpc-compose/latest.json").exists());
    assert!(tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());

    let down = run_cli(
        tmpdir.path(),
        &[
            "down",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--scancel-bin",
            scancel.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&down);
    let payload: Value = serde_json::from_str(&stdout_text(&down)).expect("down json");
    assert_eq!(payload["job_id"], Value::from("12345"));
    assert_eq!(payload["cancelled"], Value::from(true));
    assert_eq!(payload["tracking_removed"], Value::from(true));
    assert!(
        fs::read_to_string(scancel_log)
            .expect("scancel log")
            .contains("12345")
    );
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
    assert!(!tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());
}

#[test]
fn run_command_sanitizes_default_script_path_for_service_names() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  cache_dir: {}
services:
  "svc/name":
    image: {}
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let run = run_cli(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "svc/name",
            "--",
            "/bin/true",
        ],
    );
    assert_success(&run);
    assert!(
        tmpdir
            .path()
            .join("hpc-compose-run-svc_x2f_name.sbatch")
            .exists()
    );
    assert!(stdout_text(&run).contains(".hpc-compose/latest-run.json"));
}

#[test]
fn run_command_executes_one_off_service_and_tracks_latest_run() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: demo
services:
  db:
    image: {}
    command: /bin/true
  app:
    image: {}
    depends_on:
      - db
    command: /bin/false
"#,
            local_image.display(),
            local_image.display()
        ),
    );
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let script_out = tmpdir.path().join("run.sbatch");

    let run = run_cli(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "app",
            "--",
            "/bin/echo",
            "--",
            "one-off",
        ],
    );
    assert_success(&run);
    let stdout = stdout_text(&run);
    assert!(stdout.contains("Submitted batch job 12345"));
    assert!(stdout.contains("watching job 12345"));
    assert!(stdout.contains(".hpc-compose/latest-run.json"));

    let latest_run = tmpdir.path().join(".hpc-compose/latest-run.json");
    assert!(latest_run.exists());
    let record: Value = serde_json::from_str(&fs::read_to_string(&latest_run).expect("latest run"))
        .expect("latest run json");
    assert_eq!(record["kind"], Value::from("run"));
    assert_eq!(record["service_name"], Value::from("app"));
    assert_eq!(
        record["command_override"],
        serde_json::json!(["/bin/echo", "--", "one-off"])
    );

    let rendered = fs::read_to_string(&script_out).expect("rendered run script");
    assert!(rendered.contains("hpc-compose:app"));
    assert!(!rendered.contains("hpc-compose:db"));
    assert_eq!(rendered.matches("  register_service ").count(), 1);
}

#[test]
fn up_command_passes_job_dependencies_on_sbatch_cli() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  after_job:
    id: "12345"
    condition: afterok
  dependency: singleton
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("dependency.sbatch");

    let up = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&up);
    let sbatch_args = fs::read_to_string(&sbatch_log).expect("sbatch log");
    assert!(sbatch_args.contains("--dependency=afterok:12345,singleton"));
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(!script.contains("#SBATCH --dependency"));
}

#[test]
fn run_command_passes_scheduler_dependency_to_sbatch_cli_and_tracks_latest_run() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  after_job:
    id: "12345"
    condition: afterok
  dependency: singleton
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let script_out = tmpdir.path().join("run-dependency.sbatch");

    let run = run_cli(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "app",
            "--",
            "/bin/true",
        ],
    );
    assert_success(&run);
    let sbatch_args = fs::read_to_string(&sbatch_log).expect("sbatch log");
    assert!(sbatch_args.contains("--dependency=afterok:12345,singleton"));
    assert!(tmpdir.path().join(".hpc-compose/latest-run.json").exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
    let script = fs::read_to_string(script_out).expect("script");
    assert!(!script.contains("#SBATCH --dependency"));
}

#[test]
fn run_command_rejects_arrays_before_sbatch_and_script_write() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "array-run.yaml",
        &format!(
            r#"
x-slurm:
  array: 0-3
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("run-array.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "app",
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("x-slurm.array requires --detach"));
    assert!(!script_out.exists());
    assert!(!sbatch_log.exists());
}

#[test]
fn up_host_backend_detach_renders_direct_host_runtime() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "host.yaml",
        &format!(
            r#"
name: host-demo
runtime:
  backend: host
x-slurm:
  job_name: host-demo
  time: "00:02:00"
  cache_dir: {}
services:
  app:
    command:
      - bash
      - -lc
      - echo host-runtime
"#,
            tmpdir.path().join("cache").display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("host.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(script.contains("hpc-compose:app"));
    assert!(script.contains("'echo host-runtime'"));
    assert!(!script.contains("--container-image"));
    assert!(!script.contains("--container-env"));
    assert!(!script.contains("apptainer exec"));
    assert!(!script.contains("singularity exec"));
    assert!(
        fs::read_to_string(sbatch_log)
            .expect("sbatch log")
            .contains("host.sbatch")
    );
}

#[test]
fn up_gres_overrides_gpu_aliases_in_allocation_and_service_steps() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("trainer.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "gres.yaml",
        &format!(
            r#"
name: gres-demo
x-slurm:
  job_name: gres-demo
  time: "00:02:00"
  cache_dir: {}
  gres: gpu:h100:4
services:
  trainer:
    image: {}
    command:
      - python
      - train.py
    x-slurm:
      gres: gpu:h100:1
"#,
            tmpdir.path().join("cache").display(),
            local_image.display()
        ),
    );
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("gres.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let script = fs::read_to_string(script_out).expect("script");
    assert!(script.contains("#SBATCH --gres=gpu:h100:4"));
    assert!(!script.contains("#SBATCH --gpus=8"));
    assert!(script.contains("--gres=gpu:h100:1"));
    assert!(!script.contains("--gpus=2"));
}

fn write_fake_when_tool(tmpdir: &std::path::Path, name: &str, stdout: &str) -> std::path::PathBuf {
    let path = tmpdir.join(name);
    write_script(
        &path,
        &format!(
            "#!/bin/bash\nset -euo pipefail\ncat <<'HPC_COMPOSE_FAKE_OUT'\n{stdout}HPC_COMPOSE_FAKE_OUT\n"
        ),
    );
    path
}

fn write_fake_weather_tool(tmpdir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    let path = tmpdir.join(name);
    write_script(&path, body);
    path
}

fn write_weather_profile(tmpdir: &std::path::Path) {
    fs::create_dir_all(tmpdir.join(".hpc-compose")).expect("profile dir");
    fs::write(
        tmpdir.join(".hpc-compose/cluster.toml"),
        r#"
schema_version = 1

[site]
name = "gpu-cluster"
"#,
    )
    .expect("cluster profile");
}

#[test]
fn weather_text_output_summarizes_live_cluster_conditions() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    write_weather_profile(tmpdir.path());
    let sinfo = write_fake_weather_tool(
        tmpdir.path(),
        "sinfo-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' 'gpu|idle|1|gpu:a100:4|none' 'gpu|allocated|2|gpu:a100:4|' 'cpu|idle|2|N/A|' 'cpu|allocated|2|N/A|' 'cpu|down|1|N/A|scheduled maintenance'\n",
    );
    let squeue = write_fake_weather_tool(
        tmpdir.path(),
        "squeue-weather",
        "#!/bin/bash\nset -euo pipefail\nif [[ \" $* \" == *\" --start \"* ]]; then printf '%s\\n' 'N/A'; else printf '%s\\n' 'RUNNING|weather-user' 'PENDING|weather-user' 'PENDING|other-user'; fi\n",
    );
    let sshare = write_fake_weather_tool(
        tmpdir.path(),
        "sshare-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' 'project|weather-user|0.75'\n",
    );
    let sprio = write_fake_weather_tool(
        tmpdir.path(),
        "sprio-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' '42|62000|51000|1000|0'\n",
    );

    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "weather",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sshare-bin",
            sshare.to_str().expect("path"),
            "--sprio-bin",
            sprio.to_str().expect("path"),
        ],
        &[("USER", "weather-user")],
    );

    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("CLUSTER WEATHER: gpu-cluster"));
    assert!(stdout.contains("Condition: Partly Busy"));
    assert!(stdout.contains("GPU nodes: 1/3 free"));
    assert!(stdout.contains("Your jobs: 1 running, 1 pending"));
    assert!(stdout.contains("Fairshare: account=project value=0.750"));
    assert!(stdout.contains("scheduled maintenance"));
}

#[test]
fn weather_json_output_exposes_stable_machine_fields() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let sinfo = write_fake_weather_tool(
        tmpdir.path(),
        "sinfo-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' 'gpu|idle|1|gpu:h100:8|' 'gpu|allocated|1|gpu:h100:8|' 'cpu|idle|2|N/A|'\n",
    );
    let squeue = write_fake_weather_tool(
        tmpdir.path(),
        "squeue-weather",
        "#!/bin/bash\nset -euo pipefail\nif [[ \" $* \" == *\" --start \"* ]]; then printf '%s\\n' 'N/A'; else printf '%s\\n' 'RUNNING|weather-user' 'PENDING|weather-user'; fi\n",
    );
    let sshare = write_fake_weather_tool(
        tmpdir.path(),
        "sshare-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' 'project|weather-user|0.5'\n",
    );
    let sprio = write_fake_weather_tool(
        tmpdir.path(),
        "sprio-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' '99|50000|40000|1000|0'\n",
    );

    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "weather",
            "--format",
            "json",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sshare-bin",
            sshare.to_str().expect("path"),
            "--sprio-bin",
            sprio.to_str().expect("path"),
        ],
        &[
            ("USER", "weather-user"),
            ("SLURM_CLUSTER_NAME", "env-cluster"),
        ],
    );

    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("weather json");
    assert_eq!(value["cluster"], Value::from("env-cluster"));
    assert_eq!(value["condition"], Value::from("clear"));
    assert_eq!(value["nodes"]["gpu"]["free_nodes"], Value::from(1));
    assert_eq!(
        value["nodes"]["gpu"]["models"][0]["model"],
        Value::from("h100")
    );
    assert_eq!(value["queue"]["pending_jobs"], Value::from(1));
    assert_eq!(value["user"]["running_jobs"], Value::from(1));
    assert_eq!(value["fairshare"]["account"], Value::from("project"));
    assert_eq!(value["priority"]["top_job_id"], Value::from("99"));
}

#[test]
fn weather_succeeds_with_optional_probe_warnings() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let sinfo = write_fake_weather_tool(
        tmpdir.path(),
        "sinfo-weather",
        "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' 'cpu|idle|2|N/A|'\n",
    );
    let squeue = write_fake_weather_tool(
        tmpdir.path(),
        "squeue-weather",
        "#!/bin/bash\nset -euo pipefail\nif [[ \" $* \" == *\" --start \"* ]]; then exit 0; else printf '%s\\n' 'PENDING|weather-user'; fi\n",
    );
    let failing = write_fake_weather_tool(
        tmpdir.path(),
        "optional-weather",
        "#!/bin/bash\nset -euo pipefail\necho 'not configured' >&2\nexit 1\n",
    );

    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "weather",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sshare-bin",
            failing.to_str().expect("path"),
            "--sprio-bin",
            failing.to_str().expect("path"),
        ],
        &[("USER", "weather-user")],
    );

    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("Warnings:"));
    assert!(stdout.contains("not configured"));
}

#[test]
fn weather_fails_when_core_live_probes_are_unavailable() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let failing = write_fake_weather_tool(
        tmpdir.path(),
        "failing-weather",
        "#!/bin/bash\nset -euo pipefail\nexit 2\n",
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "weather",
            "--sinfo-bin",
            failing.to_str().expect("path"),
            "--squeue-bin",
            failing.to_str().expect("path"),
        ],
    );

    assert_failure(&output);
    assert!(stderr_text(&output).contains("weather probes failed"));
}

#[test]
fn weather_uses_profile_binary_overrides_for_all_weather_tools() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let settings_dir = tmpdir.path().join(".hpc-compose");
    fs::create_dir_all(&settings_dir).expect("settings dir");
    let log = tmpdir.path().join("weather-tools.log");
    let sinfo = write_fake_weather_tool(
        tmpdir.path(),
        "profile-sinfo",
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf 'sinfo:%s\\n' \"$*\" >> '{}'\nprintf '%s\\n' 'gpu|idle|1|gpu:h100:8|'\n",
            log.display()
        ),
    );
    let squeue = write_fake_weather_tool(
        tmpdir.path(),
        "profile-squeue",
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf 'squeue:%s\\n' \"$*\" >> '{}'\nif [[ \" $* \" == *\" --start \"* ]]; then printf '%s\\n' 'N/A'; else printf '%s\\n' 'RUNNING|profile-user'; fi\n",
            log.display()
        ),
    );
    let sshare = write_fake_weather_tool(
        tmpdir.path(),
        "profile-sshare",
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf 'sshare:%s\\n' \"$*\" >> '{}'\nprintf '%s\\n' 'acct|profile-user|0.6'\n",
            log.display()
        ),
    );
    let sprio = write_fake_weather_tool(
        tmpdir.path(),
        "profile-sprio",
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf 'sprio:%s\\n' \"$*\" >> '{}'\nprintf '%s\\n' '42|100|10|0|0'\n",
            log.display()
        ),
    );
    fs::write(
        settings_dir.join("settings.toml"),
        format!(
            r#"
version = 1
default_profile = "weather"

[profiles.weather.binaries]
sinfo = "{}"
squeue = "{}"
sshare = "{}"
sprio = "{}"
"#,
            sinfo.display(),
            squeue.display(),
            sshare.display(),
            sprio.display()
        ),
    )
    .expect("settings");

    let output = run_cli_with_env(
        tmpdir.path(),
        &["--profile", "weather", "weather", "--format", "json"],
        &[("USER", "profile-user")],
    );

    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("weather json");
    assert_eq!(value["nodes"]["gpu"]["free_nodes"], Value::from(1));
    assert_eq!(value["user"]["running_jobs"], Value::from(1));
    assert_eq!(value["fairshare"]["account"], Value::from("acct"));
    assert_eq!(value["priority"]["top_job_id"], Value::from("42"));
    let log = fs::read_to_string(log).expect("weather log");
    assert!(log.contains("sinfo:-h -o %P|%T|%D|%G|%E"));
    assert!(log.contains("squeue:-h -o %T|%u"));
    assert!(log.contains("squeue:--start -h -o %S"));
    assert!(log.contains("sshare:-n -P -u profile-user -o Account,User,FairShare"));
    assert!(log.contains("sprio:-h -u profile-user -o %.18i|%Y|%F|%P|%Q"));
}

fn write_when_compose(tmpdir: &std::path::Path, partition: &str) -> std::path::PathBuf {
    let local_image = tmpdir.join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    write_compose(
        tmpdir,
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  partition: {partition}
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    )
}

#[test]
fn when_free_node_condition_submits_and_tracks_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let sinfo = write_fake_when_tool(tmpdir.path(), "sinfo-when", "idle|4\nmixed|12\n");
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("when.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--partition",
            "gpu8",
            "--free-nodes",
            "4",
            "--skip-prepare",
            "--no-preflight",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );

    assert_success(&output);
    assert!(stdout_text(&output).contains("Submitted batch job 12345"));
    assert!(tmpdir.path().join(".hpc-compose/latest.json").exists());
    let sbatch_args = fs::read_to_string(&sbatch_log).expect("sbatch log");
    assert!(sbatch_args.contains(script_out.to_str().expect("path")));
}

#[test]
fn when_free_node_timeout_does_not_submit() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let sinfo = write_fake_when_tool(tmpdir.path(), "sinfo-when", "idle|3\n");
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--partition",
            "gpu8",
            "--free-nodes",
            "4",
            "--timeout",
            "0s",
            "--skip-prepare",
            "--no-preflight",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    assert_failure(&output);
    assert!(stderr_text(&output).contains("conditions were not satisfied"));
    assert!(!sbatch_log.exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
}

#[test]
fn when_after_job_afterany_submits_from_sacct_terminal_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let squeue = write_fake_when_tool(tmpdir.path(), "squeue-when", "");
    let sacct = write_fake_when_tool(tmpdir.path(), "sacct-when", "COMPLETED+\n");
    let sbatch = write_fake_sbatch(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--after-job",
            "12345",
            "--skip-prepare",
            "--no-preflight",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    assert_success(&output);
    assert!(stdout_text(&output).contains("Submitted batch job 12345"));
}

#[test]
fn when_after_job_afterok_fails_on_failed_terminal_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let squeue = write_fake_when_tool(tmpdir.path(), "squeue-when", "");
    let sacct = write_fake_when_tool(tmpdir.path(), "sacct-when", "FAILED\n");
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--after-job",
            "12345",
            "--after-job-condition",
            "afterok",
            "--skip-prepare",
            "--no-preflight",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    assert_failure(&output);
    assert!(stderr_text(&output).contains("can never satisfy afterok"));
    assert!(!sbatch_log.exists());
}

#[test]
fn when_combines_time_window_and_free_nodes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let sinfo = write_fake_when_tool(tmpdir.path(), "sinfo-when", "idle|4\n");
    let sbatch = write_fake_sbatch(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--between",
            "00:00-23:59",
            "--partition",
            "gpu8",
            "--free-nodes",
            "4",
            "--skip-prepare",
            "--no-preflight",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    assert_success(&output);
    assert!(stdout_text(&output).contains("Submitted batch job 12345"));
}

#[test]
fn when_json_output_includes_conditions_and_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let sinfo = write_fake_when_tool(tmpdir.path(), "sinfo-when", "idle|4\n");
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("when-json.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--partition",
            "gpu8",
            "--free-nodes",
            "4",
            "--skip-prepare",
            "--no-preflight",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );

    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("when json");
    assert_eq!(value["triggered"], Value::from(true));
    assert_eq!(value["conditions"][0]["kind"], Value::from("free_nodes"));
    assert_eq!(value["conditions"][0]["satisfied"], Value::from(true));
    assert_eq!(value["submission"]["job_id"], Value::from("12345"));
    assert_eq!(
        value["submission"]["script_path"],
        Value::from(script_out.display().to_string())
    );
    assert!(
        value["submission"]["tracked_metadata_path"]
            .as_str()
            .is_some_and(|path| path.ends_with(".hpc-compose/latest.json"))
    );
}

#[test]
fn when_rejects_partition_mismatch_before_monitoring() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_when_compose(tmpdir.path(), "gpu8");
    let sinfo_log = tmpdir.path().join("sinfo-called.log");
    let sinfo = tmpdir.path().join("sinfo-when");
    write_script(
        &sinfo,
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> '{}'\nprintf 'idle|4\\n'\n",
            sinfo_log.display()
        ),
    );
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let output = run_cli(
        tmpdir.path(),
        &[
            "when",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--partition",
            "cpu",
            "--free-nodes",
            "1",
            "--skip-prepare",
            "--no-preflight",
            "--sinfo-bin",
            sinfo.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    assert_failure(&output);
    assert!(stderr_text(&output).contains("--partition cpu must match x-slurm.partition gpu8"));
    assert!(!sinfo_log.exists());
    assert!(!sbatch_log.exists());
}

#[test]
fn local_submit_rejects_scheduler_arrays_and_dependencies() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let array_compose = write_compose(
        tmpdir.path(),
        "array.yaml",
        &format!(
            r#"
x-slurm:
  array: 0-3
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let array = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            array_compose.to_str().expect("path"),
        ],
    );
    assert_failure(&array);
    assert!(stderr_text(&array).contains("--local does not support x-slurm.array"));

    let dependency_compose = write_compose(
        tmpdir.path(),
        "dependency.yaml",
        &format!(
            r#"
x-slurm:
  after_job: "12345"
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let dependency = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            dependency_compose.to_str().expect("path"),
        ],
    );
    assert_failure(&dependency);
    assert!(stderr_text(&dependency).contains("--local does not support Slurm job dependencies"));
}

#[test]
fn array_submit_requires_detached_mode() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  array: 0-3
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let sbatch = write_fake_sbatch(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("x-slurm.array requires --detach"));
}

#[test]
fn run_image_mode_submits_ephemeral_one_service_plan() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let script_out = tmpdir.path().join("ephemeral.sbatch");

    let run = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--mem",
            "2G",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/echo",
            "--",
            "hello",
        ],
    );
    assert_success(&run);
    let stdout = stdout_text(&run);
    assert!(stdout.contains("Submitted batch job 12345"));
    assert!(stdout.contains(".hpc-compose/latest-run.json"));
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(script.contains("#SBATCH --mem=2G"));
    assert!(script.contains("hpc-compose:run"));
    assert!(script.contains("'/bin/echo'"));
    assert!(script.contains("'--'"));
}

#[test]
fn run_image_mode_rejects_service_like_mixed_form() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "app",
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    let stderr = stderr_text(&output);
    assert!(stderr.contains("cannot include a"));
    assert!(stderr.contains("service name"));
}

#[test]
fn run_image_rejects_invalid_env_entries_before_launch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("ephemeral.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--env",
            "BROKEN",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--env entries must use KEY=VALUE syntax"));
    assert!(!script_out.exists());
    assert!(!sbatch_log.exists());
}

#[test]
fn run_image_dataset_and_output_bind_dataset_ro_and_export_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let dataset = tmpdir.path().join("dataset");
    fs::create_dir_all(&dataset).expect("dataset dir");
    let results = tmpdir.path().join("results");

    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let script_out = tmpdir.path().join("ephemeral.sbatch");

    let run = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--dataset",
            dataset.to_str().expect("path"),
            "--output",
            results.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "python",
            "infer.py",
        ],
    );
    assert_success(&run);

    // The dataset is bound read-only at the dataset container destination.
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(
        script.contains(&format!("{}:/hpc-compose/dataset:ro", dataset.display())),
        "dataset must be bound read-only:\n{script}"
    );
    // Both in-job env vars point the command at the container destinations.
    assert!(script.contains("HPC_COMPOSE_DATASET_DIR=/hpc-compose/dataset"));
    assert!(script.contains("HPC_COMPOSE_OUTPUT_DIR=/hpc-compose/job/output"));
    // --output flips the artifacts pipeline on; the output path is collected.
    assert!(script.contains("ARTIFACTS_DIR="));

    // The submission record exports artifacts into the host --output directory.
    let record: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/latest-run.json"))
            .expect("latest run"),
    )
    .expect("latest run json");
    assert_eq!(
        record["artifact_export_dir"],
        Value::from(results.display().to_string())
    );
}

#[test]
fn run_image_dataset_missing_path_bails_before_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let missing = tmpdir.path().join("does-not-exist");
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);
    let script_out = tmpdir.path().join("ephemeral.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--dataset",
            missing.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--dataset path does not exist"));
    // No rendering and no submission happen when the dataset is missing.
    assert!(!script_out.exists());
    assert!(!sbatch_log.exists());
}

#[test]
fn run_dataset_and_output_require_image_mode() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let dataset = tmpdir.path().join("dataset");
    fs::create_dir_all(&dataset).expect("dataset dir");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--dataset",
            dataset.to_str().expect("path"),
            "--output",
            tmpdir.path().join("results").to_str().expect("path"),
            "app",
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    // The diagnostic is line-wrapped by the error renderer, so assert on tokens
    // rather than a contiguous phrase.
    let stderr = stderr_text(&output);
    assert!(stderr.contains("--dataset"));
    assert!(stderr.contains("--output"));
    assert!(stderr.contains("require"));
    assert!(stderr.contains("--image"));
}

#[test]
fn run_image_local_tracks_latest_run_backend_without_main_latest() {
    if std::env::consts::OS != "linux" {
        return;
    }

    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let script_out = tmpdir.path().join("ephemeral.local.sh");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_success(&output);
    assert!(script_out.exists());
    assert!(tmpdir.path().join(".hpc-compose/latest-run.json").exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
    let record: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/latest-run.json"))
            .expect("latest run"),
    )
    .expect("latest run json");
    assert_eq!(record["kind"], Value::from("run"));
    assert_eq!(record["backend"], Value::from("local"));
}

#[test]
fn run_image_local_invalid_env_does_not_render_or_track() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let script_out = tmpdir.path().join("ephemeral.local.sh");

    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            local_image.to_str().expect("path"),
            "--local",
            "--env",
            "BROKEN",
            "--skip-prepare",
            "--no-preflight",
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--env entries must use KEY=VALUE syntax"));
    assert!(!script_out.exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest-run.json").exists());
}

#[test]
fn shell_command_invokes_srun_pty_with_image_resources_and_env() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let srun_log = tmpdir.path().join("srun.log");
    let srun = tmpdir.path().join("srun-shell");
    write_script(
        &srun,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf 'args:%s\n' "$*" >> '{}'
printf 'env:%s\n' "${{FOO:-}}" >> '{}'
exit 0
"#,
            srun_log.display(),
            srun_log.display()
        ),
    );

    let shell = run_cli(
        tmpdir.path(),
        &[
            "shell",
            "--image",
            local_image.to_str().expect("path"),
            "--gpus",
            "1",
            "--env",
            "FOO=bar",
            "--srun-bin",
            srun.to_str().expect("path"),
        ],
    );
    assert_success(&shell);
    let log = fs::read_to_string(&srun_log).expect("srun log");
    assert!(log.contains("--pty"));
    assert!(log.contains(&format!("--container-image={}", local_image.display())));
    assert!(log.contains("--gpus=1"));
    assert!(log.contains("--container-env=FOO"));
    assert!(log.contains("bash -l"));
    assert!(log.contains("env:bar"));
}

#[test]
fn shell_rejects_invalid_env_before_srun() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let srun_log = tmpdir.path().join("srun.log");
    let srun = tmpdir.path().join("srun-shell-invalid-env");
    write_script(
        &srun,
        &format!(
            "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
            srun_log.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "shell",
            "--image",
            local_image.to_str().expect("path"),
            "--env",
            "BROKEN",
            "--srun-bin",
            srun.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--env entries must use KEY=VALUE syntax"));
    assert!(!srun_log.exists());
}

fn write_latest_notebook_record(root: &Path, config_snapshot_yaml: Option<&str>) {
    let metadata = root.join(".hpc-compose");
    fs::create_dir_all(&metadata).expect("metadata dir");
    let mut record = serde_json::json!({
        "schema_version": 1,
        "backend": "slurm",
        "kind": "notebook",
        "job_id": "12345",
        "submitted_at": 42,
        "compose_file": root.join("hpc-compose-notebook-jupyter.yaml"),
        "submit_dir": root,
        "script_path": root.join("hpc-compose-notebook.sbatch"),
        "cache_dir": root.join("cache"),
        "batch_log": root.join("hpc-compose-12345.out"),
        "service_logs": {},
        "service_name": "notebook",
        "requested_walltime": {
            "original": "01:00:00",
            "seconds": 3600
        },
        "provenance": {
            "tool_version": "test",
            "image_refs": {
                "notebook": "jupyter/scipy-notebook:latest"
            }
        }
    });
    if let Some(snapshot) = config_snapshot_yaml {
        record["config_snapshot_yaml"] = Value::from(snapshot);
    }
    fs::write(
        metadata.join("latest-notebook.json"),
        serde_json::to_string_pretty(&record).expect("record json"),
    )
    .expect("latest notebook record");
}

#[test]
fn notebook_dry_run_renders_jupyter_launcher_without_submitting() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_out = tmpdir.path().join("notebook.sbatch");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "jupyter",
            "--gpus",
            "1",
            "--volume",
            "./project:/workspace",
            "--dry-run",
            "--no-preflight",
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(
        script_out.exists(),
        "dry-run should write the launcher script"
    );
    let script = fs::read_to_string(&script_out).expect("read script");
    assert!(
        script.contains("jupyter") && script.contains("lab"),
        "rendered script should launch jupyter lab:\n{script}"
    );
    assert!(
        script.contains("--ServerApp.token"),
        "rendered script should pass the generated token:\n{script}"
    );
    // Dry-run must not create any tracking metadata.
    assert!(
        !tmpdir
            .path()
            .join(".hpc-compose/latest-notebook.json")
            .exists()
    );
}

#[test]
fn notebook_dry_run_format_json_emits_json_preview() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_out = tmpdir.path().join("notebook.sbatch");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "jupyter",
            "--dry-run",
            "--format",
            "json",
            "--no-preflight",
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("dry-run json");
    assert_eq!(payload["dry_run"], true);
    assert_eq!(payload["submitted"], false);
    assert_eq!(payload["kind"], "jupyter");
    assert_eq!(payload["script_path"], script_out.display().to_string());
    assert!(payload["cache_dir"].as_str().is_some());
    assert!(
        script_out.exists(),
        "dry-run json should still write the launcher script"
    );
    assert!(
        !tmpdir
            .path()
            .join(".hpc-compose/latest-notebook.json")
            .exists()
    );
}

#[test]
fn notebook_dry_run_renders_vscode_command_when_image_supplied() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_out = tmpdir.path().join("notebook.sbatch");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "vscode",
            "--image",
            "ghcr.io/example/code:1",
            "--tunnel-name",
            "my-tunnel",
            "--dry-run",
            "--no-preflight",
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let script = fs::read_to_string(&script_out).expect("read script");
    assert!(
        script.contains("code") && script.contains("tunnel"),
        "vscode script should run `code tunnel`:\n{script}"
    );
    assert!(
        script.contains("my-tunnel"),
        "vscode script should embed the tunnel name:\n{script}"
    );
}

#[test]
fn notebook_promote_writes_batch_spec_from_latest_record() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let notebook = tmpdir.path().join("analysis.ipynb");
    fs::write(
        &notebook,
        r#"{"cells":[{"cell_type":"code","source":["%pip install numpy\n"]}]}"#,
    )
    .expect("notebook");
    let requirements = tmpdir.path().join("requirements.txt");
    fs::write(&requirements, "numpy\n").expect("requirements");
    write_latest_notebook_record(tmpdir.path(), None);

    let output = run_cli(
        tmpdir.path(),
        &[
            "--offline",
            "notebook",
            "promote",
            notebook.to_str().expect("path"),
            "--requirements",
            requirements.to_str().expect("path"),
            "--prepare-command",
            "pip install --no-cache-dir pandas",
            "--param",
            "SEED=1",
        ],
    );
    assert_success(&output);
    let promoted = tmpdir.path().join("analysis.promoted.yaml");
    assert!(promoted.exists(), "promote should write default output");
    let yaml = fs::read_to_string(&promoted).expect("promoted yaml");
    assert!(yaml.contains("python"));
    assert!(yaml.contains("papermill"));
    assert!(yaml.contains("/hpc-compose/notebook-promote/analysis.ipynb"));
    assert!(yaml.contains("/hpc-compose/notebook-promote/analysis.promoted.ipynb"));
    assert!(yaml.contains("${SEED:-1}"));
    assert!(yaml.contains("x-runtime"));
    assert!(yaml.contains("pip install --no-cache-dir -r /hpc-compose/prepare/requirements.txt"));
    assert!(yaml.contains("pip install --no-cache-dir pandas"));
    assert!(yaml.contains("pip install --no-cache-dir papermill"));
    assert!(yaml.contains("jupyter/scipy-notebook:latest"));
    let validate = run_cli(
        tmpdir.path(),
        &[
            "--offline",
            "validate",
            "-f",
            promoted.to_str().expect("path"),
        ],
    );
    assert_success(&validate);
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("ad-hoc install") && stderr.contains("config_snapshot_yaml"),
        "promote should warn about install cells and legacy records:\n{stderr}"
    );
    assert!(stdout_text(&output).contains("hpc-compose plan -f"));
}

#[test]
fn notebook_promote_uses_record_snapshot_when_present() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let notebook = tmpdir.path().join("analysis.ipynb");
    fs::write(&notebook, r#"{"cells":[]}"#).expect("notebook");
    let output_path = tmpdir.path().join("batch.yaml");
    fs::write(&output_path, "old: true\n").expect("old output");
    let snapshot = r#"name: interactive
services:
  notebook:
    image: custom/notebook:1
    working_dir: /workspace
    volumes:
      - ./project:/workspace
    command:
      - jupyter
      - lab
    readiness:
      type: log
      pattern: ready
"#;
    write_latest_notebook_record(tmpdir.path(), Some(snapshot));

    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "promote",
            notebook.to_str().expect("path"),
            "--output",
            output_path.to_str().expect("path"),
            "--force",
        ],
    );
    assert_success(&output);
    let yaml = fs::read_to_string(&output_path).expect("promoted yaml");
    assert!(yaml.contains("custom/notebook:1"));
    assert!(yaml.contains("./project:/workspace"));
    assert!(yaml.contains("python"));
    assert!(yaml.contains("papermill"));
    assert!(!yaml.contains("readiness"));
    let validate = run_cli(
        tmpdir.path(),
        &[
            "--offline",
            "validate",
            "-f",
            output_path.to_str().expect("path"),
        ],
    );
    assert_success(&validate);
    assert!(
        !stderr_text(&output).contains("config_snapshot_yaml"),
        "snapshot-backed promote should not emit the legacy-record warning"
    );
}

#[test]
fn notebook_trailing_server_args_still_parse_after_promote_subcommand_added() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_out = tmpdir.path().join("notebook.sbatch");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "jupyter",
            "--dry-run",
            "--no-preflight",
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "--NotebookApp.foo=bar",
        ],
    );
    assert_success(&output);
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(
        script.contains("--NotebookApp.foo=bar"),
        "trailing server arg should still be forwarded:\n{script}"
    );
}

#[test]
fn notebook_vscode_requires_explicit_image() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "vscode",
            "--dry-run",
            "--no-preflight",
        ],
    );
    assert_failure(&output);
    let combined = format!("{}\n{}", stdout_text(&output), stderr_text(&output));
    assert!(
        combined.contains("requires --image"),
        "vscode without --image should fail clearly:\n{combined}"
    );
}

#[test]
fn notebook_rejects_file_flag() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "name: demo\nservices:\n  app:\n    image: redis:7\n    command: /bin/true\n",
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "-f",
            compose.to_str().expect("path"),
            "--dry-run",
            "--no-preflight",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("does not accept -f/--file"));
}

#[test]
fn notebook_rejects_invalid_timeout() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--timeout",
            "not-a-duration",
            "--dry-run",
            "--no-preflight",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--timeout"));
}

#[test]
fn notebook_local_submits_and_tracks_then_bails_on_readiness_timeout() {
    // Notebook reuses the local supervisor path; with a fake enroot that does
    // not print a Jupyter URL, the readiness gate must time out cleanly while
    // still having written a tracked notebook record.
    if std::env::consts::OS != "linux" {
        return;
    }
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let enroot = write_fake_enroot(tmpdir.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "notebook",
            "--kind",
            "jupyter",
            "--image",
            local_image.to_str().expect("path"),
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--timeout",
            "1s",
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    // Readiness cannot be reached, so the command fails.
    assert_failure(&output);
    let combined = format!("{}\n{}", stdout_text(&output), stderr_text(&output));
    assert!(
        combined.contains("did not become ready"),
        "expected a readiness-timeout message:\n{combined}"
    );
    // The notebook record must still have been tracked.
    let latest = tmpdir.path().join(".hpc-compose/latest-notebook.json");
    assert!(latest.exists(), "notebook record should be tracked");
    let record: Value =
        serde_json::from_str(&fs::read_to_string(&latest).expect("latest notebook"))
            .expect("latest notebook json");
    assert_eq!(record["kind"], Value::from("notebook"));
    assert_eq!(record["backend"], Value::from("local"));
    assert_eq!(record["service_name"], Value::from("notebook"));

    // `cancel` must stop the orphaned local supervisor and clean up.
    let cancel = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            record["compose_file"]
                .as_str()
                .expect("compose_file")
                .parse::<std::path::PathBuf>()
                .expect("path")
                .to_str()
                .expect("path"),
            "--yes",
        ],
    );
    assert_success(&cancel);
}

#[test]
fn cancel_without_job_id_targets_newest_run_record() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let mut main_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("main.sbatch"),
        &plan,
        "11111",
    )
    .expect("main record");
    main_record.submitted_at = 10;
    write_submission_record(&main_record).expect("write main");

    let mut run_record = build_submission_record_with_options(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("run.sbatch"),
        &plan,
        "22222",
        &SubmissionRecordBuildOptions {
            kind: SubmissionKind::Run,
            service_name: Some("app".into()),
            command_override: Some(vec!["/bin/true".into()]),
            ..SubmissionRecordBuildOptions::default()
        },
    )
    .expect("run record");
    run_record.submitted_at = 11;
    write_submission_record(&run_record).expect("write run");

    let cancel = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel);
    assert_eq!(
        fs::read_to_string(&scancel_log)
            .expect("scancel log")
            .trim(),
        "22222"
    );
}

#[test]
fn cancel_explicit_untracked_job_json_reports_no_tracking_removed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let output = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "99999",
            "--scancel-bin",
            scancel.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("cancel json");
    assert_eq!(payload["job_id"], Value::from("99999"));
    assert_eq!(payload["cancelled"], Value::from(true));
    assert_eq!(payload["tracking_removed"], Value::from(false));
    assert_eq!(
        fs::read_to_string(scancel_log).expect("scancel log").trim(),
        "99999"
    );
}

#[test]
fn cancel_local_record_without_state_reports_not_running_and_removes_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let mut record = build_submission_record_with_backend_and_options(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("local.sh"),
        &plan,
        "local-missing-state",
        SubmissionBackend::Local,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("write record");
    assert!(
        tmpdir
            .path()
            .join(".hpc-compose/jobs/local-missing-state.json")
            .exists()
    );
    assert!(!state_path_for_record(&record).exists());

    let output = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "local-missing-state",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("cancel json");
    assert_eq!(payload["job_id"], Value::from("local-missing-state"));
    assert_eq!(payload["cancelled"], Value::from(false));
    assert_eq!(payload["tracking_removed"], Value::from(true));
    assert!(
        !tmpdir
            .path()
            .join(".hpc-compose/jobs/local-missing-state.json")
            .exists()
    );
}

#[test]
fn cancel_local_record_with_stale_pid_reports_not_running_and_removes_tracking() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let mut record = build_submission_record_with_backend_and_options(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("local.sh"),
        &plan,
        "local-stale-pid",
        SubmissionBackend::Local,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("write record");
    let state_path = state_path_for_record(&record);
    fs::create_dir_all(state_path.parent().expect("state parent")).expect("state dir");
    fs::write(&state_path, r#"{"supervisor_pid":999999999}"#).expect("state");

    let output = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "local-stale-pid",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("cancel json");
    assert_eq!(payload["job_id"], Value::from("local-stale-pid"));
    assert_eq!(payload["cancelled"], Value::from(false));
    assert_eq!(payload["tracking_removed"], Value::from(true));
    assert!(
        !tmpdir
            .path()
            .join(".hpc-compose/jobs/local-stale-pid.json")
            .exists()
    );
}

#[test]
fn tracked_job_resolution_rejects_duplicate_job_ids_without_file() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for project in ["alpha", "beta"] {
        let project_dir = tmpdir.path().join(project);
        fs::create_dir_all(&project_dir).expect("project dir");
        let cache_dir = project_dir.join("cache");
        let compose = write_prepare_compose(&project_dir, &cache_dir);
        let plan = runtime_plan(&compose);
        let mut record = build_submission_record(
            &compose,
            &project_dir,
            &project_dir.join("job.sbatch"),
            &plan,
            "424242",
        )
        .expect("record");
        record.submitted_at = if project == "alpha" { 1 } else { 2 };
        write_submission_record(&record).expect("write record");
    }

    let score = run_cli(tmpdir.path(), &["score", "424242", "--format", "json"]);
    assert_failure(&score);
    let stderr = stderr_text(&score);
    assert!(stderr.contains("multiple tracked submissions with job id '424242'"));
    assert!(stderr.contains("disambiguate"), "stderr:\n{stderr}");
}

#[test]
fn status_explicit_job_id_prefers_current_compose_record_over_repo_duplicate() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let mut current_compose = None;
    for project in ["alpha", "beta"] {
        let project_dir = tmpdir.path().join(project);
        fs::create_dir_all(&project_dir).expect("project dir");
        let cache_dir = project_dir.join("cache");
        let compose = write_prepare_compose(&project_dir, &cache_dir);
        let plan = runtime_plan(&compose);
        let mut record = build_submission_record(
            &compose,
            &project_dir,
            &project_dir.join("job.sbatch"),
            &plan,
            "424242",
        )
        .expect("record");
        record.submitted_at = if project == "alpha" { 1 } else { 2 };
        write_submission_record(&record).expect("write record");
        if project == "alpha" {
            current_compose = Some(compose);
        }
    }
    let compose = current_compose.expect("current compose");
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "424242",
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
    assert_eq!(payload["record"]["job_id"], Value::from("424242"));
    assert_eq!(
        payload["record"]["compose_file"],
        Value::from(compose.display().to_string())
    );
    assert_eq!(payload["scheduler"]["state"], Value::from("COMPLETED"));
}

#[test]
fn status_duplicate_job_id_without_file_reports_disambiguation() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for project in ["alpha", "beta"] {
        let project_dir = tmpdir.path().join(project);
        fs::create_dir_all(&project_dir).expect("project dir");
        let cache_dir = project_dir.join("cache");
        let compose = write_prepare_compose(&project_dir, &cache_dir);
        let plan = runtime_plan(&compose);
        let mut record = build_submission_record(
            &compose,
            &project_dir,
            &project_dir.join("job.sbatch"),
            &plan,
            "525252",
        )
        .expect("record");
        record.submitted_at = if project == "alpha" { 1 } else { 2 };
        write_submission_record(&record).expect("write record");
    }

    let status = run_cli(
        tmpdir.path(),
        &["status", "--job-id", "525252", "--format", "json"],
    );
    assert_failure(&status);
    let stderr = stderr_text(&status);
    assert!(stderr.contains("multiple tracked submissions with job id '525252'"));
    assert!(stderr.contains("disambiguate"), "stderr:\n{stderr}");
}

#[test]
fn cancel_with_purge_cache_requires_tracked_artifact_snapshot() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("job.sbatch"),
        &plan,
        "12345",
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    if let Some(parent) = plan.ordered_services[0].runtime_image.parent() {
        fs::create_dir_all(parent).expect("runtime image dir");
    }
    fs::write(&plan.ordered_services[0].runtime_image, "runtime").expect("runtime image");

    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);
    let cancel = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--purge-cache",
            "--yes",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_failure(&cancel);
    assert!(
        stderr_text(&cancel).contains("refusing --purge-cache"),
        "stderr:\n{}",
        stderr_text(&cancel)
    );
    assert!(plan.ordered_services[0].runtime_image.exists());
    assert!(
        !scancel_log.exists()
            || fs::read_to_string(&scancel_log)
                .expect("scancel log")
                .trim()
                .is_empty()
    );
}

#[test]
fn submit_json_keeps_stdout_parseable_when_resume_diff_blocks_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let resume_dir = tmpdir.path().join("resume");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  cache_dir: {}
  resume:
    path: {}
services:
  app:
    image: {}
    command: /bin/true
"#,
            tmpdir.path().join("cache").display(),
            resume_dir.display(),
            local_image.display()
        ),
    );
    let sbatch = write_fake_sbatch(tmpdir.path());

    let first_submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&first_submit);

    fs::write(
        &compose,
        format!(
            r#"
name: demo
x-slurm:
  cache_dir: {}
  resume:
    path: {}
services:
  app:
    image: {}
    command:
      - /bin/echo
      - changed
"#,
            tmpdir.path().join("cache").display(),
            resume_dir.display(),
            local_image.display()
        ),
    )
    .expect("rewrite compose");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    assert!(stdout_text(&submit).trim().is_empty());
    assert!(stderr_text(&submit).contains("resume config drift detected"));

    let diff_only = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--resume-diff-only",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&diff_only);
    assert!(stdout_text(&diff_only).trim().is_empty());
    assert!(stderr_text(&diff_only).contains("--resume-diff-only does not support --format json"));
}

#[test]
fn status_reports_missing_record_cleanly() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let status = run_cli(
        tmpdir.path(),
        &["status", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&status);
    assert!(stderr_text(&status).contains("no tracked submission metadata exists"));
}

#[test]
fn debug_reports_missing_and_failed_tracked_runs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let missing = run_cli(
        tmpdir.path(),
        &[
            "debug",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&missing);
    let missing_payload: Value =
        serde_json::from_str(&stdout_text(&missing)).expect("debug missing json");
    assert_eq!(missing_payload["tracked"], Value::from(false));
    assert!(
        missing_payload["recommendation"]
            .as_str()
            .unwrap_or_default()
            .contains("hpc-compose plan")
    );

    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    fs::write(
        log_dir.join(log_file_name_for_service("app")),
        "service start\nservice boom\n",
    )
    .expect("service log");
    let batch_log = tmpdir
        .path()
        .join(".hpc-compose/logs/hpc-compose-12345.out");
    fs::create_dir_all(batch_log.parent().expect("batch log dir")).expect("batch log dir");
    fs::write(&batch_log, "batch fail\n").expect("batch log");

    let squeue_state = tmpdir.path().join("debug-squeue.state");
    let sacct_state = tmpdir.path().join("debug-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "FAILED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let debug = run_cli(
        tmpdir.path(),
        &[
            "debug",
            "-f",
            compose.to_str().expect("path"),
            "--lines",
            "1",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&debug);
    let debug_stdout = stdout_text(&debug);
    assert!(debug_stdout.contains("state: FAILED"));
    assert!(debug_stdout.contains("Batch log"));
    assert!(debug_stdout.contains("batch fail"));
    assert!(debug_stdout.contains("Service log tails:"));
    assert!(debug_stdout.contains("service boom"));
    assert!(debug_stdout.contains("hpc-compose debug"));
    assert!(debug_stdout.contains("--preflight"));
}

#[test]
fn debug_preflight_includes_findings_and_fails_on_preflight_error() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing_image = tmpdir.path().join("missing.sqsh");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
"#,
            missing_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "debug",
            "--preflight",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_failure(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("debug json");
    assert_eq!(payload["tracked"], Value::from(false));
    assert!(payload["preflight"].is_object());
    assert!(stderr_text(&output).contains("preflight failed"));
}

#[test]
fn debug_job_id_uses_resolved_record_compose_file() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let nested = tmpdir.path().join("nested");
    fs::create_dir_all(&nested).expect("nested dir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&nested, &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let squeue_state = tmpdir.path().join("debug-resolved-squeue.state");
    let sacct_state = tmpdir.path().join("debug-resolved-sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let debug = run_cli(
        tmpdir.path(),
        &[
            "debug",
            "--job-id",
            "12345",
            "--preflight",
            "--format",
            "json",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&debug);
    let payload: Value = serde_json::from_str(&stdout_text(&debug)).expect("debug json");
    assert_eq!(payload["tracked"], Value::from(true));
    assert_eq!(
        payload["compose_file"],
        Value::from(compose.display().to_string())
    );
    assert_eq!(payload["job_id"], Value::from("12345"));
    assert!(payload["preflight"].is_object());
}

#[test]
fn submit_watch_covers_completed_and_failed_states() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());

    let success_squeue_state = tmpdir.path().join("watch-success-squeue.state");
    let success_sacct_state = tmpdir.path().join("watch-success-sacct.state");
    let success_squeue = write_fake_squeue(tmpdir.path(), &success_squeue_state);
    let success_sacct = write_fake_sacct(tmpdir.path(), &success_sacct_state);
    let success_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &success_squeue_state,
        &success_sacct_state,
        "COMPLETED",
        "ready",
        2,
    );

    let success = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--watch-mode",
            "line",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            success_sbatch.to_str().expect("path"),
            "--squeue-bin",
            success_squeue.to_str().expect("path"),
            "--sacct-bin",
            success_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&success);
    let success_stdout = stdout_text(&success);
    assert!(success_stdout.contains("watching job 12345"));
    assert!(!success_stdout.contains("scheduler state: unknown (local-only)"));
    assert!(success_stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(success_stdout.contains("[app] ready"));

    let failure_squeue_state = tmpdir.path().join("watch-failure-squeue.state");
    let failure_sacct_state = tmpdir.path().join("watch-failure-sacct.state");
    let failure_squeue = write_fake_squeue(tmpdir.path(), &failure_squeue_state);
    let failure_sacct = write_fake_sacct(tmpdir.path(), &failure_sacct_state);
    let failure_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &failure_squeue_state,
        &failure_sacct_state,
        "FAILED",
        "boom",
        0,
    );

    let failure = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--watch-mode",
            "line",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            failure_sbatch.to_str().expect("path"),
            "--squeue-bin",
            failure_squeue.to_str().expect("path"),
            "--sacct-bin",
            failure_sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failure);
    assert!(stdout_text(&failure).contains("[app] boom"));
    assert!(stderr_text(&failure).contains("finished in scheduler state FAILED"));
}

#[test]
fn submit_watch_queue_waits_for_running_before_watch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let squeue_state = tmpdir.path().join("watch-queue-squeue.state");
    let sacct_state = tmpdir.path().join("watch-queue-sacct.state");
    // The real fake squeue/sacct just render whatever these state files hold;
    // the counter wrapper below owns all transitions so nothing depends on
    // wall-clock timing.
    let real_squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    let service_log = log_dir.join(log_file_name_for_service("app"));

    // Invocation-counted squeue: the PENDING -> RUNNING -> gone transition is
    // driven by how many times squeue is polled, not by a background subshell
    // racing the CLI's 1s poll cadence. This makes the ordering deterministic
    // regardless of full-suite load, which is what made the old wall-clock
    // fixture flaky (the 1s RUNNING window was not reliably observed).
    //
    // With PENDING for the first two polls the wait loop always prints at least
    // one "queue state: PENDING" line before the RUNNING poll opens the watch
    // view. The RUNNING poll (exactly once) publishes the service logs and arms
    // the terminal state, then the latch makes squeue report the job as gone so
    // sacct drives the watch to COMPLETED.
    let counter_file = tmpdir.path().join("watch-queue-squeue.count");
    let latch_file = tmpdir.path().join("watch-queue-squeue.latch");
    let squeue = tmpdir.path().join("squeue-counter");
    write_script(
        &squeue,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
count="$(cat '{counter}' 2>/dev/null || echo 0)"
count=$((count + 1))
printf '%s\n' "$count" > '{counter}'
if [[ -e '{latch}' ]]; then
  printf 'NONE\n' > '{squeue_state}'
elif (( count <= 2 )); then
  printf 'PENDING\n' > '{squeue_state}'
else
  printf 'RUNNING\n' > '{squeue_state}'
  mkdir -p '{log_dir}'
  printf 'booting\nready\n' > '{service_log}'
  printf 'COMPLETED\n' > '{sacct_state}'
  : > '{latch}'
fi
exec '{real_squeue}' "$@"
"#,
            counter = counter_file.display(),
            latch = latch_file.display(),
            squeue_state = squeue_state.display(),
            log_dir = log_dir.display(),
            service_log = service_log.display(),
            sacct_state = sacct_state.display(),
            real_squeue = real_squeue.display(),
        ),
    );

    // sbatch just seeds the initial PENDING state and clears any stale sacct
    // record; every subsequent transition is owned by the counter squeue.
    let sbatch = tmpdir.path().join("sbatch-watch-queue");
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
mkdir -p '{}'
printf 'PENDING\n' > '{}'
rm -f '{}'
echo "Submitted batch job 12345"
"#,
            log_dir.display(),
            squeue_state.display(),
            sacct_state.display(),
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--watch-queue",
            "--watch-mode",
            "line",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("waiting for job 12345 to start"));
    assert!(stdout.contains("queue state: PENDING (squeue)"));
    assert!(stdout.contains("watching job 12345"));
    // Now deterministic: the wait loop observes PENDING, then RUNNING, then the
    // watch view opens — strictly in that order. This is the strong assertion
    // the old fixture wanted but could not reliably make under load.
    let pending_at = stdout
        .find("queue state: PENDING")
        .expect("pending queue state");
    let running_at = stdout
        .find("queue state: RUNNING")
        .expect("running queue state");
    let watching_at = stdout.find("watching job 12345").expect("watch starts");
    assert!(
        pending_at < running_at && running_at < watching_at,
        "expected PENDING < RUNNING < watching, got {pending_at} / {running_at} / {watching_at}\n{stdout}"
    );
    assert!(stdout.contains("[app] ready"));
}

#[test]
fn reach_prints_multiplexed_tunnel_for_tcp_readiness_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().to_path_buf();
    let compose = project.join("reach.yaml");
    fs::write(
        &compose,
        format!(
            r#"name: reach-test
x-slurm:
  cache_dir: {}
  time: "00:10:00"
services:
  api:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: tcp
      port: 8000
"#,
            cache_dir.display()
        ),
    )
    .expect("compose");
    let plan = runtime_plan(&compose);
    let script = project.join("reach.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    let record = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "55555",
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    // --open + --format json is rejected up front (before any scheduler contact).
    let bad = run_cli(
        &project,
        &[
            "reach",
            "api",
            "-f",
            compose.to_str().expect("path"),
            "--open",
            "--format",
            "json",
        ],
    );
    assert!(!bad.status.success());
    assert!(stderr_text(&bad).contains("--open cannot be combined with --format json"));

    // Happy path: JSON with the readiness-derived port and a multiplexed ssh line.
    // No fake scheduler bins are needed — build_status_snapshot degrades and the
    // compute node falls back to a placeholder.
    let out = run_cli(
        &project,
        &[
            "reach",
            "api",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("reach json");
    assert_eq!(value["service"], Value::from("api"));
    assert_eq!(value["remote_port"], Value::from(8000));
    assert_eq!(value["local_port"], Value::from(8000));
    assert!(
        value["url"]
            .as_str()
            .expect("url")
            .contains("127.0.0.1:8000")
    );
    let ssh = value["ssh_command"].as_str().expect("ssh_command");
    assert!(ssh.contains("-L 8000:"), "forward present: {ssh}");
    assert!(
        ssh.contains("ControlMaster=auto"),
        "multiplexing present: {ssh}"
    );
}

#[test]
fn pull_prints_multiplexed_rsync_line_and_copies_nothing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().to_path_buf();
    let compose = project.join("pull.yaml");
    fs::write(
        &compose,
        format!(
            r#"name: pull-test
x-slurm:
  cache_dir: {}
  time: "00:10:00"
  artifacts:
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/out/**
services:
  app:
    image: docker://python:3.12
    command: ["true"]
"#,
            cache_dir.display()
        ),
    )
    .expect("compose");
    let plan = runtime_plan(&compose);
    let script = project.join("pull.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    let record = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "77777",
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    // No manifest yet -> pull fails with the actionable collection error.
    let missing = run_cli(
        &project,
        &[
            "pull",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "77777",
            "--format",
            "json",
        ],
    );
    assert!(!missing.status.success());
    assert!(stderr_text(&missing).contains("artifact manifest does not exist"));

    // Write a manifest (default paths + one bundle) plus the payload files.
    let manifest_path = artifact_manifest_path_for_record(&record);
    fs::create_dir_all(manifest_path.parent().expect("manifest parent")).expect("manifest dir");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": 1,
  "job_id": "77777",
  "collect_policy": "always",
  "collected_at": "2026-01-01T00:00:00Z",
  "job_outcome": "completed",
  "copied_relative_paths": ["a.txt", "b.txt"],
  "bundles": { "ckpt": { "copied_relative_paths": ["c.bin"] } }
}"#,
    )
    .expect("manifest");
    let payload = artifact_payload_dir_for_record(&record);
    fs::create_dir_all(&payload).expect("payload dir");
    fs::write(payload.join("a.txt"), "12345").expect("a"); // 5 bytes
    fs::write(payload.join("b.txt"), "67890").expect("b"); // 5 bytes
    fs::write(payload.join("c.bin"), "0123456789").expect("c"); // 10 bytes

    let out = run_cli(
        &project,
        &[
            "pull",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "77777",
            "--into",
            "./results",
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("pull json");
    assert_eq!(value["files"].as_u64(), Some(3));
    assert_eq!(value["bytes"].as_u64(), Some(20));
    assert_eq!(value["bundles"], serde_json::json!(["ckpt"]));
    let cmd = value["suggested_command"]
        .as_str()
        .expect("suggested_command");
    assert!(cmd.starts_with("rsync -avz -e 'ssh "), "rsync line: {cmd}");
    assert!(cmd.contains("ControlMaster=auto"), "multiplexing: {cmd}");
    assert!(
        cmd.contains("<login-node>:"),
        "host placeholder when login_host unset: {cmd}"
    );
    assert!(cmd.contains("./results/"), "destination: {cmd}");
    // Read-only: pull copies nothing, so the local destination is never created.
    assert!(!project.join("results").exists());
}

#[test]
fn experiment_show_aggregates_one_object_and_writes_nothing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().to_path_buf();
    let compose = project.join("experiment.yaml");
    fs::write(
        &compose,
        format!(
            r#"name: experiment-test
x-slurm:
  cache_dir: {}
  time: "00:10:00"
  artifacts:
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/out/**
services:
  api:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: tcp
      port: 8000
  worker:
    image: docker://python:3.12
    command: ["true"]
"#,
            cache_dir.display()
        ),
    )
    .expect("compose");
    let plan = runtime_plan(&compose);
    let script = project.join("experiment.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    // A record carrying provenance exercises the provenance block.
    let record = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "44444",
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions {
            provenance: Some(JobProvenance {
                tool_version: "9.9.9".to_string(),
                git: Some(GitProvenance {
                    sha: "deadbeef".to_string(),
                    dirty: false,
                    branch: Some("main".to_string()),
                }),
                image_refs: std::collections::BTreeMap::new(),
                source_content_hash: None,
            }),
            ..SubmissionRecordBuildOptions::default()
        },
    )
    .expect("record");
    write_submission_record(&record).expect("write record");

    // Write a manifest so `results` is populated by the pure manifest read.
    let manifest_path = artifact_manifest_path_for_record(&record);
    fs::create_dir_all(manifest_path.parent().expect("manifest parent")).expect("manifest dir");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": 1,
  "job_id": "44444",
  "collect_policy": "always",
  "collected_at": "2026-01-01T00:00:00Z",
  "job_outcome": "completed",
  "copied_relative_paths": ["out/metrics.json"]
}"#,
    )
    .expect("manifest");

    // Unknown job id yields the shared tracked-job hint (no scheduler contact).
    let missing = run_cli(
        &project,
        &[
            "experiment",
            "show",
            "00000",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert!(!missing.status.success());
    assert!(stderr_text(&missing).contains("was not found"));

    // Happy path: one JSON object with the aggregate keys. No fake scheduler
    // bins are provided, so build_status_snapshot degrades to placeholders.
    let out = run_cli(
        &project,
        &[
            "experiment",
            "show",
            "44444",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("experiment json");
    assert_eq!(value["job_id"], Value::from("44444"));
    assert_eq!(value["name"], Value::from("experiment-test"));
    assert!(value.get("state").is_some(), "state present");
    // services[] carries per-service rows; the TCP service gets a tunnel hint.
    let services = value["services"].as_array().expect("services array");
    assert_eq!(services.len(), 2);
    let api = services
        .iter()
        .find(|service| service["name"] == "api")
        .expect("api service");
    let hint = api["tunnel_hint"].as_str().expect("tunnel_hint");
    assert!(hint.contains("-L 8000:"), "forward present: {hint}");
    assert!(hint.contains("ControlMaster=auto"), "multiplexing: {hint}");
    // provenance block round-trips from the record.
    assert_eq!(value["provenance"]["tool_version"], Value::from("9.9.9"));
    assert_eq!(value["provenance"]["git"]["sha"], Value::from("deadbeef"));
    // results came from the pure manifest read.
    assert_eq!(
        value["results"]["copied_relative_paths"],
        serde_json::json!(["out/metrics.json"])
    );
    // next_commands names only shipped commands plus the ssh multiplex hint.
    let next = value["next_commands"].as_array().expect("next_commands");
    assert!(
        next.iter()
            .any(|command| command.as_str().is_some_and(
                |command| command.starts_with("ssh ") && command.contains("ControlMaster=auto")
            )),
        "ssh multiplex hint present in next_commands"
    );

    // Read-only: the command writes no files and never creates the export dir.
    assert!(!project.join("results").exists());
}

/// Writes one tracked main-kind record for `job_id` under the compose file's
/// metadata root, pinning `submitted_at` so "latest" ordering is deterministic.
fn write_tag_note_record(
    compose: &std::path::Path,
    project: &std::path::Path,
    job_id: &str,
    submitted_at: u64,
) {
    let plan = runtime_plan(compose);
    let script = project.join(format!("{job_id}.sbatch"));
    fs::write(&script, "#!/bin/bash\n").expect("script");
    let mut record =
        build_submission_record(compose, project, &script, &plan, job_id).expect("record");
    record.submitted_at = submitted_at;
    write_submission_record(&record).expect("write record");
}

#[test]
fn experiment_tag_round_trips_through_show_and_jobs_list() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, cache_root.path());
    write_tag_note_record(&compose, &project, "55555", 10);
    let compose_arg = compose.to_str().expect("path");

    // Add two tags; the JSON output is the sorted full tag set.
    let out = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "lr-bug",
            "baseline",
            "--job-id",
            "55555",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("tag json");
    assert_eq!(value["job_id"], Value::from("55555"));
    assert_eq!(value["tags"], serde_json::json!(["baseline", "lr-bug"]));

    // Re-adding an existing tag is an idempotent no-op, not an error.
    let out = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "baseline",
            "--job-id",
            "55555",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("tag json");
    assert_eq!(value["tags"], serde_json::json!(["baseline", "lr-bug"]));

    // Tags surface in `experiment show` (json + text).
    let show = run_cli(
        &project,
        &[
            "experiment",
            "show",
            "55555",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&show);
    let value: Value = serde_json::from_str(&stdout_text(&show)).expect("show json");
    assert_eq!(value["tags"], serde_json::json!(["baseline", "lr-bug"]));
    let show_text = run_cli(
        &project,
        &["experiment", "show", "55555", "-f", compose_arg],
    );
    assert_success(&show_text);
    assert!(stdout_text(&show_text).contains("tags:  baseline, lr-bug"));

    // Tags surface in `jobs list` (json + text rows).
    let jobs = run_cli(tmpdir.path(), &["jobs", "list", "--format", "json"]);
    assert_success(&jobs);
    let payload: Value = serde_json::from_str(&stdout_text(&jobs)).expect("jobs json");
    let job = payload["jobs"]
        .as_array()
        .expect("jobs array")
        .iter()
        .find(|job| job["job_id"] == "55555")
        .expect("job 55555")
        .clone();
    assert_eq!(job["tags"], serde_json::json!(["baseline", "lr-bug"]));
    let jobs_text = run_cli(tmpdir.path(), &["jobs", "list"]);
    assert_success(&jobs_text);
    assert!(stdout_text(&jobs_text).contains("tags=baseline,lr-bug"));

    // Removing works and removing an absent tag is a no-op.
    let out = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "--remove",
            "lr-bug",
            "--remove",
            "never-there",
            "--job-id",
            "55555",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&out);
    let value: Value = serde_json::from_str(&stdout_text(&out)).expect("tag json");
    assert_eq!(value["tags"], serde_json::json!(["baseline"]));

    // Text confirmation names the job and the remaining tag set.
    let text = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "--remove",
            "baseline",
            "--job-id",
            "55555",
            "-f",
            compose_arg,
        ],
    );
    assert_success(&text);
    assert!(stdout_text(&text).contains("tags for job 55555: (none)"));
}

#[test]
fn experiment_note_appends_in_order_with_timestamps() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, cache_root.path());
    write_tag_note_record(&compose, &project, "66666", 10);
    let compose_arg = compose.to_str().expect("path");

    let first = run_cli(
        &project,
        &[
            "experiment",
            "note",
            "diverged after epoch 3",
            "--job-id",
            "66666",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&first);
    let second = run_cli(
        &project,
        &[
            "experiment",
            "note",
            "  restarting with lower lr  ",
            "--job-id",
            "66666",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&second);
    let value: Value = serde_json::from_str(&stdout_text(&second)).expect("note json");
    assert_eq!(value["job_id"], Value::from("66666"));
    let notes = value["notes"].as_array().expect("notes array");
    assert_eq!(notes.len(), 2);
    // Append-only order with trimming and real timestamps.
    assert_eq!(notes[0]["text"], Value::from("diverged after epoch 3"));
    assert_eq!(notes[1]["text"], Value::from("restarting with lower lr"));
    let first_at = notes[0]["created_at"].as_u64().expect("created_at");
    let second_at = notes[1]["created_at"].as_u64().expect("created_at");
    assert!(first_at > 0);
    assert!(second_at >= first_at);

    // Notes surface in `experiment show` (json + text) and count in jobs list.
    let show = run_cli(
        &project,
        &[
            "experiment",
            "show",
            "66666",
            "-f",
            compose_arg,
            "--format",
            "json",
        ],
    );
    assert_success(&show);
    let value: Value = serde_json::from_str(&stdout_text(&show)).expect("show json");
    let notes = value["notes"].as_array().expect("show notes");
    assert_eq!(notes.len(), 2);
    let show_text = run_cli(
        &project,
        &["experiment", "show", "66666", "-f", compose_arg],
    );
    assert_success(&show_text);
    let text = stdout_text(&show_text);
    assert!(text.contains("Notes:"), "notes section: {text}");
    assert!(text.contains("diverged after epoch 3"), "note text: {text}");

    let jobs = run_cli(tmpdir.path(), &["jobs", "list", "--format", "json"]);
    assert_success(&jobs);
    let payload: Value = serde_json::from_str(&stdout_text(&jobs)).expect("jobs json");
    let job = payload["jobs"]
        .as_array()
        .expect("jobs array")
        .iter()
        .find(|job| job["job_id"] == "66666")
        .expect("job 66666")
        .clone();
    assert_eq!(job["note_count"], Value::from(2));
}

#[test]
fn experiment_tag_never_repoints_or_stales_the_latest_pointer() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, cache_root.path());
    write_tag_note_record(&compose, &project, "11111", 10);
    write_tag_note_record(&compose, &project, "22222", 20);
    let compose_arg = compose.to_str().expect("path");

    // Tagging the OLDER job must not repoint (or even rewrite) latest.json.
    let latest_path = latest_record_path_for(&compose);
    let pointer_before = fs::read(&latest_path).expect("latest before");
    let out = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "old-run",
            "--job-id",
            "11111",
            "-f",
            compose_arg,
        ],
    );
    assert_success(&out);
    let pointer_after = fs::read(&latest_path).expect("latest after");
    assert_eq!(
        pointer_before, pointer_after,
        "tagging a non-latest job must leave the latest pointer byte-identical"
    );

    // Tagging the LATEST job updates both the record and the pointer duplicate.
    let out = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "fresh",
            "--job-id",
            "22222",
            "-f",
            compose_arg,
        ],
    );
    assert_success(&out);
    let record = load_submission_record(&compose, Some("22222")).expect("record");
    assert_eq!(record.tags, vec!["fresh".to_string()]);
    let pointer: SubmissionRecord =
        serde_json::from_str(&fs::read_to_string(&latest_path).expect("latest"))
            .expect("latest record");
    assert_eq!(
        pointer.job_id, "22222",
        "pointer still names the latest job"
    );
    assert_eq!(pointer.tags, vec!["fresh".to_string()], "duplicate synced");

    // The no-id default path still resolves the newest job (with its tag).
    let show = run_cli(
        &project,
        &["experiment", "show", "-f", compose_arg, "--format", "json"],
    );
    assert_success(&show);
    let value: Value = serde_json::from_str(&stdout_text(&show)).expect("show json");
    assert_eq!(value["job_id"], Value::from("22222"));
    assert_eq!(value["tags"], serde_json::json!(["fresh"]));
}

#[test]
fn experiment_tag_and_note_reject_invalid_input() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let project = tmpdir.path().to_path_buf();
    let compose = write_prepare_compose(&project, cache_root.path());
    write_tag_note_record(&compose, &project, "77777", 10);
    let compose_arg = compose.to_str().expect("path");

    // Invalid charset fails cleanly and mutates nothing.
    let bad = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "bad tag!",
            "--job-id",
            "77777",
            "-f",
            compose_arg,
        ],
    );
    assert!(!bad.status.success());
    assert!(
        stderr_text(&bad).contains("unsupported characters"),
        "got: {}",
        stderr_text(&bad)
    );
    let record = load_submission_record(&compose, Some("77777")).expect("record");
    assert!(record.tags.is_empty(), "failed tag must not be persisted");

    // Neither tags nor --remove is a usage error.
    let empty = run_cli(
        &project,
        &["experiment", "tag", "--job-id", "77777", "-f", compose_arg],
    );
    assert!(!empty.status.success());
    assert!(
        stderr_text(&empty).contains("at least one tag"),
        "got: {}",
        stderr_text(&empty)
    );

    // A whitespace-only note is rejected.
    let blank = run_cli(
        &project,
        &[
            "experiment",
            "note",
            "   ",
            "--job-id",
            "77777",
            "-f",
            compose_arg,
        ],
    );
    assert!(!blank.status.success());
    assert!(
        stderr_text(&blank).contains("must not be empty"),
        "got: {}",
        stderr_text(&blank)
    );

    // An unknown job id yields the shared tracked-job hint.
    let missing = run_cli(
        &project,
        &[
            "experiment",
            "tag",
            "baseline",
            "--job-id",
            "00000",
            "-f",
            compose_arg,
        ],
    );
    assert!(!missing.status.success());
    assert!(
        stderr_text(&missing).contains("was not found"),
        "got: {}",
        stderr_text(&missing)
    );
}

fn results_trial(trial_id: &str, index: usize, vars: &[(&str, &str)]) -> SweepManifestTrial {
    SweepManifestTrial {
        trial_id: trial_id.to_string(),
        index,
        variables: vars
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
        config_key: String::new(),
        replicate: 0,
        seed: None,
        script_path: PathBuf::from(format!("{trial_id}.sbatch")),
        job_id: None,
        record_path: None,
        submitted_at: None,
        submit_error: None,
        objective: None,
        objective_error: None,
        observed_at: None,
    }
}

#[test]
fn sweep_results_tabulates_trials_and_leaves_manifest_unchanged() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sweep_id = "sweep-results-test";
    // Two trials with differing variable keys exercise the sorted column union;
    // no job ids means status is "unknown" with zero scheduler contact.
    let manifest = SweepManifest {
        schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
        sweep_id: sweep_id.to_string(),
        compose_file: compose.clone(),
        submitted_at: 1,
        matrix: "full".to_string(),
        compose_file_sha256: None,
        seed: None,
        total_combinations: 2,
        objective: None,
        best_trial: None,
        stopped_at: None,
        stop_reason: None,
        trials: vec![
            results_trial("t000", 0, &[("lr", "0.1"), ("bs", "32")]),
            results_trial("t001", 1, &[("lr", "0.2"), ("wd", "0.01")]),
        ],
    };
    write_sweep_manifest(&manifest).expect("write manifest");
    let manifest_path = sweep_manifest_path_for(&compose, sweep_id);
    let before = fs::read(&manifest_path).expect("manifest bytes before");

    // CSV: header is trial_id, index, sorted-union vars (bs, lr, wd), status, objective.
    let csv = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "results",
            "-f",
            compose.to_str().expect("path"),
            "--sweep-id",
            sweep_id,
            "--format",
            "csv",
        ],
    );
    assert_success(&csv);
    let csv_out = stdout_text(&csv);
    let mut lines = csv_out.lines();
    assert_eq!(
        lines.next().expect("csv header"),
        "\"trial_id\",\"index\",\"bs\",\"lr\",\"wd\",\"status\",\"objective\""
    );
    let row0 = lines.next().expect("csv row 0");
    // t000 has bs+lr but no wd (empty cell); status is unknown with no job id.
    assert!(
        row0.starts_with("\"t000\",\"0\",\"32\",\"0.1\",\"\",\"unknown\","),
        "unexpected row: {row0}"
    );

    // JSON: stable sorted variable_columns and one row per trial.
    let json = run_cli(
        tmpdir.path(),
        &[
            "sweep",
            "results",
            "-f",
            compose.to_str().expect("path"),
            "--sweep-id",
            sweep_id,
            "--format",
            "json",
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("sweep results json");
    assert_eq!(value["sweep_id"], Value::from("sweep-results-test"));
    assert_eq!(
        value["variable_columns"],
        serde_json::json!(["bs", "lr", "wd"])
    );
    assert_eq!(value["rows"].as_array().expect("rows").len(), 2);

    // Read-only: unlike `sweep observe`, results must not rewrite the manifest.
    let after = fs::read(&manifest_path).expect("manifest bytes after");
    assert_eq!(before, after, "sweep results modified the manifest");
}

#[test]
fn submit_watch_queue_warns_once_when_pending_past_threshold() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let squeue_state = tmpdir.path().join("watch-queue-pending-squeue.state");
    let sacct_state = tmpdir.path().join("watch-queue-pending-sacct.state");
    let real_squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    let service_log = log_dir.join(log_file_name_for_service("app"));

    // Invocation-counted squeue (see `submit_watch_queue_waits_for_running_...`
    // for the pattern). Keeping the job PENDING for two polls guarantees the
    // wait loop polls twice while pending; the second poll is one `POLL_INTERVAL`
    // (1s) after the first, so the >=1s queue-warn threshold is crossed
    // deterministically instead of relying on a wall-clock `sleep`.
    let counter_file = tmpdir.path().join("watch-queue-pending-squeue.count");
    let latch_file = tmpdir.path().join("watch-queue-pending-squeue.latch");
    let squeue = tmpdir.path().join("squeue-counter-pending");
    write_script(
        &squeue,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
count="$(cat '{counter}' 2>/dev/null || echo 0)"
count=$((count + 1))
printf '%s\n' "$count" > '{counter}'
if [[ -e '{latch}' ]]; then
  printf 'NONE\n' > '{squeue_state}'
elif (( count <= 2 )); then
  cat > '{squeue_state}' <<'PENDING_STATE'
STATE=PENDING
REASON=Priority
START=2026-04-07T12:34:56
PENDING_STATE
else
  printf 'RUNNING\n' > '{squeue_state}'
  mkdir -p '{log_dir}'
  printf 'booting\nready\n' > '{service_log}'
  printf 'COMPLETED\n' > '{sacct_state}'
  : > '{latch}'
fi
exec '{real_squeue}' "$@"
"#,
            counter = counter_file.display(),
            latch = latch_file.display(),
            squeue_state = squeue_state.display(),
            log_dir = log_dir.display(),
            service_log = service_log.display(),
            sacct_state = sacct_state.display(),
            real_squeue = real_squeue.display(),
        ),
    );

    let sbatch = tmpdir.path().join("sbatch-watch-queue-pending");
    write_script(
        &sbatch,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
mkdir -p '{}'
cat > '{}' <<'PENDING_STATE'
STATE=PENDING
REASON=Priority
START=2026-04-07T12:34:56
PENDING_STATE
rm -f '{}'
echo "Submitted batch job 12345"
"#,
            log_dir.display(),
            squeue_state.display(),
            sacct_state.display(),
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--watch-queue",
            "--queue-warn-after",
            "1s",
            "--watch-mode",
            "line",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("pending reason: Priority"));
    assert!(stdout.contains("start time: 2026-04-07T12:34:56"));
    assert!(stdout.contains("watching job 12345"));
    let stderr = stderr_text(&output);
    assert!(stderr.contains("warning: job 12345 still PENDING after 00:00:01"));
    assert!(stderr.contains("pending reason: Priority"));
    assert_eq!(stderr.matches("still PENDING").count(), 1);
}

#[test]
fn submit_watch_queue_rejects_incompatible_flags() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            vec!["up", "--watch-queue", "--detach"],
            "the argument '--watch-queue' cannot be used with '--detach'",
        ),
        (
            vec!["up", "--watch-queue", "--dry-run"],
            "the argument '--watch-queue' cannot be used with '--dry-run'",
        ),
        (
            vec!["up", "--watch-queue", "--local"],
            "the argument '--watch-queue' cannot be used with '--local'",
        ),
        (vec!["up", "--queue-warn-after", "1s"], "--watch-queue"),
    ];
    for (args, expected) in cases {
        let output = run_cli(tmpdir.path(), &args);
        assert_failure(&output);
        assert!(
            stderr_text(&output).contains(expected),
            "stderr should contain {expected:?}; got {}",
            stderr_text(&output)
        );
    }
}

#[test]
fn submit_watch_skips_when_job_id_is_not_trackable() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sbatch = tmpdir.path().join("sbatch-no-job-id");
    write_script(
        &sbatch,
        "#!/bin/bash\nset -euo pipefail\necho 'submitted without parsable id'\n",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--watch-mode",
            "line",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let stdout = stdout_text(&submit);
    assert!(stdout.contains("did not include a numeric Slurm job id"));
    assert!(stdout.contains("skipping watch because the submission is not trackable"));
}

#[test]
fn logs_follow_streams_appended_lines() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "start\n").expect("log");

    let mut child = Command::new(bin_path())
        .current_dir(tmpdir.path())
        .args([
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--follow",
            "--grep",
            "follow",
            "--lines",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn logs");

    thread::sleep(Duration::from_millis(250));
    let mut file = OpenOptions::new()
        .append(true)
        .open(&log_path)
        .expect("open log");
    writeln!(file, "ignored-line").expect("append ignored");
    writeln!(file, "follow-line").expect("append");
    file.flush().expect("flush");
    thread::sleep(Duration::from_millis(1400));
    child.kill().expect("kill");
    let output = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[app] follow-line"));
    assert!(!stdout.contains("ignored-line"));
}

#[test]
fn diff_command_reports_compact_resource_and_outcome_changes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");

    let first_snapshot = r#"
name: demo
runtime:
  backend: pyxis
x-slurm:
  time: "00:10:00"
  cpus_per_task: 2
  mem: 4G
services:
  app:
    image: python:3.10
    command:
      - python
      - train.py
"#;
    let second_snapshot = r#"
name: demo
runtime:
  backend: pyxis
x-slurm:
  time: "00:20:00"
  cpus_per_task: 4
  mem: 8G
services:
  app:
    image: python:3.11
    command:
      - python
      - train.py
"#;
    let first_options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some(first_snapshot.to_string()),
        ..SubmissionRecordBuildOptions::default()
    };
    let second_options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some(second_snapshot.to_string()),
        ..SubmissionRecordBuildOptions::default()
    };
    let mut first = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "11111",
        SubmissionBackend::Local,
        &first_options,
    )
    .expect("first record");
    first.submitted_at = 10;
    let mut second = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "22222",
        SubmissionBackend::Local,
        &second_options,
    )
    .expect("second record");
    second.submitted_at = 20;
    first.provenance = Some(JobProvenance {
        tool_version: "0.0.1".to_string(),
        git: Some(GitProvenance {
            sha: "aaaaaaa".to_string(),
            dirty: false,
            branch: Some("main".to_string()),
        }),
        image_refs: std::collections::BTreeMap::new(),
        source_content_hash: None,
    });
    second.provenance = Some(JobProvenance {
        tool_version: "0.0.2".to_string(),
        git: Some(GitProvenance {
            sha: "bbbbbbb".to_string(),
            dirty: true,
            branch: Some("main".to_string()),
        }),
        image_refs: std::collections::BTreeMap::new(),
        source_content_hash: None,
    });
    write_submission_record(&first).expect("write first");
    write_submission_record(&second).expect("write second");
    let first_state_path = state_path_for_record(&first);
    let second_state_path = state_path_for_record(&second);
    fs::create_dir_all(first_state_path.parent().unwrap()).expect("first state dir");
    fs::create_dir_all(second_state_path.parent().unwrap()).expect("second state dir");
    fs::write(
        &first_state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": "local",
            "job_status": "FAILED",
            "job_exit_code": 1,
            "supervisor_pid": null,
            "services": [{"service_name": "app", "last_exit_code": 1}]
        }))
        .expect("first state json"),
    )
    .expect("first state");
    fs::write(
        &second_state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": "local",
            "job_status": "COMPLETED",
            "job_exit_code": 0,
            "supervisor_pid": null,
            "services": [{"service_name": "app", "last_exit_code": 0, "completed_successfully": true}]
        }))
        .expect("second state json"),
    )
    .expect("second state");

    let text = run_cli(tmpdir.path(), &["diff", "11111", "22222"]);
    assert_success(&text);
    let text_stdout = stdout_text(&text);
    assert!(text_stdout.contains("11111"));
    assert!(text_stdout.contains("22222"));
    assert!(text_stdout.contains("scheduler.state"));
    assert!(text_stdout.contains("x-slurm.time"));
    assert!(text_stdout.contains("services.app.image"));

    let json = run_cli(
        tmpdir.path(),
        &["diff", "11111", "22222", "--format", "json"],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("diff json");
    assert_eq!(value["left"]["job_id"], Value::from("11111"));
    assert_eq!(value["right"]["job_id"], Value::from("22222"));
    assert!(
        value["resource_changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["path"] == "x-slurm.time")
    );
    // Provenance pinned on each record (#9) is surfaced as provenance_changes.
    let provenance_changes = value["provenance_changes"]
        .as_array()
        .expect("provenance_changes array");
    assert!(
        provenance_changes
            .iter()
            .any(|change| change["path"] == "provenance.tool_version")
    );
    assert!(
        provenance_changes
            .iter()
            .any(|change| change["path"] == "provenance.git.sha")
    );
    assert!(text_stdout.contains("provenance.tool_version"));

    let third = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "33333",
        SubmissionBackend::Local,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("third record");
    write_submission_record(&third).expect("write third");
    let missing_snapshot = run_cli(tmpdir.path(), &["diff", "11111", "33333"]);
    assert_success(&missing_snapshot);
    assert!(stdout_text(&missing_snapshot).contains("config snapshot unavailable"));
}

#[test]
fn diff_malformed_config_snapshot_reports_note_and_keeps_outcome_changes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");

    let left_options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some("x-slurm: [not valid".to_string()),
        ..SubmissionRecordBuildOptions::default()
    };
    let right_options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some(
            "x-slurm:\n  time: \"00:20:00\"\nservices:\n  app:\n    image: python:3.11\n"
                .to_string(),
        ),
        ..SubmissionRecordBuildOptions::default()
    };
    let mut left = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "44444",
        SubmissionBackend::Local,
        &left_options,
    )
    .expect("left record");
    left.submitted_at = 10;
    let mut right = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "55555",
        SubmissionBackend::Local,
        &right_options,
    )
    .expect("right record");
    right.submitted_at = 20;
    write_submission_record(&left).expect("write left");
    write_submission_record(&right).expect("write right");

    let left_state_path = state_path_for_record(&left);
    let right_state_path = state_path_for_record(&right);
    fs::create_dir_all(left_state_path.parent().unwrap()).expect("left state dir");
    fs::create_dir_all(right_state_path.parent().unwrap()).expect("right state dir");
    fs::write(
        &left_state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": "local",
            "job_status": "COMPLETED",
            "job_exit_code": 0,
            "services": [{"service_name": "app", "last_exit_code": 0, "completed_successfully": true}]
        }))
        .expect("left state json"),
    )
    .expect("left state");
    fs::write(
        &right_state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": "local",
            "job_status": "FAILED",
            "job_exit_code": 7,
            "services": [{"service_name": "app", "last_exit_code": 7}]
        }))
        .expect("right state json"),
    )
    .expect("right state");

    let diff = run_cli(
        tmpdir.path(),
        &["diff", "44444", "55555", "--format", "json"],
    );
    assert_success(&diff);
    let value: Value = serde_json::from_str(&stdout_text(&diff)).expect("diff json");
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note.as_str().is_some_and(
                |note| note.contains("config snapshot for job 44444 could not be parsed")
            ))
    );
    assert!(
        value["outcome_changes"]
            .as_array()
            .expect("outcome changes")
            .iter()
            .any(|change| change["path"] == "scheduler.state")
    );
    assert!(
        value["outcome_changes"]
            .as_array()
            .expect("outcome changes")
            .iter()
            .any(|change| change["path"] == "services.app.last_exit_code")
    );
}

/// Builds and persists a completed local record with the given config snapshot
/// and a written state file, returning the record so callers can reference it.
fn write_matrix_record(
    compose: &std::path::Path,
    project: &std::path::Path,
    script: &std::path::Path,
    plan: &hpc_compose::prepare::RuntimePlan,
    job_id: &str,
    config_snapshot_yaml: &str,
) -> SubmissionRecord {
    let options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some(config_snapshot_yaml.to_string()),
        ..SubmissionRecordBuildOptions::default()
    };
    let record = build_submission_record_with_backend_and_options(
        compose,
        project,
        script,
        plan,
        job_id,
        SubmissionBackend::Local,
        &options,
    )
    .expect("matrix record");
    write_submission_record(&record).expect("write matrix record");
    let state_path = state_path_for_record(&record);
    fs::create_dir_all(state_path.parent().unwrap()).expect("state dir");
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "backend": "local",
            "job_status": "COMPLETED",
            "job_exit_code": 0,
            "services": [{"service_name": "app", "last_exit_code": 0, "completed_successfully": true}]
        }))
        .expect("state json"),
    )
    .expect("write state");
    record
}

#[test]
fn diff_jobs_builds_nway_matrix_text_and_csv() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");

    // Same partition across all three; differing time + per-service image.
    let snapshot = |time: &str, image: &str| {
        format!(
            "name: demo\nx-slurm:\n  partition: gpu\n  time: \"{time}\"\nservices:\n  app:\n    image: {image}\n",
        )
    };
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "11111",
        &snapshot("00:10:00", "python:3.10"),
    );
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "22222",
        &snapshot("00:20:00", "python:3.11"),
    );
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "33333",
        &snapshot("00:30:00", "python:3.12"),
    );

    // JSON: assert the run columns and a differing row's positionally-aligned cells.
    let json = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--jobs",
            "11111,22222,33333",
            "--matrix-format",
            "json",
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("matrix json");
    let runs = value["runs"].as_array().expect("runs array");
    assert_eq!(
        runs.iter()
            .map(|run| run["job_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["11111", "22222", "33333"]
    );
    let rows = value["rows"].as_array().expect("rows array");
    let time_row = rows
        .iter()
        .find(|row| row["path"] == "x-slurm.time")
        .expect("time row present");
    assert_eq!(time_row["section"], "resources");
    assert_eq!(
        time_row["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|cell| cell.as_str().unwrap())
            .collect::<Vec<_>>(),
        ["00:10:00", "00:20:00", "00:30:00"]
    );
    // The identical partition field is collapsed (no row).
    assert!(!rows.iter().any(|row| row["path"] == "x-slurm.partition"));

    // Text: legend lists every run, and differing rows are shown.
    let text = run_cli(tmpdir.path(), &["diff", "--jobs", "11111,22222,33333"]);
    assert_success(&text);
    let text_stdout = stdout_text(&text);
    assert!(text_stdout.contains("11111"));
    assert!(text_stdout.contains("22222"));
    assert!(text_stdout.contains("33333"));
    assert!(text_stdout.contains("x-slurm.time"));
    assert!(text_stdout.contains("services.app.image"));

    // CSV: header is section,field,<job_id>... with quoted cells.
    let csv = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--jobs",
            "11111,22222,33333",
            "--matrix-format",
            "csv",
        ],
    );
    assert_success(&csv);
    let csv_stdout = stdout_text(&csv);
    let header = csv_stdout.lines().next().expect("csv header");
    assert_eq!(
        header,
        "\"section\",\"field\",\"11111\",\"22222\",\"33333\""
    );
    assert!(csv_stdout.lines().any(|line| {
        line.starts_with("\"resources\",\"x-slurm.time\",")
            && line.contains("\"00:10:00\"")
            && line.contains("\"00:30:00\"")
    }));
}

#[test]
fn diff_across_sweep_skips_unsubmitted_trials_with_note() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");

    let snapshot = |time: &str| {
        format!(
            "name: demo\nx-slurm:\n  partition: gpu\n  time: \"{time}\"\nservices:\n  app:\n    image: python:3.10\n"
        )
    };
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "70001",
        &snapshot("00:10:00"),
    );
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "70002",
        &snapshot("00:20:00"),
    );

    let sweep_id = "sweep-across-test";
    let mut submitted_a = results_trial("t000", 0, &[("lr", "0.1")]);
    submitted_a.job_id = Some("70001".to_string());
    let mut submitted_b = results_trial("t001", 1, &[("lr", "0.2")]);
    submitted_b.job_id = Some("70002".to_string());
    // Third trial was never submitted (job_id None) -> skipped with a note.
    let unsubmitted = results_trial("t002", 2, &[("lr", "0.3")]);
    let manifest = SweepManifest {
        schema_version: SWEEP_MANIFEST_SCHEMA_VERSION,
        sweep_id: sweep_id.to_string(),
        compose_file: compose.clone(),
        submitted_at: 1,
        matrix: "full".to_string(),
        compose_file_sha256: None,
        seed: None,
        total_combinations: 3,
        objective: None,
        best_trial: None,
        stopped_at: None,
        stop_reason: None,
        trials: vec![submitted_a, submitted_b, unsubmitted],
    };
    write_sweep_manifest(&manifest).expect("write manifest");

    let json = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--across",
            sweep_id,
            "-f",
            compose.to_str().unwrap(),
            "--matrix-format",
            "json",
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("matrix json");
    // Only the two submitted trials become columns.
    assert_eq!(
        value["runs"]
            .as_array()
            .expect("runs")
            .iter()
            .map(|run| run["job_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["70001", "70002"]
    );
    // The unsubmitted trial is reported as a skip note.
    assert!(
        value["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .is_some_and(|note| note.contains("t002") && note.contains("not been submitted")))
    );
    // The differing time field is still surfaced across the submitted trials.
    assert!(
        value["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .any(|row| row["path"] == "x-slurm.time")
    );
}

#[test]
fn diff_single_job_bails_without_second_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_prepare_compose(&project, &cache_dir);
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    write_matrix_record(
        &compose,
        &project,
        &script,
        &plan,
        "90001",
        "name: demo\nservices:\n  app:\n    image: python:3.10\n",
    );

    // A single positional id (pairwise mode missing its second id) bails clearly.
    let output = run_cli(tmpdir.path(), &["diff", "90001"]);
    assert!(!output.status.success());
    assert!(stderr_text(&output).contains("pairwise diff requires two tracked job ids"));
}

/// Writes the `diff --against-spec` compose fixture: a swappable image tag and
/// an env value interpolated from `$SPEC_DIFF_LR` (default 0.001), so both a
/// file edit and an env-only change can be exercised.
fn write_spec_diff_compose(
    project: &std::path::Path,
    cache_dir: &std::path::Path,
    image: &str,
) -> PathBuf {
    write_compose(
        project,
        "compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {cache}
services:
  app:
    image: {image}
    command:
      - python
      - train.py
    environment:
      LEARNING_RATE: "${{SPEC_DIFF_LR:-0.001}}"
"#,
            cache = cache_dir.display(),
        ),
    )
}

/// Mints an effective-config snapshot through the same pipeline `up` uses (the
/// `config` command serializes the identical redacted effective config) and
/// persists it on a tracked record, so `diff --against-spec` compares
/// effective-vs-effective exactly like a real submission.
fn write_spec_diff_record(
    cwd: &std::path::Path,
    compose: &std::path::Path,
    project: &std::path::Path,
    script: &std::path::Path,
    plan: &hpc_compose::prepare::RuntimePlan,
    job_id: &str,
    lr: &str,
) -> SubmissionRecord {
    let config = run_cli_with_env(
        cwd,
        &["config", "-f", compose.to_str().expect("path")],
        &[("SPEC_DIFF_LR", lr)],
    );
    assert_success(&config);
    let snapshot_yaml = stdout_text(&config);
    assert!(
        snapshot_yaml.contains("LEARNING_RATE"),
        "snapshot must carry the interpolated env value:\n{snapshot_yaml}"
    );
    let options = SubmissionRecordBuildOptions {
        config_snapshot_yaml: Some(snapshot_yaml),
        ..SubmissionRecordBuildOptions::default()
    };
    let record = build_submission_record_with_backend_and_options(
        compose,
        project,
        script,
        plan,
        job_id,
        SubmissionBackend::Local,
        &options,
    )
    .expect("spec diff record");
    write_submission_record(&record).expect("write spec diff record");
    record
}

#[test]
fn diff_against_spec_reports_file_and_env_changes_since_snapshot() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_spec_diff_compose(&project, &cache_dir, "python:3.11-slim");
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    write_spec_diff_record(
        tmpdir.path(),
        &compose,
        &project,
        &script,
        &plan,
        "31111",
        "0.001",
    );

    // Untouched file + same env -> explicitly no changes (latest record is
    // resolved without --job-id).
    let clean = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("SPEC_DIFF_LR", "0.001")],
    );
    assert_success(&clean);
    assert!(
        stdout_text(&clean).contains("no changes since job 31111"),
        "expected the clean banner, got:\n{}",
        stdout_text(&clean)
    );

    // Edit the compose file (image tag) AND change the env var: both surface,
    // the env change at its resolved path even though the file text for it is
    // untouched.
    write_spec_diff_compose(&project, &cache_dir, "python:3.12-slim");
    let drifted = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--job-id",
            "31111",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("SPEC_DIFF_LR", "0.002")],
    );
    assert_success(&drifted);
    let text = stdout_text(&drifted);
    assert!(text.contains("changes since job 31111"), "got:\n{text}");
    assert!(text.contains("services.app.image"), "got:\n{text}");
    assert!(
        text.contains("python:3.11-slim -> python:3.12-slim"),
        "got:\n{text}"
    );
    assert!(
        text.contains("services.app.environment.LEARNING_RATE"),
        "got:\n{text}"
    );

    // JSON carries the same rows under the versioned diff-spec envelope; left
    // is the past run's snapshot, right is the current spec.
    let json = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
        &[("SPEC_DIFF_LR", "0.002")],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("diff-spec json");
    assert_eq!(value["schema_version"], Value::from(1));
    assert_eq!(value["job_id"], Value::from("31111"));
    assert!(
        value["resource_changes"]
            .as_array()
            .expect("resource changes")
            .iter()
            .any(|change| change["path"] == "services.app.image")
    );
    assert!(
        value["config_changes"]
            .as_array()
            .expect("config changes")
            .iter()
            .any(
                |change| change["path"] == "services.app.environment.LEARNING_RATE"
                    && change["left"] == "0.001"
                    && change["right"] == "0.002"
            )
    );
}

#[test]
fn diff_against_spec_fail_on_change_gates_exit_code() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_spec_diff_compose(&project, &cache_dir, "python:3.11-slim");
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    write_spec_diff_record(
        tmpdir.path(),
        &compose,
        &project,
        &script,
        &plan,
        "31112",
        "0.001",
    );

    // No drift -> exit 0 with or without the gate.
    let clean = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--fail-on-change",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("SPEC_DIFF_LR", "0.001")],
    );
    assert_success(&clean);
    assert!(stdout_text(&clean).contains("no changes since job 31112"));

    // Drift (env-only change) -> the report still prints on stdout, and the
    // gate fails the command with the generic first-party code 1.
    let gated = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--fail-on-change",
            "-f",
            compose.to_str().expect("path"),
        ],
        &[("SPEC_DIFF_LR", "0.002")],
    );
    assert_failure(&gated);
    assert_eq!(gated.status.code(), Some(1));
    assert!(stdout_text(&gated).contains("services.app.environment.LEARNING_RATE"));
    assert!(stderr_text(&gated).contains("config changed since job 31112"));

    // JSON mode: the full diff-spec envelope is still flushed to stdout before
    // the gate fails the command.
    let gated_json = run_cli_with_env(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--fail-on-change",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
        &[("SPEC_DIFF_LR", "0.002")],
    );
    assert_failure(&gated_json);
    assert_eq!(gated_json.status.code(), Some(1));
    let value: Value = serde_json::from_str(&stdout_text(&gated_json)).expect("diff-spec json");
    assert_eq!(value["schema_version"], Value::from(1));
    assert!(
        value["config_changes"]
            .as_array()
            .expect("config changes")
            .iter()
            .any(|change| change["path"] == "services.app.environment.LEARNING_RATE")
    );
}

#[test]
fn diff_against_spec_reapplies_sweep_trial_variables() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_spec_diff_compose(&project, &cache_dir, "python:3.11-slim");
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");

    // Mint the snapshot exactly as the trial ran: SPEC_DIFF_LR swept to 0.5,
    // away from the compose default of 0.001.
    let config = run_cli_with_env(
        tmpdir.path(),
        &["config", "-f", compose.to_str().expect("path")],
        &[("SPEC_DIFF_LR", "0.5")],
    );
    assert_success(&config);
    let options = SubmissionRecordBuildOptions {
        kind: SubmissionKind::SweepTrial,
        config_snapshot_yaml: Some(stdout_text(&config)),
        sweep: Some(SweepTrialMetadata {
            sweep_id: "lr-sweep".to_string(),
            trial_id: "t0".to_string(),
            trial_index: 0,
            variables: std::collections::BTreeMap::from([(
                "SPEC_DIFF_LR".to_string(),
                "0.5".to_string(),
            )]),
        }),
        ..SubmissionRecordBuildOptions::default()
    };
    let record = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "31113",
        SubmissionBackend::Local,
        &options,
    )
    .expect("sweep trial record");
    write_submission_record(&record).expect("write sweep trial record");

    // No env set here: without the overlay the compose default (0.001) would
    // read as drift against the trial's 0.5 even though the spec is untouched.
    let clean = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--job-id",
            "31113",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let text = stdout_text(&clean);
    assert!(
        text.contains("no changes since job 31113"),
        "swept variables must not read as drift:\n{text}"
    );
    assert!(text.contains("sweep trial (lr-sweep/t0)"), "got:\n{text}");

    // A real spec edit still surfaces through the overlay, while the swept
    // variable stays quiet.
    write_spec_diff_compose(&project, &cache_dir, "python:3.12-slim");
    let drifted = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--job-id",
            "31113",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&drifted);
    let text = stdout_text(&drifted);
    assert!(text.contains("services.app.image"), "got:\n{text}");
    assert!(!text.contains("LEARNING_RATE"), "got:\n{text}");
}

#[test]
fn diff_against_spec_without_snapshot_bails_cleanly() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".git")).expect("git root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let project = tmpdir.path().join("project");
    fs::create_dir_all(&project).expect("project");
    let compose = write_spec_diff_compose(&project, &cache_dir, "python:3.11-slim");
    let plan = runtime_plan(&compose);
    let script = project.join("job.sbatch");
    fs::write(&script, "#!/bin/bash\n").expect("script");
    // A record built without a config snapshot (like `run`-style submissions
    // and pre-snapshot records).
    let record = build_submission_record_with_backend_and_options(
        &compose,
        &project,
        &script,
        &plan,
        "32222",
        SubmissionBackend::Local,
        &SubmissionRecordBuildOptions::default(),
    )
    .expect("record without snapshot");
    write_submission_record(&record).expect("write record");

    let output = run_cli(
        tmpdir.path(),
        &[
            "diff",
            "--against-spec",
            "--job-id",
            "32222",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(
        stderr_text(&output).contains("has no config snapshot to compare against"),
        "expected the clean no-snapshot bail, got:\n{}",
        stderr_text(&output)
    );
}

#[test]
fn submit_dry_run_skips_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("dry-run.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--dry-run",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let out = stdout_text(&output);
    assert!(out.contains("dry run: skipping sbatch submission"));
    assert!(!out.contains("Submitted batch job"));
    assert!(script_out.exists());
}

#[test]
fn up_dry_run_skips_preflight_prepare_scheduler_tools_and_locks() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_prepare_compose(tmpdir.path(), cache_root.path());
    let tool_log = tmpdir.path().join("tool-invocations.log");
    let enroot = write_failing_tool(tmpdir.path(), "enroot-fail", &tool_log);
    let srun = write_failing_tool(tmpdir.path(), "srun-fail", &tool_log);
    let sbatch = write_failing_tool(tmpdir.path(), "sbatch-fail", &tool_log);
    let apptainer = write_failing_tool(tmpdir.path(), "apptainer-fail", &tool_log);
    let singularity = write_failing_tool(tmpdir.path(), "singularity-fail", &tool_log);
    let script_out = tmpdir.path().join("static-preview.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--dry-run",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
            "--apptainer-bin",
            apptainer.to_str().expect("path"),
            "--singularity-bin",
            singularity.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("dry run: skipping sbatch submission"));
    assert!(
        script_out.exists(),
        "explicit --script-out is still honored"
    );
    assert!(
        !tool_log.exists(),
        "dry-run must not invoke preflight, prepare, or scheduler tools:\n{}",
        fs::read_to_string(&tool_log).unwrap_or_default()
    );
    assert!(
        !tmpdir.path().join(".hpc-compose/locks").exists(),
        "dry-run should not create an up invocation lock directory"
    );
    let plan = runtime_plan(&compose);
    assert!(
        !plan.ordered_services[0].runtime_image.exists(),
        "dry-run must not prepare runtime artifacts"
    );
}

#[test]
fn offline_rejects_real_up_remote_before_side_effects() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_prepare_compose(tmpdir.path(), cache_root.path());

    let output = run_cli(
        tmpdir.path(),
        &[
            "--offline",
            "up",
            "--remote=fakehost",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--offline forbids real up submission"));
}

#[test]
fn offline_rejects_weather_but_allows_static_validate() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let weather = run_cli(tmpdir.path(), &["--offline", "weather"]);
    assert_failure(&weather);
    assert!(stderr_text(&weather).contains("--offline forbids Slurm scheduler weather queries"));

    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "services:\n  app:\n    image: python:3.12\n    command: /bin/true\n",
    );
    let validate = run_cli(
        tmpdir.path(),
        &[
            "--offline",
            "validate",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&validate);
}

#[test]
fn submit_preflight_error_does_not_call_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing_image = tmpdir.path().join("missing.sqsh");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
"#,
            missing_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch_log = tmpdir.path().join("sbatch.log");
    let sbatch = write_fake_sbatch_with_log(tmpdir.path(), &sbatch_log);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let stderr = stderr_text(&submit);
    assert!(stderr.contains("preflight failed"));
    assert!(stderr.contains("local image for service 'app' does not exist"));
    assert!(
        !sbatch_log.exists()
            || fs::read_to_string(&sbatch_log)
                .expect("sbatch log")
                .is_empty()
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn submit_local_rejects_non_linux_hosts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "services:\n  app:\n    image: {}\n    command: /bin/true\n",
            local_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("--local is only supported on Linux hosts"));
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_dry_run_renders_local_launcher() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            "services:\n  app:\n    image: {}\n    command: /bin/true\n",
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let script_out = tmpdir.path().join("local-launch.sh");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--dry-run",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("submit json");
    assert_eq!(payload["backend"], Value::from("local"));
    assert_eq!(payload["launched"], Value::from(false));
    assert_eq!(payload["submitted"], Value::from(false));
    assert!(script_out.exists());
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(!script.contains("#SBATCH"));
    assert!(script.contains("HPC_COMPOSE_LOCAL_ENROOT_BIN"));
    assert!(script.contains("local srun shim requires --container-image"));
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_apptainer_dry_run_renders_apptainer_launcher() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sif");
    fs::write(&local_image, "sif").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
runtime:
  backend: apptainer
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let script_out = tmpdir.path().join("local-apptainer.sh");
    let apptainer = tmpdir.path().join("site-apptainer");
    write_script(
        &apptainer,
        "#!/bin/bash\nprintf 'fake apptainer\\n' >/dev/null\n",
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--dry-run",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--apptainer-bin",
            apptainer.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("submit json");
    assert_eq!(payload["backend"], Value::from("local"));
    let script = fs::read_to_string(&script_out).expect("script");
    assert!(script.contains(&format!(
        "local -a runtime_cmd=({} 'exec')",
        shell_quote(apptainer.to_str().expect("path"))
    )));
    assert!(script.contains("runtime_cmd+=(--bind \"$runtime_mounts\")"));
    assert!(!script.contains("srun_cmd+=(--container-image="));
}

#[cfg(target_os = "linux")]
#[test]
fn test_local_apptainer_runs_through_local_supervisor() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sif");
    fs::write(&local_image, "sif").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: local-apptainer-smoke
runtime:
  backend: apptainer
services:
  app:
    image: {}
    command: /bin/sh -lc "echo apptainer-local-ok"
"#,
            local_image.display()
        ),
    );
    let runtime_log = tmpdir.path().join("apptainer.log");
    let apptainer = tmpdir.path().join("apptainer");
    write_script(
        &apptainer,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> '{}'
cmd="${{1:-}}"
shift || true
case "$cmd" in
  exec|run)
    while (($#)); do
      case "$1" in
        --bind)
          shift 2
          ;;
        --nv)
          shift
          ;;
        --*)
          shift
          ;;
        *)
          shift
          break
          ;;
      esac
    done
    if [[ "$cmd" == "run" && $# == 0 ]]; then
      exit 0
    fi
    exec "$@"
    ;;
  *)
    exit 0
    ;;
esac
"#,
            runtime_log.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--timeout",
            "20s",
            "-f",
            compose.to_str().expect("path"),
            "--apptainer-bin",
            apptainer.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("smoke test passed"));
    let log = fs::read_to_string(runtime_log).expect("runtime log");
    assert!(log.contains("exec"));
    assert!(log.contains(local_image.to_str().expect("image path")));
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_still_rejects_singularity_backend() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sif");
    fs::write(&local_image, "sif").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
runtime:
  backend: singularity
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--dry-run",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains(
        "--local currently supports only runtime.backend=pyxis or runtime.backend=apptainer"
    ));
}

fn write_fake_devcluster_checkout(tmpdir: &Path, exec_status: i32) -> PathBuf {
    fs::create_dir_all(tmpdir.join("scripts")).expect("scripts dir");
    fs::create_dir_all(tmpdir.join("dev-cluster")).expect("dev-cluster dir");
    fs::write(
        tmpdir.join("dev-cluster/compose.yaml"),
        "name: fake-devcluster\n",
    )
    .expect("compose");
    let log = tmpdir.join("devcluster.log");
    let script = tmpdir.join("scripts/devcluster.sh");
    write_script(
        &script,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
cmd="${{1:-}}"
printf '%s' "$cmd" >> '{}'
shift || true
for arg in "$@"; do
  printf ' <%s>' "$arg" >> '{}'
done
printf '\n' >> '{}'
case "$cmd" in
  up)
    exit 0
    ;;
  exec)
    echo "smoke test passed: 123"
    exit {exec_status}
    ;;
  *)
    exit 2
    ;;
esac
"#,
            log.display(),
            log.display(),
            log.display()
        ),
    );
    log
}

#[test]
fn test_submit_dev_cluster_delegates_to_checked_in_wrapper() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let log = write_fake_devcluster_checkout(tmpdir.path(), 0);
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
runtime:
  backend: host
services:
  app:
    command: /bin/true
"#,
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--dev-cluster",
            "--time",
            "00:02:00",
            "--timeout",
            "42s",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("smoke test passed: 123"));
    let log = fs::read_to_string(log).expect("devcluster log");
    let project_dir = fs::canonicalize(tmpdir.path()).expect("canonical tmpdir");
    assert!(log.contains(&format!("up <--project> <{}>", project_dir.display())));
    assert!(log.contains(
        "exec <hpc-compose> <test> <--submit> <--time> <00:02:00> <--timeout> <42s> <-f> </workspace/compose.yaml>"
    ));
    assert!(log.contains("<--skip-prepare>"));
    assert!(log.contains("<--no-preflight>"));
    assert!(log.contains("<--format> <json>"));
    assert!(!log.contains("--dev-cluster"));
}

#[test]
fn test_submit_dev_cluster_propagates_child_exit_status() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let _log = write_fake_devcluster_checkout(tmpdir.path(), 7);
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
runtime:
  backend: host
services:
  app:
    command: /bin/true
"#,
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "test",
            "--submit",
            "--dev-cluster",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert_eq!(output.status.code(), Some(7));
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_lifecycle_covers_status_ps_watch_artifacts_and_stats() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  artifacts:
    export_dir: ./exports/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/result.txt
services:
  server:
    image: {}
    command: /bin/sh -lc "printf 'ready\n'; sleep 2"
    readiness:
      type: log
      pattern: ready
      timeout_seconds: 5
  client:
    image: {}
    command: /bin/sh -lc "cat \"$HPC_COMPOSE_NODELIST_FILE\" > /hpc-compose/job/result.txt"
    depends_on:
      server:
        condition: service_healthy
"#,
            local_image.display(),
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id").to_string();
    assert_eq!(submit_json["backend"], Value::from("local"));
    assert_eq!(submit_json["launched"], Value::from(true));
    assert_eq!(submit_json["submitted"], Value::from(false));

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--format",
            "json",
        ],
    );
    assert_success(&status);
    let status_json: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    assert_eq!(status_json["record"]["backend"], Value::from("local"));
    assert_eq!(
        status_json["scheduler"]["source"],
        Value::from("local_only")
    );

    let ps = run_cli(
        tmpdir.path(),
        &[
            "ps",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--format",
            "json",
        ],
    );
    assert_success(&ps);
    let ps_json: Value = serde_json::from_str(&stdout_text(&ps)).expect("ps json");
    assert_eq!(ps_json["record"]["backend"], Value::from("local"));
    assert_eq!(ps_json["services"].as_array().map(Vec::len), Some(2));

    let watch = run_cli(
        tmpdir.path(),
        &[
            "watch",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--lines",
            "20",
        ],
    );
    assert_success(&watch);
    assert!(stdout_text(&watch).contains("watching job"));

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
        ],
    );
    assert_success(&artifacts);
    assert!(
        tmpdir
            .path()
            .join("exports")
            .join(&job_id)
            .join("result.txt")
            .exists()
    );

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--format",
            "json",
        ],
    );
    assert_success(&stats);
    let stats_json: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(stats_json["record"]["backend"], Value::from("local"));
    assert_eq!(stats_json["source"], Value::from("sampler"));
    assert!(
        stats_json["notes"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|note| note == "Slurm step statistics are unavailable for locally launched jobs")
    );

    let failing_sacct = write_fake_sacct_failure(tmpdir.path());
    let accounting = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--accounting",
            "--format",
            "json",
            "--sacct-bin",
            failing_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&accounting);
    let accounting_json: Value =
        serde_json::from_str(&stdout_text(&accounting)).expect("accounting json");
    assert_eq!(
        accounting_json["accounting"]["available"],
        Value::from(false)
    );
    assert!(
        accounting_json["accounting"]["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("locally launched jobs")
    );
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_service_assertions_pass_and_surface_in_status() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose-assert-pass.yaml",
        &format!(
            r#"
services:
  train:
    image: {}
    command: /bin/sh -lc "mkdir -p /hpc-compose/job/model && printf checkpoint > /hpc-compose/job/model/checkpoint.pt"
    assert:
      exit_code: 0
      artifacts_contain: "model/*.pt"
      max_duration_seconds: 30
"#,
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id");

    let status_json =
        wait_for_service_assertion_status(tmpdir.path(), &compose, job_id, "train", "passed");
    assert_eq!(status_json["scheduler"]["state"], Value::from("COMPLETED"));
    let service = &status_json["services"][0];
    assert_eq!(service["assertions"]["configured"], Value::from(true));
    assert_eq!(
        service["assertions"]["artifacts_contain"],
        Value::from("/hpc-compose/job/model/*.pt")
    );
    assert_eq!(
        service["assertions"]["failures"].as_array().map(Vec::len),
        Some(0)
    );

    let status_text = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            job_id,
        ],
    );
    assert_success(&status_text);
    assert!(stdout_text(&status_text).contains("assert service 'train': status=passed"));
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_service_assertion_failures_mark_job_failed() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let enroot = write_fake_enroot(tmpdir.path());

    let missing_artifact = write_compose(
        tmpdir.path(),
        "compose-assert-missing-artifact.yaml",
        &format!(
            r#"
services:
  train:
    image: {}
    command: /bin/true
    assert:
      exit_code: 0
      artifacts_contain: "model/*.pt"
"#,
            local_image.display()
        ),
    );
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            missing_artifact.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id");
    let status_json = wait_for_service_assertion_status(
        tmpdir.path(),
        &missing_artifact,
        job_id,
        "train",
        "failed",
    );
    assert_eq!(status_json["scheduler"]["state"], Value::from("FAILED"));
    assert!(
        status_json["services"][0]["assertions"]["failures"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|failure| failure
                .as_str()
                .unwrap_or_default()
                .contains("artifacts_contain"))
    );

    let wrong_exit = write_compose(
        tmpdir.path(),
        "compose-assert-wrong-exit.yaml",
        &format!(
            r#"
services:
  train:
    image: {}
    command: /bin/sh -lc "exit 7"
    assert:
      exit_code: 0
    x-slurm:
      failure_policy:
        mode: ignore
"#,
            local_image.display()
        ),
    );
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            wrong_exit.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id");
    let status_json =
        wait_for_service_assertion_status(tmpdir.path(), &wrong_exit, job_id, "train", "failed");
    assert_eq!(status_json["scheduler"]["state"], Value::from("FAILED"));
    assert!(
        status_json["services"][0]["assertions"]["failures"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|failure| failure
                .as_str()
                .unwrap_or_default()
                .contains("expected exit_code 0, got 7"))
    );

    let slow = write_compose(
        tmpdir.path(),
        "compose-assert-duration.yaml",
        &format!(
            r#"
services:
  train:
    image: {}
    command: /bin/sh -lc "sleep 2"
    assert:
      max_duration_seconds: 1
"#,
            local_image.display()
        ),
    );
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            slow.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id");
    let status_json =
        wait_for_service_assertion_status(tmpdir.path(), &slow, job_id, "train", "failed");
    assert_eq!(status_json["scheduler"]["state"], Value::from("FAILED"));
    assert!(
        status_json["services"][0]["assertions"]["duration_seconds"]
            .as_u64()
            .unwrap_or_default()
            > 1
    );
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_completed_dependency_runs_pipeline_in_order() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose-dag.yaml",
        &format!(
            r#"
services:
  preprocess:
    image: {}
    command: /bin/sh -lc "printf 'preprocess\n' >> /hpc-compose/job/order.txt"
  train:
    image: {}
    command: /bin/sh -lc "printf 'train\n' >> /hpc-compose/job/order.txt"
    depends_on:
      preprocess:
        condition: service_completed_successfully
  postprocess:
    image: {}
    command: /bin/sh -lc "printf 'postprocess\n' >> /hpc-compose/job/order.txt"
    depends_on:
      train:
        condition: service_completed_successfully
"#,
            local_image.display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id").to_string();
    let order_path = tmpdir
        .path()
        .join(".hpc-compose")
        .join(&job_id)
        .join("order.txt");
    let mut order = String::new();
    for _ in 0..40 {
        order = fs::read_to_string(&order_path).unwrap_or_default();
        if order.contains("postprocess") {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(order, "preprocess\ntrain\npostprocess\n");

    let mut status_json = Value::Null;
    for _ in 0..40 {
        let status = run_cli(
            tmpdir.path(),
            &[
                "status",
                "-f",
                compose.to_str().expect("path"),
                "--job-id",
                &job_id,
                "--format",
                "json",
            ],
        );
        assert_success(&status);
        status_json = serde_json::from_str(&stdout_text(&status)).expect("status json");
        let all_services_completed = status_json["services"]
            .as_array()
            .expect("services")
            .iter()
            .all(|service| service["completed_successfully"].as_bool() == Some(true));
        if all_services_completed {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let services = status_json["services"].as_array().expect("services");
    assert!(
        services
            .iter()
            .all(|service| service["completed_successfully"].as_bool() == Some(true))
    );
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_completed_dependency_blocks_downstream_after_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose-dag-failure.yaml",
        &format!(
            r#"
services:
  preprocess:
    image: {}
    command: /bin/sh -lc "printf 'preprocess\n' >> /hpc-compose/job/order.txt; exit 23"
  train:
    image: {}
    command: /bin/sh -lc "printf 'train\n' >> /hpc-compose/job/order.txt"
    depends_on:
      preprocess:
        condition: service_completed_successfully
"#,
            local_image.display(),
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id").to_string();

    let mut status_json = Value::Null;
    for _ in 0..40 {
        let status = run_cli(
            tmpdir.path(),
            &[
                "status",
                "-f",
                compose.to_str().expect("path"),
                "--job-id",
                &job_id,
                "--format",
                "json",
            ],
        );
        assert_success(&status);
        status_json = serde_json::from_str(&stdout_text(&status)).expect("status json");
        if status_json["scheduler"]["terminal"].as_bool() == Some(true) {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(status_json["scheduler"]["failed"], Value::from(true));
    let order_path = tmpdir
        .path()
        .join(".hpc-compose")
        .join(&job_id)
        .join("order.txt");
    let order = fs::read_to_string(order_path).unwrap_or_default();
    assert_eq!(order, "preprocess\n");
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_cancel_terminates_supervisor() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/sh -lc "trap 'exit 0' TERM; while true; do sleep 1; done"
"#,
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let submit_json: Value = serde_json::from_str(&stdout_text(&submit)).expect("submit json");
    let job_id = submit_json["job_id"].as_str().expect("job id").to_string();

    let cancel = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            &job_id,
            "--format",
            "json",
        ],
    );
    assert_success(&cancel);
    let cancel_json: Value = serde_json::from_str(&stdout_text(&cancel)).expect("cancel json");
    assert_eq!(cancel_json["cancelled"], Value::from(true));
    let tracking_removed = cancel_json["tracking_removed"].as_bool() == Some(true);

    let mut terminal = None;
    let mut missing_tracking = false;
    for _ in 0..40 {
        let status = run_cli(
            tmpdir.path(),
            &[
                "status",
                "-f",
                compose.to_str().expect("path"),
                "--job-id",
                &job_id,
                "--format",
                "json",
            ],
        );
        if status.status.success() {
            let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
            if payload["scheduler"]["state"].as_str() == Some("CANCELLED") {
                terminal = Some(payload);
                break;
            }
            let transient_supervisor_race = payload["scheduler"]["state"].as_str()
                == Some("FAILED")
                && payload["scheduler"]["detail"]
                    .as_str()
                    .unwrap_or("")
                    .contains("exited before recording a terminal outcome");
            if !transient_supervisor_race
                && payload["scheduler"]["terminal"].as_bool() == Some(true)
            {
                terminal = Some(payload);
                break;
            }
        } else if stderr_text(&status).contains("tracked job")
            && stderr_text(&status).contains("was not found")
        {
            missing_tracking = true;
            break;
        } else {
            panic!(
                "unexpected status failure\nstdout:\n{}\nstderr:\n{}",
                stdout_text(&status),
                stderr_text(&status)
            );
        }
        thread::sleep(Duration::from_millis(250));
    }
    if tracking_removed {
        assert!(
            missing_tracking,
            "expected tracked metadata to be removed after local cancel"
        );
        assert!(
            !tmpdir
                .path()
                .join(".hpc-compose/jobs")
                .join(format!("{job_id}.json"))
                .exists()
        );
    } else {
        let status_json = terminal.expect("terminal local status");
        assert_eq!(status_json["record"]["backend"], Value::from("local"));
        assert_eq!(status_json["scheduler"]["state"], Value::from("CANCELLED"));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_failure_rolls_back_tracked_latest_record() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let batch_log_dir = tmpdir.path().join("batch-log-dir");
    fs::create_dir_all(&batch_log_dir).expect("batch log dir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  output: {}
services:
  app:
    image: {}
    command: /bin/true
"#,
            batch_log_dir.display(),
            local_image.display()
        ),
    );
    let previous_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("previous.sbatch"),
        &runtime_plan(&compose),
        "12345",
    )
    .expect("previous record");
    write_submission_record(&previous_record).expect("write previous");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            write_fake_enroot(tmpdir.path()).to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    assert!(stderr_text(&submit).contains("failed to open"));

    let latest = load_submission_record(&compose, None).expect("latest record after rollback");
    assert_eq!(latest.job_id, "12345");

    let jobs_dir = tmpdir.path().join(".hpc-compose/jobs");
    let records = fs::read_dir(&jobs_dir)
        .expect("jobs dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
        .count();
    assert_eq!(records, 1);
}

#[cfg(target_os = "linux")]
#[test]
fn submit_local_rejects_multi_node_distributed_and_extra_srun_args() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");

    let multi_node = write_compose(
        tmpdir.path(),
        "multi-node.yaml",
        &format!(
            "x-slurm:\n  nodes: 2\nservices:\n  app:\n    image: {}\n    command: /bin/true\n  helper:\n    image: {}\n    command: /bin/true\n",
            local_image.display(),
            local_image.display()
        ),
    );
    let multi_node_output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            multi_node.to_str().expect("path"),
        ],
    );
    assert_failure(&multi_node_output);
    assert!(stderr_text(&multi_node_output).contains("single-host specs"));

    let distributed = write_compose(
        tmpdir.path(),
        "distributed.yaml",
        &format!(
            r#"
x-slurm:
  nodes: 2
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      nodes: 2
"#,
            local_image.display()
        ),
    );
    let distributed_output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            distributed.to_str().expect("path"),
        ],
    );
    assert_failure(&distributed_output);
    assert!(stderr_text(&distributed_output).contains("distributed placement"));

    let partitioned = write_compose(
        tmpdir.path(),
        "partitioned.yaml",
        &format!(
            r#"
x-slurm:
  nodes: 3
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      placement:
        node_range: "0-1"
"#,
            local_image.display()
        ),
    );
    let partitioned_output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            partitioned.to_str().expect("path"),
        ],
    );
    assert_failure(&partitioned_output);
    assert!(stderr_text(&partitioned_output).contains("partitioned placement"));

    let extra_srun = write_compose(
        tmpdir.path(),
        "extra-srun.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      extra_srun_args:
        - --mpi=none
"#,
            local_image.display()
        ),
    );
    let extra_srun_output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            extra_srun.to_str().expect("path"),
        ],
    );
    assert_failure(&extra_srun_output);
    assert!(stderr_text(&extra_srun_output).contains("x-slurm.extra_srun_args"));

    let mpi = write_compose(
        tmpdir.path(),
        "mpi.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      mpi:
        type: pmix
"#,
            local_image.display()
        ),
    );
    let mpi_output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--local",
            "--skip-prepare",
            "--no-preflight",
            "-f",
            mpi.to_str().expect("path"),
        ],
    );
    assert_failure(&mpi_output);
    assert!(stderr_text(&mpi_output).contains("x-slurm.mpi"));
}

#[test]
fn submit_reports_script_write_errors_before_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let script_out = tmpdir.path().join("missing/script/out.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            write_fake_sbatch(tmpdir.path()).to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("failed to write rendered script"));
}

#[test]
fn down_auto_exports_tracked_artifacts_before_teardown() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    assert!(
        manifest.exists(),
        "artifact manifest should exist after submit"
    );
    let export_dir = tmpdir.path().join("results/12345");
    assert!(
        !export_dir.exists(),
        "artifacts must not be exported until teardown"
    );

    let down = run_cli(
        tmpdir.path(),
        &[
            "down",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&down);
    // The runtime root (and its collected payload) is reaped, but auto-export ran
    // first, so results survive in the configured export_dir.
    assert!(
        export_dir.exists()
            && fs::read_dir(&export_dir)
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false),
        "down should auto-export tracked artifacts before teardown"
    );
    assert!(
        !tmpdir.path().join(".hpc-compose/12345").exists(),
        "down should reap the runtime root after exporting"
    );
}

#[test]
fn down_no_export_skips_artifact_export() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let export_dir = tmpdir.path().join("results/12345");

    let down = run_cli(
        tmpdir.path(),
        &[
            "down",
            "-f",
            compose.to_str().expect("path"),
            "--yes",
            "--no-export",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&down);
    assert!(
        !export_dir.exists(),
        "down --no-export must not export artifacts"
    );
    assert!(
        !tmpdir.path().join(".hpc-compose/12345").exists(),
        "down --no-export should still reap the runtime root"
    );
}

#[test]
fn artifacts_command_exports_collected_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    assert!(tracked_manifest.exists(), "artifact manifest should exist");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("success")
    );
    assert!(
        tracked_manifest_value["copied_relative_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item.as_str() == Some("metrics/meta.json"))
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("did not match any paths"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    let value: Value = serde_json::from_str(&stdout_text(&artifacts)).expect("artifacts json");
    assert!(
        value["export_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/results/12345")
    );
    assert!(
        value["exported_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .ends_with("/results/12345/metrics/meta.json"))
    );
    assert_eq!(
        fs::read_to_string(tmpdir.path().join("results/12345/metrics/meta.json"))
            .expect("exported"),
        fs::read_to_string(
            tmpdir
                .path()
                .join(".hpc-compose/12345/artifacts/payload/metrics/meta.json")
        )
        .expect("payload")
    );

    fs::remove_file(
        tmpdir
            .path()
            .join(".hpc-compose/12345/artifacts/payload/metrics/meta.json"),
    )
    .expect("remove payload");
    let warning_artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--job-id",
            "12345",
        ],
    );
    assert_success(&warning_artifacts);
    let warning_value: Value =
        serde_json::from_str(&stdout_text(&warning_artifacts)).expect("warning artifacts json");
    assert!(
        warning_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("collected payload path"))
    );
}

#[test]
fn artifact_collection_handles_overlapping_paths_without_nested_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    let compose = write_compose(
        tmpdir.path(),
        "compose-artifacts-overlap.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  artifacts:
    collect: always
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/logs/app.log
      - /hpc-compose/job/logs
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let payload_root = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/payload/logs");
    assert!(payload_root.join("app.log").exists());
    assert!(!payload_root.join("logs/app.log").exists());

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    assert!(tmpdir.path().join("results/12345/logs/app.log").exists());
    assert!(
        !tmpdir
            .path()
            .join("results/12345/logs/logs/app.log")
            .exists()
    );
}

#[test]
fn artifacts_command_exports_named_bundle_tarball_and_skips_default_bundle() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    let compose = write_compose(
        tmpdir.path(),
        "compose-artifacts-bundles.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  metrics:
    interval_seconds: 1
  artifacts:
    collect: always
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/metrics/**
    bundles:
      logs:
        paths:
          - /hpc-compose/job/logs/**
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
            "--job-id",
            "12345",
            "--bundle",
            "logs",
            "--bundle",
            "logs",
            "--tarball",
        ],
    );
    assert_success(&artifacts);
    let value: Value = serde_json::from_str(&stdout_text(&artifacts)).expect("artifacts json");
    assert_eq!(value["selected_bundles"], serde_json::json!(["logs"]));
    assert!(
        value["tarball_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|path| path.as_str().unwrap_or_default().ends_with("/logs.tar.gz"))
    );
    assert!(
        tmpdir
            .path()
            .join("results/12345/bundles/logs/logs/app.log")
            .exists()
    );
    assert!(tmpdir.path().join("results/12345/logs.tar.gz").exists());
    assert!(
        tmpdir
            .path()
            .join("results/12345/_hpc-compose/bundles/logs.json")
            .exists()
    );
    assert!(
        !tmpdir
            .path()
            .join("results/12345/metrics/meta.json")
            .exists()
    );
}

#[test]
fn artifacts_unknown_job_id_fails_without_export_side_effects() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "99999",
            "--bundle",
            "logs",
            "--tarball",
        ],
    );
    assert_failure(&artifacts);
    assert!(!stderr_text(&artifacts).trim().is_empty());
    assert!(!tmpdir.path().join("results/99999").exists());
    assert!(!tmpdir.path().join("logs.tar.gz").exists());
}

#[test]
fn artifacts_unknown_bundle_fails_without_export_side_effects() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--bundle",
            "unknown",
            "--tarball",
        ],
    );
    assert_failure(&artifacts);
    assert!(stderr_text(&artifacts).contains("artifact bundle 'unknown' is not available"));
    assert!(!tmpdir.path().join("results/12345/bundles/unknown").exists());
    assert!(!tmpdir.path().join("results/12345/unknown.tar.gz").exists());
    assert!(
        !tmpdir
            .path()
            .join("results/12345/metrics/meta.json")
            .exists()
    );
}

#[test]
fn artifact_collection_policy_skips_when_job_outcome_does_not_match() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "on_success");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun_failure(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("failure")
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("does not match policy 'on_success'"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &["artifacts", "-f", compose.to_str().expect("path")],
    );
    assert_success(&artifacts);
    let out = stdout_text(&artifacts);
    assert!(out.contains("exported paths: 0"));
}

#[test]
fn submit_multi_node_mpi_example_pins_helper_and_tracks_allocation_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_example_compose(tmpdir.path(), "multi-node-mpi.yaml", &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun_log = tmpdir.path().join("srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch = write_fake_sbatch_runs_script_with_nodelist(
        tmpdir.path(),
        "sbatch-multi-node-mpi",
        "node01,node02",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let srun_text = fs::read_to_string(&srun_log).expect("srun log");
    assert!(srun_text.contains("--job-name=hpc-compose:bootstrap"));
    assert!(srun_text.contains("--nodes=1"));
    assert!(srun_text.contains("--ntasks=1"));
    assert!(srun_text.contains("--nodelist=node01"));
    assert!(srun_text.contains("--job-name=hpc-compose:mpi"));
    assert!(srun_text.contains("--nodes=2"));
    assert!(srun_text.contains("--ntasks-per-node=2"));
    assert!(srun_text.contains("--mpi=pmix"));
    assert!(srun_text.contains("env:node01|2|node01 node02|/hpc-compose/job/allocation/nodes.txt"));
    assert!(
        srun_text.contains("mpi_env:/hpc-compose/job/allocation/mpi-hostfiles/mpi.hostfile|pmix")
    );
    let hostfile = fs::read_to_string(
        tmpdir
            .path()
            .join(".hpc-compose/12345/allocation/mpi-hostfiles/mpi.hostfile"),
    )
    .expect("mpi hostfile");
    assert_eq!(hostfile, "node01 slots=2\nnode02 slots=2\n");

    let state: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/state.json")).expect("state"),
    )
    .expect("state json");
    let services = state["services"].as_array().expect("services");
    let bootstrap = services
        .iter()
        .find(|service| service["service_name"] == "bootstrap")
        .expect("bootstrap state");
    let mpi = services
        .iter()
        .find(|service| service["service_name"] == "mpi")
        .expect("mpi state");
    assert_eq!(bootstrap["placement_mode"], "primary_node");
    assert_eq!(bootstrap["nodes"], 1);
    assert_eq!(bootstrap["nodelist"], "node01");
    assert_eq!(mpi["placement_mode"], "distributed");
    assert_eq!(mpi["nodes"], 2);
    assert_eq!(mpi["ntasks_per_node"], 2);
    assert_eq!(mpi["nodelist"], "node01 node02");
}

#[test]
fn submit_partitioned_multi_node_services_emit_subset_nodelists_and_service_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "partitioned.yaml",
        &format!(
            r#"
name: partitioned-demo
x-slurm:
  job_name: partitioned-demo
  nodes: 8
  cache_dir: {}
services:
  a:
    image: {}
    command: /bin/true
    x-slurm:
      placement:
        node_range: "0-3"
  ps:
    image: {}
    command: /bin/true
    x-slurm:
      placement:
        share_with: a
  b:
    image: {}
    command: /bin/true
    x-slurm:
      ntasks_per_node: 2
      mpi:
        type: pmix
      placement:
        node_range: "4-7"
  excluded:
    image: {}
    command: /bin/true
    x-slurm:
      placement:
        node_count: 2
        exclude: "1,7"
        allow_overlap: true
"#,
            cache_dir.display(),
            local_image.display(),
            local_image.display(),
            local_image.display(),
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun_log = tmpdir.path().join("partitioned-srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch = write_fake_sbatch_runs_script_with_nodelist(
        tmpdir.path(),
        "sbatch-partitioned",
        "node01,node02,node03,node04,node05,node06,node07,node08",
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect);
    let inspect_text = stdout_text(&inspect);
    assert!(inspect_text.contains("step geometry: mode=partitioned nodes=4"));
    assert!(inspect_text.contains("node_indices=0,1,2,3"));
    assert!(inspect_text.contains("exclude_indices=1,7"));

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let srun_text = fs::read_to_string(&srun_log).expect("srun log");
    assert!(srun_text.contains("--job-name=hpc-compose:a"));
    assert!(srun_text.contains("--nodes=4"));
    assert!(srun_text.contains("--nodelist=node01,node02,node03,node04"));
    assert!(srun_text.contains("--job-name=hpc-compose:b"));
    assert!(srun_text.contains("--nodelist=node05,node06,node07,node08"));
    assert!(srun_text.contains("--mpi=pmix"));
    assert!(srun_text.contains("--exclude=node02,node08"));
    assert!(srun_text.contains(
        "service_env:node01|4|node01 node02 node03 node04|/hpc-compose/job/allocation/service-nodelists/a.nodes.txt"
    ));

    let hostfile = fs::read_to_string(
        tmpdir
            .path()
            .join(".hpc-compose/12345/allocation/mpi-hostfiles/b.hostfile"),
    )
    .expect("mpi hostfile");
    assert_eq!(
        hostfile,
        "node05 slots=2\nnode06 slots=2\nnode07 slots=2\nnode08 slots=2\n"
    );

    let state: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/12345/state.json")).expect("state"),
    )
    .expect("state json");
    let services = state["services"].as_array().expect("services");
    let a = services
        .iter()
        .find(|service| service["service_name"] == "a")
        .expect("a state");
    let ps = services
        .iter()
        .find(|service| service["service_name"] == "ps")
        .expect("ps state");
    let b = services
        .iter()
        .find(|service| service["service_name"] == "b")
        .expect("b state");
    assert_eq!(a["placement_mode"], "partitioned");
    assert_eq!(a["nodelist"], "node01 node02 node03 node04");
    assert_eq!(ps["nodelist"], a["nodelist"]);
    assert_eq!(b["nodelist"], "node05 node06 node07 node08");
}

#[test]
fn inspect_and_submit_multi_node_torchrun_example_show_distributed_geometry() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_example_compose(tmpdir.path(), "multi-node-torchrun.yaml", &cache_dir);

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect);
    let inspect_text = stdout_text(&inspect);
    assert!(inspect_text.contains("allocation geometry: nodes=2"));
    assert!(inspect_text.contains("step geometry: mode=distributed nodes=2"));
    assert!(inspect_text.contains("--nodes=2"));
    assert!(inspect_text.contains("--ntasks-per-node=1"));
    assert!(inspect_text.contains("HPC_COMPOSE_DIST_RDZV_ENDPOINT"));

    let enroot = write_fake_enroot(tmpdir.path());
    let srun_log = tmpdir.path().join("torchrun-srun.log");
    let srun = write_fake_srun_capture(tmpdir.path(), &srun_log);
    let sbatch = write_fake_sbatch_runs_script_with_nodelist(
        tmpdir.path(),
        "sbatch-multi-node-torchrun",
        "node01,node02",
    );

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "--no-preflight",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);

    let srun_text = fs::read_to_string(&srun_log).expect("srun log");
    assert!(srun_text.contains("--job-name=hpc-compose:trainer"));
    assert!(srun_text.contains("--nodes=2"));
    assert!(srun_text.contains("--ntasks-per-node=1"));
    assert!(!srun_text.contains("--nodelist=node01"));
    assert!(srun_text.contains("env:node01|2|node01 node02|/hpc-compose/job/allocation/nodes.txt"));
    assert!(srun_text.contains("dist_env:node01|"));
    assert!(srun_text.contains("|2|4|8"));
}

#[test]
fn inspect_distributed_ml_templates_show_launcher_geometry() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();

    for template in [
        "multi-node-deepspeed.yaml",
        "multi-node-accelerate.yaml",
        "multi-node-jax.yaml",
    ] {
        let compose = write_example_compose(tmpdir.path(), template, &cache_dir);
        let inspect = run_cli(
            tmpdir.path(),
            &[
                "inspect",
                "-f",
                compose.to_str().expect("path"),
                "--verbose",
            ],
        );
        assert_success(&inspect);
        let inspect_text = stdout_text(&inspect);
        assert!(inspect_text.contains("step geometry: mode=distributed nodes=2"));
        assert!(inspect_text.contains("--ntasks-per-node=1"));
        assert!(inspect_text.contains("HPC_COMPOSE_DIST_RDZV_ENDPOINT"));
    }

    for template in ["multi-node-horovod.yaml", "nccl-tests.yaml"] {
        let compose = write_example_compose(tmpdir.path(), template, &cache_dir);
        let inspect = run_cli(
            tmpdir.path(),
            &[
                "inspect",
                "-f",
                compose.to_str().expect("path"),
                "--verbose",
            ],
        );
        assert_success(&inspect);
        let inspect_text = stdout_text(&inspect);
        assert!(inspect_text.contains("step geometry: mode=distributed nodes=2"));
        assert!(inspect_text.contains("--ntasks-per-node=4"));
        assert!(inspect_text.contains("--mpi=pmix"));
    }
}

#[test]
fn clean_command_removes_old_job_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    // Submit a job to create tracking metadata
    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");
    let runtime_dir = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(runtime_dir.join("logs")).expect("job runtime dir");
    fs::write(runtime_dir.join("logs/app.log"), "hello\n").expect("job log");

    // clean --all should keep the only tracked job.
    let clean_all = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "--yes",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean_all);
    let clean_all_payload: Value =
        serde_json::from_str(&stdout_text(&clean_all)).expect("clean all json");
    assert_eq!(clean_all_payload["removed_job_ids"], serde_json::json!([]));
    assert_eq!(
        clean_all_payload["kept_job_ids"],
        serde_json::json!(["12345"])
    );
    assert_eq!(
        clean_all_payload["latest_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(
        clean_all_payload["latest_pointer_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(
        clean_all_payload["latest_job_id_after"],
        Value::from("12345")
    );
    assert!(runtime_dir.exists());
    assert!(tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());

    // clean --age 0 should remove the job because it is older than "now".
    let clean_age = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--yes",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean_age);
    let clean_age_payload: Value =
        serde_json::from_str(&stdout_text(&clean_age)).expect("clean age json");
    assert_eq!(clean_age_payload["mode"], Value::from("age"));
    assert_eq!(clean_age_payload["dry_run"], Value::from(false));
    assert_eq!(
        clean_age_payload["removed_job_ids"],
        serde_json::json!(["12345"])
    );
    assert_eq!(clean_age_payload["kept_job_ids"], serde_json::json!([]));
    assert_eq!(
        clean_age_payload["latest_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(
        clean_age_payload["latest_pointer_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(clean_age_payload["latest_job_id_after"], Value::Null);
    assert!(!tmpdir.path().join(".hpc-compose/jobs/12345.json").exists());
    assert!(!runtime_dir.exists());
    assert!(!tmpdir.path().join(".hpc-compose/latest.json").exists());
}

#[test]
fn clean_text_reports_selected_jobs_and_kept_ids() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);

    let mut old_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-old.sbatch"),
        &plan,
        "11111",
    )
    .expect("old record");
    old_record.submitted_at = 1;
    write_submission_record(&old_record).expect("write old");

    let mut latest_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-latest.sbatch"),
        &plan,
        "22222",
    )
    .expect("latest record");
    latest_record.submitted_at = u64::MAX / 2;
    write_submission_record(&latest_record).expect("write latest");

    fs::create_dir_all(tmpdir.path().join(".hpc-compose/11111/logs")).expect("old runtime");
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/22222/logs")).expect("latest runtime");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--yes",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let stdout = stdout_text(&clean);
    assert!(stdout.contains("mode: age"));
    assert!(stdout.contains("effective latest before: 22222"));
    assert!(stdout.contains("pointer before: 22222"));
    assert!(stdout.contains("effective latest after: 22222"));
    assert!(stdout.contains("selected jobs: 1"));
    assert!(stdout.contains("selected ids: 11111"));
    assert!(stdout.contains("kept ids: 22222"));
    assert!(stdout.contains("removed 11111"));
}

#[test]
fn clean_all_preserves_latest_tracked_submission() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let sbatch_first = tmpdir.path().join("sbatch-first");
    write_script(
        &sbatch_first,
        "#!/bin/bash\nset -euo pipefail\necho \"Submitted batch job 11111\"\n",
    );
    let first_submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch_first.to_str().expect("path"),
        ],
    );
    assert_success(&first_submit);
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/11111/logs")).expect("first job dir");

    let sbatch_second = tmpdir.path().join("sbatch-second");
    write_script(
        &sbatch_second,
        "#!/bin/bash\nset -euo pipefail\necho \"Submitted batch job 22222\"\n",
    );
    let second_submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch_second.to_str().expect("path"),
        ],
    );
    assert_success(&second_submit);
    fs::create_dir_all(tmpdir.path().join(".hpc-compose/22222/logs")).expect("second job dir");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "--yes",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["11111"]));
    assert_eq!(payload["kept_job_ids"], serde_json::json!(["22222"]));
    assert_eq!(payload["latest_job_id_before"], Value::from("22222"));
    assert_eq!(
        payload["latest_pointer_job_id_before"],
        Value::from("22222")
    );
    assert_eq!(payload["latest_job_id_after"], Value::from("22222"));
    assert!(!tmpdir.path().join(".hpc-compose/jobs/11111.json").exists());
    assert!(tmpdir.path().join(".hpc-compose/jobs/22222.json").exists());
    assert!(!tmpdir.path().join(".hpc-compose/11111").exists());
    assert!(tmpdir.path().join(".hpc-compose/22222").exists());

    let latest: Value = serde_json::from_str(
        &fs::read_to_string(tmpdir.path().join(".hpc-compose/latest.json")).expect("latest"),
    )
    .expect("latest json");
    assert_eq!(latest["job_id"], Value::from("22222"));
}

#[test]
fn clean_dry_run_does_not_remove_state_and_reports_json_contract() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let submit = run_cli(
        tmpdir.path(),
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            write_fake_sbatch(tmpdir.path()).to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");

    let runtime_dir = tmpdir.path().join(".hpc-compose/12345");
    fs::create_dir_all(runtime_dir.join("logs")).expect("runtime dir");
    fs::write(runtime_dir.join("logs/app.log"), "hello\n").expect("runtime log");
    let record_path = tmpdir.path().join(".hpc-compose/jobs/12345.json");
    let latest_path = tmpdir.path().join(".hpc-compose/latest.json");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--dry-run",
            "--disk-usage",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(
        payload["compose_file"],
        Value::from(compose.display().to_string())
    );
    assert_eq!(payload["mode"], Value::from("age"));
    assert_eq!(payload["dry_run"], Value::from(true));
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["12345"]));
    assert_eq!(payload["kept_job_ids"], serde_json::json!([]));
    assert_eq!(payload["latest_job_id_before"], Value::from("12345"));
    assert_eq!(
        payload["latest_pointer_job_id_before"],
        Value::from("12345")
    );
    assert_eq!(payload["latest_job_id_after"], Value::Null);
    assert!(payload["total_bytes_reclaimed"].as_u64().unwrap_or(0) > 0);
    let jobs = payload["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["job_id"], Value::from("12345"));
    assert_eq!(jobs[0]["selected"], Value::from(true));
    assert!(jobs[0]["bytes_reclaimed"].as_u64().unwrap_or(0) > 0);
    assert!(record_path.exists());
    assert!(runtime_dir.exists());
    assert!(latest_path.exists());
}

#[test]
fn clean_deep_reports_and_reaps_orphan_runtime_and_expired_rendezvous() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let orphan_runtime = cache_dir.join("runtime/99999");
    fs::create_dir_all(orphan_runtime.join("cache")).expect("orphan runtime");
    fs::write(orphan_runtime.join("cache/blob"), "payload").expect("orphan payload");

    let record = build_record(
        &cache_dir,
        RendezvousRegisterRequest {
            name: "model-server".to_string(),
            job_id: "99999".to_string(),
            service: Some("api".to_string()),
            host: "node01".to_string(),
            port: 8000,
            protocol: "http".to_string(),
            path: Some("/".to_string()),
            ttl_seconds: 1,
            metadata: BTreeMap::new(),
        },
        1,
    )
    .expect("rendezvous record");
    let historical_path = register(&cache_dir, &record).expect("register rendezvous");
    let latest_path = cache_dir.join("rendezvous/model-server/latest.json");

    let dry_run = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "--deep",
            "--dry-run",
            "--disk-usage",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&dry_run);
    let payload: Value = serde_json::from_str(&stdout_text(&dry_run)).expect("clean deep json");
    assert_eq!(payload["dry_run"], Value::from(true));
    assert_eq!(payload["removed_job_ids"], serde_json::json!([]));
    assert_eq!(
        payload["deep"]["cache_dir"],
        Value::from(cache_dir.display().to_string())
    );
    assert_eq!(
        payload["deep"]["orphan_runtime_dirs"][0]["job_id"],
        Value::from("99999")
    );
    assert_eq!(
        payload["deep"]["orphan_runtime_dirs"][0]["selected"],
        Value::from(true)
    );
    assert!(
        payload["deep"]["orphan_runtime_dirs"][0]["bytes_reclaimed"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    let removed_rendezvous = payload["deep"]["rendezvous"]["removed"]
        .as_array()
        .expect("removed rendezvous");
    assert_eq!(removed_rendezvous.len(), 2);
    assert!(orphan_runtime.exists());
    assert!(historical_path.exists());
    assert!(latest_path.exists());

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--all",
            "--deep",
            "--yes",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean deep json");
    assert_eq!(payload["dry_run"], Value::from(false));
    assert_eq!(
        payload["deep"]["orphan_runtime_dirs"][0]["job_id"],
        Value::from("99999")
    );
    assert!(!orphan_runtime.exists());
    assert!(!historical_path.exists());
    assert!(!latest_path.exists());
}

#[test]
fn clean_uses_recorded_submit_dir_for_runtime_cleanup() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_root = tmpdir.path().join("repo");
    let submit_root = tmpdir.path().join("submit-dir");
    fs::create_dir_all(&compose_root).expect("compose root");
    fs::create_dir_all(&submit_root).expect("submit root");

    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&compose_root, &cache_dir);
    let sbatch = write_fake_sbatch(&submit_root);

    let submit = run_cli(
        &submit_root,
        &[
            "up",
            "--detach",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    let mut record = load_submission_record(&compose, Some("12345")).expect("record");
    record.submitted_at = 1;
    write_submission_record(&record).expect("rewrite record");

    let submit_runtime_dir = submit_root.join(".hpc-compose/12345");
    fs::create_dir_all(submit_runtime_dir.join("logs")).expect("runtime dir");
    fs::write(submit_runtime_dir.join("logs/app.log"), "hello\n").expect("runtime log");

    let clean = run_cli(
        &compose_root,
        &[
            "clean",
            "--age",
            "0",
            "--yes",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["12345"]));
    assert!(!submit_runtime_dir.exists());
    assert!(!compose_root.join(".hpc-compose/jobs/12345.json").exists());
    assert!(!compose_root.join(".hpc-compose/latest.json").exists());
}

#[test]
fn clean_repairs_latest_pointer_and_removes_it_when_no_jobs_remain() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix time")
        .as_secs();

    let mut old_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-old.sbatch"),
        &plan,
        "11111",
    )
    .expect("old record");
    old_record.submitted_at = now.saturating_sub(10 * 86_400);
    write_submission_record(&old_record).expect("write old");

    let mut new_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-new.sbatch"),
        &plan,
        "22222",
    )
    .expect("new record");
    new_record.submitted_at = now.saturating_sub(1);
    write_submission_record(&new_record).expect("write new");

    fs::write(
        latest_record_path_for(&compose),
        serde_json::to_vec_pretty(&old_record).expect("stale latest"),
    )
    .expect("overwrite latest");

    let first_clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "7",
            "--yes",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&first_clean);
    let first_payload: Value =
        serde_json::from_str(&stdout_text(&first_clean)).expect("first clean json");
    assert_eq!(
        first_payload["removed_job_ids"],
        serde_json::json!(["11111"])
    );
    assert_eq!(first_payload["latest_job_id_before"], Value::from("22222"));
    assert_eq!(
        first_payload["latest_pointer_job_id_before"],
        Value::from("11111")
    );
    assert_eq!(first_payload["latest_job_id_after"], Value::from("22222"));
    let latest_after_first: Value = serde_json::from_str(
        &fs::read_to_string(latest_record_path_for(&compose)).expect("latest after first"),
    )
    .expect("latest json");
    assert_eq!(latest_after_first["job_id"], Value::from("22222"));

    let second_clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "0",
            "--yes",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&second_clean);
    let second_payload: Value =
        serde_json::from_str(&stdout_text(&second_clean)).expect("second clean json");
    assert_eq!(
        second_payload["removed_job_ids"],
        serde_json::json!(["22222"])
    );
    assert_eq!(second_payload["latest_job_id_after"], Value::Null);
    assert!(!latest_record_path_for(&compose).exists());
}

#[test]
fn clean_reports_missing_latest_pointer_separately_from_effective_latest() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("unix time")
        .as_secs();

    let mut old_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-old.sbatch"),
        &plan,
        "11111",
    )
    .expect("old record");
    old_record.submitted_at = now.saturating_sub(10 * 86_400);
    write_submission_record(&old_record).expect("write old");

    let mut new_record = build_submission_record(
        &compose,
        tmpdir.path(),
        &tmpdir.path().join("submit-new.sbatch"),
        &plan,
        "22222",
    )
    .expect("new record");
    new_record.submitted_at = now.saturating_sub(1);
    write_submission_record(&new_record).expect("write new");

    let mut missing_pointer = old_record.clone();
    missing_pointer.job_id = "99999".into();
    fs::write(
        latest_record_path_for(&compose),
        serde_json::to_vec_pretty(&missing_pointer).expect("missing latest"),
    )
    .expect("overwrite latest");

    let clean = run_cli(
        tmpdir.path(),
        &[
            "clean",
            "--age",
            "7",
            "--yes",
            "--format",
            "json",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&clean);
    let payload: Value = serde_json::from_str(&stdout_text(&clean)).expect("clean json");
    assert_eq!(payload["removed_job_ids"], serde_json::json!(["11111"]));
    assert_eq!(payload["latest_job_id_before"], Value::from("22222"));
    assert_eq!(
        payload["latest_pointer_job_id_before"],
        Value::from("99999")
    );
    assert_eq!(payload["latest_job_id_after"], Value::from("22222"));

    let latest_after: Value = serde_json::from_str(
        &fs::read_to_string(latest_record_path_for(&compose)).expect("latest after"),
    )
    .expect("latest json");
    assert_eq!(latest_after["job_id"], Value::from("22222"));
}

#[test]
fn clean_requires_strategy_flag() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("write");

    let output = run_cli(
        tmpdir.path(),
        &["clean", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&output);
    let err = stderr_text(&output);
    assert!(
        err.contains("--age") || err.contains("--all"),
        "error should mention required flags: {err}"
    );
}

#[test]
fn run_image_mode_rejects_empty_image_before_any_backend() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let script_out = tmpdir.path().join("ephemeral.sbatch");
    let missing_bin = tmpdir.path().join("does-not-exist-bin");
    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "--image",
            "",
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            missing_bin.to_str().expect("path"),
            "--srun-bin",
            missing_bin.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    assert!(
        stderr_text(&output).contains("run --image requires a non-empty image"),
        "stderr:\n{}",
        stderr_text(&output)
    );
    assert!(!script_out.exists());
}

#[test]
fn run_service_mode_rejects_unknown_service_naming_it() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let local_image = tmpdir.path().join("image.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: run-unknown
x-slurm:
  cache_dir: {}
services:
  app:
    image: {}
    command: ["echo", "base"]
"#,
            cache_dir.display(),
            local_image.display()
        ),
    );
    let missing_bin = tmpdir.path().join("does-not-exist-bin");
    let output = run_cli(
        tmpdir.path(),
        &[
            "run",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--sbatch-bin",
            missing_bin.to_str().expect("path"),
            "--srun-bin",
            missing_bin.to_str().expect("path"),
            "nope",
            "--",
            "/bin/true",
        ],
    );
    assert_failure(&output);
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("service 'nope' does not exist in"),
        "stderr:\n{stderr}"
    );
}

#[test]
fn shell_mode_rejects_empty_image_before_srun() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let missing_bin = tmpdir.path().join("does-not-exist-srun");
    let output = run_cli(
        tmpdir.path(),
        &[
            "shell",
            "--image",
            "",
            "--srun-bin",
            missing_bin.to_str().expect("path"),
        ],
    );
    assert_failure(&output);
    assert!(
        stderr_text(&output).contains("shell --image requires a non-empty image"),
        "stderr:\n{}",
        stderr_text(&output)
    );
}
