mod support;

use std::fs;

use serde_json::Value;
use support::*;

#[test]
fn germinate_dry_run_renders_minimized_canary_script() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: train
x-slurm:
  job_name: train
  time: "04:00:00"
  mem: 64G
  cpus_per_task: 8
  gpus: 4
  cache_dir: {}
services:
  trainer:
    image: python:3.12-slim
    command: python train.py
    x-slurm:
      cpus_per_task: 8
"#,
            cache_root.path().display()
        ),
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "germinate",
            "-f",
            compose.to_str().expect("path"),
            "--dry-run",
            "--no-preflight",
            "--skip-prepare",
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("germinate json");
    assert_eq!(value["dry_run"], Value::from(true));
    let script_path = value["script_path"].as_str().expect("script path");
    let script = fs::read_to_string(script_path).expect("script");
    assert!(script.contains("#SBATCH --job-name=train-canary"));
    assert!(script.contains("#SBATCH --time=00:01:00"));
    assert!(script.contains("#SBATCH --cpus-per-task=1"));
    assert!(script.contains("#SBATCH --mem=1G"));
    assert!(script.contains("#SBATCH --gpus=1"));
    assert!(script.contains("METRICS_INTERVAL_SECONDS=5"));
}

#[test]
fn germinate_success_writes_latest_canary_without_touching_latest_main() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: rightsize-demo
x-slurm:
  job_name: rightsize-demo
  time: "02:00:00"
  mem: 64G
  cache_dir: {}
services:
  training:
    image: python:3.12-slim
    command: python train.py
    x-slurm:
      cpus_per_task: 8
"#,
            cache_root.path().display()
        ),
    );
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let accounting_output = tmpdir.path().join("accounting.output");
    fs::write(
        &accounting_output,
        "\
12345|rightsize-demo-canary|COMPLETED|0:0|60|1|60|00:01:00|cpu=1,mem=1G|cpu=1,mem=1G|12300M|cpu=00:01:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:01:00
12345.0|hpc-compose:training|COMPLETED|0:0|60|1|600|00:10:00|cpu=1,mem=1G|cpu=1,mem=1G|12300M|cpu=00:10:00|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:01:00
",
    )
    .expect("accounting output");
    let sstat_output = tmpdir.path().join("sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.0|1|00:10:00|12000M|12300M|cpu=1,mem=1G|cpu=00:10:00
",
    )
    .expect("sstat output");

    let output = run_cli(
        tmpdir.path(),
        &[
            "germinate",
            "-f",
            compose.to_str().expect("path"),
            "--no-preflight",
            "--skip-prepare",
            "--sbatch-bin",
            write_fake_sbatch(tmpdir.path()).to_str().expect("path"),
            "--srun-bin",
            write_fake_srun(tmpdir.path()).to_str().expect("path"),
            "--squeue-bin",
            write_fake_squeue(tmpdir.path(), &squeue_state)
                .to_str()
                .expect("path"),
            "--sacct-bin",
            write_fake_sacct_accounting(tmpdir.path(), &sacct_state, &accounting_output)
                .to_str()
                .expect("path"),
            "--sstat-bin",
            write_fake_sstat(tmpdir.path(), &sstat_output)
                .to_str()
                .expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("germinate json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert!(
        value["yaml_patch"]
            .as_str()
            .unwrap_or_default()
            .contains("x-slurm:")
    );
    assert!(
        !value["yaml_patch"]
            .as_str()
            .unwrap_or_default()
            .contains("time:")
    );

    let latest_canary = tmpdir.path().join(".hpc-compose/latest-canary.json");
    let latest_main = tmpdir.path().join(".hpc-compose/latest.json");
    assert!(latest_canary.exists());
    assert!(!latest_main.exists());
    let record: Value =
        serde_json::from_str(&fs::read_to_string(latest_canary).expect("canary record"))
            .expect("record json");
    assert_eq!(record["kind"], Value::from("canary"));
}

#[test]
fn germinate_rejects_arrays_with_explicit_message() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "array.yaml",
        &format!(
            r#"
x-slurm:
  array: "1-4"
  cache_dir: {}
services:
  app:
    image: alpine:3.20
    command: echo hi
"#,
            cache_root.path().display()
        ),
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "germinate",
            "-f",
            compose.to_str().expect("path"),
            "--dry-run",
            "--no-preflight",
            "--skip-prepare",
        ],
    );
    assert_failure(&output);
    assert!(stderr_text(&output).contains("does not support x-slurm.array"));
}

#[test]
fn rendezvous_cli_register_resolve_list_and_prune_round_trip() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();

    let register = run_cli(
        tmpdir.path(),
        &[
            "rendezvous",
            "register",
            "model-server",
            "--host",
            "node01",
            "--port",
            "8000",
            "--path",
            "/v1",
            "--job-id",
            "4242",
            "--service",
            "server",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&register);
    let registered: Value = serde_json::from_str(&stdout_text(&register)).expect("register json");
    assert_eq!(
        registered["record"]["url"],
        Value::from("http://node01:8000/v1")
    );

    let resolve = run_cli(
        tmpdir.path(),
        &[
            "rendezvous",
            "resolve",
            "model-server",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&resolve);
    let resolved: Value = serde_json::from_str(&stdout_text(&resolve)).expect("resolve json");
    assert_eq!(resolved["job_id"], Value::from("4242"));

    let list = run_cli(
        tmpdir.path(),
        &[
            "rendezvous",
            "list",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&list);
    let listed: Value = serde_json::from_str(&stdout_text(&list)).expect("list json");
    assert_eq!(listed.as_array().expect("array").len(), 1);

    let prune = run_cli(
        tmpdir.path(),
        &[
            "rendezvous",
            "prune",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&prune);
    let pruned: Value = serde_json::from_str(&stdout_text(&prune)).expect("prune json");
    assert_eq!(pruned["removed"].as_array().expect("removed").len(), 0);
}

#[test]
fn rendezvous_render_injects_client_env_and_provider_registration() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "rendezvous.yaml",
        &format!(
            r#"
name: rdzv-demo
x-slurm:
  cache_dir: {}
  rendezvous: model-server
services:
  server:
    image: python:3.12-slim
    command: python -m http.server 8000
    readiness:
      type: sleep
      seconds: 1
    x-slurm:
      rendezvous:
        register:
          name: model-server
          port: 8000
          protocol: http
          path: /v1
  client:
    image: curlimages/curl:8.10.1
    command: curl "$HPC_COMPOSE_RDZV_MODEL_SERVER_URL/models"
"#,
            cache_root.path().display()
        ),
    );
    let output = run_cli(
        tmpdir.path(),
        &[
            "render",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("render json");
    let script = value["script"].as_str().expect("script");
    assert!(script.contains("resolve_rendezvous_dependencies"));
    assert!(script.contains("HPC_COMPOSE_RDZV_MODEL_SERVER_URL"));
    assert!(script.contains("SERVICE_RDZV_NAMES[rdzv_index]='model-server'"));
    assert!(script.contains("if wait_until_server_ready \"$pid\" \"$service_name\"; then"));
    assert!(script.contains("register_service_rendezvous_by_index \"$rdzv_index\""));
}
