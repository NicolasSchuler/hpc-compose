mod support;

use std::fs;
use std::process::Command;

use serde_json::Value;
use support::*;

#[test]
fn validate_and_render_commands_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_success(&validate);
    assert!(stdout_text(&validate).contains("spec is valid"));

    let validate_json = run_cli(
        tmpdir.path(),
        &[
            "validate",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&validate_json);
    let validate_value: Value =
        serde_json::from_str(&stdout_text(&validate_json)).expect("validate json");
    assert_eq!(validate_value["valid"], Value::from(true));
    assert_eq!(validate_value["service_count"], Value::from(1));
    assert_eq!(validate_value["services"][0], Value::from("app"));

    let script_path = tmpdir.path().join("job.sbatch");
    let render = run_cli(
        tmpdir.path(),
        &[
            "render",
            "-f",
            compose.to_str().expect("path"),
            "--output",
            script_path.to_str().expect("path"),
        ],
    );
    assert_success(&render);
    let script = fs::read_to_string(&script_path).expect("script");
    assert!(script.contains("#SBATCH --job-name=demo"));
    assert!(script.contains("--container-image="));

    let render_json = run_cli(
        tmpdir.path(),
        &[
            "render",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&render_json);
    let render_value: Value =
        serde_json::from_str(&stdout_text(&render_json)).expect("render json");
    assert_eq!(render_value["output_path"], Value::Null);
    assert!(
        render_value["script"]
            .as_str()
            .unwrap_or_default()
            .contains("#SBATCH --job-name=demo")
    );
}

#[test]
fn inspect_and_preflight_commands_cover_dev_workflow() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_mount_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let inspect = run_cli(
        tmpdir.path(),
        &["inspect", "-f", compose.to_str().expect("path")],
    );
    assert_success(&inspect);
    assert!(
        stdout_text(&inspect)
            .contains("rebuild on submit because x-enroot.prepare.mounts are present")
    );

    let preflight = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&preflight);
    let preflight_stderr = stderr_text(&preflight);
    assert!(preflight_stderr.contains("Summary:"));
    assert!(preflight_stderr.contains("Passed checks:"));
    assert!(preflight_stderr.contains("srun reports Pyxis container support"));
    assert!(preflight_stderr.contains("cache directory is writable"));

    let inspect_json = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect_json);
    let inspect_value: Value =
        serde_json::from_str(&stdout_text(&inspect_json)).expect("inspect json");
    assert_eq!(
        inspect_value["ordered_services"][0]["name"],
        Value::from("app")
    );

    let preflight_json = run_cli(
        tmpdir.path(),
        &[
            "preflight",
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
    assert_success(&preflight_json);
    let preflight_value: Value =
        serde_json::from_str(&stdout_text(&preflight_json)).expect("preflight json");
    assert!(
        preflight_value["summary"]["passed_checks"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );

    let strict = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--strict",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&strict);
    assert!(stderr_text(&strict).contains("preflight reported warnings"));
}

#[test]
fn mpi_config_is_exposed_in_machine_readable_outputs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "mpi.yaml",
        &format!(
            r#"
name: mpi-json
x-slurm:
  nodes: 2
  cache_dir: "{}"
services:
  worker:
    image: debian:bookworm-slim
    command: /usr/local/bin/worker
    x-slurm:
      nodes: 2
      ntasks_per_node: 2
      mpi:
        type: pmix
"#,
            cache_root.path().display()
        ),
    );

    let config = run_cli(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&config);
    let config_value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
    assert_eq!(
        config_value["services"]["worker"]["x-slurm"]["mpi"]["type"],
        Value::from("pmix")
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect);
    let inspect_value: Value = serde_json::from_str(&stdout_text(&inspect)).expect("inspect json");
    assert_eq!(
        inspect_value["ordered_services"][0]["slurm"]["mpi"]["type"],
        Value::from("pmix")
    );
}

#[test]
fn service_hooks_are_exposed_in_machine_readable_outputs_and_render() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "hooks.yaml",
        &format!(
            r#"
name: hooks-json
x-slurm:
  cache_dir: "{}"
services:
  trainer:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      prologue: |
        module load cuda/12.1
      epilogue:
        context: container
        script: |
          echo "job=${{SLURM_JOB_ID}}"
"#,
            cache_root.path().display()
        ),
    );

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_success(&validate);

    let config = run_cli(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&config);
    let config_value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
    assert_eq!(
        config_value["services"]["trainer"]["x-slurm"]["prologue"]["context"],
        Value::from("host")
    );
    assert!(
        config_value["services"]["trainer"]["x-slurm"]["epilogue"]["script"]
            .as_str()
            .unwrap_or_default()
            .contains("${SLURM_JOB_ID}")
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect);
    let inspect_value: Value = serde_json::from_str(&stdout_text(&inspect)).expect("inspect json");
    assert_eq!(
        inspect_value["ordered_services"][0]["slurm"]["epilogue"]["context"],
        Value::from("container")
    );

    let render = run_cli(
        tmpdir.path(),
        &[
            "render",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&render);
    let render_value: Value = serde_json::from_str(&stdout_text(&render)).expect("render json");
    let script = render_value["script"].as_str().unwrap_or_default();
    assert!(script.contains("trainer.host-prologue.sh"));
    assert!(script.contains("trainer.container-wrapper.sh"));
}

#[test]
fn validate_rejects_dependency_on_ignore_service() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
services:
  app:
    image: redis:7
    depends_on:
      - sidecar
  sidecar:
    image: redis:7
    x-slurm:
      failure_policy:
        mode: ignore
"#,
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("cannot be depended on"));
}

#[test]
fn validate_surfaces_interpolation_errors() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
services:
  app:
    image: ${MISSING_IMAGE}
"#,
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("missing variable 'MISSING_IMAGE'"));
}

#[test]
fn schema_command_emits_checked_in_schema() {
    let output = run_cli(&repo_root(), &["schema"]);
    assert_success(&output);
    assert!(stderr_text(&output).is_empty());

    let stdout = stdout_text(&output);
    let value: Value = serde_json::from_str(&stdout).expect("schema json");
    assert_eq!(
        value["$schema"],
        Value::from("https://json-schema.org/draft/2020-12/schema")
    );
    assert_eq!(value["additionalProperties"], Value::from(false));
    assert!(value["properties"]["services"].is_object());
    assert!(value["properties"]["x-slurm"].is_object());
    assert_eq!(
        value["$defs"]["dependencyCondition"]["properties"]["condition"]["enum"][1],
        Value::from("service_healthy")
    );
    assert_eq!(
        value["$defs"]["mpi"]["properties"]["type"]["enum"],
        serde_json::json!(["pmix", "pmi2", "pmi1", "openmpi"])
    );

    let checked_in = fs::read_to_string(repo_root().join("schema/hpc-compose.schema.json"))
        .expect("checked-in schema");
    let expected = if checked_in.ends_with('\n') {
        checked_in
    } else {
        format!("{checked_in}\n")
    };
    assert_eq!(stdout, expected);
}

#[test]
fn shipped_examples_render_to_stable_scripts() {
    let repo = repo_root();
    let examples = [
        "minimal-batch.yaml",
        "training-resume.yaml",
        "multi-node-mpi.yaml",
        "postgres-etl.yaml",
    ];

    for example in examples {
        let output = Command::new(bin_path())
            .current_dir(&repo)
            .args(["render", "-f"])
            .arg(repo.join("examples").join(example))
            .env_remove("CACHE_DIR")
            .output()
            .expect("render example");
        assert_success(&output);
        let script = stdout_text(&output);
        assert!(script.starts_with("#!/bin/bash\n# shellcheck shell=bash\n"));
        assert!(script.contains("set -euo pipefail\n"));
        assert!(script.contains("resolve_allocation_metadata()"));
        assert!(script.contains("monitor_services()"));
        assert!(!script.contains("${CACHE_DIR:-"));
    }
}

#[test]
fn rendered_minimal_batch_keeps_stable_header_section() {
    let repo = repo_root();
    let output = Command::new(bin_path())
        .current_dir(&repo)
        .args(["render", "-f"])
        .arg(repo.join("examples/minimal-batch.yaml"))
        .env_remove("CACHE_DIR")
        .output()
        .expect("render minimal example");
    assert_success(&output);

    let script = stdout_text(&output);
    let header = script.split("\n\n").next().expect("rendered script header");
    assert_eq!(
        header,
        "#!/bin/bash\n# shellcheck shell=bash\n# shellcheck disable=SC2016\n# Generated by hpc-compose for job minimal-batch\n#SBATCH --job-name=minimal-batch\n#SBATCH --nodes=1\n#SBATCH --time=00:10:00\n#SBATCH --cpus-per-task=2\n#SBATCH --mem=4G"
    );
    assert!(script.contains("local -a service_cmd=('/bin/sh' '-lc'"));
    assert!(script.contains("register_service 'app'"));
}
