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

    let default_script = tmpdir.path().join("hpc-compose.sbatch");
    assert!(!default_script.exists());
    let plan = run_cli(
        tmpdir.path(),
        &["plan", "-f", compose.to_str().expect("path")],
    );
    assert_success(&plan);
    let plan_stdout = stdout_text(&plan);
    assert!(plan_stdout.contains("spec is valid"));
    assert!(plan_stdout.contains("app"));
    assert!(!default_script.exists());

    let plan_tree = run_cli(
        tmpdir.path(),
        &["plan", "--tree", "-f", compose.to_str().expect("path")],
    );
    assert_success(&plan_tree);
    assert!(stdout_text(&plan_tree).contains("app"));
    assert!(!default_script.exists());

    let plan_script = run_cli(
        tmpdir.path(),
        &[
            "plan",
            "--show-script",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&plan_script);
    let plan_script_stdout = stdout_text(&plan_script);
    assert!(plan_script_stdout.contains("Rendered script:"));
    assert!(plan_script_stdout.contains("#SBATCH --job-name=demo"));
    assert!(!default_script.exists());

    let plan_json = run_cli(
        tmpdir.path(),
        &[
            "plan",
            "-f",
            compose.to_str().expect("path"),
            "--show-script",
            "--format",
            "json",
        ],
    );
    assert_success(&plan_json);
    let plan_value: Value = serde_json::from_str(&stdout_text(&plan_json)).expect("plan json");
    assert_eq!(plan_value["valid"], Value::from(true));
    assert_eq!(
        plan_value["runtime_plan"]["ordered_services"][0]["name"],
        Value::from("app")
    );
    assert!(
        plan_value["script"]
            .as_str()
            .unwrap_or_default()
            .contains("#SBATCH --job-name=demo")
    );
    assert!(!default_script.exists());
}

#[test]
fn inspect_dependencies_outputs_text_dot_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache");
    let image = tmpdir.path().join("image.sqsh");
    fs::write(&image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: deps
x-slurm:
  cache_dir: {}
services:
  db:
    image: {}
    command: ["sleep", "1"]
    readiness:
      type: sleep
      seconds: 1
  api:
    image: {}
    command: ["echo", "api"]
    depends_on:
      db:
        condition: service_healthy
"#,
            cache_dir.display(),
            image.display(),
            image.display()
        ),
    );

    let text = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--dependencies",
        ],
    );
    assert_success(&text);
    let text_out = stdout_text(&text);
    assert!(text_out.contains("dependency graph:"));
    assert!(text_out.contains("db -> api condition=service_healthy readiness=sleep"));

    let dot = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--dependencies",
            "--dependencies-format",
            "dot",
        ],
    );
    assert_success(&dot);
    let dot_out = stdout_text(&dot);
    assert!(dot_out.contains("digraph hpc_compose_dependencies"));
    assert!(dot_out.contains("\"db\" -> \"api\""));

    let json = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--dependencies",
            "--format",
            "json",
        ],
    );
    assert_success(&json);
    let value: Value = serde_json::from_str(&stdout_text(&json)).expect("deps json");
    assert_eq!(value["edges"][0]["from"], Value::from("db"));
    assert_eq!(value["edges"][0]["to"], Value::from("api"));

    let invalid = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--dependencies",
            "--dependencies-format",
            "dot",
            "--format",
            "json",
        ],
    );
    assert_failure(&invalid);
    assert!(stderr_text(&invalid).contains("cannot be combined"));
}

#[test]
fn validate_rejects_unsupported_spec_version_with_migration_hint() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
version: "2"
services:
  app:
    image: redis:7
"#,
    );

    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    let stderr = stderr_text(&validate);
    assert!(stderr.contains("unsupported hpc-compose spec version '2'"));
    assert!(stderr.contains("steps was renamed to services in v2"));
    assert!(stderr.contains("docs/migration-v2.md"));
}

#[test]
fn extends_is_visible_to_validate_config_plan_and_render() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::write(
        tmpdir.path().join("base.yaml"),
        r#"
name: base
x-slurm:
  job_name: from-base
services:
  app:
    image: redis:7
    command: echo base
    volumes:
      - ./base-data:/data
"#,
    )
    .expect("base");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
extends: base.yaml
name: child
services:
  app:
    command: echo child
    volumes:
      - ./child-data:/data
"#,
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
    assert_eq!(config_value["services"]["app"]["image"], "redis:7");
    assert_eq!(config_value["services"]["app"]["command"], "echo child");
    let volumes = config_value["services"]["app"]["volumes"]
        .as_array()
        .expect("volumes");
    assert_eq!(volumes.len(), 1);
    assert_eq!(volumes[0], "./child-data:/data");
    assert!(stdout_text(&config).find("extends").is_none());

    let plan = run_cli(
        tmpdir.path(),
        &[
            "plan",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&plan);
    let plan_value: Value = serde_json::from_str(&stdout_text(&plan)).expect("plan json");
    assert_eq!(plan_value["runtime_plan"]["name"], "from-base");

    let render = run_cli(
        tmpdir.path(),
        &["render", "-f", compose.to_str().expect("path")],
    );
    assert_success(&render);
    assert!(stdout_text(&render).contains("echo child"));
}

#[test]
fn lint_reports_opinionated_findings_and_allow_warnings_controls_exit() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join("shared")).expect("shared");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
x-slurm:
  mem: 256M
  cpus_per_task: 2
services:
  app:
    image: redis:7
    depends_on:
      - redis
  redis:
    image: redis:7
  sidecar:
    image: redis:7
    volumes:
      - ./shared:/shared
    x-slurm:
      failure_policy:
        mode: ignore
"#,
    );

    let lint = run_cli(
        tmpdir.path(),
        &["lint", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&lint);
    let stdout = stdout_text(&lint);
    assert!(stdout.contains("HPC001"));
    assert!(stdout.contains("HPC002"));
    assert!(stdout.contains("HPC003"));
    assert!(stderr_text(&lint).contains("lint found"));

    let lint_json = run_cli(
        tmpdir.path(),
        &[
            "lint",
            "-f",
            compose.to_str().expect("path"),
            "--allow-warnings",
            "--format",
            "json",
        ],
    );
    assert_success(&lint_json);
    let payload: Value = serde_json::from_str(&stdout_text(&lint_json)).expect("lint json");
    assert_eq!(payload["passed"], Value::from(true));
    assert_eq!(payload["error_count"], Value::from(0));
    assert!(payload["warning_count"].as_u64().unwrap_or_default() >= 3);
}

#[test]
fn render_emits_array_directive_and_forwards_array_environment() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  array: 0-9%2
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let script_path = tmpdir.path().join("array.sbatch");

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
    assert!(script.contains("#SBATCH --array=0-9%2"));
    assert!(script.contains("SLURM_ARRAY_TASK_ID"));
    assert!(script.contains("SLURM_ARRAY_JOB_ID"));
}

#[test]
fn render_applies_settings_resource_profile_defaults() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        r#"
version = 1

[resource_profiles.gpu-small]
partition = "gpu"
mem = "16G"
gpus = 1
cpus_per_task = 4
"#,
    )
    .expect("settings");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  resources: gpu-small
  mem: 32G
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );

    let render = run_cli(
        tmpdir.path(),
        &["render", "-f", compose.to_str().expect("path")],
    );
    assert_success(&render);
    let script = stdout_text(&render);
    assert!(script.contains("#SBATCH --partition=gpu"));
    assert!(script.contains("#SBATCH --gpus=1"));
    assert!(script.contains("#SBATCH --cpus-per-task=4"));
    assert!(script.contains("#SBATCH --mem=32G"));
    assert!(!script.contains("#SBATCH --mem=16G"));
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
        stdout_text(&inspect).contains("rebuild on prepare because prepare.mounts are present")
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

    let missing_image = tmpdir.path().join("missing.sqsh");
    let missing_compose = write_compose(
        tmpdir.path(),
        "missing-image.yaml",
        &format!(
            r#"
name: missing-image
x-slurm:
  cache_dir: "{}"
services:
  app:
    image: {}
    command: /bin/true
"#,
            cache_dir.display(),
            missing_image.display()
        ),
    );
    let quiet = run_cli(
        tmpdir.path(),
        &[
            "--quiet",
            "preflight",
            "-f",
            missing_compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&quiet);
    let quiet_stderr = stderr_text(&quiet);
    assert!(quiet_stderr.contains("Summary:"));
    assert!(quiet_stderr.contains("preflight failed"));
}

#[test]
fn config_variables_scopes_and_redacts_sensitive_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
name: config-vars
x-slurm:
  cache_dir: ${CACHE_DIR}
services:
  app:
    image: redis:7
    command: /bin/sh -lc "printf '%s' ${API_TOKEN}"
"#,
    );
    let cache_dir = cache_root.path().display().to_string();
    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--variables",
            "--format",
            "json",
        ],
        &[
            ("CACHE_DIR", cache_dir.as_str()),
            ("API_TOKEN", "super-secret-token"),
            ("UNUSED_SECRET", "should-not-appear"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("variables json");
    assert_eq!(payload["variables"]["CACHE_DIR"], Value::from(cache_dir));
    assert_eq!(payload["variables"]["API_TOKEN"], Value::from("<redacted>"));
    assert!(payload["variables"].get("UNUSED_SECRET").is_none());
    assert_eq!(payload["sources"]["API_TOKEN"], Value::from("processenv"));

    let output = run_cli_with_env(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--variables",
            "--show-values",
            "--format",
            "json",
        ],
        &[
            ("CACHE_DIR", cache_root.path().to_str().expect("cache path")),
            ("API_TOKEN", "super-secret-token"),
        ],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("variables json");
    assert_eq!(
        payload["variables"]["API_TOKEN"],
        Value::from("super-secret-token")
    );
}

#[test]
fn mpi_config_is_exposed_in_machine_readable_outputs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    fs::create_dir_all(tmpdir.path().join("site-mpi")).expect("site mpi");
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
        profile: openmpi
        implementation: openmpi
        launcher: srun
        expected_ranks: 4
        host_mpi:
          bind_paths:
            - ./site-mpi:/opt/site-mpi:ro
          env:
            MPI_DIR: /opt/site-mpi
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
    assert_eq!(
        config_value["services"]["worker"]["x-slurm"]["mpi"]["implementation"],
        Value::from("openmpi")
    );
    assert_eq!(
        config_value["services"]["worker"]["x-slurm"]["mpi"]["profile"],
        Value::from("openmpi")
    );
    assert_eq!(
        config_value["services"]["worker"]["x-slurm"]["mpi"]["expected_ranks"],
        Value::from(4)
    );
    assert_eq!(
        config_value["services"]["worker"]["x-slurm"]["mpi"]["host_mpi"]["env"]["MPI_DIR"],
        Value::from("/opt/site-mpi")
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
    assert_eq!(
        inspect_value["ordered_services"][0]["slurm"]["mpi"]["profile"],
        Value::from("openmpi")
    );
    let rendered_mounts = inspect_value["ordered_services"][0]["volumes"]
        .as_array()
        .expect("volumes");
    assert!(rendered_mounts.iter().any(|mount| {
        mount
            .as_str()
            .unwrap_or_default()
            .contains("site-mpi:/opt/site-mpi:ro")
    }));
    assert!(
        inspect_value["ordered_services"][0]["environment"]
            .as_array()
            .expect("environment")
            .iter()
            .any(|entry| entry[0] == "MPI_DIR")
    );
}

#[test]
fn mpi_profile_validation_rejects_unsupported_launcher_and_rank_mismatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let invalid_launcher = write_compose(
        tmpdir.path(),
        "invalid-launcher.yaml",
        &format!(
            r#"
services:
  worker:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      mpi:
        type: pmix
        launcher: mpirun
x-slurm:
  cache_dir: "{}"
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", invalid_launcher.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("mpirun"));

    let rank_mismatch = write_compose(
        tmpdir.path(),
        "rank-mismatch.yaml",
        &format!(
            r#"
name: rank-mismatch
x-slurm:
  nodes: 2
  cache_dir: "{}"
services:
  worker:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      nodes: 2
      ntasks_per_node: 2
      mpi:
        type: pmix
        expected_ranks: 3
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", rank_mismatch.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("expected_ranks=3"));
}

#[test]
fn mpi_profile_validation_rejects_profile_implementation_conflict() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "profile-conflict.yaml",
        &format!(
            r#"
services:
  worker:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      mpi:
        type: pmix
        profile: openmpi
        implementation: mpich
x-slurm:
  cache_dir: "{}"
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("profile=openmpi conflicts"));
}

#[test]
fn mpi_profiles_validate_for_openmpi_mpich_and_intel_mpi() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "profiles.yaml",
        &format!(
            r#"
services:
  open:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      mpi:
        type: pmix
        profile: openmpi
        implementation: openmpi
  mpich:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      mpi:
        type: pmi2
        profile: mpich
        implementation: mpich
  intel:
    image: debian:bookworm-slim
    command: /bin/true
    x-slurm:
      mpi:
        type: pmi2
        profile: intel_mpi
        implementation: intel_mpi
x-slurm:
  cache_dir: "{}"
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_success(&validate);
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
      hooks:
        - on: restart
          script: |
            echo "restart ${{HPC_COMPOSE_SERVICE_NAME}}"
        - on: window_exhausted
          context: host
          script: |
            echo "window exhausted"
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
    assert_eq!(
        config_value["services"]["trainer"]["x-slurm"]["hooks"][0]["on"],
        Value::from("restart")
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
    assert_eq!(
        inspect_value["ordered_services"][0]["slurm"]["hooks"][1]["on"],
        Value::from("window_exhausted")
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
    assert!(script.contains("trainer.host-event-restart-0.sh"));
    assert!(script.contains("trainer.host-event-window_exhausted-1.sh"));
    assert!(script.contains("trainer.container-wrapper.sh"));

    let schema = run_cli(tmpdir.path(), &["schema"]);
    assert_success(&schema);
    assert!(stdout_text(&schema).contains("\"serviceEventHook\""));
    assert!(stdout_text(&schema).contains("\"window_exhausted\""));
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
fn spec_foundation_aliases_and_normalizations_surface_in_outputs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "foundation.yaml",
        r#"
modules:
  - cuda/${CUDA_VERSION}
steps:
  single:
    image: redis:7
    command: echo ${TOKEN}
  multi:
    image: redis:7
    command: |
      echo ${TOKEN}
      python train.py
  list:
    image: redis:7
    command:
      - echo
      - ${TOKEN}
  scripted:
    image: redis:7
    script: |
      echo ${TOKEN}
      python train.py
    modules:
      - netcdf/${NETCDF_VERSION}
  explicit:
    image: redis:7
    command:
      - /bin/sh
      - -lc
      - |
        echo ${TOKEN}
        python train.py
"#,
    );

    let env = [
        ("CUDA_VERSION", "12.4"),
        ("NETCDF_VERSION", "4.9"),
        ("TOKEN", "expanded"),
    ];
    let validate = run_cli_with_env(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
        &env,
    );
    assert_success(&validate);

    let plan = run_cli_with_env(
        tmpdir.path(),
        &[
            "plan",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
        &env,
    );
    assert_success(&plan);
    let plan_value: Value = serde_json::from_str(&stdout_text(&plan)).expect("plan json");
    assert_eq!(plan_value["valid"], Value::from(true));

    let config = run_cli_with_env(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
        &env,
    );
    assert_success(&config);
    let config_value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
    assert_eq!(
        config_value["x-env"]["modules"]["load"],
        serde_json::json!(["cuda/12.4"])
    );
    assert_eq!(
        config_value["services"]["single"]["command"],
        Value::from("echo ${TOKEN}")
    );
    assert_eq!(
        config_value["services"]["list"]["command"],
        serde_json::json!(["echo", "expanded"])
    );
    assert_eq!(
        config_value["services"]["multi"]["command"],
        serde_json::json!(["/bin/sh", "-lc", "echo ${TOKEN}\npython train.py\n"])
    );
    assert_eq!(
        config_value["services"]["scripted"]["command"][0],
        config_value["services"]["explicit"]["command"][0]
    );
    assert_eq!(
        config_value["services"]["scripted"]["command"][1],
        config_value["services"]["explicit"]["command"][1]
    );
    assert_eq!(
        config_value["services"]["scripted"]["x-env"]["modules"]["load"],
        serde_json::json!(["netcdf/4.9"])
    );
}

#[test]
fn services_and_steps_together_fail_clearly() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "both.yaml",
        r#"
services:
  app:
    image: redis:7
steps:
  other:
    image: redis:7
"#,
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    assert!(stderr_text(&validate).contains("both top-level 'services' and 'steps'"));
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
        Value::from("http://json-schema.org/draft-07/schema")
    );
    assert_eq!(value["additionalProperties"], Value::from(false));
    assert_eq!(
        value["properties"]["version"]["oneOf"],
        serde_json::json!([
            {
                "type": "string",
                "const": "1"
            },
            {
                "type": "integer",
                "const": 1
            }
        ])
    );
    assert!(value["properties"]["extends"].is_object());
    assert!(value["properties"]["services"].is_object());
    assert!(value["properties"]["steps"].is_object());
    assert!(value["properties"]["modules"].is_object());
    assert!(value["properties"]["x-slurm"].is_object());
    assert!(value["definitions"]["rootExtends"].is_object());
    assert!(value["definitions"]["serviceExtends"].is_object());
    assert!(value["definitions"]["service"]["properties"]["extends"].is_object());
    assert!(value["definitions"]["service"]["properties"]["script"].is_object());
    assert!(value["definitions"]["service"]["properties"]["modules"].is_object());
    assert_eq!(
        value["definitions"]["dependencyCondition"]["properties"]["condition"]["enum"],
        serde_json::json!([
            "service_started",
            "service_healthy",
            "service_completed_successfully"
        ])
    );
    assert_eq!(
        value["definitions"]["mpi"]["properties"]["type"]["pattern"],
        Value::from("^[A-Za-z0-9_][A-Za-z0-9_.+-]*$")
    );
    assert_eq!(
        value["definitions"]["mpi"]["properties"]["launcher"]["enum"],
        serde_json::json!(["srun"])
    );
    assert_eq!(
        value["definitions"]["mpi"]["properties"]["implementation"]["enum"][0],
        Value::from("openmpi")
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
