mod support;

use std::collections::BTreeSet;
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
    let validate_stdout = stdout_text(&validate);
    assert!(validate_stdout.contains("spec is valid"));
    // Success output points along the authoring -> run funnel.
    assert!(validate_stdout.contains("Next:"));
    assert!(validate_stdout.contains(&format!("hpc-compose plan -f '{}'", compose.display())));
    assert!(validate_stdout.contains(&format!("hpc-compose up -f '{}'", compose.display())));

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
    // The "Next:" hint is text-only and must never leak into --format json.
    assert!(!stdout_text(&validate_json).contains("Next:"));
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
    // The rendered script can embed resolved secrets, so `render -o` must create
    // it owner-only (0600) like real-submission paths, not with the default umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&script_path)
            .expect("script meta")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "render -o must write the script as 0600"
        );
    }

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
    // The rendered diagnostic may line-wrap the doc path, so assert on stable
    // substrings rather than the full (wrappable) path.
    assert!(stderr.contains("docker-compose"));
    assert!(stderr.contains("migration"));
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
      - /shared/hpc-compose-lint:/shared
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
    let stderr = stderr_text(&lint);
    assert!(
        stdout.contains("HPC001"),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("HPC002"),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("HPC003"),
        "stdout:\n{stdout}\nstderr:\n{stderr}"
    );
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
fn lint_reports_node_local_cache_and_volume_findings() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        r#"
x-slurm:
  cache_dir: /tmp/hpc-compose-cache
services:
  app:
    image: redis:7
    volumes:
      - /tmp/data:/data
"#,
    );

    let lint = run_cli(
        tmpdir.path(),
        &["lint", "-f", compose.to_str().expect("path")],
    );
    let stdout = stdout_text(&lint);
    assert!(
        stdout.contains("HPC004") && stdout.contains("cache_dir"),
        "expected HPC004 in stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("HPC005") && stdout.contains("/tmp/data"),
        "expected HPC005 in stdout:\n{stdout}"
    );
    // Advisory rules must not be auto-fixable.
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
    let payload: Value = serde_json::from_str(&stdout_text(&lint_json)).expect("lint json");
    let fixable = payload["fixable_count"].as_u64().unwrap_or(0);
    assert_eq!(fixable, 0, "node-local findings must not be fixable");
}

#[test]
fn lint_fix_makes_depends_on_condition_explicit_and_preserves_comments() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "# top-level comment\nx-slurm:\n  cache_dir: ./cache\nservices:\n  # service comment\n  app: # inline\n    image: redis:7\n    depends_on:\n      - redis\n  redis:\n    image: redis:7\n",
    );

    // Dry-run must not modify the file but must propose the change.
    let dry = run_cli(
        tmpdir.path(),
        &[
            "lint",
            "-f",
            compose.to_str().expect("path"),
            "--fix",
            "--dry-run",
        ],
    );
    let dry_stdout = stdout_text(&dry);
    assert!(
        dry_stdout.contains("+        condition: service_started"),
        "dry-run diff should propose condition line:\n{dry_stdout}"
    );
    // File unchanged.
    let after_dry = fs::read_to_string(&compose).expect("read after dry-run");
    assert!(
        after_dry.contains("- redis"),
        "dry-run must not rewrite the file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&compose, fs::Permissions::from_mode(0o600))
            .expect("make compose private");
    }

    // Real fix writes the file. --allow-warnings keeps the exit code clean
    // because HPC001 (readiness mismatch) is advisory and stays after the fix.
    let fix = run_cli(
        tmpdir.path(),
        &[
            "lint",
            "-f",
            compose.to_str().expect("path"),
            "--fix",
            "--allow-warnings",
        ],
    );
    assert_success(&fix);
    let fix_stdout = stdout_text(&fix);
    assert!(
        fix_stdout.contains("Applied 1 fix(es)"),
        "expected applied summary:\n{fix_stdout}"
    );

    let after = fs::read_to_string(&compose).expect("read after fix");
    assert!(
        after.contains("redis:\n        condition: service_started"),
        "expected explicit condition after fix:\n{after}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = fs::metadata(&compose)
            .expect("compose metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "lint --fix must preserve private mode");
    }
    let leftovers = fs::read_dir(tmpdir.path())
        .expect("read temp dir")
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.contains(".tmp."))
        .collect::<Vec<_>>();
    assert!(
        leftovers.is_empty(),
        "lint --fix should not leave atomic write temp files behind: {leftovers:?}"
    );
    // Comments outside the rewritten block survive byte-for-byte.
    assert!(after.contains("# top-level comment"));
    assert!(after.contains("# service comment"));
    assert!(after.contains("# inline"));

    // Re-running lint should no longer report HPC006 for this edge.
    let again = run_cli(
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
    let payload: Value = serde_json::from_str(&stdout_text(&again)).expect("lint json");
    let codes = payload["findings"]
        .as_array()
        .map(|findings| {
            findings
                .iter()
                .filter_map(|finding| finding["code"].as_str().map(ToString::to_string))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    assert!(
        !codes.contains("HPC006"),
        "HPC006 should be resolved after --fix; remaining codes: {codes:?}"
    );
}

#[test]
fn lint_fix_is_noop_when_nothing_is_fixable() {
    // A spec whose only findings are advisory (node-local cache/volume) has no
    // fixable edges, so --fix must not rewrite the file or print a summary.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        "x-slurm:\n  cache_dir: /tmp/hpc-compose-cache\nservices:\n  app:\n    image: redis:7\n    volumes:\n      - /tmp/data:/data\n",
    );
    let before = fs::read_to_string(&compose).expect("read before");

    let fix = run_cli(
        tmpdir.path(),
        &[
            "lint",
            "-f",
            compose.to_str().expect("path"),
            "--fix",
            "--allow-warnings",
        ],
    );
    assert_success(&fix);
    let fix_stdout = stdout_text(&fix);
    assert!(
        !fix_stdout.contains("Applied"),
        "no fix should be applied when nothing is fixable:\n{fix_stdout}"
    );
    let after = fs::read_to_string(&compose).expect("read after");
    assert_eq!(
        before, after,
        "compose file must be unchanged when no fixes apply"
    );
}

#[test]
fn validate_suggests_dependency_condition_typo() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: redis:7
    depends_on:
      redis:
        condition: service_start
  redis:
    image: redis:7
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    let combined = format!("{}\n{}", stdout_text(&validate), stderr_text(&validate));
    // miette may wrap the help text across lines, so check the pieces.
    assert!(
        combined.contains("Did you") && combined.contains("service_started"),
        "expected a did-you-mean suggestion for the typo:\n{combined}"
    );
}

#[test]
fn validate_suggests_unknown_service_key_typo() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  app:
    image: redis:7
    comand: /bin/true
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    let combined = format!("{}\n{}", stdout_text(&validate), stderr_text(&validate));
    assert!(
        combined.contains("Did you") && combined.contains("command"),
        "expected a did-you-mean suggestion for the unknown key:\n{combined}"
    );
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
fn notify_email_renders_mail_user_and_normalized_mail_type() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "notify.yaml",
        r#"
name: notify-demo
x-slurm:
  notify:
    email:
      to: user@example.com
      on:
        - fail
        - start
        - end
services:
  app:
    image: redis:7
    command: /bin/true
"#,
    );

    let render = run_cli(
        tmpdir.path(),
        &["render", "-f", compose.to_str().expect("path")],
    );
    assert_success(&render);
    let script = stdout_text(&render);
    assert!(script.contains("#SBATCH --mail-user=user@example.com"));
    assert!(script.contains("#SBATCH --mail-type=BEGIN,END,FAIL"));
}

#[test]
fn render_cli_applies_gres_precedence_from_compose() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "gres.yaml",
        r#"
name: gres-demo
x-slurm:
  gres: gpu:h100:4
  gpus: 8
services:
  trainer:
    image: redis:7
    command: /bin/true
    x-slurm:
      gres: gpu:h100:2
      gpus: 4
"#,
    );

    let render = run_cli(
        tmpdir.path(),
        &["render", "-f", compose.to_str().expect("path")],
    );
    assert_success(&render);
    let script = stdout_text(&render);
    assert!(script.contains("#SBATCH --gres=gpu:h100:4"));
    assert!(!script.contains("#SBATCH --gpus=8"));
    assert!(script.contains("--gres=gpu:h100:2"));
    assert!(!script.contains("--gpus=4"));
}

#[test]
fn render_cli_emits_all_first_class_binding_flags_from_yaml() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "bindings.yaml",
        r#"
name: binding-demo
x-slurm:
  gpus_per_node: 4
  gpus_per_task: 1
  cpus_per_gpu: 8
  mem_per_gpu: 40G
  gpu_bind: closest
  cpu_bind: cores
  mem_bind: local
  distribution: block:block
  hint: nomultithread
services:
  trainer:
    image: redis:7
    command: /bin/true
    x-slurm:
      gpus_per_node: 2
      gpus_per_task: 1
      cpus_per_gpu: 6
      mem_per_gpu: 20G
      gpu_bind: closest
      cpu_bind: cores
      mem_bind: local
      distribution: cyclic
      hint: compute_bound
"#,
    );

    let render = run_cli(
        tmpdir.path(),
        &["render", "-f", compose.to_str().expect("path")],
    );
    assert_success(&render);
    let script = stdout_text(&render);
    for expected in [
        "#SBATCH --gpus-per-node=4",
        "#SBATCH --gpus-per-task=1",
        "#SBATCH --cpus-per-gpu=8",
        "#SBATCH --mem-per-gpu=40G",
        "#SBATCH --gpu-bind=closest",
        "#SBATCH --cpu-bind=cores",
        "#SBATCH --mem-bind=local",
        "#SBATCH --distribution=block:block",
        "#SBATCH --hint=nomultithread",
        "--gpus-per-node=2",
        "--gpus-per-task=1",
        "--cpus-per-gpu=6",
        "--mem-per-gpu=20G",
        "--gpu-bind=closest",
        "--cpu-bind=cores",
        "--mem-bind=local",
        "--distribution=cyclic",
        "--hint=compute_bound",
    ] {
        assert!(script.contains(expected), "missing {expected}");
    }
}

#[test]
fn config_effective_normalizes_notify_empty_on_and_all() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cases = [
        (
            "empty-on.yaml",
            "on: []",
            serde_json::json!(["end", "fail"]),
        ),
        (
            "all.yaml",
            "on:\n        - start\n        - all\n        - fail",
            serde_json::json!(["all"]),
        ),
    ];

    for (name, on_yaml, expected) in cases {
        let compose = write_compose(
            tmpdir.path(),
            name,
            &format!(
                r#"
name: notify-config
x-slurm:
  notify:
    email:
      to: user@example.com
      {on_yaml}
services:
  app:
    image: redis:7
    command: /bin/true
"#
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
        let value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
        assert_eq!(value["x-slurm"]["notify"]["email"]["on"], expected);
    }
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
fn config_effective_outputs_new_slurm_resource_fields_and_defaults() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "resources.yaml",
        &format!(
            r#"
name: resource-config
x-slurm:
  cache_dir: {}
  nodes: 4
  ntasks: 16
  ntasks_per_node: 4
  cpus_per_task: 2
  gres: gpu:a100:4
  gpus_per_node: 1
  gpus_per_task: 1
  cpus_per_gpu: 4
  mem_per_gpu: 24G
  gpu_bind: closest
  cpu_bind: cores
  mem_bind: local
  distribution: block:block
  hint: nomultithread
  metrics: {{}}
  notify:
    email:
      to: ops@example.com
      on:
        - fail
        - start
  rendezvous:
    discover:
      - api
    timeout_seconds: 30
    require: true
services:
  app:
    image: redis:7
    command: /bin/true
    x-slurm:
      placement:
        node_count: 2
        start_index: 1
        exclude: "2"
        allow_overlap: true
      ntasks: 3
      ntasks_per_node: 1
      cpus_per_task: 4
      gpus_per_node: 1
      gpus_per_task: 1
      cpus_per_gpu: 6
      mem_per_gpu: 12G
      gpu_bind: closest
      time_limit: "00:10:00"
      mpi:
        type: pmix
        expected_ranks: 3
      rendezvous:
        register:
          name: api
          port: 8080
          protocol: http
          path: /health
          ttl_seconds: 60
          metadata:
            role: api
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
    let value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
    let slurm = &value["x-slurm"];
    assert_eq!(
        slurm["cache_dir"],
        Value::from(cache_root.path().display().to_string())
    );
    assert_eq!(slurm["nodes"], Value::from(4));
    assert_eq!(slurm["ntasks"], Value::from(16));
    assert_eq!(slurm["ntasks_per_node"], Value::from(4));
    assert_eq!(slurm["cpus_per_task"], Value::from(2));
    assert_eq!(slurm["gres"], Value::from("gpu:a100:4"));
    assert_eq!(slurm["gpus_per_node"], Value::from(1));
    assert_eq!(slurm["gpus_per_task"], Value::from(1));
    assert_eq!(slurm["cpus_per_gpu"], Value::from(4));
    assert_eq!(slurm["mem_per_gpu"], Value::from("24G"));
    assert_eq!(slurm["gpu_bind"], Value::from("closest"));
    assert_eq!(slurm["cpu_bind"], Value::from("cores"));
    assert_eq!(slurm["mem_bind"], Value::from("local"));
    assert_eq!(slurm["distribution"], Value::from("block:block"));
    assert_eq!(slurm["hint"], Value::from("nomultithread"));
    assert_eq!(
        slurm["metrics"],
        serde_json::json!({
            "enabled": true,
            "interval_seconds": 5,
            "collectors": ["gpu", "slurm"]
        })
    );
    assert_eq!(
        slurm["notify"]["email"],
        serde_json::json!({"to": "ops@example.com", "on": ["start", "fail"]})
    );
    assert_eq!(slurm["rendezvous"]["discover"], serde_json::json!(["api"]));
    assert_eq!(slurm["rendezvous"]["timeout_seconds"], Value::from(30));
    assert_eq!(slurm["rendezvous"]["require"], Value::from(true));

    let service_slurm = &value["services"]["app"]["x-slurm"];
    assert_eq!(service_slurm["placement"]["node_count"], Value::from(2));
    assert_eq!(service_slurm["placement"]["start_index"], Value::from(1));
    assert_eq!(service_slurm["placement"]["exclude"], Value::from("2"));
    assert_eq!(
        service_slurm["placement"]["allow_overlap"],
        Value::from(true)
    );
    assert_eq!(service_slurm["ntasks"], Value::from(3));
    assert_eq!(service_slurm["ntasks_per_node"], Value::from(1));
    assert_eq!(service_slurm["cpus_per_task"], Value::from(4));
    assert_eq!(service_slurm["gpus_per_node"], Value::from(1));
    assert_eq!(service_slurm["gpus_per_task"], Value::from(1));
    assert_eq!(service_slurm["cpus_per_gpu"], Value::from(6));
    assert_eq!(service_slurm["mem_per_gpu"], Value::from("12G"));
    assert_eq!(service_slurm["gpu_bind"], Value::from("closest"));
    assert_eq!(service_slurm["time_limit"], Value::from("00:10:00"));
    assert_eq!(service_slurm["mpi"]["expected_ranks"], Value::from(3));
    assert_eq!(
        service_slurm["rendezvous"]["register"]["metadata"]["role"],
        Value::from("api")
    );
    assert_eq!(
        service_slurm["failure_policy"]["mode"],
        Value::from("fail_job")
    );
}

#[test]
fn config_effective_round_trips_parallelism_at_both_scopes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "parallelism.yaml",
        &format!(
            r#"
name: parallelism-config
x-slurm:
  cache_dir: "{}"
  nodes: 2
  gpus_per_node: 2
  parallelism:
    tensor: 2
    pipeline: 2
services:
  trainer:
    image: pytorch:latest
    command: /train.sh
    x-slurm:
      gpus_per_node: 4
      parallelism:
        tensor: 4
        pipeline: 1
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
    let value: Value = serde_json::from_str(&stdout_text(&config)).expect("config json");
    assert_eq!(
        value["x-slurm"]["parallelism"],
        serde_json::json!({ "tensor": 2, "pipeline": 2 })
    );
    assert_eq!(
        value["services"]["trainer"]["x-slurm"]["parallelism"],
        serde_json::json!({ "tensor": 4, "pipeline": 1 })
    );
}

#[test]
fn validate_rejects_parallelism_gpu_mismatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "parallelism-mismatch.yaml",
        &format!(
            r#"
name: parallelism-mismatch
x-slurm:
  cache_dir: "{}"
services:
  trainer:
    image: pytorch:latest
    command: /train.sh
    x-slurm:
      nodes: 1
      gpus_per_node: 2
      parallelism:
        tensor: 2
        pipeline: 2
"#,
            cache_root.path().display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert!(
        !output.status.success(),
        "validate must fail on a parallelism/GPU mismatch"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("parallelism") && stderr.contains("gpus_per_node"),
        "expected a scoped parallelism/GPU mismatch diagnostic, got: {stderr}"
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
    assert_eq!(
        value["definitions"]["positiveInteger"]["minimum"],
        Value::from(1)
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
fn schema_matches_new_resource_validation_surface() {
    let output = run_cli(&repo_root(), &["schema"]);
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("schema json");
    let slurm = &value["definitions"]["slurm"]["properties"];
    let service_slurm = &value["definitions"]["serviceSlurm"]["properties"];
    let placement = &value["definitions"]["servicePlacement"]["properties"];

    for field in [
        "ntasks",
        "ntasks_per_node",
        "gpus_per_node",
        "gpus_per_task",
        "cpus_per_gpu",
    ] {
        assert_eq!(
            slurm[field]["$ref"],
            Value::from("#/definitions/positiveInteger"),
            "top-level x-slurm.{field}"
        );
        assert_eq!(
            service_slurm[field]["$ref"],
            Value::from("#/definitions/positiveInteger"),
            "service x-slurm.{field}"
        );
    }
    assert_eq!(
        placement["node_count"]["$ref"],
        Value::from("#/definitions/positiveInteger")
    );
    assert_eq!(placement["node_percent"]["minimum"], Value::from(1));
    assert_eq!(placement["node_percent"]["maximum"], Value::from(100));
    assert_eq!(
        placement["start_index"]["$ref"],
        Value::from("#/definitions/nonNegativeInteger")
    );
    assert_eq!(
        value["definitions"]["mpi"]["properties"]["expected_ranks"]["$ref"],
        Value::from("#/definitions/positiveInteger")
    );
    for scope in ["slurm", "serviceSlurm"] {
        assert_eq!(
            value["definitions"][scope]["properties"]["parallelism"]["$ref"],
            Value::from("#/definitions/parallelism"),
            "{scope} x-slurm.parallelism ref"
        );
    }
    let parallelism = &value["definitions"]["parallelism"];
    assert_eq!(
        parallelism["properties"]["tensor"]["$ref"],
        Value::from("#/definitions/positiveInteger")
    );
    assert_eq!(
        parallelism["properties"]["pipeline"]["$ref"],
        Value::from("#/definitions/positiveInteger")
    );
    assert_eq!(
        parallelism["required"],
        serde_json::json!(["tensor", "pipeline"])
    );
    assert_eq!(parallelism["additionalProperties"], Value::from(false));
}

fn load_schema_json() -> Value {
    let output = run_cli(&repo_root(), &["schema"]);
    assert_success(&output);
    serde_json::from_str(&stdout_text(&output)).expect("schema json")
}

fn schema_definition_keys(value: &Value, definition: &str) -> BTreeSet<String> {
    value["definitions"][definition]["properties"]
        .as_object()
        .unwrap_or_else(|| panic!("definition `{definition}` has no properties"))
        .keys()
        .cloned()
        .collect()
}

#[test]
fn schema_root_and_service_keys_match_parser_whitelists() {
    let value = load_schema_json();

    let root_keys: BTreeSet<&str> = [
        "extends", "name", "modules", "runtime", "secrets", "services", "steps", "sweep",
        "version", "x-env", "x-slurm",
    ]
    .into_iter()
    .collect();
    let actual_root: BTreeSet<String> = value["properties"]
        .as_object()
        .expect("root properties")
        .keys()
        .cloned()
        .collect();
    let root_ref: BTreeSet<String> = root_keys.iter().map(|s| s.to_string()).collect();
    assert_eq!(actual_root, root_ref, "root property key drift");

    let service_keys: BTreeSet<&str> = [
        "extends",
        "image",
        "command",
        "entrypoint",
        "script",
        "environment",
        "modules",
        "volumes",
        "working_dir",
        "depends_on",
        "readiness",
        "healthcheck",
        "assert",
        "x-env",
        "x-slurm",
        "x-runtime",
        "x-enroot",
    ]
    .into_iter()
    .collect();
    let actual_service = schema_definition_keys(&value, "service");
    let service_ref: BTreeSet<String> = service_keys.iter().map(|s| s.to_string()).collect();
    assert_eq!(actual_service, service_ref, "service property key drift");
}

#[test]
fn schema_definition_property_keys_match_exhaustive_catalog() {
    let value = load_schema_json();

    let catalog: &[(&str, &[&str])] = &[
        (
            "slurm",
            &[
                "resources",
                "job_name",
                "partition",
                "account",
                "qos",
                "time",
                "nodes",
                "ntasks",
                "ntasks_per_node",
                "cpus_per_task",
                "mem",
                "gres",
                "gpus",
                "gpus_per_node",
                "gpus_per_task",
                "cpus_per_gpu",
                "mem_per_gpu",
                "gpu_bind",
                "cpu_bind",
                "mem_bind",
                "distribution",
                "hint",
                "constraint",
                "output",
                "error",
                "chdir",
                "array",
                "after_job",
                "dependency",
                "cache_dir",
                "enroot_temp_dir",
                "runtime_root",
                "cleanup",
                "scratch",
                "stage_in",
                "stage_out",
                "burst_buffer",
                "metrics",
                "artifacts",
                "resume",
                "notify",
                "setup",
                "submit_args",
                "rendezvous",
                "parallelism",
            ],
        ),
        ("cleanup", &["runtime_cache"]),
        (
            "serviceSlurm",
            &[
                "nodes",
                "placement",
                "ntasks",
                "ntasks_per_node",
                "cpus_per_task",
                "gpus",
                "gres",
                "gpus_per_node",
                "gpus_per_task",
                "cpus_per_gpu",
                "mem_per_gpu",
                "gpu_bind",
                "cpu_bind",
                "mem_bind",
                "distribution",
                "hint",
                "time_limit",
                "extra_srun_args",
                "mpi",
                "failure_policy",
                "prologue",
                "epilogue",
                "hooks",
                "scratch",
                "rendezvous",
                "parallelism",
            ],
        ),
        ("scratch", &["scope", "base", "mount", "cleanup"]),
        ("stageIn", &["from", "to", "mode", "hf"]),
        ("hfStageSource", &["repo", "revision", "kind"]),
        ("stageOut", &["from", "to", "when", "mode"]),
        ("burstBuffer", &["directives"]),
        ("metrics", &["enabled", "interval_seconds", "collectors"]),
        ("artifacts", &["collect", "export_dir", "paths", "bundles"]),
        ("artifactBundle", &["paths"]),
        ("resume", &["path"]),
        ("notify", &["email"]),
        ("emailNotify", &["to", "on"]),
        ("serviceScratch", &["enabled"]),
        (
            "servicePlacement",
            &[
                "node_range",
                "node_count",
                "node_percent",
                "share_with",
                "start_index",
                "exclude",
                "allow_overlap",
            ],
        ),
        ("serviceEventHook", &["on", "context", "script"]),
        (
            "mpi",
            &[
                "type",
                "profile",
                "implementation",
                "launcher",
                "expected_ranks",
                "host_mpi",
            ],
        ),
        ("hostMpi", &["bind_paths", "env"]),
        ("parallelism", &["tensor", "pipeline"]),
        (
            "failurePolicy",
            &[
                "mode",
                "max_restarts",
                "backoff_seconds",
                "window_seconds",
                "max_restarts_in_window",
            ],
        ),
        (
            "serviceAssert",
            &["exit_code", "artifacts_contain", "max_duration_seconds"],
        ),
        (
            "healthcheck",
            &[
                "test",
                "timeout",
                "disable",
                "interval",
                "retries",
                "start_period",
            ],
        ),
        ("serviceRendezvous", &["register"]),
        ("prepare", &["commands", "mounts", "env", "root"]),
        ("softwareEnv", &["modules", "spack", "env"]),
        ("runtime", &["backend", "gpu"]),
        ("serviceRuntime", &["prepare"]),
        ("serviceEnroot", &["prepare"]),
        (
            "sweep",
            &["parameters", "matrix", "objective", "replicates"],
        ),
        (
            "sweepObjective",
            &[
                "direction",
                "log_pattern",
                "group",
                "json_path",
                "json_field",
                "scaling_axis",
            ],
        ),
    ];

    for (def_name, expected_keys) in catalog {
        let expected: BTreeSet<String> = expected_keys.iter().map(|s| s.to_string()).collect();
        let actual = schema_definition_keys(&value, def_name);
        assert_eq!(
            actual, expected,
            "property key drift in definition `{def_name}`"
        );

        assert_eq!(
            value["definitions"][def_name]["additionalProperties"],
            Value::from(false),
            "definition `{def_name}` must have additionalProperties: false"
        );
    }
}

#[test]
fn sweep_objective_definition_includes_scaling_axis() {
    let value = load_schema_json();
    let scaling_axis = &value["definitions"]["sweepObjective"]["properties"]["scaling_axis"];
    assert_eq!(
        scaling_axis["type"],
        Value::from("string"),
        "sweepObjective.scaling_axis must be a string property"
    );
    assert!(
        scaling_axis["description"]
            .as_str()
            .is_some_and(|d| d.contains("scaling report")),
        "sweepObjective.scaling_axis must document the scaling report"
    );
}

#[test]
fn schema_enum_values_match_rust_variants() {
    let value = load_schema_json();
    let slurm = &value["definitions"]["slurm"]["properties"];
    let service_slurm = &value["definitions"]["serviceSlurm"]["properties"];

    let cases: &[(&str, Value, &[&str])] = &[
        // runtime enums
        (
            "runtime.backend",
            value["definitions"]["runtime"]["properties"]["backend"]["enum"].clone(),
            &["pyxis", "apptainer", "singularity", "host"],
        ),
        (
            "runtime.gpu",
            value["definitions"]["runtime"]["properties"]["gpu"]["enum"].clone(),
            &["auto", "none", "nvidia"],
        ),
        // mpi enums
        (
            "mpi.implementation",
            value["definitions"]["mpi"]["properties"]["implementation"]["enum"].clone(),
            &[
                "openmpi",
                "mpich",
                "intel_mpi",
                "mvapich2",
                "cray_mpi",
                "hpe_mpi",
                "unknown",
            ],
        ),
        (
            "mpi.profile",
            value["definitions"]["mpi"]["properties"]["profile"]["enum"].clone(),
            &["openmpi", "mpich", "intel_mpi"],
        ),
        (
            "mpi.launcher",
            value["definitions"]["mpi"]["properties"]["launcher"]["enum"].clone(),
            &["srun"],
        ),
        // scratch enums
        (
            "scratch.scope",
            value["definitions"]["scratch"]["properties"]["scope"]["enum"].clone(),
            &["shared", "node_local"],
        ),
        (
            "scratch.cleanup",
            value["definitions"]["scratch"]["properties"]["cleanup"]["enum"].clone(),
            &["always", "on_success", "never"],
        ),
        (
            "cleanup.runtime_cache",
            value["definitions"]["cleanup"]["properties"]["runtime_cache"]["enum"].clone(),
            &["never", "on_success", "always"],
        ),
        // stage enums
        (
            "stage_mode",
            value["definitions"]["stageMode"]["enum"].clone(),
            &["rsync", "copy"],
        ),
        (
            "stage_out.when",
            value["definitions"]["stageOut"]["properties"]["when"]["enum"].clone(),
            &["always", "on_success", "on_failure"],
        ),
        // metrics
        (
            "metrics.collectors[item]",
            value["definitions"]["metrics"]["properties"]["collectors"]["items"]["enum"].clone(),
            &["gpu", "slurm"],
        ),
        // artifacts
        (
            "artifacts.collect",
            value["definitions"]["artifacts"]["properties"]["collect"]["enum"].clone(),
            &["always", "on_success", "on_failure"],
        ),
        // notify
        (
            "email.on[item]",
            value["definitions"]["emailNotify"]["properties"]["on"]["items"]["enum"].clone(),
            &["start", "end", "fail", "all"],
        ),
        // failure_policy
        (
            "failure_policy.mode",
            value["definitions"]["failurePolicy"]["properties"]["mode"]["enum"].clone(),
            &["fail_job", "ignore", "restart_on_failure"],
        ),
        // service hooks
        (
            "service_event_hook.on",
            value["definitions"]["serviceEventHook"]["properties"]["on"]["enum"].clone(),
            &["restart", "window_exhausted"],
        ),
        (
            "service_hook.context",
            value["definitions"]["serviceHook"]["oneOf"][1]["properties"]["context"]["enum"]
                .clone(),
            &["host", "container"],
        ),
        // dependency conditions
        (
            "depends_on.condition",
            value["definitions"]["dependencyCondition"]["properties"]["condition"]["enum"].clone(),
            &[
                "service_started",
                "service_healthy",
                "service_completed_successfully",
            ],
        ),
        (
            "after_job.condition",
            value["definitions"]["jobDependency"]["oneOf"][1]["properties"]["condition"]["enum"]
                .clone(),
            &["afterany", "afterok", "afternotok"],
        ),
        // top-level dependency mode
        (
            "dependency",
            slurm["dependency"]["enum"].clone(),
            &["singleton"],
        ),
    ];

    // Touch service_slurm so the binding isn't flagged unused
    let _ = &service_slurm["mpi"];

    for (label, actual, expected) in cases {
        let expected_json: Vec<Value> = expected.iter().map(|s| Value::from(*s)).collect();
        assert_eq!(
            actual,
            &Value::from(expected_json),
            "enum value drift for `{label}`"
        );
    }
}

#[test]
fn schema_settings_command_emits_checked_in_schema() {
    let output = run_cli(&repo_root(), &["schema", "--kind", "settings"]);
    assert_success(&output);
    assert!(stderr_text(&output).is_empty());

    let stdout = stdout_text(&output);
    let value: Value = serde_json::from_str(&stdout).expect("settings schema json");
    assert_eq!(
        value["$schema"],
        Value::from("http://json-schema.org/draft-07/schema")
    );
    assert_eq!(value["additionalProperties"], Value::from(false));
    assert_eq!(value["properties"]["version"]["const"], Value::from(1));
    assert!(value["properties"]["defaults"].is_object());
    assert!(value["properties"]["profiles"].is_object());
    assert!(value["properties"]["resource_profiles"].is_object());

    let checked_in =
        fs::read_to_string(repo_root().join("schema/hpc-compose-settings.schema.json"))
            .expect("checked-in settings schema");
    let expected = if checked_in.ends_with('\n') {
        checked_in
    } else {
        format!("{checked_in}\n")
    };
    assert_eq!(stdout, expected);
}

#[test]
fn settings_schema_definition_keys_match_exhaustive_catalog() {
    let output = run_cli(&repo_root(), &["schema", "--kind", "settings"]);
    assert_success(&output);
    let value: Value = serde_json::from_str(&stdout_text(&output)).expect("settings schema json");

    let catalog: &[(&str, &[&str])] = &[
        (
            "profileDefaults",
            &[
                "compose_file",
                "env_files",
                "env",
                "binaries",
                "cache",
                "login_host",
                "login_user",
            ],
        ),
        (
            "binaryOverrides",
            &[
                "enroot",
                "apptainer",
                "singularity",
                "salloc",
                "sbatch",
                "srun",
                "scontrol",
                "sinfo",
                "squeue",
                "sacct",
                "sstat",
                "scancel",
                "sshare",
                "sprio",
            ],
        ),
        ("cacheSettings", &["dir", "enroot_temp_dir"]),
        (
            "resourceProfile",
            &[
                "partition",
                "account",
                "qos",
                "time",
                "nodes",
                "ntasks",
                "ntasks_per_node",
                "cpus_per_task",
                "mem",
                "gres",
                "gpus",
                "gpus_per_node",
                "gpus_per_task",
                "cpus_per_gpu",
                "mem_per_gpu",
                "gpu_bind",
                "cpu_bind",
                "mem_bind",
                "distribution",
                "hint",
                "constraint",
            ],
        ),
        (
            "watchSettings",
            &["sort", "wrap", "refresh_ms", "metrics_refresh_ms", "mouse"],
        ),
    ];

    for (def_name, expected_keys) in catalog {
        let expected: BTreeSet<String> = expected_keys.iter().map(|s| s.to_string()).collect();
        let actual = schema_definition_keys(&value, def_name);
        assert_eq!(
            actual, expected,
            "settings property key drift in definition `{def_name}`"
        );
        assert_eq!(
            value["definitions"][def_name]["additionalProperties"],
            Value::from(false),
            "settings definition `{def_name}` must have additionalProperties: false"
        );
    }
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

#[test]
fn secrets_from_file_resolve_and_are_redacted_in_config() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    fs::write(tmpdir.path().join("token.txt"), "hf-secret-value-123\n").expect("secret file");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: secrets-demo
x-slurm:
  cache_dir: {}
secrets:
  hf_token:
    file: ./token.txt
services:
  app:
    image: redis:7
    environment:
      HF_TOKEN: ${{hf_token}}
      MODEL: llama
    command: ["/bin/sh", "-lc", "echo prefix-${{hf_token}}"]
"#,
            cache_root.path().display()
        ),
    );

    // validate must accept the secrets block and resolve ${hf_token}.
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_success(&validate);

    // config must redact the secret-sourced value but keep benign values.
    let config = run_cli(
        tmpdir.path(),
        &["config", "-f", compose.to_str().expect("path")],
    );
    let config_stdout = stdout_text(&config);
    assert!(
        config_stdout.contains("HF_TOKEN: <redacted>"),
        "secret value must be redacted in config:\n{config_stdout}"
    );
    assert!(
        config_stdout.contains("MODEL: llama"),
        "benign value must remain:\n{config_stdout}"
    );
    assert!(
        !config_stdout.contains("hf-secret-value-123"),
        "raw secret value must never appear:\n{config_stdout}"
    );
    assert!(
        config_stdout.contains("echo prefix-<redacted>"),
        "secret interpolation outside environment must be redacted:\n{config_stdout}"
    );

    let config_json = run_cli(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&config_json);
    let config_json_stdout = stdout_text(&config_json);
    assert!(
        !config_json_stdout.contains("hf-secret-value-123"),
        "config JSON must not leak raw secret values:\n{config_json_stdout}"
    );
    let config_payload: Value =
        serde_json::from_str(&config_json_stdout).expect("config json redacted");
    assert_eq!(
        config_payload["services"]["app"]["environment"]["HF_TOKEN"],
        Value::from("<redacted>")
    );
    assert_eq!(
        config_payload["services"]["app"]["command"][2],
        Value::from("echo prefix-<redacted>")
    );

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
    let inspect_json_stdout = stdout_text(&inspect_json);
    assert!(
        !inspect_json_stdout.contains("hf-secret-value-123"),
        "inspect JSON must not leak raw secret values:\n{inspect_json_stdout}"
    );
    assert!(
        inspect_json_stdout.contains("echo prefix-<redacted>"),
        "runtime plan command should redact secret interpolation:\n{inspect_json_stdout}"
    );

    let inspect_verbose = run_cli(
        tmpdir.path(),
        &[
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--verbose",
        ],
    );
    assert_success(&inspect_verbose);
    let inspect_verbose_stdout = stdout_text(&inspect_verbose);
    assert!(
        !inspect_verbose_stdout.contains("hf-secret-value-123"),
        "inspect verbose text must not leak raw secret values:\n{inspect_verbose_stdout}"
    );
    assert!(
        inspect_verbose_stdout.contains("echo prefix-<redacted>"),
        "inspect verbose command should redact secret interpolation:\n{inspect_verbose_stdout}"
    );

    // --show-values must reveal the secret for operators who opt in.
    let revealed = run_cli(
        tmpdir.path(),
        &[
            "config",
            "-f",
            compose.to_str().expect("path"),
            "--show-values",
        ],
    );
    assert!(stdout_text(&revealed).contains("HF_TOKEN: hf-secret-value-123"));
    assert!(stdout_text(&revealed).contains("echo prefix-hf-secret-value-123"));
}

#[test]
fn secrets_from_env_resolve_via_process_env() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: secrets-env-demo
x-slurm:
  cache_dir: {}
secrets:
  db_password:
    env: DB_PASSWORD
services:
  app:
    image: redis:7
    environment:
      DB_PASSWORD: ${{db_password}}
    command: /bin/true
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli_with_env(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
        &[("DB_PASSWORD", "s3cret")],
    );
    assert_success(&validate);
    let config = run_cli_with_env(
        tmpdir.path(),
        &["config", "-f", compose.to_str().expect("path")],
        &[("DB_PASSWORD", "s3cret")],
    );
    let config_stdout = stdout_text(&config);
    assert!(config_stdout.contains("DB_PASSWORD: <redacted>"));
    assert!(!config_stdout.contains("s3cret"));
}

#[test]
fn secrets_reject_when_both_file_and_env_set() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    fs::write(tmpdir.path().join("tok.txt"), "x\n").expect("secret file");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
name: secrets-bad
x-slurm:
  cache_dir: {}
secrets:
  bad:
    file: ./tok.txt
    env: SOME_VAR
services:
  app:
    image: redis:7
    command: /bin/true
"#,
            cache_root.path().display()
        ),
    );
    let validate = run_cli(
        tmpdir.path(),
        &["validate", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&validate);
    let combined = format!("{}\n{}", stdout_text(&validate), stderr_text(&validate));
    assert!(
        combined.contains("exactly one of 'file' or 'env'"),
        "expected one-of validation error:\n{combined}"
    );
}

// --- staged-input content-addressed store (CAS) ---

fn seed_staged_input(
    cache_dir: &std::path::Path,
    kind: hpc_compose::cache::dataset::StagedInputKind,
    uri: &str,
) -> std::path::PathBuf {
    use hpc_compose::cache::dataset::{StagedInputProof, StagedInputSpec, ensure_staged_input};
    let spec = StagedInputSpec::new(kind, uri, Some("v1".into()));
    let (dir, _action) = ensure_staged_input(cache_dir, &spec, |dest| {
        fs::write(dest.join("payload.bin"), b"data").expect("payload");
        Ok(StagedInputProof::default())
    })
    .expect("seed staged input");
    dir
}

#[test]
fn cache_list_reports_staged_dataset_entries() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    let dir = seed_staged_input(
        &cache_dir,
        hpc_compose::cache::dataset::StagedInputKind::Dataset,
        "hf://org/cifar10",
    );
    assert!(dir.is_dir());

    let list_text = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "list",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&list_text);
    assert!(stdout_text(&list_text).contains("dataset"));

    let list_json = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "list",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&list_json);
    let value: Value = serde_json::from_str(&stdout_text(&list_json)).expect("list json");
    let entries = value.as_array().expect("entries array");
    let dataset = entries
        .iter()
        .find(|e| e["kind"] == "dataset")
        .expect("dataset entry present");
    assert_eq!(dataset["uri"], Value::from("hf://org/cifar10"));
    assert_eq!(dataset["revision"], Value::from("v1"));
    assert_eq!(
        dataset["artifact_path"],
        Value::from(dir.display().to_string())
    );
}

#[test]
fn cache_prune_age_removes_staged_inputs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    fs::create_dir_all(&cache_dir).expect("cache dir");
    let dir = seed_staged_input(
        &cache_dir,
        hpc_compose::cache::dataset::StagedInputKind::Model,
        "hf://org/llm",
    );
    assert!(dir.is_dir());

    let prune = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--age",
            "0",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--yes",
        ],
    );
    assert_success(&prune);
    assert!(!dir.exists(), "staged model dir removed by prune --age 0");
}

#[test]
fn rendered_script_unchanged_by_staged_input_cache() {
    // A CAS entry living under the same cache_dir must not change the rendered
    // batch script, and must never leak a datasets/models path or hf:// URI.
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let before = run_cli(
        tmpdir.path(),
        &[
            "plan",
            "--show-script",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&before);
    let before_value: Value = serde_json::from_str(&stdout_text(&before)).expect("before json");
    let before_script = before_value["script"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(!before_script.is_empty());

    // Seed staged inputs (dataset + model) under the very same cache dir.
    seed_staged_input(
        &cache_dir,
        hpc_compose::cache::dataset::StagedInputKind::Dataset,
        "hf://org/cifar10",
    );
    seed_staged_input(
        &cache_dir,
        hpc_compose::cache::dataset::StagedInputKind::Model,
        "hf://org/llm",
    );

    let after = run_cli(
        tmpdir.path(),
        &[
            "plan",
            "--show-script",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&after);
    let after_value: Value = serde_json::from_str(&stdout_text(&after)).expect("after json");
    let after_script = after_value["script"].as_str().unwrap_or_default();

    assert_eq!(
        before_script, after_script,
        "presence of a CAS entry must not alter the rendered script"
    );
    assert!(
        !after_script.contains("/datasets/") && !after_script.contains("/models/"),
        "no staged-input cache path may leak into the rendered script"
    );
    assert!(
        !after_script.contains("hf://"),
        "no staged-input URI may leak into the rendered script"
    );
}
