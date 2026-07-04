mod support;

use std::fs;

use serde_json::Value;
use support::*;

#[test]
fn prepare_rejects_removed_force_alias() {
    // `--force` was a deprecated alias for `prepare --force-rebuild`; it has
    // been removed so `--force` only ever means "overwrite file" (new/evolve).
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = run_cli(tmpdir.path(), &["prepare", "--force"]);
    assert_failure(&output);
    let stderr = stderr_text(&output);
    assert!(stderr.contains("unexpected argument '--force'"));
    assert!(stderr.contains("--force-rebuild"));
}

#[test]
fn prepare_and_cache_commands_manage_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let enroot = write_fake_enroot(tmpdir.path());

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
    let prepare_stderr = stderr_text(&prepare);
    assert!(prepare_stderr.contains("[run] Preparing runtime artifacts"));
    assert!(prepare_stderr.contains("[done] Preparing runtime artifacts"));
    let prepare_stdout = stdout_text(&prepare);
    assert!(prepare_stdout.contains("BUILD") && prepare_stdout.contains("app"));
    assert!(plan.ordered_services[0].runtime_image.exists());
    assert!(
        hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists()
    );

    let prepare_json = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&prepare_json);
    let prepare_value: Value =
        serde_json::from_str(&stdout_text(&prepare_json)).expect("prepare json");
    assert_eq!(
        prepare_value["services"][0]["service_name"],
        Value::from("app")
    );

    let list = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "list",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&list);
    let list_stdout = stdout_text(&list);
    assert!(list_stdout.contains("prepared"));
    assert!(list_stdout.contains("base"));

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
    let list_value: Value = serde_json::from_str(&stdout_text(&list_json)).expect("list json");
    assert!(
        list_value
            .as_array()
            .map(|entries| entries.len())
            .unwrap_or(0)
            >= 2
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
        ],
    );
    assert_success(&inspect);
    let inspect_stdout = stdout_text(&inspect);
    assert!(inspect_stdout.contains("manifest kind: prepared"));
    assert!(inspect_stdout.contains("current reuse expectation: cache hit"));

    let inspect_json = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--format",
            "json",
        ],
    );
    assert_success(&inspect_json);
    let inspect_value: Value =
        serde_json::from_str(&stdout_text(&inspect_json)).expect("inspect json");
    assert_eq!(
        inspect_value["services"][0]["service_name"],
        Value::from("app")
    );
    assert_eq!(
        inspect_value["services"][0]["runtime_artifact"]["manifest"]["kind"],
        Value::from("prepared")
    );

    for artifact in [
        hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image),
        hpc_compose::cache::manifest_path_for(&hpc_compose::prepare::base_image_path(
            &plan.cache_dir,
            &plan.ordered_services[0],
        )),
    ] {
        let mut manifest: Value =
            serde_json::from_str(&fs::read_to_string(&artifact).expect("manifest")).expect("json");
        manifest["created_at"] = Value::from(1_u64);
        manifest["last_used_at"] = Value::from(1_u64);
        fs::write(
            &artifact,
            serde_json::to_vec_pretty(&manifest).expect("serialize"),
        )
        .expect("rewrite manifest");
    }

    let prune = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--yes",
            "--age",
            "1",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&prune);
    assert!(stdout_text(&prune).contains("removed: 2"));
    assert!(!plan.ordered_services[0].runtime_image.exists());

    let prune_json = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--yes",
            "--age",
            "1",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&prune_json);
    let prune_value: Value = serde_json::from_str(&stdout_text(&prune_json)).expect("prune json");
    assert_eq!(prune_value["mode"], Value::from("age"));
    assert_eq!(prune_value["removed_count"], Value::from(0));
}

#[test]
fn cache_prune_age_with_cache_dir_skips_broken_context_resolution() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        r#"
version = 1
default_profile = "dev"

[profiles.dev]
compose_file = "missing-compose.yaml"
"#,
    )
    .expect("settings");

    let prune = run_cli(
        tmpdir.path(),
        &[
            "--profile",
            "dev",
            "cache",
            "prune",
            "--yes",
            "--age",
            "1",
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&prune);
    let payload: Value = serde_json::from_str(&stdout_text(&prune)).expect("prune json");
    assert_eq!(
        payload["cache_dir"],
        Value::from(cache_dir.display().to_string())
    );
    assert_eq!(payload["mode"], Value::from("age"));
    assert_eq!(payload["removed_count"], Value::from(0));
}

#[test]
fn cache_list_uses_profile_cache_dir_when_omitted() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir.path(),
        "profile-compose.yaml",
        r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install --no-cache-dir click
"#,
    );
    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        format!(
            r#"
version = 1
default_profile = "dev"

[profiles.dev]
compose_file = "profile-compose.yaml"

[profiles.dev.cache]
dir = "{}"
"#,
            cache_dir.display()
        ),
    )
    .expect("settings");

    let enroot = write_fake_enroot(tmpdir.path());
    let prepare = run_cli(
        tmpdir.path(),
        &[
            "--profile",
            "dev",
            "prepare",
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let list = run_cli(tmpdir.path(), &["--profile", "dev", "cache", "list"]);
    assert_success(&list);
    let list_stdout = stdout_text(&list);
    assert!(list_stdout.contains(&format!("cache dir: {}", cache_dir.display())));
    assert!(list_stdout.contains("prepared"));
    assert!(list_stdout.contains("base"));
}

#[test]
fn cache_inspect_host_backend_reports_no_image_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_compose(
        tmpdir.path(),
        "host-compose.yaml",
        &format!(
            r#"
runtime:
  backend: host
x-slurm:
  cache_dir: {}
services:
  app:
    command: /bin/true
"#,
            cache_dir.display()
        ),
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--format",
            "json",
        ],
    );
    assert_success(&inspect);
    let payload: Value = serde_json::from_str(&stdout_text(&inspect)).expect("inspect json");
    let service = &payload["services"][0];
    assert_eq!(service["service_name"], Value::from("app"));
    assert_eq!(service["source_image"], Value::from("host"));
    assert_eq!(service["base_artifact"], Value::Null);
    assert_eq!(
        service["current_reuse_expectation"],
        Value::from("host runtime")
    );
    assert_eq!(service["runtime_artifact"]["path"], Value::from(""));
    assert_eq!(
        service["runtime_artifact"]["artifact_present"],
        Value::from(false)
    );
    assert_eq!(service["runtime_artifact"]["manifest"], Value::Null);
}

#[test]
fn cache_inspect_local_sqsh_reports_present_and_missing_without_base_artifact() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let present = tmpdir.path().join("present.sqsh");
    let missing = tmpdir.path().join("missing.sqsh");
    fs::write(&present, "sqsh").expect("present sqsh");
    let compose = write_compose(
        tmpdir.path(),
        "local-sqsh.yaml",
        &format!(
            r#"
x-slurm:
  cache_dir: {}
services:
  present:
    image: {}
    command: /bin/true
  missing:
    image: {}
    command: /bin/true
"#,
            cache_dir.display(),
            present.display(),
            missing.display()
        ),
    );

    let inspect = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect);
    let payload: Value = serde_json::from_str(&stdout_text(&inspect)).expect("inspect json");
    let services = payload["services"].as_array().expect("services");
    let present_service = services
        .iter()
        .find(|service| service["service_name"] == "present")
        .expect("present service");
    let missing_service = services
        .iter()
        .find(|service| service["service_name"] == "missing")
        .expect("missing service");

    assert_eq!(present_service["base_artifact"], Value::Null);
    assert_eq!(missing_service["base_artifact"], Value::Null);
    assert_eq!(
        present_service["runtime_artifact"]["artifact_present"],
        Value::from(true)
    );
    assert_eq!(
        missing_service["runtime_artifact"]["artifact_present"],
        Value::from(false)
    );
    assert_eq!(
        present_service["current_reuse_expectation"],
        Value::from("local image present")
    );
    assert_eq!(
        missing_service["current_reuse_expectation"],
        Value::from("local image missing")
    );
}

#[test]
fn cache_prune_argument_validation_and_all_unused_path_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose_a = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let plan_a = runtime_plan(&compose_a);

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose_a.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let no_strategy = run_cli(tmpdir.path(), &["cache", "prune"]);
    assert_failure(&no_strategy);
    assert!(stderr_text(&no_strategy).contains("requires either --age DAYS or --all-unused"));

    let invalid_combo = run_cli(
        tmpdir.path(),
        &["cache", "prune", "--age", "1", "--all-unused"],
    );
    assert_failure(&invalid_combo);
    assert!(stderr_text(&invalid_combo).contains("only one strategy"));

    let compose_b = write_compose(
        tmpdir.path(),
        "compose-other.yaml",
        &format!(
            r#"
name: other
x-slurm:
  cache_dir: {}
services:
  redis:
    image: redis:7
"#,
            cache_dir.display()
        ),
    );

    let prune_unused = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--yes",
            "--all-unused",
            "-f",
            compose_b.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
        ],
    );
    assert_success(&prune_unused);
    assert!(stdout_text(&prune_unused).contains("removed: 2"));
    assert!(!plan_a.ordered_services[0].runtime_image.exists());
}

#[test]
fn cache_prune_all_unused_keeps_current_plan_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let plan = runtime_plan(&compose);

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
    let runtime_image = plan.ordered_services[0].runtime_image.clone();
    let base_image =
        hpc_compose::prepare::base_image_path(&plan.cache_dir, &plan.ordered_services[0]);
    assert!(runtime_image.exists());
    assert!(base_image.exists());

    let prune = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--yes",
            "--all-unused",
            "-f",
            compose.to_str().expect("path"),
            "--cache-dir",
            cache_dir.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&prune);
    let payload: Value = serde_json::from_str(&stdout_text(&prune)).expect("prune json");
    assert_eq!(payload["mode"], Value::from("all_unused"));
    assert_eq!(payload["removed_count"], Value::from(0));
    assert!(runtime_image.exists());
    assert!(base_image.exists());
}

#[test]
fn cache_prune_all_unused_defaults_to_plan_cache_dir() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose_a = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let plan_a = runtime_plan(&compose_a);

    let prepare = run_cli(
        tmpdir.path(),
        &[
            "prepare",
            "-f",
            compose_a.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
        ],
    );
    assert_success(&prepare);

    let compose_b = write_compose(
        tmpdir.path(),
        "compose-plan-derived-cache.yaml",
        &format!(
            r#"
name: other
x-slurm:
  cache_dir: {}
services:
  redis:
    image: redis:7
"#,
            cache_dir.display()
        ),
    );

    let prune_unused = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--yes",
            "--all-unused",
            "-f",
            compose_b.to_str().expect("path"),
        ],
    );
    assert_success(&prune_unused);
    assert!(stdout_text(&prune_unused).contains("removed: 2"));
    assert!(!plan_a.ordered_services[0].runtime_image.exists());
    assert!(
        !hpc_compose::prepare::base_image_path(&plan_a.cache_dir, &plan_a.ordered_services[0])
            .exists()
    );
}

#[test]
fn cache_prune_age_uses_profile_context_cache_dir() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    let compose = write_compose(
        tmpdir.path(),
        "profile-compose.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - -m
      - main
    x-enroot:
      prepare:
        commands:
          - pip install --no-cache-dir click
"#,
            cache_dir.display()
        ),
    );
    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        r#"
version = 1
default_profile = "dev"

[profiles.dev]
compose_file = "profile-compose.yaml"
"#,
    )
    .expect("settings");

    let enroot = write_fake_enroot(tmpdir.path());
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
    let plan = runtime_plan(&compose);

    for artifact in [
        hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image),
        hpc_compose::cache::manifest_path_for(&hpc_compose::prepare::base_image_path(
            &plan.cache_dir,
            &plan.ordered_services[0],
        )),
    ] {
        let mut manifest: Value =
            serde_json::from_str(&fs::read_to_string(&artifact).expect("manifest")).expect("json");
        manifest["created_at"] = Value::from(1_u64);
        manifest["last_used_at"] = Value::from(1_u64);
        fs::write(
            &artifact,
            serde_json::to_vec_pretty(&manifest).expect("serialize"),
        )
        .expect("rewrite manifest");
    }

    let prune = run_cli(
        tmpdir.path(),
        &[
            "--profile",
            "dev",
            "cache",
            "prune",
            "--yes",
            "--age",
            "1",
            "--format",
            "json",
        ],
    );
    assert_success(&prune);
    let payload: Value = serde_json::from_str(&stdout_text(&prune)).expect("prune json");
    assert_eq!(
        payload["cache_dir"],
        Value::from(cache_dir.display().to_string())
    );
    assert_eq!(payload["mode"], Value::from("age"));
    assert_eq!(payload["removed_count"], Value::from(2));
    assert!(!plan.ordered_services[0].runtime_image.exists());
    assert!(
        !hpc_compose::prepare::base_image_path(&plan.cache_dir, &plan.ordered_services[0]).exists()
    );
}
