mod support;

use std::fs;

use serde_json::Value;
use support::*;

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
    assert!(stdout_text(&prepare).contains("BUILD service 'app' runtime image"));
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
