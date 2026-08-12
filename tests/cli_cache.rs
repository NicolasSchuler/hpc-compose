use std::fs;
use std::path::Path;

use crate::support::*;
use hpc_compose::cache::{CacheEntryKind, CacheEntryManifest};
use serde_json::Value;

fn json_path(path: &Path) -> String {
    serde_json::to_string(&path.display().to_string()).expect("path json")
}

fn write_cache_manifest(artifact: &Path, kind: CacheEntryKind, service_name: &str) {
    let manifest = CacheEntryManifest {
        kind,
        artifact_path: artifact.display().to_string(),
        service_names: vec![service_name.to_string()],
        cache_key: "cache-key".into(),
        source_image: "docker://redis:7".into(),
        registry: Some("registry-1.docker.io".into()),
        prepare_commands: Vec::new(),
        prepare_env: Vec::new(),
        prepare_root: None,
        prepare_mounts: Vec::new(),
        force_rebuild_due_to_mounts: false,
        created_at: 1,
        last_used_at: 2,
        tool_version: "test".into(),
        uri: None,
        revision: None,
        content_digest: None,
    };
    let manifest_path = hpc_compose::cache::manifest_path_for(artifact);
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent).expect("manifest parent");
    }
    fs::write(
        manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest json"),
    )
    .expect("write manifest");
}

fn write_mixed_cache_inspect_compose(root: &Path, cache_dir: &Path) -> std::path::PathBuf {
    let local_sif = root.join("local.sif");
    fs::write(&local_sif, "sif").expect("local sif");
    fs::create_dir_all(root.join("deps")).expect("deps");
    write_compose(
        root,
        "cache-inspect.yaml",
        &format!(
            r#"
runtime:
  backend: apptainer
x-slurm:
  cache_dir: {}
services:
  prepared:
    image: redis:7
    command: /bin/true
    x-runtime:
      prepare:
        commands:
          - /bin/true
        mounts:
          - ./deps:/deps
  local:
    image: {}
    command: /bin/true
    depends_on:
      prepared:
        condition: service_started
"#,
            cache_dir.display(),
            local_sif.display()
        ),
    )
}

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
fn cache_inspect_host_backend_ignores_cwd_artifact_sidecar() {
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
    fs::write(tmpdir.path().join("artifact.sqsh.json"), "{ malformed")
        .expect("hostile cwd sidecar");
    let plan = runtime_plan(&compose);

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
    let expected_json = format!(
        r#"{{
  "schema_version": 1,
  "cache_dir": {cache_dir},
  "services": [
    {{
      "service_name": "app",
      "source_image": "host",
      "base_registry": null,
      "base_artifact": null,
      "runtime_artifact": {{
        "path": "",
        "artifact_present": false,
        "manifest_path": "artifact.sqsh.json",
        "manifest": null
      }},
      "current_reuse_expectation": "host runtime",
      "note": null
    }}
  ]
}}"#,
        cache_dir = json_path(&plan.cache_dir),
    );
    assert_eq!(stdout_text(&inspect), format!("{expected_json}\n"));
    assert!(stderr_text(&inspect).is_empty());

    let payload: Value = serde_json::from_str(&expected_json).expect("inspect json");
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

    let inspect_text = run_cli(
        tmpdir.path(),
        &[
            "--color",
            "never",
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
        ],
    );
    assert_success(&inspect_text);
    assert_eq!(
        stdout_text(&inspect_text),
        concat!(
            "service: app\n",
            "source image: host\n",
            "runtime artifact: \n",
            "artifact present: no\n",
            "manifest path: artifact.sqsh.json\n",
            "manifest present: no\n",
            "current reuse expectation: host runtime\n",
            "\n",
        )
    );
    assert!(stderr_text(&inspect_text).is_empty());
}

#[test]
fn cache_inspect_preserves_exact_json_text_order_and_forced_note() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_mixed_cache_inspect_compose(tmpdir.path(), cache_root.path());
    let plan = runtime_plan(&compose);
    assert_eq!(
        plan.ordered_services
            .iter()
            .map(|service| service.name.as_str())
            .collect::<Vec<_>>(),
        ["prepared", "local"]
    );
    let prepared = &plan.ordered_services[0];
    let local = &plan.ordered_services[1];
    assert!(prepared.prepare.as_ref().expect("prepare").force_rebuild);
    assert!(local.runtime_image.exists());

    let base_path = hpc_compose::runtime_plan::base_image_path_for_backend(
        &plan.cache_dir,
        prepared,
        plan.runtime.backend,
    );
    let base_manifest = hpc_compose::cache::manifest_path_for(&base_path);
    let runtime_manifest = hpc_compose::cache::manifest_path_for(&prepared.runtime_image);
    let local_manifest = hpc_compose::cache::manifest_path_for(&local.runtime_image);

    let inspect_json = run_cli(
        tmpdir.path(),
        &[
            "--color",
            "never",
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--format",
            "json",
        ],
    );
    assert_success(&inspect_json);
    let expected_json = format!(
        r#"{{
  "schema_version": 1,
  "cache_dir": {cache_dir},
  "services": [
    {{
      "service_name": "prepared",
      "source_image": "docker://redis:7",
      "base_registry": "registry-1.docker.io",
      "base_artifact": {{
        "path": {base_path},
        "artifact_present": false,
        "manifest_path": {base_manifest},
        "manifest": null
      }},
      "runtime_artifact": {{
        "path": {runtime_path},
        "artifact_present": false,
        "manifest_path": {runtime_manifest},
        "manifest": null
      }},
      "current_reuse_expectation": "rebuild on prepare",
      "note": "this service rebuilds on prepare because prepare.mounts are present"
    }},
    {{
      "service_name": "local",
      "source_image": {local_path},
      "base_registry": null,
      "base_artifact": null,
      "runtime_artifact": {{
        "path": {local_path},
        "artifact_present": true,
        "manifest_path": {local_manifest},
        "manifest": null
      }},
      "current_reuse_expectation": "local image present",
      "note": null
    }}
  ]
}}"#,
        cache_dir = json_path(&plan.cache_dir),
        base_path = json_path(&base_path),
        base_manifest = json_path(&base_manifest),
        runtime_path = json_path(&prepared.runtime_image),
        runtime_manifest = json_path(&runtime_manifest),
        local_path = json_path(&local.runtime_image),
        local_manifest = json_path(&local_manifest),
    );
    assert_eq!(stdout_text(&inspect_json), format!("{expected_json}\n"));
    assert!(stderr_text(&inspect_json).is_empty());

    let inspect_text = run_cli(
        tmpdir.path(),
        &[
            "--color",
            "never",
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&inspect_text);
    let expected_text = format!(
        concat!(
            "service: prepared\n",
            "source image: docker://redis:7\n",
            "base artifact: {base_path}\n",
            "base registry: registry-1.docker.io\n",
            "artifact present: no\n",
            "manifest path: {base_manifest}\n",
            "manifest present: no\n",
            "runtime artifact: {runtime_path}\n",
            "artifact present: no\n",
            "manifest path: {runtime_manifest}\n",
            "manifest present: no\n",
            "current reuse expectation: rebuild on prepare\n",
            "note: this service rebuilds on prepare because prepare.mounts are present\n",
            "\n",
            "service: local\n",
            "source image: {local_path}\n",
            "runtime artifact: {local_path}\n",
            "artifact present: yes\n",
            "manifest path: {local_manifest}\n",
            "manifest present: no\n",
            "current reuse expectation: local image present\n",
            "\n",
        ),
        base_path = base_path.display(),
        base_manifest = base_manifest.display(),
        runtime_path = prepared.runtime_image.display(),
        runtime_manifest = runtime_manifest.display(),
        local_path = local.runtime_image.display(),
        local_manifest = local_manifest.display(),
    );
    assert_eq!(stdout_text(&inspect_text), expected_text);
    assert!(stderr_text(&inspect_text).is_empty());
}

#[test]
fn cache_inspect_filters_before_io_and_reports_base_errors_before_runtime() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let compose = write_mixed_cache_inspect_compose(tmpdir.path(), cache_root.path());
    let plan = runtime_plan(&compose);
    let prepared = &plan.ordered_services[0];
    let base_path = hpc_compose::runtime_plan::base_image_path_for_backend(
        &plan.cache_dir,
        prepared,
        plan.runtime.backend,
    );
    let base_manifest = hpc_compose::cache::manifest_path_for(&base_path);
    let runtime_manifest = hpc_compose::cache::manifest_path_for(&prepared.runtime_image);
    fs::create_dir_all(base_manifest.parent().expect("base parent")).expect("base parent");
    fs::create_dir_all(runtime_manifest.parent().expect("runtime parent")).expect("runtime parent");
    fs::write(&base_manifest, "{ malformed base").expect("malformed base");
    fs::write(&runtime_manifest, "{ malformed runtime").expect("malformed runtime");

    let selected = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "local",
            "--format",
            "json",
        ],
    );
    assert_success(&selected);
    let selected_payload: Value =
        serde_json::from_str(&stdout_text(&selected)).expect("selected json");
    assert_eq!(
        selected_payload["services"]
            .as_array()
            .expect("services")
            .len(),
        1
    );
    assert_eq!(selected_payload["services"][0]["service_name"], "local");

    let unknown = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "missing",
        ],
    );
    assert_success(&unknown);
    assert_eq!(stdout_text(&unknown), "");
    assert!(stderr_text(&unknown).is_empty());

    let case_mismatch = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "LOCAL",
            "--format",
            "json",
        ],
    );
    assert_success(&case_mismatch);
    let expected_empty_json = format!(
        "{{\n  \"schema_version\": 1,\n  \"cache_dir\": {},\n  \"services\": []\n}}\n",
        json_path(&plan.cache_dir)
    );
    assert_eq!(stdout_text(&case_mismatch), expected_empty_json);
    assert!(stderr_text(&case_mismatch).is_empty());

    let base_error = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "prepared",
            "--format",
            "json",
        ],
    );
    assert_failure(&base_error);
    assert_eq!(stdout_text(&base_error), "");
    let base_stderr = stderr_text(&base_error);
    let base_manifest_name = base_manifest
        .file_name()
        .and_then(|name| name.to_str())
        .expect("base manifest name");
    let runtime_manifest_name = runtime_manifest
        .file_name()
        .and_then(|name| name.to_str())
        .expect("runtime manifest name");
    assert!(
        base_stderr.contains("failed to parse") && base_stderr.contains(base_manifest_name),
        "{base_stderr}"
    );
    assert!(
        !base_stderr.contains(runtime_manifest_name),
        "{base_stderr}"
    );

    write_cache_manifest(&base_path, CacheEntryKind::Base, "prepared");
    let runtime_error = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "inspect",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "prepared",
            "--format",
            "json",
        ],
    );
    assert_failure(&runtime_error);
    assert_eq!(stdout_text(&runtime_error), "");
    let runtime_stderr = stderr_text(&runtime_error);
    assert!(
        runtime_stderr.contains("failed to parse")
            && runtime_stderr.contains(runtime_manifest_name),
        "{runtime_stderr}"
    );
}

#[test]
fn cache_inspect_keeps_artifact_and_sidecar_presence_independent_for_sif_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let artifact_only = tmpdir.path().join("artifact-only.sif");
    let sidecar_only = tmpdir.path().join("sidecar-only.sif");
    fs::write(&artifact_only, "sif").expect("artifact-only sif");
    let compose = write_compose(
        tmpdir.path(),
        "sif-cache-inspect.yaml",
        &format!(
            r#"
runtime:
  backend: singularity
x-slurm:
  cache_dir: {}
services:
  remote:
    image: redis:7
    command: /bin/true
  artifact_only:
    image: {}
    command: /bin/true
    depends_on:
      remote:
        condition: service_started
  sidecar_only:
    image: {}
    command: /bin/true
    depends_on:
      artifact_only:
        condition: service_started
"#,
            cache_root.path().display(),
            artifact_only.display(),
            sidecar_only.display(),
        ),
    );
    let plan = runtime_plan(&compose);
    assert_eq!(
        plan.ordered_services
            .iter()
            .map(|service| service.name.as_str())
            .collect::<Vec<_>>(),
        ["remote", "artifact_only", "sidecar_only"]
    );
    let remote = &plan.ordered_services[0];
    let remote_base = hpc_compose::runtime_plan::base_image_path_for_backend(
        &plan.cache_dir,
        remote,
        plan.runtime.backend,
    );
    assert_eq!(remote.runtime_image, remote_base);
    assert_eq!(
        remote
            .runtime_image
            .extension()
            .and_then(|ext| ext.to_str()),
        Some("sif")
    );
    write_cache_manifest(&remote.runtime_image, CacheEntryKind::Base, "remote");
    write_cache_manifest(&sidecar_only, CacheEntryKind::Prepared, "sidecar_only");

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
    assert_eq!(
        services
            .iter()
            .map(|service| service["service_name"].as_str().expect("name"))
            .collect::<Vec<_>>(),
        ["remote", "artifact_only", "sidecar_only"]
    );

    let remote_report = &services[0];
    assert_eq!(
        remote_report["base_artifact"]["path"],
        remote_report["runtime_artifact"]["path"]
    );
    assert_eq!(
        remote_report["base_artifact"]["artifact_present"],
        Value::from(false)
    );
    assert_eq!(
        remote_report["runtime_artifact"]["artifact_present"],
        Value::from(false)
    );
    assert_ne!(remote_report["base_artifact"]["manifest"], Value::Null);
    assert_eq!(
        remote_report["base_artifact"]["manifest"],
        remote_report["runtime_artifact"]["manifest"]
    );

    let artifact_report = &services[1];
    assert_eq!(
        artifact_report["runtime_artifact"]["artifact_present"],
        Value::from(true)
    );
    assert_eq!(artifact_report["runtime_artifact"]["manifest"], Value::Null);
    assert_eq!(
        artifact_report["current_reuse_expectation"],
        "local image present"
    );

    let sidecar_report = &services[2];
    assert_eq!(
        sidecar_report["runtime_artifact"]["artifact_present"],
        Value::from(false)
    );
    assert_ne!(sidecar_report["runtime_artifact"]["manifest"], Value::Null);
    assert_eq!(
        sidecar_report["current_reuse_expectation"],
        "local image missing"
    );
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
fn cache_prune_all_unused_non_tty_prompt_shows_plan_preview() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let prune = run_cli(
        tmpdir.path(),
        &[
            "cache",
            "prune",
            "--all-unused",
            "-f",
            compose.to_str().expect("path"),
        ],
    );

    assert_failure(&prune);
    let stderr = stderr_text(&prune);
    assert!(stderr.contains("destructive action preview"));
    assert!(stderr.contains(&format!("cache dir: {}", cache_dir.display())));
    assert!(stderr.contains("selected artifacts/manifests: 0"));
    assert!(stderr.contains("estimated bytes: 0"));
    assert!(stderr.contains("requires --yes"));
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
