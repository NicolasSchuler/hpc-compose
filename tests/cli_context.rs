mod support;

use std::fs;

use hpc_compose::context::Settings;
use serde_json::Value;
use support::*;

fn write_profile_settings(
    root: &std::path::Path,
    profile: &str,
    compose_file: &str,
    cache_dir: &std::path::Path,
    enroot_bin: &std::path::Path,
    sbatch_bin: &std::path::Path,
    srun_bin: &std::path::Path,
) {
    fs::create_dir_all(root.join(".hpc-compose")).expect("settings dir");
    fs::write(
        root.join(".hpc-compose/settings.toml"),
        format!(
            r#"
version = 1
default_profile = "{profile}"

[profiles.{profile}]
compose_file = "{compose_file}"

[profiles.{profile}.env]
CACHE_DIR = "{cache_dir}"

[profiles.{profile}.binaries]
enroot = "{enroot_bin}"
sbatch = "{sbatch_bin}"
srun = "{srun_bin}"
"#,
            cache_dir = cache_dir.display(),
            enroot_bin = enroot_bin.display(),
            sbatch_bin = sbatch_bin.display(),
            srun_bin = srun_bin.display(),
        ),
    )
    .expect("settings");
}

#[test]
fn submit_uses_profile_compose_env_and_binary_overrides() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");

    let compose = write_compose(
        tmpdir.path(),
        "profile-compose.yaml",
        &format!(
            r#"
name: profiled
x-slurm:
  job_name: profiled
  cache_dir: ${{CACHE_DIR}}
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = tmpdir.path().join("sbatch-profile");
    write_script(
        &sbatch,
        "#!/bin/bash\nset -euo pipefail\necho 'Submitted batch job 42424'\n",
    );
    write_profile_settings(
        tmpdir.path(),
        "dev",
        compose.file_name().and_then(|v| v.to_str()).expect("name"),
        &cache_dir,
        &enroot,
        &sbatch,
        &srun,
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "--profile",
            "dev",
            "submit",
            "--skip-prepare",
            "--no-preflight",
        ],
    );
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("Submitted batch job 42424"));
    assert!(stdout.contains("cache dir:"));
}

#[test]
fn context_json_reports_sources_and_runtime_paths() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = fs::canonicalize(tmpdir.path()).expect("canonical root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = write_compose(
        tmpdir.path(),
        "profile-compose.yaml",
        &format!(
            r#"
name: profiled
x-slurm:
  cache_dir: ${{CACHE_DIR}}
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    write_profile_settings(
        tmpdir.path(),
        "dev",
        compose.file_name().and_then(|v| v.to_str()).expect("name"),
        &cache_dir,
        &enroot,
        &sbatch,
        &srun,
    );

    let output = run_cli(
        tmpdir.path(),
        &["--profile", "dev", "context", "--format", "json"],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("context json");
    assert_eq!(payload["selected_profile"], Value::from("dev"));
    assert_eq!(payload["cwd"], Value::from(root.display().to_string()));
    assert_eq!(
        payload["settings_base_dir"],
        Value::from(root.display().to_string())
    );
    assert_eq!(payload["compose_file"]["source"], Value::from("profile"));
    assert_eq!(
        payload["binaries"]["srun"]["source"],
        Value::from("profile")
    );
    assert_eq!(
        payload["runtime_paths"]["cache_dir"]["source"],
        Value::from("compose")
    );
    assert_eq!(
        payload["interpolation_var_sources"]["CACHE_DIR"],
        Value::from("profile")
    );
    assert_eq!(
        payload["runtime_paths"]["compose_dir"],
        Value::from(root.display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["current_submit_dir"],
        Value::from(root.display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["default_script_path"],
        Value::from(root.join("hpc-compose.sbatch").display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["runtime_job_root_pattern"],
        Value::from(root.join(".hpc-compose/{job_id}").display().to_string())
    );
    assert_eq!(payload["compose_load_error"], Value::Null);
}

#[test]
fn context_json_reports_builtin_cache_dir_source_when_unset() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = fs::canonicalize(tmpdir.path()).expect("canonical root");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    write_compose(
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

    let output = run_cli(tmpdir.path(), &["context", "--format", "json"]);
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("context json");
    assert_eq!(payload["compose_file"]["source"], Value::from("builtin"));
    assert_eq!(
        payload["compose_file"]["value"],
        Value::from(root.join("compose.yaml").display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["cache_dir"]["source"],
        Value::from("builtin")
    );
    assert_eq!(payload["compose_load_error"], Value::Null);
}

#[test]
fn context_json_reports_nested_submit_dir_separately_from_compose_dir() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let nested = tmpdir.path().join("nested/workdir");
    fs::create_dir_all(&nested).expect("nested cwd");
    let root = fs::canonicalize(tmpdir.path()).expect("canonical root");
    let nested_cwd = fs::canonicalize(&nested).expect("canonical nested");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = write_compose(
        tmpdir.path(),
        "profile-compose.yaml",
        &format!(
            r#"
name: profiled
x-slurm:
  cache_dir: ${{CACHE_DIR}}
services:
  app:
    image: {}
    command: /bin/true
"#,
            local_image.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    write_profile_settings(
        tmpdir.path(),
        "dev",
        compose.file_name().and_then(|v| v.to_str()).expect("name"),
        &cache_dir,
        &enroot,
        &sbatch,
        &srun,
    );

    let output = run_cli(
        &nested,
        &["--profile", "dev", "context", "--format", "json"],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("context json");
    assert_eq!(
        payload["cwd"],
        Value::from(nested_cwd.display().to_string())
    );
    assert_eq!(
        payload["settings_base_dir"],
        Value::from(root.display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["compose_dir"],
        Value::from(root.display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["current_submit_dir"],
        Value::from(nested_cwd.display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["default_script_path"],
        Value::from(root.join("hpc-compose.sbatch").display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["runtime_job_root_pattern"],
        Value::from(
            nested_cwd
                .join(".hpc-compose/{job_id}")
                .display()
                .to_string()
        )
    );
    assert_eq!(payload["compose_load_error"], Value::Null);
}

#[test]
fn context_json_reports_resolution_even_when_compose_is_missing() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = fs::canonicalize(tmpdir.path()).expect("canonical root");
    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        r#"
version = 1
default_profile = "dev"

[profiles.dev]
compose_file = "missing.yaml"
"#,
    )
    .expect("settings");

    let output = run_cli(
        tmpdir.path(),
        &["--profile", "dev", "context", "--format", "json"],
    );
    assert_success(&output);
    let payload: Value = serde_json::from_str(&stdout_text(&output)).expect("context json");
    assert_eq!(payload["selected_profile"], Value::from("dev"));
    assert_eq!(
        payload["compose_file"]["value"],
        Value::from(root.join("missing.yaml").display().to_string())
    );
    assert!(
        payload["compose_load_error"]
            .as_str()
            .expect("compose load error")
            .contains("failed to read spec")
    );
    assert!(payload["runtime_paths"]["cache_dir"].is_null());
    assert!(payload["runtime_paths"]["resume_dir"].is_null());
    assert!(payload["runtime_paths"]["artifact_export_dir"].is_null());
    assert_eq!(
        payload["runtime_paths"]["default_script_path"],
        Value::from(root.join("hpc-compose.sbatch").display().to_string())
    );
    assert_eq!(
        payload["runtime_paths"]["metadata_root"]["value"],
        Value::from(root.join(".hpc-compose").display().to_string())
    );
}

#[test]
fn context_text_reports_resume_export_and_interpolation_sources() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    fs::write(tmpdir.path().join(".env"), "FROM_DOTENV=hello\n").expect("dotenv");
    write_compose(
        tmpdir.path(),
        "compose-context.yaml",
        &format!(
            r#"
name: context-demo
x-slurm:
  cache_dir: ./cache
  resume:
    path: /shared/runs/demo
  artifacts:
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/logs/**
services:
  app:
    image: {}
    environment:
      FROM_DOTENV: ${{FROM_DOTENV}}
    command: /bin/true
"#,
            local_image.display()
        ),
    );

    let output = run_cli(tmpdir.path(), &["context", "--format", "text"]);
    assert_success(&output);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("compose file:"));
    assert!(stdout.contains("runtime paths:"));
    assert!(stdout.contains("interpolation vars:"));
}

#[test]
fn setup_interactive_writes_settings_and_is_idempotent() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let setup_input = "dev\ncompose.yaml\n.env,.env.dev\nCACHE_DIR=/shared/cache\nsrun=/opt/slurm/bin/srun\ndev\n";
    let first = run_cli_with_stdin(tmpdir.path(), &["setup"], setup_input);
    assert_success(&first);
    let settings_path = tmpdir.path().join(".hpc-compose/settings.toml");
    assert!(settings_path.exists());
    let first_contents = fs::read_to_string(&settings_path).expect("settings");
    assert!(first_contents.contains("default_profile = \"dev\""));
    assert!(first_contents.contains("[profiles.dev]"));
    assert!(first_contents.contains("compose_file = \"compose.yaml\""));
    assert!(first_contents.contains("CACHE_DIR = \"/shared/cache\""));
    assert!(first_contents.contains("srun = \"/opt/slurm/bin/srun\""));
    let parsed: Settings = toml::from_str(&first_contents).expect("parse settings");
    let profile = parsed.profiles.get("dev").expect("dev profile");
    assert_eq!(
        profile.env_files,
        vec![".env".to_string(), ".env.dev".to_string()]
    );

    let second = run_cli_with_stdin(tmpdir.path(), &["setup"], setup_input);
    assert_success(&second);
    let second_contents = fs::read_to_string(&settings_path).expect("settings");
    assert_eq!(first_contents, second_contents);
}

#[test]
fn validate_strict_env_detects_missing_fallback_and_accepts_profile_env() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = write_compose(
        tmpdir.path(),
        "strict-compose.yaml",
        r#"
services:
  app:
    image: redis:7
    command: "echo ${NEEDED_VAR:-fallback}"
"#,
    );

    let strict_fail = run_cli(
        tmpdir.path(),
        &[
            "validate",
            "-f",
            compose.to_str().expect("path"),
            "--strict-env",
        ],
    );
    assert_failure(&strict_fail);
    assert!(
        stderr_text(&strict_fail).contains("strict env validation failed"),
        "stderr: {}",
        stderr_text(&strict_fail)
    );

    fs::create_dir_all(tmpdir.path().join(".hpc-compose")).expect("settings dir");
    fs::write(
        tmpdir.path().join(".hpc-compose/settings.toml"),
        r#"
version = 1
default_profile = "strict"

[profiles.strict]
compose_file = "strict-compose.yaml"

[profiles.strict.env]
NEEDED_VAR = "present"
"#,
    )
    .expect("settings");

    let strict_ok = run_cli(
        tmpdir.path(),
        &["--profile", "strict", "validate", "--strict-env"],
    );
    assert_success(&strict_ok);
}

#[test]
fn validate_strict_env_ignores_comment_only_fallbacks() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("local image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    # ${{IGNORED_ONLY_IN_COMMENT:-fallback}}
"#,
            local_image.display()
        ),
    );

    let output = run_cli(
        tmpdir.path(),
        &[
            "validate",
            "--strict-env",
            "-f",
            compose.to_str().expect("path"),
        ],
    );
    assert_success(&output);
    assert!(stdout_text(&output).contains("spec is valid"));
}
