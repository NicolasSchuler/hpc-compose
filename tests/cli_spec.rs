mod support;

use std::fs;

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
