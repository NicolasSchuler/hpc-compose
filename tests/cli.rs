use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use hpc_compose::planner::build_plan;
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
use hpc_compose::spec::ComposeSpec;
use serde_json::Value;

fn bin_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_hpc-compose"))
}

fn run_cli(cwd: &Path, args: &[&str]) -> Output {
    Command::new(bin_path())
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run cli")
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("fake-enroot.sh");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
cmd="${1:-}"
shift || true
case "$cmd" in
  import)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  create)
    name=""
    while (($#)); do
      case "$1" in
        -n|--name)
          name="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$ENROOT_DATA_PATH/$name"
    ;;
  start)
    exit 0
    ;;
  export)
    output=""
    while (($#)); do
      case "$1" in
        -o|--output)
          output="$2"
          shift 2
          ;;
        -f|--force)
          shift
          ;;
        *)
          shift
          ;;
      esac
    done
    mkdir -p "$(dirname "$output")"
    touch "$output"
    ;;
  remove)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"#,
    );
    path
}

fn write_fake_srun(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
exit 0
"#,
    );
    path
}

fn write_fake_sbatch(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
echo "Submitted batch job 12345"
"#,
    );
    path
}

fn write_compose(tmpdir: &Path, name: &str, body: &str) -> PathBuf {
    let path = tmpdir.join(name);
    fs::write(&path, body).expect("write compose");
    path
}

fn runtime_plan(path: &Path) -> RuntimePlan {
    let spec = ComposeSpec::load(path).expect("load spec");
    let plan = build_plan(path, spec).expect("build plan");
    build_runtime_plan(&plan)
}

fn write_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir,
        "compose.yaml",
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
    )
}

fn write_mount_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::create_dir_all(tmpdir.join("deps")).expect("deps dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir,
        "compose-mount.yaml",
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
        mounts:
          - ./deps:/deps
"#,
            cache_dir.display()
        ),
    )
}

#[test]
fn validate_and_render_commands_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let validate = run_cli(tmpdir.path(), &["validate", "-f", compose.to_str().expect("path")]);
    assert_success(&validate);
    assert!(stdout_text(&validate).contains("spec is valid"));

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
}

#[test]
fn inspect_and_preflight_commands_cover_dev_workflow() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let compose = write_mount_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    let inspect = run_cli(tmpdir.path(), &["inspect", "-f", compose.to_str().expect("path")]);
    assert_success(&inspect);
    assert!(stdout_text(&inspect).contains("rebuild on submit because x-enroot.prepare.mounts are present"));

    let preflight = run_cli(
        tmpdir.path(),
        &[
            "preflight",
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
    assert_success(&preflight);
    let preflight_stderr = stderr_text(&preflight);
    assert!(preflight_stderr.contains("OK  srun reports Pyxis container support"));
    assert!(preflight_stderr.contains("OK  cache directory is writable"));

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
fn prepare_and_cache_commands_manage_artifacts() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
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
    assert!(hpc_compose::cache::manifest_path_for(&plan.ordered_services[0].runtime_image).exists());

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
        fs::write(&artifact, serde_json::to_vec_pretty(&manifest).expect("serialize"))
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
}

#[test]
fn cache_prune_argument_validation_and_all_unused_path_work() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
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
fn submit_command_runs_end_to_end_with_fake_tools() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let plan = runtime_plan(&compose);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
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
    assert!(submit_stdout.contains("Submitted batch job 12345"));
    assert!(submit_stdout.contains("rendered script:"));
    assert!(submit_stdout.contains("log  service 'app':"));
    assert!(script_out.exists());
    assert!(plan.ordered_services[0].runtime_image.exists());
}

#[test]
fn submit_skip_prepare_reuses_existing_artifact() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_dir = tmpdir.path().join("cache");
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

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
            "submit",
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
    assert!(!stdout_text(&submit).contains("BUILD service 'app' runtime image"));
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
}
