use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;

use hpc_compose::planner::build_plan;
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
use hpc_compose::render::log_file_name_for_service;
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

fn run_cli_with_stdin(cwd: &Path, args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(bin_path())
        .current_dir(cwd)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cli");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    child.wait_with_output().expect("wait output")
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

fn write_fake_srun_failure_policy(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
job_name=""
for arg in "$@"; do
  case "$arg" in
    --job-name=*)
      job_name="${arg#--job-name=}"
      break
      ;;
  esac
done
state_root="${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/fake-srun"
mkdir -p "$state_root"
key="$(printf '%s' "$job_name" | tr -c 'A-Za-z0-9._-' '_')"
count_file="$state_root/${key}.count"
count=0
if [[ -f "$count_file" ]]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
case "$job_name" in
  hpc-compose:app)
    if (( count == 1 )); then
      exit 41
    fi
    exit 0
    ;;
  hpc-compose:sidecar)
    exit 42
    ;;
  hpc-compose:flaky)
    exit 43
    ;;
  *)
    exit 0
    ;;
esac
"#,
    );
    path
}

fn write_fake_srun_failure(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
exit 17
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

fn write_fake_sbatch_runs_script(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path" >/dev/null 2>&1
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

fn write_fake_sbatch_runs_script_with_job_output(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script-with-output");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path"
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

fn write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script-ignore-exit");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path" >/dev/null 2>&1 || true
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

fn write_fake_scancel(tmpdir: &Path, log_path: &Path, success: bool) -> PathBuf {
    let path = tmpdir.join(if success { "scancel" } else { "scancel-fail" });
    let body = if success {
        format!(
            "#!/bin/bash\nset -euo pipefail\necho \"$@\" >> '{}'\n",
            log_path.display()
        )
    } else {
        format!(
            "#!/bin/bash\nset -euo pipefail\necho \"$@\" >> '{}'\necho 'permission denied' >&2\nexit 17\n",
            log_path.display()
        )
    };
    write_script(&path, &body);
    path
}

fn write_fake_squeue(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("squeue");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
state="$(cat '{}' 2>/dev/null || true)"
case "$state" in
  ""|NONE)
    exit 0
    ;;
  *)
    echo "$state"
    ;;
esac
"#,
            state_file.display()
        ),
    );
    path
}

fn write_fake_sacct(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("sacct");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
state="$(cat '{}' 2>/dev/null || true)"
if [[ -n "$state" && "$state" != "NONE" ]]; then
  echo "$state"
fi
"#,
            state_file.display()
        ),
    );
    path
}

fn write_fake_sstat(tmpdir: &Path, output_file: &Path) -> PathBuf {
    let path = tmpdir.join("sstat");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
cat '{}' 2>/dev/null || true
"#,
            output_file.display()
        ),
    );
    path
}

fn write_fake_sstat_failure(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sstat-fail");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
echo "job accounting unavailable" >&2
exit 23
"#,
    );
    path
}

fn write_fake_nvidia_smi(
    tmpdir: &Path,
    gpu_output_file: &Path,
    process_output_file: &Path,
) -> PathBuf {
    let path = tmpdir.join("nvidia-smi");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
case "$*" in
  *"--query-gpu="*)
    cat '{}'
    ;;
  *"--query-compute-apps="*)
    cat '{}'
    ;;
  *)
    echo "unsupported query" >&2
    exit 1
    ;;
esac
"#,
            gpu_output_file.display(),
            process_output_file.display()
        ),
    );
    path
}

fn write_fake_watch_sbatch(
    tmpdir: &Path,
    squeue_state: &Path,
    sacct_state: &Path,
    terminal_state: &str,
    final_log_line: &str,
    gap_seconds: u64,
) -> PathBuf {
    let path = tmpdir.join(format!("sbatch-{}", terminal_state.to_lowercase()));
    let log_dir = tmpdir.join(".hpc-compose/12345/logs");
    let service_log = log_dir.join(log_file_name_for_service("app"));
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
mkdir -p '{}'
printf 'PENDING\n' > '{}'
rm -f '{}'
(
  sleep 1
  printf 'RUNNING\n' > '{}'
  printf 'booting\n' > '{}'
  sleep 1
  printf '{}\n' >> '{}'
  printf 'NONE\n' > '{}'
  sleep {}
  printf '{}\n' > '{}'
) >/dev/null 2>&1 &
echo "Submitted batch job 12345"
"#,
            log_dir.display(),
            squeue_state.display(),
            sacct_state.display(),
            squeue_state.display(),
            service_log.display(),
            final_log_line,
            service_log.display(),
            squeue_state.display(),
            gap_seconds,
            terminal_state,
            sacct_state.display()
        ),
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

fn safe_cache_dir() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
    fs::create_dir_all(&root).expect("cache root");
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(root)
        .expect("cache tempdir")
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

fn write_metrics_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    write_compose(
        tmpdir,
        "compose-metrics.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  metrics:
    interval_seconds: 60
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display()
        ),
    )
}

fn write_artifacts_compose_with_paths(
    tmpdir: &Path,
    cache_dir: &Path,
    collect_policy: &str,
    paths: &[&str],
) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::write(tmpdir.join("app/main.py"), "print('hello')\n").expect("main.py");
    let paths_yaml = paths
        .iter()
        .map(|path| format!("      - {path}"))
        .collect::<Vec<_>>()
        .join("\n");
    write_compose(
        tmpdir,
        &format!("compose-artifacts-{collect_policy}.yaml"),
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  metrics:
    interval_seconds: 1
  artifacts:
    collect: {}
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
{}
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display(),
            collect_policy,
            paths_yaml
        ),
    )
}

fn write_artifacts_compose(tmpdir: &Path, cache_dir: &Path, collect_policy: &str) -> PathBuf {
    write_artifacts_compose_with_paths(
        tmpdir,
        cache_dir,
        collect_policy,
        &[
            "/hpc-compose/job/metrics/**",
            "/hpc-compose/job/missing/*.txt",
        ],
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

fn write_env_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
    fs::create_dir_all(tmpdir.join("app")).expect("app dir");
    fs::write(
        tmpdir.join(".env"),
        "SECRET_TOKEN=super-secret\nMESSAGE=hi-from-dotenv\n",
    )
    .expect("dotenv");
    write_compose(
        tmpdir,
        "compose-env.yaml",
        &format!(
            r#"
name: env-demo
x-slurm:
  job_name: env-demo
  time: "00:10:00"
  cache_dir: {}
services:
  app:
    image: python:3.11-slim
    volumes:
      - ./app:/workspace
    environment:
      SECRET_TOKEN: $SECRET_TOKEN
    command:
      - python
      - -c
      - print("${{MESSAGE}}")
"#,
            cache_dir.display()
        ),
    )
}

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
fn submit_command_runs_end_to_end_with_fake_tools() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
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
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
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

#[test]
fn submit_restart_on_failure_restarts_once_and_status_reports_state() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  app:
    image: {}
    command: /bin/true
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 3
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    let app = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "app")
        .expect("app service");
    assert_eq!(app["failure_policy_mode"], "restart_on_failure");
    assert_eq!(app["restart_count"], 1);
    assert_eq!(app["max_restarts"], 3);
    assert_eq!(app["last_exit_code"], 0);
}

#[test]
fn submit_ignore_policy_allows_job_success_with_failed_sidecar() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  main:
    image: {}
    command: /bin/true
  sidecar:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: ignore
"#,
            local_image.display(),
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));

    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let payload: Value = serde_json::from_str(&stdout_text(&status)).expect("status json");
    let sidecar = payload["services"]
        .as_array()
        .expect("services")
        .iter()
        .find(|service| service["service_name"] == "sidecar")
        .expect("sidecar service");
    assert_eq!(sidecar["failure_policy_mode"], "ignore");
    assert_eq!(sidecar["last_exit_code"], 42);
}

#[test]
fn submit_restart_on_failure_exhausted_retries_fails_job() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let local_image = tmpdir.path().join("local.sqsh");
    fs::write(&local_image, "sqsh").expect("image");
    let compose = write_compose(
        tmpdir.path(),
        "compose.yaml",
        &format!(
            r#"
services:
  flaky:
    image: {}
    command: /bin/false
    x-slurm:
      failure_policy:
        mode: restart_on_failure
        max_restarts: 1
        backoff_seconds: 1
"#,
            local_image.display()
        ),
    );
    let srun = write_fake_srun_failure_policy(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_with_job_output(tmpdir.path());
    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--skip-prepare",
            "--no-preflight",
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );
    assert_failure(&submit);
    let combined = format!("{}\n{}", stdout_text(&submit), stderr_text(&submit));
    assert!(combined.contains("after 1/1 restarts"));
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
fn submit_succeeds_when_tracking_metadata_cannot_be_written() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose_root = tmpdir.path().join("readonly-compose");
    fs::create_dir_all(&compose_root).expect("compose root");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(&compose_root, &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("submit.sbatch");

    let mut perms = fs::metadata(&compose_root).expect("meta").permissions();
    perms.set_mode(0o555);
    fs::set_permissions(&compose_root, perms).expect("chmod readonly");

    let submit = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--script-out",
            script_out.to_str().expect("path"),
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            sbatch.to_str().expect("path"),
        ],
    );

    let mut restore = fs::metadata(&compose_root).expect("meta").permissions();
    restore.set_mode(0o755);
    fs::set_permissions(&compose_root, restore).expect("chmod restore");

    assert_success(&submit);
    assert!(stdout_text(&submit).contains("Submitted batch job 12345"));
    assert!(stdout_text(&submit).contains("tracking metadata could not be written"));
    assert!(
        stderr_text(&submit)
            .contains("warning: job submitted, but failed to write tracking metadata")
    );
    assert!(!compose_root.join(".hpc-compose/latest.json").exists());
}

#[test]
fn status_and_logs_commands_use_submission_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("squeue.state");
    let sacct_state = tmpdir.path().join("sacct.state");
    fs::write(&squeue_state, "NONE\n").expect("squeue state");
    fs::write(&sacct_state, "COMPLETED\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

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
        ],
    );
    assert_success(&submit);
    let metadata = tmpdir.path().join(".hpc-compose/latest.json");
    assert!(metadata.exists());

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "alpha\nbeta\n").expect("log");
    let batch_log = tmpdir.path().join("slurm-12345.out");
    fs::write(&batch_log, "batch-line\n").expect("batch log");

    let status = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status);
    let status_stdout = stdout_text(&status);
    assert!(status_stdout.contains("job id: 12345"));
    assert!(status_stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(status_stdout.contains("compose file:"));
    assert!(status_stdout.contains("batch log:"));
    assert!(status_stdout.contains("log  service 'app':"));

    let status_json = run_cli(
        tmpdir.path(),
        &[
            "status",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--json",
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&status_json);
    let value: Value = serde_json::from_str(&stdout_text(&status_json)).expect("status json");
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    assert_eq!(value["scheduler"]["state"], Value::from("COMPLETED"));
    assert!(
        value["record"]["batch_log"]
            .as_str()
            .unwrap_or_default()
            .ends_with("slurm-12345.out")
    );

    let logs = run_cli(
        tmpdir.path(),
        &[
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--lines",
            "1",
        ],
    );
    assert_success(&logs);
    assert!(stdout_text(&logs).contains("[app] beta"));
}

#[test]
fn stats_command_reports_live_step_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let squeue_state = tmpdir.path().join("stats-squeue.state");
    let sacct_state = tmpdir.path().join("stats-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.batch|1|00:00:01|1M|1M|cpu=1|cpu=00:00:01
12345.0|1|00:00:10|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:10,gres/gpuutil=65,gres/gpumem=1024M
12345.extern|1|00:00:01|1M|1M|cpu=1|cpu=00:00:01
12345.1|2|00:00:20|256M|512M|cpu=2,mem=8G|cpu=00:00:20
",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

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
        ],
    );
    assert_success(&submit);

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let stdout = stdout_text(&stats);
    assert!(stdout.contains("job id: 12345"));
    assert!(stdout.contains("stats source: sstat"));
    assert!(stdout.contains("step: 12345.0"));
    assert!(stdout.contains("step: 12345.1"));
    assert!(stdout.contains("gpu util: 65"));
    assert!(!stdout.contains("12345.batch"));
    assert!(!stdout.contains("12345.extern"));

    let stats_json = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_json);
    let value: Value = serde_json::from_str(&stdout_text(&stats_json)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("12345"));
    assert_eq!(value["available"], Value::from(true));
    assert_eq!(value["source"], Value::from("sstat"));
    assert_eq!(value["record"]["job_id"], Value::from("12345"));
    let steps = value["steps"].as_array().expect("steps");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0]["gpu_util"], Value::from("65"));
    assert_eq!(steps[0]["gpu_mem"], Value::from("1024M"));
    assert_eq!(steps[0]["gpu_count"], Value::from("1"));
}

#[test]
fn stats_command_prefers_sampler_metrics_when_present() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_metrics_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());
    let squeue_state = tmpdir.path().join("sampler-squeue.state");
    let sacct_state = tmpdir.path().join("sampler-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sampler-sstat.output");
    fs::write(
        &sstat_output,
        "\
JobID|NTasks|AveCPU|AveRSS|MaxRSS|AllocTRES|TRESUsageInAve
12345.0|1|00:00:11|512M|1G|cpu=1,mem=4G,gres/gpu=1|cpu=00:00:11
",
    )
    .expect("sstat output");
    let _runtime_sstat = write_fake_sstat(tmpdir.path(), &sstat_output);
    let stats_sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let gpu_output = tmpdir.path().join("nvidia-smi-gpu.output");
    fs::write(
        &gpu_output,
        "0, GPU-aaaa, NVIDIA H100, 91, 77, 4096, 8192, 55, 220, 300\n",
    )
    .expect("gpu output");
    let gpu_processes = tmpdir.path().join("nvidia-smi-proc.output");
    fs::write(&gpu_processes, "GPU-aaaa, 4242, python, 2048\n").expect("gpu proc output");
    let _nvidia_smi = write_fake_nvidia_smi(tmpdir.path(), &gpu_output, &gpu_processes);

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
        ],
    );
    assert_success(&submit);

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["source"], Value::from("sampler"));
    assert_eq!(
        value["sampler"]["gpu"]["gpus"][0]["utilization_gpu"],
        Value::from("91")
    );
    assert_eq!(
        value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
    assert_eq!(value["steps"][0]["step_id"], Value::from("12345.0"));
    assert_eq!(value["steps"][0]["ave_cpu"], Value::from("00:00:11"));
    assert!(
        value["metrics_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/.hpc-compose/12345/metrics")
    );

    let stats_explicit = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--json",
            "--sstat-bin",
            stats_sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_explicit);
    let explicit_value: Value =
        serde_json::from_str(&stdout_text(&stats_explicit)).expect("explicit stats json");
    assert_eq!(explicit_value["source"], Value::from("sampler"));
    assert_eq!(explicit_value["record"]["job_id"], Value::from("12345"));
    assert_eq!(
        explicit_value["sampler"]["gpu"]["processes"][0]["pid"],
        Value::from("4242")
    );
}

#[test]
fn stats_command_supports_explicit_job_id_without_metadata() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-explicit-squeue.state");
    let sacct_state = tmpdir.path().join("stats-explicit-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);
    let sstat_output = tmpdir.path().join("sstat-explicit.output");
    fs::write(
        &sstat_output,
        "67890.0|1|00:00:02|64M|128M|cpu=1,mem=1G|cpu=00:00:02\n",
    )
    .expect("sstat output");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let stats_text = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats_text);
    assert_eq!(
        stdout_text(&stats_text)
            .matches("GPU accounting metrics are unavailable")
            .count(),
        1
    );

    let stats = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_success(&stats);
    let value: Value = serde_json::from_str(&stdout_text(&stats)).expect("stats json");
    assert_eq!(value["job_id"], Value::from("67890"));
    assert_eq!(value["available"], Value::from(true));
    assert!(value["record"].is_null());
}

#[test]
fn stats_command_reports_unavailable_for_pending_and_completed_jobs() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let sstat_output = tmpdir.path().join("sstat-empty.output");
    fs::write(&sstat_output, "").expect("empty sstat");
    let sstat = write_fake_sstat(tmpdir.path(), &sstat_output);

    let pending_squeue_state = tmpdir.path().join("pending-squeue.state");
    let pending_sacct_state = tmpdir.path().join("pending-sacct.state");
    fs::write(&pending_squeue_state, "PENDING\n").expect("pending squeue");
    fs::write(&pending_sacct_state, "NONE\n").expect("pending sacct");
    let pending_squeue = write_fake_squeue(tmpdir.path(), &pending_squeue_state);
    let pending_sacct = write_fake_sacct(tmpdir.path(), &pending_sacct_state);
    let pending = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            pending_squeue.to_str().expect("path"),
            "--sacct-bin",
            pending_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&pending);
    let pending_value: Value = serde_json::from_str(&stdout_text(&pending)).expect("pending json");
    assert_eq!(pending_value["available"], Value::from(false));
    assert!(
        pending_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("not running yet")
    );

    let completed_squeue_state = tmpdir.path().join("completed-squeue.state");
    let completed_sacct_state = tmpdir.path().join("completed-sacct.state");
    fs::write(&completed_squeue_state, "NONE\n").expect("completed squeue");
    fs::write(&completed_sacct_state, "COMPLETED\n").expect("completed sacct");
    let completed_squeue = write_fake_squeue(tmpdir.path(), &completed_squeue_state);
    let completed_sacct = write_fake_sacct(tmpdir.path(), &completed_sacct_state);
    let completed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "55555",
            "--json",
            "--sstat-bin",
            sstat.to_str().expect("path"),
            "--squeue-bin",
            completed_squeue.to_str().expect("path"),
            "--sacct-bin",
            completed_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&completed);
    let completed_value: Value =
        serde_json::from_str(&stdout_text(&completed)).expect("completed json");
    assert_eq!(completed_value["available"], Value::from(false));
    assert!(
        completed_value["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no longer running")
    );
}

#[test]
fn stats_command_surfaces_sstat_failures_and_malformed_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let squeue_state = tmpdir.path().join("stats-fail-squeue.state");
    let sacct_state = tmpdir.path().join("stats-fail-sacct.state");
    fs::write(&squeue_state, "RUNNING\n").expect("squeue state");
    fs::write(&sacct_state, "NONE\n").expect("sacct state");
    let squeue = write_fake_squeue(tmpdir.path(), &squeue_state);
    let sacct = write_fake_sacct(tmpdir.path(), &sacct_state);

    let sstat_fail = write_fake_sstat_failure(tmpdir.path());
    let failed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--sstat-bin",
            sstat_fail.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("sstat failed for job 42"));
    assert!(stderr_text(&failed).contains("job accounting unavailable"));

    let malformed_output = tmpdir.path().join("sstat-malformed.output");
    fs::write(&malformed_output, "12345.0|1|00:00:01\n").expect("malformed output");
    let sstat_bad = write_fake_sstat(tmpdir.path(), &malformed_output);
    let malformed = run_cli(
        tmpdir.path(),
        &[
            "stats",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
            "--sstat-bin",
            sstat_bad.to_str().expect("path"),
            "--squeue-bin",
            squeue.to_str().expect("path"),
            "--sacct-bin",
            sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&malformed);
    assert!(stderr_text(&malformed).contains("malformed sstat output"));
}

#[test]
fn cancel_uses_tracked_or_explicit_job_id() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let scancel_log = tmpdir.path().join("scancel.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, true);

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
        ],
    );
    assert_success(&submit);

    let cancel_latest = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_latest);
    assert!(stdout_text(&cancel_latest).contains("cancelled job: 12345"));

    let cancel_explicit = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "67890",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_success(&cancel_explicit);
    assert!(stdout_text(&cancel_explicit).contains("cancelled job: 67890"));

    let log = fs::read_to_string(scancel_log).expect("scancel log");
    assert!(log.contains("12345"));
    assert!(log.contains("67890"));
}

#[test]
fn cancel_reports_missing_record_and_scancel_failure() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let missing = run_cli(
        tmpdir.path(),
        &["cancel", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&missing);
    assert!(stderr_text(&missing).contains("no tracked submission metadata exists"));

    let scancel_log = tmpdir.path().join("scancel-fail.log");
    let scancel = write_fake_scancel(tmpdir.path(), &scancel_log, false);
    let failed = run_cli(
        tmpdir.path(),
        &[
            "cancel",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "42",
            "--scancel-bin",
            scancel.to_str().expect("path"),
        ],
    );
    assert_failure(&failed);
    assert!(stderr_text(&failed).contains("scancel failed for job 42"));
    assert!(stderr_text(&failed).contains("permission denied"));
}

#[test]
fn status_reports_missing_record_cleanly() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);

    let status = run_cli(
        tmpdir.path(),
        &["status", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&status);
    assert!(stderr_text(&status).contains("no tracked submission metadata exists"));
}

#[test]
fn submit_watch_covers_completed_and_failed_states() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());

    let success_squeue_state = tmpdir.path().join("watch-success-squeue.state");
    let success_sacct_state = tmpdir.path().join("watch-success-sacct.state");
    let success_squeue = write_fake_squeue(tmpdir.path(), &success_squeue_state);
    let success_sacct = write_fake_sacct(tmpdir.path(), &success_sacct_state);
    let success_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &success_squeue_state,
        &success_sacct_state,
        "COMPLETED",
        "ready",
        2,
    );

    let success = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--watch",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            success_sbatch.to_str().expect("path"),
            "--squeue-bin",
            success_squeue.to_str().expect("path"),
            "--sacct-bin",
            success_sacct.to_str().expect("path"),
        ],
    );
    assert_success(&success);
    let success_stdout = stdout_text(&success);
    assert!(success_stdout.contains("watching job 12345"));
    assert!(!success_stdout.contains("scheduler state: unknown (local-only)"));
    assert!(success_stdout.contains("scheduler state: COMPLETED (sacct)"));
    assert!(success_stdout.contains("[app] ready"));

    let failure_squeue_state = tmpdir.path().join("watch-failure-squeue.state");
    let failure_sacct_state = tmpdir.path().join("watch-failure-sacct.state");
    let failure_squeue = write_fake_squeue(tmpdir.path(), &failure_squeue_state);
    let failure_sacct = write_fake_sacct(tmpdir.path(), &failure_sacct_state);
    let failure_sbatch = write_fake_watch_sbatch(
        tmpdir.path(),
        &failure_squeue_state,
        &failure_sacct_state,
        "FAILED",
        "boom",
        0,
    );

    let failure = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "-f",
            compose.to_str().expect("path"),
            "--watch",
            "--enroot-bin",
            enroot.to_str().expect("path"),
            "--srun-bin",
            srun.to_str().expect("path"),
            "--sbatch-bin",
            failure_sbatch.to_str().expect("path"),
            "--squeue-bin",
            failure_squeue.to_str().expect("path"),
            "--sacct-bin",
            failure_sacct.to_str().expect("path"),
        ],
    );
    assert_failure(&failure);
    assert!(stdout_text(&failure).contains("[app] boom"));
    assert!(stderr_text(&failure).contains("finished in scheduler state FAILED"));
}

#[test]
fn logs_follow_streams_appended_lines() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

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
        ],
    );
    assert_success(&submit);

    let log_dir = tmpdir.path().join(".hpc-compose/12345/logs");
    fs::create_dir_all(&log_dir).expect("log dir");
    let log_path = log_dir.join(log_file_name_for_service("app"));
    fs::write(&log_path, "start\n").expect("log");

    let mut child = Command::new(bin_path())
        .current_dir(tmpdir.path())
        .args([
            "logs",
            "-f",
            compose.to_str().expect("path"),
            "--service",
            "app",
            "--follow",
            "--lines",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn logs");

    thread::sleep(Duration::from_millis(250));
    let mut file = OpenOptions::new()
        .append(true)
        .open(&log_path)
        .expect("open log");
    writeln!(file, "follow-line").expect("append");
    file.flush().expect("flush");
    thread::sleep(Duration::from_millis(1400));
    child.kill().expect("kill");
    let output = child.wait_with_output().expect("wait");
    assert!(String::from_utf8_lossy(&output.stdout).contains("[app] follow-line"));
}

#[test]
fn inspect_json_preflight_json_and_init_cover_new_modes() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_env_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

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
    assert!(inspect_verbose_stdout.contains("environment:"));
    assert!(inspect_verbose_stdout.contains("  - SECRET_TOKEN"));
    assert!(!inspect_verbose_stdout.contains("SECRET_TOKEN=super-secret"));
    assert!(inspect_verbose_stdout.contains(&format!(
        "{}:/workspace",
        tmpdir.path().join("app").display()
    )));
    assert!(inspect_verbose_stdout.contains("/hpc-compose/job"));
    assert!(inspect_verbose_stdout.contains("effective srun args:"));

    let inspect_json = run_cli(
        tmpdir.path(),
        &["inspect", "-f", compose.to_str().expect("path"), "--json"],
    );
    assert_success(&inspect_json);
    let inspect_json_stdout = stdout_text(&inspect_json);
    assert!(inspect_json_stdout.contains("super-secret"));
    assert!(inspect_json_stdout.contains("hi-from-dotenv"));

    let preflight_json = run_cli(
        tmpdir.path(),
        &[
            "preflight",
            "-f",
            compose.to_str().expect("path"),
            "--json",
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
    assert!(preflight_value["passed_checks"].is_array());

    for template in [
        "dev-python-app",
        "app-redis-worker",
        "llm-curl-workflow",
        "llm-curl-workflow-workdir",
        "llama-app",
        "llama-uv-worker",
        "vllm-uv-worker",
    ] {
        let output = tmpdir.path().join(format!("{template}.yaml"));
        let init = run_cli(
            tmpdir.path(),
            &[
                "init",
                "--template",
                template,
                "--name",
                "custom-init",
                "--cache-dir",
                "/tmp/custom-cache",
                "--output",
                output.to_str().expect("path"),
                "--force",
            ],
        );
        assert_success(&init);
        assert!(output.exists());
        let validate = run_cli(
            tmpdir.path(),
            &["validate", "-f", output.to_str().expect("path")],
        );
        assert_success(&validate);
        let rendered = fs::read_to_string(&output).expect("rendered template");
        assert!(rendered.contains("name: custom-init"));
        assert!(rendered.contains("job_name: custom-init"));
        assert!(rendered.contains("cache_dir: /tmp/custom-cache"));
        assert!(stdout_text(&init).contains("hpc-compose submit --watch -f"));
    }
}

#[test]
fn init_interactive_uses_prompted_values() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let output = tmpdir.path().join("interactive-init.yaml");
    let init = run_cli_with_stdin(
        tmpdir.path(),
        &[
            "init",
            "--output",
            output.to_str().expect("path"),
            "--force",
        ],
        "2\ninteractive-app\n/tmp/interactive-cache\n",
    );
    assert_success(&init);
    let rendered = fs::read_to_string(&output).expect("rendered");
    assert!(rendered.contains("name: interactive-app"));
    assert!(rendered.contains("job_name: interactive-app"));
    assert!(rendered.contains("cache_dir: /tmp/interactive-cache"));
    let stdout = stdout_text(&init);
    assert!(stdout.contains("Choose a template:"));
    assert!(stdout.contains("hpc-compose submit --watch -f"));
}

#[test]
fn submit_dry_run_skips_sbatch() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());
    let script_out = tmpdir.path().join("dry-run.sbatch");

    let output = run_cli(
        tmpdir.path(),
        &[
            "submit",
            "--dry-run",
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
    assert_success(&output);
    let out = stdout_text(&output);
    assert!(out.contains("dry run: skipping sbatch submission"));
    assert!(!out.contains("Submitted batch job"));
    assert!(script_out.exists());
}

#[test]
fn artifacts_command_exports_collected_metrics_and_json() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "always");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

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
        ],
    );
    assert_success(&submit);

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    assert!(tracked_manifest.exists(), "artifact manifest should exist");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("success")
    );
    assert!(
        tracked_manifest_value["copied_relative_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item.as_str() == Some("metrics/meta.json"))
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("did not match any paths"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--json",
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    let value: Value = serde_json::from_str(&stdout_text(&artifacts)).expect("artifacts json");
    assert!(
        value["export_dir"]
            .as_str()
            .unwrap_or_default()
            .ends_with("/results/12345")
    );
    assert!(
        value["exported_paths"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .ends_with("/results/12345/metrics/meta.json"))
    );
    assert_eq!(
        fs::read_to_string(tmpdir.path().join("results/12345/metrics/meta.json"))
            .expect("exported"),
        fs::read_to_string(
            tmpdir
                .path()
                .join(".hpc-compose/12345/artifacts/payload/metrics/meta.json")
        )
        .expect("payload")
    );
}

#[test]
fn artifact_collection_handles_overlapping_paths_without_nested_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    fs::create_dir_all(tmpdir.path().join("app")).expect("app dir");
    fs::write(tmpdir.path().join("app/main.py"), "print('hello')\n").expect("main.py");
    let compose = write_compose(
        tmpdir.path(),
        "compose-artifacts-overlap.yaml",
        &format!(
            r#"
name: demo
x-slurm:
  job_name: demo
  time: "00:10:00"
  cache_dir: {}
  artifacts:
    collect: always
    export_dir: ./results/${{SLURM_JOB_ID}}
    paths:
      - /hpc-compose/job/logs/app.log
      - /hpc-compose/job/logs
services:
  app:
    image: python:3.11-slim
    working_dir: /workspace
    volumes:
      - ./app:/workspace
    command:
      - python
      - main.py
"#,
            cache_dir.display()
        ),
    );
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script(tmpdir.path());

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
        ],
    );
    assert_success(&submit);

    let payload_root = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/payload/logs");
    assert!(payload_root.join("app.log").exists());
    assert!(!payload_root.join("logs/app.log").exists());

    let artifacts = run_cli(
        tmpdir.path(),
        &[
            "artifacts",
            "-f",
            compose.to_str().expect("path"),
            "--job-id",
            "12345",
        ],
    );
    assert_success(&artifacts);
    assert!(tmpdir.path().join("results/12345/logs/app.log").exists());
    assert!(
        !tmpdir
            .path()
            .join("results/12345/logs/logs/app.log")
            .exists()
    );
}

#[test]
fn artifact_collection_policy_skips_when_job_outcome_does_not_match() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_artifacts_compose(tmpdir.path(), &cache_dir, "on_success");
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun_failure(tmpdir.path());
    let sbatch = write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir.path());

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
        ],
    );
    assert_success(&submit);

    let tracked_manifest = tmpdir
        .path()
        .join(".hpc-compose/12345/artifacts/manifest.json");
    let tracked_manifest_value: Value =
        serde_json::from_str(&fs::read_to_string(&tracked_manifest).expect("manifest"))
            .expect("manifest json");
    assert_eq!(
        tracked_manifest_value["job_outcome"],
        Value::from("failure")
    );
    assert!(
        tracked_manifest_value["warnings"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .any(|item| item
                .as_str()
                .unwrap_or_default()
                .contains("does not match policy 'on_success'"))
    );

    let artifacts = run_cli(
        tmpdir.path(),
        &["artifacts", "-f", compose.to_str().expect("path")],
    );
    assert_success(&artifacts);
    let out = stdout_text(&artifacts);
    assert!(out.contains("exported paths: 0"));
}

#[test]
fn clean_command_removes_old_job_directories() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let cache_root = safe_cache_dir();
    let cache_dir = cache_root.path().to_path_buf();
    let compose = write_prepare_compose(tmpdir.path(), &cache_dir);
    let enroot = write_fake_enroot(tmpdir.path());
    let srun = write_fake_srun(tmpdir.path());
    let sbatch = write_fake_sbatch(tmpdir.path());

    // Submit a job to create tracking metadata
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
        ],
    );
    assert_success(&submit);

    // clean --all should report nothing to remove (only one job = latest)
    let clean_all = run_cli(
        tmpdir.path(),
        &["clean", "--all", "-f", compose.to_str().expect("path")],
    );
    assert_success(&clean_all);
    assert!(stdout_text(&clean_all).contains("no job directories to clean"));

    // clean --age 0 should remove the job (it's older than 0 days)
    let clean_age = run_cli(
        tmpdir.path(),
        &["clean", "--age", "0", "-f", compose.to_str().expect("path")],
    );
    assert_success(&clean_age);
}

#[test]
fn clean_requires_strategy_flag() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let compose = tmpdir.path().join("compose.yaml");
    fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("write");

    let output = run_cli(
        tmpdir.path(),
        &["clean", "-f", compose.to_str().expect("path")],
    );
    assert_failure(&output);
    let err = stderr_text(&output);
    assert!(
        err.contains("--age") || err.contains("--all"),
        "error should mention required flags: {err}"
    );
}

#[test]
fn completions_command_generates_output() {
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    for shell in ["bash", "zsh", "fish"] {
        let output = run_cli(tmpdir.path(), &["completions", shell]);
        assert_success(&output);
        let out = stdout_text(&output);
        assert!(
            out.contains("hpc-compose"),
            "completions for {shell} should mention hpc-compose"
        );
        assert!(out.len() > 100, "completions should be non-trivial");
    }
}
