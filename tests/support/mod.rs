#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use hpc_compose::planner::build_plan;
use hpc_compose::prepare::{RuntimePlan, build_runtime_plan};
use hpc_compose::render::log_file_name_for_service;
use hpc_compose::spec::ComposeSpec;

pub(crate) fn bin_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_hpc-compose"))
}

pub(crate) fn run_cli(cwd: &Path, args: &[&str]) -> Output {
    Command::new(bin_path())
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run cli")
}

pub(crate) fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub(crate) fn run_cli_with_stdin(cwd: &Path, args: &[&str], stdin: &str) -> Output {
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

pub(crate) fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

pub(crate) fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

pub(crate) fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

pub(crate) fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        stdout_text(output),
        stderr_text(output)
    );
}

pub(crate) fn write_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    let mut perms = fs::metadata(path).expect("meta").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

pub(crate) fn write_fake_enroot(tmpdir: &Path) -> PathBuf {
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

pub(crate) fn write_fake_srun(tmpdir: &Path) -> PathBuf {
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
    write_fake_scontrol(tmpdir);
    path
}

pub(crate) fn write_fake_srun_failure_policy(tmpdir: &Path) -> PathBuf {
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
plan_file="$state_root/${key}.plan"
count=0
if [[ -f "$count_file" ]]; then
  count="$(cat "$count_file")"
fi
count=$((count + 1))
echo "$count" > "$count_file"
if [[ -f "$plan_file" ]]; then
  line="$(sed -n "${count}p" "$plan_file")"
  if [[ -z "$line" ]]; then
    line="$(tail -n 1 "$plan_file")"
  fi
  if [[ -n "$line" ]]; then
    read -r exit_code sleep_seconds <<< "$line"
    sleep_seconds="${sleep_seconds:-0}"
    if (( sleep_seconds > 0 )); then
      sleep "$sleep_seconds"
    fi
    exit "$exit_code"
  fi
fi
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
    write_fake_scontrol(tmpdir);
    path
}

pub(crate) fn fake_srun_key(job_name: &str) -> String {
    job_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn write_fake_srun_failure_policy_plan(
    tmpdir: &Path,
    job_name: &str,
    attempts: &[(i32, u64)],
) -> PathBuf {
    let path = write_fake_srun_failure_policy(tmpdir);
    let state_root = tmpdir.join(".hpc-compose/fake-srun");
    fs::create_dir_all(&state_root).expect("create fake srun state");
    let key = fake_srun_key(job_name);
    let plan = attempts
        .iter()
        .map(|(exit_code, sleep_seconds)| format!("{exit_code} {sleep_seconds}"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(state_root.join(format!("{key}.plan")), format!("{plan}\n")).expect("write plan");
    path
}

pub(crate) fn write_fake_srun_failure(tmpdir: &Path) -> PathBuf {
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
    write_fake_scontrol(tmpdir);
    path
}

pub(crate) fn write_fake_srun_capture(tmpdir: &Path, log_path: &Path) -> PathBuf {
    let path = tmpdir.join("srun");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
if [[ "${{1:-}}" == "--help" ]]; then
  echo "usage: srun --container-image=IMAGE"
  exit 0
fi
output_path=""
for arg in "$@"; do
  case "$arg" in
    --output=*)
      output_path="${{arg#--output=}}"
      ;;
  esac
done
printf 'args:%s\n' "$*" >> '{}'
printf 'env:%s|%s|%s|%s\n' "${{HPC_COMPOSE_PRIMARY_NODE:-}}" "${{HPC_COMPOSE_NODE_COUNT:-}}" "${{HPC_COMPOSE_NODELIST:-}}" "${{HPC_COMPOSE_NODELIST_FILE:-}}" >> '{}'
if [[ -n "$output_path" ]]; then
  mkdir -p "$(dirname "$output_path")"
  printf 'ready\n' >> "$output_path"
fi
sleep 3
exit 0
"#,
            log_path.display(),
            log_path.display()
        ),
    );
    write_fake_scontrol(tmpdir);
    path
}

pub(crate) fn write_fake_scontrol(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("scontrol");
    write_script(
        &path,
        r#"#!/bin/bash
set -euo pipefail
if [[ "${1:-}" == "show" && "${2:-}" == "hostnames" ]]; then
  if [[ $# -ge 3 ]]; then
    raw="${3//,/ }"
    for host in $raw; do
      printf '%s\n' "$host"
    done
  fi
  exit 0
fi
echo "unsupported scontrol invocation" >&2
exit 1
"#,
    );
    path
}

pub(crate) fn write_fake_sbatch(tmpdir: &Path) -> PathBuf {
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

pub(crate) fn write_fake_sbatch_runs_script(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path" >/dev/null 2>&1
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

pub(crate) fn write_fake_sbatch_runs_script_with_job_output(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script-with-output");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path"
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

pub(crate) fn write_fake_sbatch_runs_script_ignoring_job_exit(tmpdir: &Path) -> PathBuf {
    let path = tmpdir.join("sbatch-run-script-ignore-exit");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST=node01
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path" >/dev/null 2>&1 || true
echo "Submitted batch job 12345"
"#,
            tmpdir.display()
        ),
    );
    path
}

pub(crate) fn write_fake_sbatch_runs_script_with_nodelist(
    tmpdir: &Path,
    file_name: &str,
    job_nodelist: &str,
) -> PathBuf {
    let path = tmpdir.join(file_name);
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
script_path="${{1:?missing script path}}"
PATH="{}:$PATH"
export SLURM_JOB_ID=12345
export SLURM_JOB_NODELIST='{}'
export SLURM_SUBMIT_DIR="$PWD"
bash "$script_path"
echo "Submitted batch job 12345"
"#,
            tmpdir.display(),
            job_nodelist
        ),
    );
    path
}

pub(crate) fn write_fake_scancel(tmpdir: &Path, log_path: &Path, success: bool) -> PathBuf {
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

pub(crate) fn write_fake_squeue(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("squeue");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
content="$(cat '{}' 2>/dev/null || true)"
format_string=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "-o" || "$prev" == "--format" ]]; then
    format_string="$arg"
  fi
  case "$arg" in
    --format=*)
      format_string="${{arg#--format=}}"
      ;;
  esac
  prev="$arg"
done
case "$content" in
  ""|NONE)
    exit 0
    ;;
  *"STATE="*)
    state=""
    reason=""
    start=""
    while IFS= read -r line; do
      case "$line" in
        STATE=*)
          state="${{line#STATE=}}"
          ;;
        REASON=*)
          reason="${{line#REASON=}}"
          ;;
        START=*)
          start="${{line#START=}}"
          ;;
      esac
    done <<< "$content"
    if [[ -z "$state" || "$state" == "NONE" ]]; then
      exit 0
    fi
    case "$format_string" in
      *"%T|%r|%S"*)
        printf '%s|%s|%s\n' "$state" "${{reason:-N/A}}" "${{start:-N/A}}"
        ;;
      *)
        printf '%s\n' "$state"
        ;;
    esac
    ;;
  *)
    while IFS= read -r line; do
      if [[ -n "$line" && "$line" != "NONE" ]]; then
        printf '%s\n' "$line"
        break
      fi
    done <<< "$content"
    ;;
esac
"#,
            state_file.display()
        ),
    );
    path
}

pub(crate) fn write_fake_sacct(tmpdir: &Path, state_file: &Path) -> PathBuf {
    let path = tmpdir.join("sacct");
    write_script(
        &path,
        &format!(
            r#"#!/bin/bash
set -euo pipefail
content="$(cat '{}' 2>/dev/null || true)"
format_string=""
prev=""
for arg in "$@"; do
  if [[ "$prev" == "--format" ]]; then
    format_string="$arg"
  fi
  case "$arg" in
    --format=*)
      format_string="${{arg#--format=}}"
      ;;
  esac
  prev="$arg"
done
case "$content" in
  ""|NONE)
    exit 0
    ;;
  *"STATE="*)
    state=""
    reason=""
    eligible=""
    start=""
    while IFS= read -r line; do
      case "$line" in
        STATE=*)
          state="${{line#STATE=}}"
          ;;
        REASON=*)
          reason="${{line#REASON=}}"
          ;;
        ELIGIBLE=*)
          eligible="${{line#ELIGIBLE=}}"
          ;;
        START=*)
          start="${{line#START=}}"
          ;;
      esac
    done <<< "$content"
    if [[ -z "$state" || "$state" == "NONE" ]]; then
      exit 0
    fi
    case "$format_string" in
      *"State,Eligible,Start,Reason"*)
        printf '%s|%s|%s|%s\n' \
          "$state" \
          "${{eligible:-Unknown}}" \
          "${{start:-Unknown}}" \
          "${{reason:-None}}"
        ;;
      *)
        printf '%s\n' "$state"
        ;;
    esac
    ;;
  *)
    while IFS= read -r line; do
      if [[ -n "$line" && "$line" != "NONE" ]]; then
        printf '%s\n' "$line"
        break
      fi
    done <<< "$content"
    ;;
esac
"#,
            state_file.display()
        ),
    );
    path
}

pub(crate) fn write_fake_sstat(tmpdir: &Path, output_file: &Path) -> PathBuf {
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

pub(crate) fn write_fake_sstat_failure(tmpdir: &Path) -> PathBuf {
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

pub(crate) fn write_fake_nvidia_smi(
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

pub(crate) fn write_fake_watch_sbatch(
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

pub(crate) fn write_compose(tmpdir: &Path, name: &str, body: &str) -> PathBuf {
    let path = tmpdir.join(name);
    fs::write(&path, body).expect("write compose");
    path
}

pub(crate) fn runtime_plan(path: &Path) -> RuntimePlan {
    let spec = ComposeSpec::load(path).expect("load spec");
    let plan = build_plan(path, spec).expect("build plan");
    build_runtime_plan(&plan)
}

pub(crate) fn safe_cache_dir() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tmp/hpc-compose-tests");
    fs::create_dir_all(&root).expect("cache root");
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(root)
        .expect("cache tempdir")
}

pub(crate) fn write_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
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

pub(crate) fn write_example_compose(
    tmpdir: &Path,
    example_name: &str,
    cache_dir: &Path,
) -> PathBuf {
    let source = repo_root().join("examples").join(example_name);
    let body = fs::read_to_string(&source).expect("read example");
    let rewritten = body.replace(
        "/shared/$USER/hpc-compose-cache",
        &cache_dir.display().to_string(),
    );
    write_compose(tmpdir, example_name, &rewritten)
}

pub(crate) fn write_metrics_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
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

pub(crate) fn write_artifacts_compose_with_paths(
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

pub(crate) fn write_artifacts_compose(
    tmpdir: &Path,
    cache_dir: &Path,
    collect_policy: &str,
) -> PathBuf {
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

pub(crate) fn write_mount_prepare_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
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

pub(crate) fn write_env_compose(tmpdir: &Path, cache_dir: &Path) -> PathBuf {
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
