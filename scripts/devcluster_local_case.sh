#!/usr/bin/env bash
# Shared runner for opt-in in-container local dev-cluster cases.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
case_name="${1:-}"
case "$case_name" in
  preemption|fs-probes) ;;
  *) printf 'usage: %s {preemption|fs-probes}\n' "$0" >&2; exit 2 ;;
esac

run_id="$(date +%s)-$$"
case_id="${case_name//[^a-zA-Z0-9]/-}-$run_id"
host_case_dir="$repo_root/.tmp/devcluster-cases/$case_id"
container_case_dir="/var/tmp/hpc-compose-devcluster-cases/$case_id"
resume_dir="/var/cache/hpc-compose/resume/devcluster-preempt-$case_id"
mkdir -p "$host_case_dir"

red='\033[31m'; green='\033[32m'; bold='\033[1m'; reset='\033[0m'
note() { printf '%b==>%b %s\n' "$bold" "$reset" "$*"; }
pass() { printf '  %bok%b   %s\n' "$green" "$reset" "$*"; }
fail() { printf '  %bFAIL%b %s\n' "$red" "$reset" "$*" >&2; exit 1; }

if docker compose version >/dev/null 2>&1; then
  engine=docker
elif podman compose version >/dev/null 2>&1; then
  engine=podman
else
  fail "need 'docker compose' or 'podman compose' on PATH (is the engine running?)"
fi
inctr() { "$engine" exec "$container" "$@"; }
inctr_at() {
  local workdir="$1"
  shift
  "$engine" exec --workdir "$workdir" "$container" "$@"
}

boot_cluster() {
  # `compose up --build` is cache-backed and reuses an unchanged running
  # container, while still recreating it when the binary or cluster config
  # changed. An explicit DEVCLUSTER_SKIP_BUILD=1 remains available to callers.
  "$repo_root/scripts/devcluster.sh" up >/dev/null
}

container_ready=0
case_ok=0
baseline_jobs="$host_case_dir/baseline-jobs.txt"

collect_diagnostics() {
  [[ "$container_ready" == 1 ]] || return 0
  inctr sinfo -a >"$host_case_dir/sinfo.txt" 2>&1 || true
  inctr squeue -a -o '%.18i %.12j %.10T %.20R' >"$host_case_dir/squeue.txt" 2>&1 || true
  inctr sacct -S now-1hour -X -P --format=JobIDRaw,JobName,State,ExitCode,Start,End \
    >"$host_case_dir/sacct.txt" 2>&1 || true
  "$engine" logs "$container" >"$host_case_dir/container.log" 2>&1 || true
  if inctr test -d "$container_case_dir"; then
    inctr tar -C "$container_case_dir" -czf - . >"$host_case_dir/runtime.tar.gz" 2>/dev/null || true
  fi
  if inctr test -d "$resume_dir"; then
    inctr tar -C "$resume_dir" -czf - . >"$host_case_dir/resume.tar.gz" 2>/dev/null || true
  fi
}

cancel_case_jobs() {
  [[ "$container_ready" == 1 ]] || return 0
  [[ -f "$baseline_jobs" ]] || return 0
  local current job_id
  current="$(inctr squeue -h -o '%A' 2>/dev/null | sort -u || true)"
  while IFS= read -r job_id; do
    [[ -n "$job_id" ]] || continue
    if ! grep -qxF "$job_id" "$baseline_jobs" 2>/dev/null; then
      inctr scancel "$job_id" >/dev/null 2>&1 || true
    fi
  done <<< "$current"
}

finish() {
  local status=$?
  if [[ "$case_ok" == 1 ]]; then
    cancel_case_jobs
    inctr rm -rf "$resume_dir" >/dev/null 2>&1 || true
    inctr rm -rf "$container_case_dir" >/dev/null 2>&1 || true
    rm -rf "$host_case_dir" 2>/dev/null || true
  else
    collect_diagnostics
    cancel_case_jobs
    inctr rm -rf "$resume_dir" >/dev/null 2>&1 || true
    note "Failure diagnostics preserved at $host_case_dir"
    note "Container-side case state preserved at $container_case_dir"
  fi
  if [[ "${DEVCLUSTER_E2E_DOWN:-0}" == "1" ]]; then
    note "Tearing the cluster down"
    "$repo_root/scripts/devcluster.sh" down >/dev/null || true
  fi
  return "$status"
}

wait_for_sacct_state() {
  local job_id="$1" want="$2" attempts="${3:-30}" state
  for _ in $(seq 1 "$attempts"); do
    state="$(inctr sacct -j "$job_id" -n -P -X --format=State 2>/dev/null | head -n1 | awk '{print $1}')"
    [[ "$state" == "$want" ]] && return 0
    sleep 1
  done
  return 1
}

slurm_next_job_id() {
  inctr scontrol show config 2>/dev/null \
    | awk '$1 == "NEXT_JOB_ID" { value=$3 } END { print value }'
}

stage_spec() {
  local name="$1"
  inctr rm -rf "$container_case_dir" >/dev/null 2>&1 || true
  inctr mkdir -p "$container_case_dir"
  inctr cp "/workspace/dev-cluster/specs/_local/$name.yaml" "$container_case_dir/compose.yaml"
}

run_preemption() {
  local result="$host_case_dir/result.json" stderr="$host_case_dir/stderr.log"
  local job_id logs row checkpoints script_path checkpoint
  stage_spec preemption
  note "Running synthetic preemption/requeue case"
  if ! inctr env DEVCLUSTER_CASE_ID="$case_id" hpc-compose test --preemption \
    -f "$container_case_dir/compose.yaml" \
    --skip-prepare --no-preflight --preemption-grace 2s \
    --time 00:01:00 --timeout 90s --format json >"$result" 2>"$stderr"; then
    sed 's/^/    | /' "$stderr" >&2
    fail "test --preemption exited non-zero"
  fi
  python3 "$repo_root/scripts/devcluster_case_assert.py" preemption "$result" \
    || { sed 's/^/    | /' "$result" >&2; fail "preemption JSON contract failed"; }
  job_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["job_id"])' "$result")"
  [[ -n "$job_id" ]] || fail "preemption result did not include a job id"
  wait_for_sacct_state "$job_id" COMPLETED 30 \
    || fail "sacct did not report COMPLETED for preemption job $job_id"
  row="$(inctr sacct -j "$job_id" -n -P -X --format=State,ExitCode 2>/dev/null | head -n1)"
  [[ "$row" == COMPLETED\|0:0* ]] || fail "unexpected sacct row for preemption job $job_id: $row"
  checkpoints="$host_case_dir/checkpoints.json"
  inctr hpc-compose checkpoints -f "$container_case_dir/compose.yaml" --job-id "$job_id" \
    --format json >"$checkpoints" 2>"$host_case_dir/checkpoints.err" \
    || fail "checkpoints failed for preemption job $job_id"
  python3 "$repo_root/scripts/devcluster_case_assert.py" preemption-checkpoints \
    "$checkpoints" --job-id "$job_id" || fail "preemption checkpoint history contract failed"
  checkpoint="$(inctr cat "$resume_dir/preemption.checkpoint" 2>/dev/null || true)"
  [[ "$checkpoint" == "signal=USR1 attempt=0" ]] \
    || fail "signal checkpoint had unexpected contents: $checkpoint"
  script_path="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["script_path"])' "$result")"
  inctr grep -q '^#SBATCH --requeue$' "$script_path" \
    || fail "rendered preemption script is missing #SBATCH --requeue"
  inctr grep -q '^#SBATCH --signal=USR1@30$' "$script_path" \
    || fail "rendered preemption script is missing the step-targeted USR1 directive"
  logs="$(inctr hpc-compose logs -f "$container_case_dir/compose.yaml" --job-id "$job_id" 2>&1 || true)"
  printf '%s' "$logs" | grep -q 'devcluster-preemption-resumed' \
    || fail "resumed attempt did not confirm checkpoint recovery"
  pass "USR1 checkpoint, requeue, resumed attempt, assertions, and sacct all passed (job $job_id)"
}

run_fs_probes() {
  local result="$host_case_dir/result.json" stderr="$host_case_dir/stderr.log"
  local row rows probe_job_id accounted_job_id next_job_after
  local probes_before="$host_case_dir/probes-before.txt"
  local probes_after="$host_case_dir/probes-after.txt"
  stage_spec fs-probes
  probe_job_id="$(slurm_next_job_id)"
  [[ "$probe_job_id" =~ ^[0-9]+$ ]] \
    || fail "could not read Slurm NEXT_JOB_ID before filesystem probe"
  inctr find /var/cache/hpc-compose -maxdepth 1 -name '.hpc-compose-fs-probe-*' -print \
    | sort >"$probes_before"
  note "Running active shared-filesystem probe"
  if ! inctr_at "$container_case_dir" hpc-compose preflight \
    -f "$container_case_dir/compose.yaml" \
    --fs-probes --format json >"$result" 2>"$stderr"; then
    sed 's/^/    | /' "$stderr" >&2
    fail "preflight --fs-probes exited non-zero"
  fi
  python3 "$repo_root/scripts/devcluster_case_assert.py" fs-probes "$result" \
    || { sed 's/^/    | /' "$result" >&2; fail "filesystem-probe JSON contract failed"; }

  row=""
  for _ in $(seq 1 20); do
    rows="$(inctr sacct -n -X -P --format=JobIDRaw,JobName,State,ExitCode 2>/dev/null || true)"
    row="$(printf '%s\n' "$rows" | awk -F'|' -v job_id="$probe_job_id" \
      '$1 == job_id && $2 == "hpc-compose-fs-probe" { print; exit }')"
    [[ -n "$row" ]] && break
    sleep 1
  done
  [[ -n "$row" ]] || fail "no accounting row appeared for filesystem probe job $probe_job_id"
  IFS='|' read -r accounted_job_id _job_name probe_state probe_exit <<< "$row"
  [[ "$accounted_job_id" == "$probe_job_id" ]] \
    || fail "filesystem probe accounting resolved job $accounted_job_id, expected $probe_job_id"
  [[ "$probe_state" == "COMPLETED" && "$probe_exit" == "0:0" ]] \
    || fail "filesystem probe job $probe_job_id ended as $probe_state/$probe_exit"
  next_job_after="$(slurm_next_job_id)"
  [[ "$next_job_after" =~ ^[0-9]+$ ]] \
    || fail "could not read Slurm NEXT_JOB_ID after filesystem probe"
  (( next_job_after == probe_job_id + 1 )) \
    || fail "filesystem probe used an unexpected allocation range ($probe_job_id -> $next_job_after)"
  inctr find /var/cache/hpc-compose -maxdepth 1 -name '.hpc-compose-fs-probe-*' -print \
    | sort >"$probes_after"
  cmp -s "$probes_before" "$probes_after" \
    || fail "successful filesystem probe left a new probe directory behind"
  pass "sbatch --wait visibility, reverse visibility, rename, headroom, and cleanup passed (job $probe_job_id)"
}

note "Booting the dev cluster"
boot_cluster
container_ready=1
trap finish EXIT

note "Waiting for the node to register idle"
idle=0
for _ in $(seq 1 90); do
  if inctr sinfo -h -o '%T' 2>/dev/null | grep -q '^idle'; then
    idle=1
    break
  fi
  sleep 1
done
[[ "$idle" == 1 ]] || fail "node did not reach idle"
pass "node is idle"
inctr squeue -h -o '%A' 2>/dev/null | sort -u >"$baseline_jobs" || true

case "$case_name" in
  preemption) run_preemption ;;
  fs-probes) run_fs_probes ;;
esac

case_ok=1
note "Local dev-cluster case '$case_name' passed"
