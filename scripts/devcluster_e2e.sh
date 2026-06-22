#!/usr/bin/env bash
# End-to-end smoke for the local single-node Slurm dev cluster.
#
# Boots the cluster (see dev-cluster/README.md) and runs hpc-compose's REAL
# `up` -> `sbatch` -> `slurmd` -> `sacct` path against it for every spec under
# dev-cluster/specs, asserting the parts unit tests can't reach: real sbatch
# submission, the job draining to COMPLETED via sacct with exit 0, expected log
# output, and that `status`/`score` render. This directly exercises the cluster
# code paths the unit suite mocks out.
#
#   scripts/devcluster_e2e.sh            boot (build if needed), run, assert
#   DEVCLUSTER_SKIP_BUILD=1 ...          reuse an existing image (CI prebuilds it)
#   DEVCLUSTER_E2E_DOWN=1 ...            tear the cluster down when finished
#
# NOT covered here (revalidate on a real cluster): the container-runtime layer
# (pyxis/enroot, apptainer) and GPU execution. The dev cluster runs services
# with `runtime.backend: host`, so those paths are out of scope by construction.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
specs_dir="$repo_root/dev-cluster/specs"

# Each entry: "<spec file>::<substring the run output/logs must contain>".
specs=(
  "hello.yaml::hello from a real"
  "multi-service.yaml::client: readiness gate + request succeeded"
)

red='\033[31m'; green='\033[32m'; bold='\033[1m'; reset='\033[0m'
note() { printf '%b==>%b %s\n' "$bold" "$reset" "$*"; }
pass() { printf '  %bok%b   %s\n' "$green" "$reset" "$*"; }
fail() { printf '  %bFAIL%b %s\n' "$red" "$reset" "$*" >&2; exit 1; }

# Pick the same engine the lifecycle wrapper would.
if docker compose version >/dev/null 2>&1; then
  engine=docker
elif podman compose version >/dev/null 2>&1; then
  engine=podman
else
  fail "need 'docker compose' or 'podman compose' on PATH (is the engine running?)"
fi

inctr() { "$engine" exec "$container" "$@"; }

cleanup_run_artifacts() {
  # The in-container run writes gitignored tracking dirs into the mounted spec
  # tree; drop them so repeat runs start clean.
  rm -rf "$specs_dir/.hpc-compose" "$specs_dir/hpc-compose.sbatch" "$repo_root/.hpc-compose" 2>/dev/null || true
}

# --- 1. boot ---------------------------------------------------------------
note "Booting the dev cluster"
"$repo_root/scripts/devcluster.sh" up >/dev/null
trap cleanup_run_artifacts EXIT

note "Waiting for the node to register idle"
idle=0
for _ in $(seq 1 90); do
  if inctr sinfo -h -o '%T' 2>/dev/null | grep -q '^idle'; then
    idle=1
    break
  fi
  sleep 1
done
[[ "$idle" == 1 ]] || fail "node did not reach 'idle' (check: scripts/devcluster.sh logs)"
pass "node is idle"

# --- 2. run + assert each spec --------------------------------------------
for entry in "${specs[@]}"; do
  spec="${entry%%::*}"
  expect_log="${entry##*::}"
  rel="dev-cluster/specs/$spec"
  note "Spec $spec"

  out="$(mktemp)"
  if inctr hpc-compose up -f "$rel" --watch-mode line >"$out" 2>&1; then
    pass "up exited 0"
  else
    sed 's/^/    | /' "$out" >&2
    fail "up exited non-zero for $spec"
  fi

  grep -q 'Submitted batch job' "$out" || { sed 's/^/    | /' "$out" >&2; fail "no sbatch submission for $spec"; }
  jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
  [[ -n "$jobid" ]] || fail "could not parse job id for $spec"
  pass "submitted via real sbatch (job $jobid)"

  grep -q 'final state: COMPLETED' "$out" || fail "watch did not report COMPLETED for job $jobid"

  # Authoritative terminal state straight from accounting, not just the watcher.
  row="$(inctr sacct -j "$jobid" -n -P --format=JobID,State,ExitCode 2>/dev/null \
    | awk -F'|' -v id="$jobid" '$1==id {print $2"|"$3; exit}')"
  state="${row%%|*}"
  exitcode="${row##*|}"
  [[ "$state" == "COMPLETED" ]] || fail "sacct State=$state (expected COMPLETED) for job $jobid"
  [[ "$exitcode" == "0:0" ]] || fail "sacct ExitCode=$exitcode (expected 0:0) for job $jobid"
  pass "sacct: COMPLETED, ExitCode 0:0"

  logs="$(inctr hpc-compose logs -f "$rel" --job-id "$jobid" 2>&1 || true)"
  printf '%s' "$logs" | grep -qF "$expect_log" \
    || fail "expected log output not found for $spec: '$expect_log'"
  pass "logs contain expected output"

  status_out="$(inctr hpc-compose status -f "$rel" --job-id "$jobid" 2>&1)" \
    || fail "status command failed for job $jobid"
  printf '%s' "$status_out" | grep -q 'COMPLETED' \
    || fail "status did not report COMPLETED for job $jobid"
  pass "status renders terminal state"

  score_out="$(inctr hpc-compose score "$jobid" 2>&1)" \
    || fail "score command failed for job $jobid"
  printf '%s' "$score_out" | grep -q 'EFFICIENCY SCORE' \
    || fail "score did not render an efficiency score for job $jobid"
  pass "score renders (sacct-backed)"

  rm -f "$out"
done

note "All dev-cluster end-to-end checks passed"

if [[ "${DEVCLUSTER_E2E_DOWN:-0}" == "1" ]]; then
  note "Tearing the cluster down"
  "$repo_root/scripts/devcluster.sh" down >/dev/null
fi
