#!/usr/bin/env bash
# End-to-end smoke for the local single-node Slurm dev cluster.
#
# Boots the cluster (see dev-cluster/README.md) and runs hpc-compose's REAL
# `up` -> `sbatch` -> `slurmd` -> `sacct` path against it for every spec under
# dev-cluster/specs, asserting the parts unit tests can't reach: real sbatch
# submission, the job draining to the expected terminal state via sacct, expected
# log output, and that `status`/`ps`/`score` render where applicable. This
# directly exercises the cluster code paths the unit suite mocks out.
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
work_dir="$repo_root/.tmp/devcluster-e2e"
work_specs_dir="$work_dir/specs"

# Per-spec assertion:
#   "<spec file>|<success|failure>|<sacct state>|<exit code or nonzero>|<log substring>"
# The harness enumerates every spec under specs/ and fails loudly on any spec
# that has no entry here, so a new spec can never be silently skipped.
expectations=(
  "artifacts.yaml|success|COMPLETED|0:0|dev-cluster artifacts produced"
  "failing-service.yaml|failure|FAILED|nonzero|intentional dev-cluster failure"
  "hello.yaml|success|COMPLETED|0:0|hello from a real"
  "ignore-policy.yaml|success|COMPLETED|0:0|ignore-policy: ok service succeeded"
  "multi-service.yaml|success|COMPLETED|0:0|client: readiness gate + request succeeded"
  "pipeline-dag.yaml|success|COMPLETED|0:0|pipeline-dag: postprocess done"
  "restart-policy.yaml|success|COMPLETED|0:0|restart-policy: completed after transient failures"
)

# Prints the full expectation registered for a spec filename, or fails.
expectation_for() {
  local name="$1" entry
  for entry in "${expectations[@]}"; do
    if [[ "${entry%%|*}" == "$name" ]]; then
      printf '%s' "$entry"
      return 0
    fi
  done
  return 1
}

expectation_field() {
  local entry="$1" field="$2"
  local _spec_name outcome state exitcode log_substring
  IFS='|' read -r _spec_name outcome state exitcode log_substring <<< "$entry"
  case "$field" in
    outcome) printf '%s' "$outcome" ;;
    state) printf '%s' "$state" ;;
    exitcode) printf '%s' "$exitcode" ;;
    log) printf '%s' "$log_substring" ;;
    *) fail "unknown expectation field '$field'" ;;
  esac
}

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

# Job ids submitted with --detach by the dedicated blocks below. The EXIT trap
# scancels any still around so a mid-run failure can't leave the single node busy
# (a leaked `sleep 300` would otherwise time out every later run at the idle gate).
detached_jobids=()
# Job ids captured from the generic loop for post-loop deep-checks: artifacts
# (pull/artifacts) and hello (read-side reader pack).
artifacts_jobid=""
hello_jobid=""

# Submit SPEC ($1) with `up --detach`, assert a real sbatch submission, register
# the job for trap-cleanup, and set $DETACHED_JOBID to the parsed id. MUST run in
# the current shell (never `$(detach_submit ...)`): a subshell would make `fail`
# abort only the subshell and append to a throwaway copy of the cleanup array.
DETACHED_JOBID=""
detach_submit() {
  local rel="$1" out jobid
  out="$(mktemp)"
  if ! inctr hpc-compose up -f "$rel" --detach >"$out" 2>&1; then
    sed 's/^/    | /' "$out" >&2
    rm -f "$out"
    fail "up --detach failed for $rel"
  fi
  if ! grep -q 'Submitted batch job' "$out"; then
    sed 's/^/    | /' "$out" >&2
    rm -f "$out"
    fail "no sbatch submission for $rel"
  fi
  jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
  rm -f "$out"
  [[ -n "$jobid" ]] || fail "could not parse job id for $rel"
  detached_jobids+=("$jobid")
  DETACHED_JOBID="$jobid"
}

# Poll sacct until job $1's allocation State equals $2, up to $3 attempts (1s
# each, default 30). Reads -X (allocation row only) and takes the leading word so
# "CANCELLED by <uid>" matches "CANCELLED".
wait_for_sacct_state() {
  local jobid="$1" want="$2" attempts="${3:-30}" st
  for _ in $(seq 1 "$attempts"); do
    st="$(inctr sacct -j "$jobid" -n -P -X --format=State 2>/dev/null | head -n1 | awk '{print $1}')"
    [[ "$st" == "$want" ]] && return 0
    sleep 1
  done
  return 1
}

# Runs on EXIT (success or failure). The harness copies specs into an owned
# gitignored work dir before running them; on rootful Docker, job metadata in that
# mounted tree may be root-owned, so clean from inside the still-running
# container first, then fall back to a host-side rm. Any requested teardown
# happens last, after cleanup, so it fires on the failure path too.
finish() {
  if [[ ${#detached_jobids[@]} -gt 0 ]]; then
    for _job in "${detached_jobids[@]}"; do
      inctr scancel "$_job" >/dev/null 2>&1 || true
    done
  fi
  inctr rm -rf /workspace/.tmp/devcluster-e2e >/dev/null 2>&1 || true
  rm -rf "$work_dir" 2>/dev/null || true
  if [[ "${DEVCLUSTER_E2E_DOWN:-0}" == "1" ]]; then
    note "Tearing the cluster down"
    "$repo_root/scripts/devcluster.sh" down >/dev/null
  fi
}

# --- 1. boot ---------------------------------------------------------------
note "Booting the dev cluster"
"$repo_root/scripts/devcluster.sh" up >/dev/null
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
[[ "$idle" == 1 ]] || fail "node did not reach 'idle' (check: scripts/devcluster.sh logs)"
pass "node is idle"

# --- 2. run + assert each spec --------------------------------------------
inctr rm -rf /workspace/.tmp/devcluster-e2e >/dev/null 2>&1 || true
rm -rf "$work_dir" 2>/dev/null || true
mkdir -p "$work_specs_dir"

shopt -s nullglob
source_specs=("$specs_dir"/*.yaml)
shopt -u nullglob
[[ ${#source_specs[@]} -gt 0 ]] || fail "no specs found under $specs_dir"
cp "${source_specs[@]}" "$work_specs_dir"/

shopt -s nullglob
spec_paths=("$work_specs_dir"/*.yaml)
shopt -u nullglob

for spec_path in "${spec_paths[@]}"; do
  spec="$(basename "$spec_path")"
  expectation="$(expectation_for "$spec")" \
    || fail "no expected outcome registered for $spec (add it to the expectations array)"
  expect_outcome="$(expectation_field "$expectation" outcome)"
  expect_state="$(expectation_field "$expectation" state)"
  expect_exitcode="$(expectation_field "$expectation" exitcode)"
  expect_log="$(expectation_field "$expectation" log)"
  rel=".tmp/devcluster-e2e/specs/$spec"
  note "Spec $spec"

  out="$(mktemp)"
  up_status=0
  if inctr hpc-compose up -f "$rel" --watch-mode line >"$out" 2>&1; then
    up_status=0
  else
    up_status=$?
  fi
  if [[ "$expect_outcome" == "success" && "$up_status" == 0 ]]; then
    pass "up exited 0"
  elif [[ "$expect_outcome" == "failure" && "$up_status" != 0 ]]; then
    pass "up exited non-zero as expected ($up_status)"
  else
    sed 's/^/    | /' "$out" >&2
    fail "up exit status $up_status did not match expected outcome '$expect_outcome' for $spec"
  fi

  grep -q 'Submitted batch job' "$out" || { sed 's/^/    | /' "$out" >&2; fail "no sbatch submission for $spec"; }
  jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
  [[ -n "$jobid" ]] || fail "could not parse job id for $spec"
  pass "submitted via real sbatch (job $jobid)"
  # Capture ids the post-loop deep-checks reuse.
  case "$spec" in
    artifacts.yaml) artifacts_jobid="$jobid" ;;
    hello.yaml) hello_jobid="$jobid" ;;
  esac

  grep -q "final state: $expect_state" "$out" \
    || fail "watch did not report $expect_state for job $jobid"

  # Authoritative terminal state straight from accounting, not just the watcher.
  # `|| true`: awk's early `exit` can SIGPIPE sacct (pipefail), and a clear
  # empty-row failure below beats a bare set -e abort if the exec itself fails.
  row="$(inctr sacct -j "$jobid" -n -P --format=JobID,State,ExitCode 2>/dev/null \
    | awk -F'|' -v id="$jobid" '$1==id {print $2"|"$3; exit}')" || true
  [[ -n "$row" ]] || fail "sacct returned no row for job $jobid (is the cluster still up?)"
  state="${row%%|*}"
  exitcode="${row##*|}"
  [[ "$state" == "$expect_state" ]] || fail "sacct State=$state (expected $expect_state) for job $jobid"
  if [[ "$expect_exitcode" == "nonzero" ]]; then
    [[ "$exitcode" != "0:0" ]] || fail "sacct ExitCode=$exitcode (expected nonzero) for job $jobid"
  else
    [[ "$exitcode" == "$expect_exitcode" ]] || fail "sacct ExitCode=$exitcode (expected $expect_exitcode) for job $jobid"
  fi
  pass "sacct: $state, ExitCode $exitcode"

  logs="$(inctr hpc-compose logs -f "$rel" --job-id "$jobid" 2>&1 || true)"
  printf '%s' "$logs" | grep -qF "$expect_log" \
    || fail "expected log output not found for $spec: '$expect_log'"
  pass "logs contain expected output"

  status_out="$(inctr hpc-compose status -f "$rel" --job-id "$jobid" 2>&1)" \
    || fail "status command failed for job $jobid"
  printf '%s' "$status_out" | grep -q "$expect_state" \
    || fail "status did not report $expect_state for job $jobid"
  pass "status renders terminal state"

  ps_out="$(inctr hpc-compose ps -f "$rel" --job-id "$jobid" --format json 2>&1)" \
    || fail "ps command failed for job $jobid"
  printf '%s' "$ps_out" | grep -q '"services"' \
    || fail "ps JSON did not include services for job $jobid"
  pass "ps renders service runtime state"

  score_out=""
  if [[ "$expect_outcome" == "success" ]]; then
    score_ok=0
    for _ in $(seq 1 10); do
      if score_out="$(inctr hpc-compose score -f "$rel" "$jobid" 2>&1)" \
        && printf '%s' "$score_out" | grep -q 'EFFICIENCY SCORE'; then
        score_ok=1
        break
      fi
      sleep 1
    done
    if [[ "$score_ok" != 1 ]]; then
      printf '%s' "$score_out" | sed 's/^/    | /' >&2
      fail "score did not render an efficiency score for job $jobid"
    fi
    pass "score renders (sacct-backed)"
  fi

  # stats/score must not emit the sstat 'Invalid field requested: AllocTRES'
  # notice (AllocTRES is a sacct field, not an sstat one).
  stats_out="$(inctr hpc-compose stats -f "$rel" --job-id "$jobid" 2>&1 || true)"
  if printf '%s\n%s' "$stats_out" "$score_out" | grep -qiE 'AllocTRES|Invalid field requested'; then
    fail "stats/sstat emitted the AllocTRES field error for job $jobid"
  fi
  pass "stats renders without the sstat AllocTRES error"

  rm -f "$out"
done

# --- 3. artifacts deep-check (real teardown manifest + pull resolution) -----
# Reuses the artifacts.yaml job from the generic loop: once `up` reported
# COMPLETED, teardown collection has copied the declared paths into the tracked
# payload dir and written manifest.json. This is the only real-manifest exercise
# of pull/artifacts (which otherwise just print a placeholder hint).
note "Artifacts deep-check"
[[ -n "$artifacts_jobid" ]] || fail "artifacts.yaml did not run in the generic loop (no captured job id)"
arts_rel=".tmp/devcluster-e2e/specs/artifacts.yaml"
# The manifest write can lag the COMPLETED report slightly; retry a few times.
pull_json=""
pull_ok=0
for _ in $(seq 1 10); do
  if pull_json="$(inctr hpc-compose pull -f "$arts_rel" --job-id "$artifacts_jobid" --format json 2>&1)"; then
    pull_ok=1
    break
  fi
  sleep 1
done
if [[ "$pull_ok" != 1 ]]; then
  printf '%s\n' "$pull_json" | sed 's/^/    | /' >&2
  fail "pull never resolved the artifact manifest for job $artifacts_jobid"
fi
files_n="$(printf '%s' "$pull_json" | grep -oE '"files":[[:space:]]*[0-9]+' | grep -oE '[0-9]+' | head -n1)"
if [[ -z "$files_n" || "$files_n" -lt 1 ]]; then
  printf '%s\n' "$pull_json" | sed 's/^/    | /' >&2
  fail "pull reported '${files_n:-?}' collected files (expected > 0) for job $artifacts_jobid"
fi
printf '%s' "$pull_json" | grep -q 'rsync ' \
  || fail "pull JSON missing a suggested rsync command for job $artifacts_jobid"
pass "pull resolved a real manifest ($files_n files) and emitted an rsync line"
inctr hpc-compose artifacts -f "$arts_rel" --job-id "$artifacts_jobid" >/dev/null 2>&1 \
  || fail "artifacts export failed for job $artifacts_jobid"
pass "artifacts exported the declared bundles"

# --- 3b. read-side smoke pack + restart-hook (reuse generic-loop job ids) ----
# Cheap: drive the tracked-state readers against genuine teardown artifacts
# (final state, metrics, slurmd logs) rather than fixture state files.
note "Read-side smoke pack"
[[ -n "$hello_jobid" ]] || fail "hello.yaml did not run (no job id for the read-side pack)"
hello_rel=".tmp/devcluster-e2e/specs/hello.yaml"
inctr hpc-compose experiment show "$hello_jobid" -f "$hello_rel" >/dev/null 2>&1 \
  || fail "experiment show failed for job $hello_jobid"
inctr hpc-compose replay --job-id "$hello_jobid" -f "$hello_rel" --watch-mode line >/dev/null 2>&1 \
  || fail "replay failed for job $hello_jobid"
inctr hpc-compose debug --job-id "$hello_jobid" -f "$hello_rel" >/dev/null 2>&1 \
  || fail "debug failed for job $hello_jobid"
inctr hpc-compose checkpoints --job-id "$hello_jobid" -f "$hello_rel" >/dev/null 2>&1 \
  || fail "checkpoints failed for job $hello_jobid"
inctr hpc-compose jobs list 2>&1 | grep -qw "$hello_jobid" \
  || fail "jobs list did not include tracked job $hello_jobid"
inctr hpc-compose clean -f "$hello_rel" --all --dry-run >/dev/null 2>&1 \
  || fail "clean --all --dry-run failed"
pass "experiment/replay/debug/checkpoints/jobs/clean render over a real run"

# --- 4. dedicated blocks (specs the generic up/watch loop cannot drive) -----
# These live under specs/_extra/ (the generic loop only globs specs/*.yaml) and
# need bespoke flows: --detach + polling, multi-job orchestration, scancel.
note "Staging dedicated-block specs"
inctr rm -rf /workspace/.tmp/devcluster-e2e/specs/_extra >/dev/null 2>&1 || true
shopt -s nullglob
extra_specs=("$specs_dir"/_extra/*.yaml)
shopt -u nullglob
[[ ${#extra_specs[@]} -gt 0 ]] || fail "no specs found under $specs_dir/_extra"
mkdir -p "$work_specs_dir/_extra"
cp "${extra_specs[@]}" "$work_specs_dir/_extra"/
# Every _extra spec must be handled by a block below; fail loudly on a new one so
# it can never be silently skipped (mirrors the generic loop's spec registry).
handled_extra=(array.yaml long-running.yaml dep-producer.yaml dep-consumer.yaml resume.yaml)
for ex in "${extra_specs[@]}"; do
  exb="$(basename "$ex")"
  found=0
  for h in "${handled_extra[@]}"; do
    [[ "$h" == "$exb" ]] && { found=1; break; }
  done
  [[ "$found" == 1 ]] \
    || fail "no dedicated block handles _extra spec '$exb' (add one in scripts/devcluster_e2e.sh)"
done

# 4a. Array job: real --array fan-out + per-task accounting + status --array.
note "Array block"
array_rel=".tmp/devcluster-e2e/specs/_extra/array.yaml"
detach_submit "$array_rel"
array_base="$DETACHED_JOBID"
pass "array submitted via real sbatch (base job $array_base)"
array_ok=0
rows=""
for _ in $(seq 1 60); do
  rows="$(inctr sacct -j "$array_base" --array -n -X -P --format=JobIDRaw,State,ExitCode 2>/dev/null || true)"
  done_n="$(printf '%s\n' "$rows" | awk -F'|' '$2=="COMPLETED" && $3=="0:0" {c++} END {print c+0}')"
  if [[ "$done_n" -ge 4 ]]; then
    array_ok=1
    break
  fi
  sleep 2
done
if [[ "$array_ok" != 1 ]]; then
  printf '%s\n' "$rows" | sed 's/^/    | /' >&2
  fail "array: not all 4 task rows reached COMPLETED 0:0"
fi
pass "array: 4 task rows COMPLETED 0:0 via sacct --array"
status_out="$(inctr hpc-compose status -f "$array_rel" --job-id "$array_base" --array 2>&1)" \
  || fail "array: status --array failed"
# status renders an "Array tasks" section with the per-task rows merged from
# squeue/sacct --array and a counts line; tasks show as raw ids, not base_index.
if ! printf '%s' "$status_out" | grep -q 'Array tasks' \
  || ! printf '%s' "$status_out" | grep -q 'COMPLETED=4'; then
  printf '%s' "$status_out" | sed 's/^/    | /' >&2
  fail "array: status --array did not render the merged per-task section (COMPLETED=4)"
fi
pass "array: status --array renders the merged per-task section (COMPLETED=4)"

# 4b. Cancel a RUNNING job: real scancel -> CANCELLED (the 3rd terminal state).
note "Cancel block"
lr_rel=".tmp/devcluster-e2e/specs/_extra/long-running.yaml"
detach_submit "$lr_rel"
lr_jobid="$DETACHED_JOBID"
pass "long-running submitted (job $lr_jobid)"
running=0
for _ in $(seq 1 60); do
  if [[ "$(inctr squeue -j "$lr_jobid" -h -o '%T' 2>/dev/null | head -n1)" == "RUNNING" ]]; then
    running=1
    break
  fi
  sleep 1
done
[[ "$running" == 1 ]] || fail "cancel: job $lr_jobid never reached RUNNING"
pass "job $lr_jobid is RUNNING"
inctr hpc-compose cancel -f "$lr_rel" --job-id "$lr_jobid" --yes >/dev/null 2>&1 \
  || fail "cancel: command failed for job $lr_jobid"
wait_for_sacct_state "$lr_jobid" CANCELLED 30 \
  || fail "cancel: sacct did not report CANCELLED for job $lr_jobid"
pass "cancel: real scancel drove the job to CANCELLED"
# cancel also tears down tracked state: the record is no longer resolvable, so a
# follow-up status by that id must now fail (a path-free proxy for removal).
if inctr hpc-compose status -f "$lr_rel" --job-id "$lr_jobid" >/dev/null 2>&1; then
  fail "cancel: tracked record for job $lr_jobid still resolvable (state not torn down)"
fi
pass "cancel: tracked state torn down (status by id no longer resolves)"

# 4c. Scheduler inter-job dependency: afterok holds the consumer PENDING until
# the producer reaches a matching terminal state, proven by the squeue reason
# (best-effort window) and a consumer-start >= producer-end ordering check.
note "Scheduler dependency block"
prod_rel=".tmp/devcluster-e2e/specs/_extra/dep-producer.yaml"
cons_rel=".tmp/devcluster-e2e/specs/_extra/dep-consumer.yaml"
detach_submit "$prod_rel"
prod_jobid="$DETACHED_JOBID"
pass "producer submitted (job $prod_jobid)"
# Consumer's afterok dependency on the producer, injected via the process env.
cons_out="$(mktemp)"
if ! inctr env PRODUCER_JOB="$prod_jobid" hpc-compose up -f "$cons_rel" --detach >"$cons_out" 2>&1; then
  sed 's/^/    | /' "$cons_out" >&2
  rm -f "$cons_out"
  fail "consumer up --detach failed"
fi
cons_jobid="$(grep -oE 'Submitted batch job [0-9]+' "$cons_out" | head -n1 | grep -oE '[0-9]+')"
rm -f "$cons_out"
[[ -n "$cons_jobid" ]] || fail "dep: could not parse consumer job id"
detached_jobids+=("$cons_jobid")
pass "consumer submitted (job $cons_jobid, afterok:$prod_jobid)"
held=0
for _ in $(seq 1 10); do
  if inctr squeue -j "$cons_jobid" -h -o '%r' 2>/dev/null | grep -qi 'Dependency'; then
    held=1
    break
  fi
  [[ "$(inctr sacct -j "$cons_jobid" -n -P -X --format=State 2>/dev/null | head -n1 | awk '{print $1}')" == "COMPLETED" ]] && break
  sleep 1
done
if [[ "$held" == 1 ]]; then
  pass "dep: consumer held PENDING with reason Dependency"
else
  note "  (did not catch the Dependency hold window; relying on the ordering check)"
fi
wait_for_sacct_state "$prod_jobid" COMPLETED 60 || fail "dep: producer did not COMPLETE"
wait_for_sacct_state "$cons_jobid" COMPLETED 60 || fail "dep: consumer did not COMPLETE"
prod_end="$(inctr sacct -j "$prod_jobid" -n -P -X --format=End 2>/dev/null | head -n1)"
cons_start="$(inctr sacct -j "$cons_jobid" -n -P -X --format=Start 2>/dev/null | head -n1)"
if [[ -n "$prod_end" && -n "$cons_start" && "$prod_end" != "Unknown" && "$cons_start" != "Unknown" ]]; then
  if [[ "$cons_start" < "$prod_end" ]]; then
    fail "dep: consumer started ($cons_start) before producer ended ($prod_end); afterok not enforced"
  fi
  pass "dep: consumer start ($cons_start) >= producer end ($prod_end); afterok ordering held"
else
  note "  (sacct Start/End unavailable; ordering check skipped)"
fi

# 4d. Resume host-dir: with resume enabled, $HPC_COMPOSE_RESUME_DIR under the
# host backend must be the real on-node path, not the unmounted container mount
# /hpc-compose/resume. The service asserts the dir is writable, so COMPLETED
# already proves it. --allow-resume-changes + a cleared dir keep it idempotent
# across harness re-runs (resume drift detection otherwise blocks a re-run).
note "Resume host-dir block"
resume_rel=".tmp/devcluster-e2e/specs/_extra/resume.yaml"
inctr rm -rf /var/cache/hpc-compose/resume >/dev/null 2>&1 || true
out="$(mktemp)"
if ! inctr hpc-compose up -f "$resume_rel" --allow-resume-changes --watch-mode line >"$out" 2>&1; then
  sed 's/^/    | /' "$out" >&2
  rm -f "$out"
  fail "resume up failed"
fi
grep -q 'final state: COMPLETED' "$out" \
  || { sed 's/^/    | /' "$out" >&2; fail "resume did not reach COMPLETED"; }
resume_jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
rm -f "$out"
resume_logs="$(inctr hpc-compose logs -f "$resume_rel" --job-id "$resume_jobid" 2>&1 || true)"
printf '%s' "$resume_logs" | grep -qF 'wrote and read back checkpoint under the resume dir' \
  || fail "resume service did not confirm a writable host resume dir for job $resume_jobid"
if printf '%s' "$resume_logs" | grep -q 'HPC_COMPOSE_RESUME_DIR=/hpc-compose/resume'; then
  fail "resume dir regressed to the unmounted container path under host backend"
fi
pass "resume: host resume dir is real + writable (not the /hpc-compose/resume mount)"

# 4e. Interactive allocation + run reuse: alloc opens a real salloc and exports
# HPC_COMPOSE_ALLOCATION; run service-mode reuses that allocation via srun rather
# than a fresh sbatch (unreachable by the unit fakes). Flags MUST precede the
# service positional, or trailing flags get swallowed into the command argv.
note "Alloc + run reuse block"
# The inner $HPC_COMPOSE_ALLOCATION must expand in the allocation, not here.
# shellcheck disable=SC2016
alloc_out="$(inctr hpc-compose alloc -f "$hello_rel" --skip-prepare --no-preflight -- /bin/sh -c 'echo "alloc-env=$HPC_COMPOSE_ALLOCATION"' 2>&1)" \
  || { printf '%s\n' "$alloc_out" | sed 's/^/    | /' >&2; fail "alloc failed"; }
printf '%s' "$alloc_out" | grep -qE 'alloc-env=[0-9]+' \
  || { printf '%s\n' "$alloc_out" | sed 's/^/    | /' >&2; fail "alloc did not export HPC_COMPOSE_ALLOCATION"; }
pass "alloc granted + exported HPC_COMPOSE_ALLOCATION"
run_out="$(inctr hpc-compose run --skip-prepare --no-preflight -f "$hello_rel" app -- /bin/sh -c 'echo run-service-ok' 2>&1)" \
  || { printf '%s\n' "$run_out" | sed 's/^/    | /' >&2; fail "run service-mode failed"; }
printf '%s' "$run_out" | grep -qF 'run-service-ok' \
  || { printf '%s\n' "$run_out" | sed 's/^/    | /' >&2; fail "run service-mode produced no output"; }
pass "run service-mode executed a fresh srun job"
reuse_out="$(inctr hpc-compose alloc -f "$hello_rel" --skip-prepare --no-preflight -- bash -lc "hpc-compose run --skip-prepare --no-preflight -f $hello_rel app -- echo reuse-ok" 2>&1)" \
  || { printf '%s\n' "$reuse_out" | sed 's/^/    | /' >&2; fail "alloc+run reuse failed"; }
printf '%s' "$reuse_out" | grep -qF 'using active Slurm allocation' \
  || { printf '%s\n' "$reuse_out" | sed 's/^/    | /' >&2; fail "run did not reuse the active allocation"; }
pass "run reuses the active allocation via srun (not a fresh sbatch)"

note "All dev-cluster end-to-end checks passed"
# Artifact cleanup and any requested teardown run in the EXIT trap (`finish`).
