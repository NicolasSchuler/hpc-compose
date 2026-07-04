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

sacct_job_count() {
  inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' '
}

# Accounting rows from just-finished jobs (e.g. the alloc/run block's sruns) can
# commit to slurmdbd many seconds after the client returns, so require a longer
# stable window: 3 consecutive identical readings over up to 30s. A short window
# turns a late-landing stale row into a false "when submitted a job" failure.
wait_for_sacct_count_stable() {
  local attempts=30 previous="" current="" stable=0
  for _ in $(seq 1 "$attempts"); do
    current="$(sacct_job_count)"
    if [[ "$current" == "$previous" ]]; then
      stable=$((stable + 1))
      if (( stable >= 3 )); then
        printf '%s' "$current"
        return 0
      fi
    else
      previous="$current"
      stable=0
    fi
    sleep 1
  done
  printf '%s' "$current"
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

  # Beyond the no-AllocTRES-error check: a completed run must render real content
  # in `stats --format json` -- the tracked job id and the authoritative scheduler
  # terminal state -- not just an empty snapshot shell.
  stats_json="$(inctr hpc-compose stats -f "$rel" --job-id "$jobid" --format json 2>&1 || true)"
  printf '%s' "$stats_json" | grep -q "\"job_id\": \"$jobid\"" \
    || { printf '%s' "$stats_json" | sed 's/^/    | /' >&2; fail "stats --format json missing job_id $jobid"; }
  printf '%s' "$stats_json" | grep -q "\"state\": \"$expect_state\"" \
    || { printf '%s' "$stats_json" | sed 's/^/    | /' >&2; fail "stats --format json did not report scheduler state $expect_state for job $jobid"; }
  pass "stats --format json reports real content (job_id + scheduler state $expect_state)"

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

# --- 3b'. metrics-probe: exercise the real perf_event_open / NVML probe ------
# The unit suite only drives mocks; this runs the genuine SYS_perf_event_open
# syscall and NVML dlopen on the real kernel, so an ABI/attr-size regression in
# the highest-risk unsafe code is caught here (degraded-but-not-crashed is fine).
note "metrics-probe (real capability probe)"
probe_json="$(inctr hpc-compose metrics-probe --duration-seconds 1 --format json 2>&1)" \
  || fail "metrics-probe exited non-zero: $probe_json"
printf '%s' "$probe_json" | grep -q '"perf_event_open"' \
  || fail "metrics-probe JSON missing the perf_event_open capability block"
printf '%s' "$probe_json" | grep -q '"schema_version"' \
  || fail "metrics-probe JSON missing schema_version"
pass "metrics-probe runs the real capability probe and emits structured JSON"

# --- 3c. dry-run: render the real sbatch, submit nothing --------------------
# The dev cluster is the safe place to preview a run: `up --dry-run` renders the
# exact sbatch it would submit and stops -- nothing reaches the scheduler. Prove
# that against the LIVE controller: accounting and the queue must be unchanged.
note "Dry-run block (renders the sbatch, submits nothing)"
dry_rel=".tmp/devcluster-e2e/specs/hello.yaml"
rendered="/workspace/.tmp/devcluster-e2e/specs/hpc-compose.sbatch"
inctr rm -f "$rendered" >/dev/null 2>&1 || true
acct_before="$(inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' ')"
queue_before="$(inctr squeue -h 2>/dev/null | wc -l | tr -d ' ')"
dry_out="$(mktemp)"
if ! inctr hpc-compose up -f "$dry_rel" --dry-run >"$dry_out" 2>&1; then
  sed 's/^/    | /' "$dry_out" >&2; rm -f "$dry_out"; fail "up --dry-run exited nonzero"
fi
grep -q 'skipping sbatch submission' "$dry_out" \
  || { sed 's/^/    | /' "$dry_out" >&2; rm -f "$dry_out"; fail "dry-run did not report skipping submission"; }
if grep -q 'Submitted batch job' "$dry_out"; then
  sed 's/^/    | /' "$dry_out" >&2; rm -f "$dry_out"; fail "dry-run reported an sbatch submission"
fi
rm -f "$dry_out"
acct_after="$(inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' ')"
queue_after="$(inctr squeue -h 2>/dev/null | wc -l | tr -d ' ')"
# sacct rows are monotonic (a real submit always adds one), so this is the
# authoritative "submitted nothing" check. squeue can only shrink as a prior
# job drains, so assert it never GREW (a dry-run submission would grow it).
[[ "$acct_after" == "$acct_before" ]] \
  || fail "dry-run changed the accounting job count ($acct_before -> $acct_after)"
[[ "$queue_after" -le "$queue_before" ]] \
  || fail "dry-run enqueued a job ($queue_before -> $queue_after)"
pass "dry-run submitted nothing (sacct=$acct_after, squeue=$queue_after unchanged)"
# The rendered script is a real, submittable sbatch -- a faithful preview.
inctr test -f "$rendered" || fail "dry-run did not render a submission script at $rendered"
inctr grep -q '^#SBATCH ' "$rendered" || fail "rendered dry-run script has no #SBATCH directives"
inctr grep -q 'dev-cluster-hello' "$rendered" || fail "rendered dry-run script is not the hello spec"
pass "dry-run rendered a valid sbatch for the spec"
# JSON contract: a machine reader sees submitted=false, job_id=null, dry_run=true.
dry_json="$(inctr hpc-compose up -f "$dry_rel" --dry-run --format json 2>&1)" \
  || fail "dry-run --format json failed"
printf '%s' "$dry_json" | grep -q '"submitted": false' \
  || { printf '%s' "$dry_json" | sed 's/^/    | /' >&2; fail "dry-run JSON missing submitted=false"; }
printf '%s' "$dry_json" | grep -q '"job_id": null' \
  || fail "dry-run JSON missing job_id=null"
pass "dry-run JSON reports submitted=false, job_id=null"
inctr rm -f "$rendered" >/dev/null 2>&1 || true

# --- 3d. read-side affordances over the live scheduler (weather, diff) -------
# Cheap reads that the unit suite can only fake: weather aggregates live
# sinfo/squeue (sshare/sprio degrade to null on this build), and diff compares two
# real tracked runs.
note "Read-side affordance pack (weather, diff)"
weather_text="$(inctr hpc-compose weather 2>&1)" || fail "weather failed"
printf '%s' "$weather_text" | grep -q 'CLUSTER WEATHER' \
  || { printf '%s' "$weather_text" | sed 's/^/    | /' >&2; fail "weather text missing the header"; }
printf '%s' "$weather_text" | grep -qE 'Nodes: [0-9]+/[0-9]+ free' \
  || fail "weather text missing the node summary"
weather_json="$(inctr hpc-compose weather --format json 2>&1)" || fail "weather --format json failed"
printf '%s' "$weather_json" | grep -q '"condition"' || fail "weather json missing condition"
printf '%s' "$weather_json" | grep -q '"total_nodes": 1' || fail "weather json missing the node count"
pass "weather renders live node/queue signals (text + json)"

# diff needs two real runs of the same spec: reuse the generic-loop hello job and
# submit one more, then assert a pairwise render and an N-way matrix.
[[ -n "$hello_jobid" ]] || fail "diff: no hello job id captured from the generic loop"
diff_out="$(mktemp)"
if ! inctr hpc-compose up -f "$hello_rel" --watch-mode line >"$diff_out" 2>&1; then
  sed 's/^/    | /' "$diff_out" >&2; rm -f "$diff_out"; fail "diff: second hello run failed"
fi
hello_jobid2="$(grep -oE 'Submitted batch job [0-9]+' "$diff_out" | head -n1 | grep -oE '[0-9]+')"
rm -f "$diff_out"
[[ -n "$hello_jobid2" ]] || fail "diff: could not parse the second hello job id"
diff_text="$(inctr hpc-compose diff "$hello_jobid" "$hello_jobid2" -f "$hello_rel" 2>&1)" \
  || fail "diff (pairwise) failed for $hello_jobid -> $hello_jobid2"
printf '%s' "$diff_text" | grep -q "$hello_jobid -> $hello_jobid2" \
  || { printf '%s' "$diff_text" | sed 's/^/    | /' >&2; fail "diff missing the pairwise header"; }
printf '%s' "$diff_text" | grep -qE '^(Outcome|Resources|Config):' \
  || fail "diff did not render the comparison sections"
pass "diff renders a pairwise comparison of two real runs ($hello_jobid -> $hello_jobid2)"
diff_json="$(inctr hpc-compose diff --jobs "$hello_jobid,$hello_jobid2" --matrix-format json -f "$hello_rel" 2>&1)" \
  || fail "diff (N-way matrix json) failed"
printf '%s' "$diff_json" | grep -q '"runs"' || fail "diff matrix json missing the runs[] array"
pass "diff renders an N-way matrix (json)"

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
handled_extra=(array.yaml long-running.yaml dep-producer.yaml dep-consumer.yaml resume.yaml when.yaml watch-tui.yaml sweep.yaml test-pass.yaml test-fail.yaml germinate.yaml down.yaml)
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

# 4f. when: evaluate live scheduler conditions WITHOUT submitting. An impossible
# --free-nodes on a single-node cluster means `when` checks once (--timeout 0s),
# declines, exits nonzero, and submits nothing.
note "When block (conditions unmet -> no submission)"
when_rel=".tmp/devcluster-e2e/specs/_extra/when.yaml"
when_acct_before="$(wait_for_sacct_count_stable)"
when_out="$(mktemp)"
when_status=0
inctr hpc-compose when -f "$when_rel" --partition compose --free-nodes 9999 --timeout 0s \
  --skip-prepare --no-preflight >"$when_out" 2>&1 || when_status=$?
if [[ "$when_status" == 0 ]]; then
  sed 's/^/    | /' "$when_out" >&2; rm -f "$when_out"
  fail "when unexpectedly succeeded; it should decline the unmet condition"
fi
grep -qi 'conditions were not satisfied' "$when_out" \
  || { sed 's/^/    | /' "$when_out" >&2; rm -f "$when_out"; fail "when did not report the unmet condition"; }
if grep -q 'Submitted batch job' "$when_out"; then
  rm -f "$when_out"; fail "when submitted a job despite the unmet condition"
fi
rm -f "$when_out"
when_acct_after="$(wait_for_sacct_count_stable)"
[[ "$when_acct_after" == "$when_acct_before" ]] \
  || fail "when changed the accounting job count ($when_acct_before -> $when_acct_after)"
pass "when evaluated live conditions, declined, and submitted nothing"

# 4g. watch TUI: drive the interactive crossterm UI under a pseudo-terminal
# (pty-run.py) against a live job. The job succeeds, so the TUI auto-exits
# (hold-on-exit defaults to failure); assert it ENTERED (1049h) and RESTORED
# (1049l) the alternate screen -- i.e. it did not leave the terminal dirty.
note "Watch TUI block (pty-driven alternate-screen UI)"
wt_rel=".tmp/devcluster-e2e/specs/_extra/watch-tui.yaml"
detach_submit "$wt_rel"
wt_jobid="$DETACHED_JOBID"
pass "watch-tui submitted (job $wt_jobid)"
wt_running=0
for _ in $(seq 1 30); do
  if [[ "$(inctr squeue -j "$wt_jobid" -h -o '%T' 2>/dev/null | head -n1)" == "RUNNING" ]]; then
    wt_running=1; break
  fi
  sleep 1
done
[[ "$wt_running" == 1 ]] || fail "watch-tui: job $wt_jobid never reached RUNNING"
wt_cap="/workspace/.tmp/devcluster-e2e/watch-tui.cap"
wt_status=0
inctr python3 /workspace/dev-cluster/pty-run.py --timeout 60 --out "$wt_cap" -- \
  hpc-compose watch --job-id "$wt_jobid" -f "$wt_rel" --watch-mode tui || wt_status=$?
[[ "$wt_status" == 0 ]] || fail "watch TUI did not exit cleanly under a pty (exit $wt_status)"
inctr grep -q '1049h' "$wt_cap" || fail "watch TUI never entered the alternate screen"
inctr grep -q '1049l' "$wt_cap" \
  || fail "watch TUI did not restore the alternate screen (terminal left dirty)"
inctr grep -q 'COMPLETED' "$wt_cap" || fail "watch TUI did not render the terminal job state"
inctr rm -f "$wt_cap" >/dev/null 2>&1 || true
pass "watch TUI entered + restored the alternate screen and tracked to COMPLETED"

# 4h. Sweep: expand one embedded `sweep` block into N independent tracked Slurm
# jobs. The generic loop cannot drive this -- `sweep` owns its submit/status/
# results verbs and a manifest tying the trials together. Submit both trials,
# register every trial job id for teardown, poll `sweep status` until both are
# terminal, then assert `sweep results` lists both and sacct agrees per trial.
note "Sweep block (embedded sweep -> N independent trials)"
sweep_rel=".tmp/devcluster-e2e/specs/_extra/sweep.yaml"
sweep_out="$(mktemp)"
if ! inctr hpc-compose sweep submit -f "$sweep_rel" --skip-prepare --no-preflight >"$sweep_out" 2>&1; then
  sed 's/^/    | /' "$sweep_out" >&2; rm -f "$sweep_out"; fail "sweep submit failed"
fi
# Register every trial's real sbatch job id for trap-cleanup ("submitted t000 job
# <id> (...)" per trial). MUST land in the current shell, not a subshell, so the
# cleanup array survives.
sweep_jobids=()
while read -r _sj; do
  [[ -n "$_sj" ]] && { sweep_jobids+=("$_sj"); detached_jobids+=("$_sj"); }
done < <(grep -oE 'submitted t[0-9r]+ job [0-9]+' "$sweep_out" | grep -oE '[0-9]+$')
rm -f "$sweep_out"
[[ ${#sweep_jobids[@]} -eq 2 ]] \
  || fail "sweep did not submit 2 trials via real sbatch (parsed ${#sweep_jobids[@]})"
pass "sweep submitted 2 trials via real sbatch (${sweep_jobids[*]})"
sweep_ok=0
sweep_status_json=""
for _ in $(seq 1 60); do
  sweep_status_json="$(inctr hpc-compose sweep status -f "$sweep_rel" --format json 2>/dev/null || true)"
  # `|| true`: grep exits 1 while no trial is COMPLETED yet, and pipefail would
  # otherwise fail the assignment and abort the whole harness via set -e.
  term_n="$(printf '%s' "$sweep_status_json" | grep -c '"scheduler_state": "COMPLETED"' || true)"
  if [[ "$term_n" -ge 2 ]]; then sweep_ok=1; break; fi
  sleep 2
done
if [[ "$sweep_ok" != 1 ]]; then
  printf '%s\n' "$sweep_status_json" | sed 's/^/    | /' >&2
  fail "sweep: both trials did not reach COMPLETED via sweep status"
fi
pass "sweep status reports both trials COMPLETED"
sweep_results_json="$(inctr hpc-compose sweep results -f "$sweep_rel" --format json 2>&1)" \
  || { printf '%s\n' "$sweep_results_json" | sed 's/^/    | /' >&2; fail "sweep results failed"; }
results_completed="$(printf '%s' "$sweep_results_json" | grep -c '"scheduler_state": "COMPLETED"' || true)"
[[ "$results_completed" -ge 2 ]] \
  || { printf '%s\n' "$sweep_results_json" | sed 's/^/    | /' >&2; fail "sweep results did not tabulate 2 COMPLETED trials"; }
for _t in t000 t001; do
  printf '%s' "$sweep_results_json" | grep -q "\"trial_id\": \"$_t\"" \
    || { printf '%s\n' "$sweep_results_json" | sed 's/^/    | /' >&2; fail "sweep results missing trial $_t"; }
done
pass "sweep results tabulated both trials (t000,t001) as COMPLETED"
# sacct is authoritative: every trial job id must be COMPLETED 0:0.
for _j in "${sweep_jobids[@]}"; do
  wait_for_sacct_state "$_j" COMPLETED 30 || fail "sweep: sacct did not report COMPLETED for trial job $_j"
  _row="$(inctr sacct -j "$_j" -n -P --format=State,ExitCode 2>/dev/null | head -n1)"
  printf '%s' "$_row" | grep -q '0:0' \
    || fail "sweep: trial job $_j ExitCode not 0:0 (sacct row: $_row)"
done
pass "sweep: sacct confirms every trial job COMPLETED 0:0"

# 4i. Test (smoke command): `test --submit` runs a short Slurm job and passes only
# when every service launched and completed successfully. Prove both verdicts: a
# healthy spec passes (exit 0, "smoke test passed"), a broken spec fails (nonzero
# exit, "smoke test failed"). --local is unavailable here (it needs the Linux
# Pyxis/Enroot supervisor; the dev cluster is host-backend only), so this uses the
# real-sbatch --submit path.
note "Test (smoke command) block"
tp_rel=".tmp/devcluster-e2e/specs/_extra/test-pass.yaml"
tf_rel=".tmp/devcluster-e2e/specs/_extra/test-fail.yaml"
tp_out="$(mktemp)"
if ! inctr hpc-compose test --submit -f "$tp_rel" --skip-prepare --no-preflight >"$tp_out" 2>&1; then
  sed 's/^/    | /' "$tp_out" >&2; rm -f "$tp_out"; fail "test --submit (passing spec) exited nonzero"
fi
grep -q 'smoke test passed' "$tp_out" \
  || { sed 's/^/    | /' "$tp_out" >&2; rm -f "$tp_out"; fail "test did not report a passing smoke test"; }
# `test` consumes the sbatch stdout internally and reports the id in its own
# verdict line ("smoke test passed: <id>"). `|| true` keeps a miss from tripping
# set -e; the conditional sacct check below only runs when an id was parsed.
tp_jobid="$(grep -oE 'smoke test passed: [0-9]+' "$tp_out" | head -n1 | grep -oE '[0-9]+' || true)"
rm -f "$tp_out"
pass "test --submit passed a healthy smoke spec (smoke test passed)"
if [[ -n "$tp_jobid" ]]; then
  wait_for_sacct_state "$tp_jobid" COMPLETED 30 \
    || fail "test: sacct did not report COMPLETED for the passing smoke job $tp_jobid"
  pass "test: sacct confirms the passing smoke job COMPLETED (job $tp_jobid)"
fi
tf_out="$(mktemp)"
tf_status=0
inctr hpc-compose test --submit -f "$tf_rel" --skip-prepare --no-preflight >"$tf_out" 2>&1 || tf_status=$?
[[ "$tf_status" != 0 ]] \
  || { sed 's/^/    | /' "$tf_out" >&2; rm -f "$tf_out"; fail "test --submit (failing spec) unexpectedly exited 0"; }
grep -q 'smoke test failed' "$tf_out" \
  || { sed 's/^/    | /' "$tf_out" >&2; rm -f "$tf_out"; fail "test did not report a failing smoke test"; }
rm -f "$tf_out"
pass "test --submit failed a broken smoke spec (nonzero exit + smoke test failed)"

# 4j. Germinate: render a minimized canary of the plan, submit it as a real
# sbatch, wait for terminal, and build a rightsize report + suggested YAML patch
# from sacct accounting. germinate blocks until the canary is terminal, so it is
# not detached (no teardown registration needed). The unit suite only drives the
# canary planner against fakes; this runs the real submit -> terminal -> sacct
# rightsize path.
note "Germinate (canary rightsize) block"
germ_rel=".tmp/devcluster-e2e/specs/_extra/germinate.yaml"
germ_out="$(mktemp)"
if ! inctr hpc-compose germinate -f "$germ_rel" --skip-prepare --no-preflight --canary-time 00:02:00 >"$germ_out" 2>&1; then
  sed 's/^/    | /' "$germ_out" >&2; rm -f "$germ_out"; fail "germinate exited nonzero"
fi
# germinate does not echo the raw "Submitted batch job" line (it consumes the
# sbatch stdout internally); its "canary job: <id>" line is the submission proof,
# and the sacct check below confirms the id is a real accounting row.
germ_jobid="$(grep -oE 'canary job: [0-9]+' "$germ_out" | head -n1 | grep -oE '[0-9]+' || true)"
[[ -n "$germ_jobid" ]] \
  || { sed 's/^/    | /' "$germ_out" >&2; rm -f "$germ_out"; fail "germinate did not report a canary job id"; }
grep -q 'suggested YAML patch' "$germ_out" \
  || { sed 's/^/    | /' "$germ_out" >&2; rm -f "$germ_out"; fail "germinate did not render a rightsize report/patch section"; }
rm -f "$germ_out"
pass "germinate submitted a real canary (job $germ_jobid) and rendered a rightsize report"
wait_for_sacct_state "$germ_jobid" COMPLETED 30 \
  || fail "germinate: sacct did not report COMPLETED for canary job $germ_jobid"
pass "germinate: sacct confirms the canary COMPLETED (job $germ_jobid)"

# 4k. Down: the tracked-teardown path. Up a long job, `down --job-id --yes`, and
# assert the real scancel drove sacct to CANCELLED AND the tracked runtime state
# was reaped (status by id no longer resolves) -- the lifecycle path `up`/`cancel`
# alone never exercises via `down`.
note "Down block (up a job, down it, assert reaped)"
down_rel=".tmp/devcluster-e2e/specs/_extra/down.yaml"
detach_submit "$down_rel"
down_jobid="$DETACHED_JOBID"
pass "down target submitted (job $down_jobid)"
down_running=0
for _ in $(seq 1 60); do
  if [[ "$(inctr squeue -j "$down_jobid" -h -o '%T' 2>/dev/null | head -n1)" == "RUNNING" ]]; then
    down_running=1; break
  fi
  sleep 1
done
[[ "$down_running" == 1 ]] || fail "down: job $down_jobid never reached RUNNING"
pass "down: job $down_jobid is RUNNING"
inctr hpc-compose down -f "$down_rel" --job-id "$down_jobid" --yes >/dev/null 2>&1 \
  || fail "down: command failed for job $down_jobid"
wait_for_sacct_state "$down_jobid" CANCELLED 30 \
  || fail "down: sacct did not report CANCELLED for job $down_jobid"
pass "down: real scancel drove the job to CANCELLED"
# down also reaps tracked state: a follow-up status by that id must now fail.
if inctr hpc-compose status -f "$down_rel" --job-id "$down_jobid" >/dev/null 2>&1; then
  fail "down: tracked record for job $down_jobid still resolvable (state not reaped)"
fi
pass "down: tracked state reaped (status by id no longer resolves)"

note "All dev-cluster end-to-end checks passed"
# Artifact cleanup and any requested teardown run in the EXIT trap (`finish`).
