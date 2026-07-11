#!/usr/bin/env bash
# End-to-end smoke for the THIN REMOTE-SUBMIT path: host -> ssh/rsync -> the dev
# cluster acting as a login-node stand-in. This drives `hpc-compose up --remote`
# from THIS host (the "laptop" side) and asserts that the project is staged over
# rsync and a real `sbatch` submission on the remote node tracks to COMPLETED.
#
# It complements scripts/devcluster_e2e.sh, which drives hpc-compose INSIDE the
# container (UC1, running directly on the login node). This script exercises UC2
# (running on macOS/elsewhere and submitting to the cluster over SSH).
#
#   scripts/devcluster_remote_e2e.sh        boot (build if needed), run, assert
#   DEVCLUSTER_SKIP_BUILD=1 ...             reuse an existing image (CI prebuilds)
#   DEVCLUSTER_E2E_DOWN=1 ...               tear the cluster down when finished
#   HPC_COMPOSE_BIN=/path/to/hpc-compose    use a specific host binary
#
# Binary resolution: $HPC_COMPOSE_BIN, then target/release, then target/debug,
# then a copy pulled out of the image (Linux CI runners can run that directly;
# macOS dev machines fall back to the locally built target/ binary).
#
# NOT covered (same host-backend scope as devcluster_e2e.sh): the container
# runtime layer, GPU, and the rest of the laptop thin client (login/logout,
# --source-hash). The one-OTP ControlMaster lifecycle has its own harness now:
# scripts/devcluster_otp_e2e.sh. This proves the rsync pre-stage + delegating
# executor against a real scheduler, plus the remote-flag dry-run's local-only
# boundary.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
if [[ "${DEVCLUSTER_REMOTE_READS:-0}" == "1" ]]; then
  work_dir="$repo_root/.tmp/devcluster-cases/remote-reads-$(date +%s)-$$"
else
  work_dir="$repo_root/.tmp/devcluster-remote-e2e"
fi
spec="$repo_root/dev-cluster/specs/hello.yaml"
ssh_port="${DEVCLUSTER_SSH_PORT:-2222}"
local_baseline_jobs="$work_dir/baseline-jobs.txt"

# Per-run dir for the SSH ControlMaster socket, so nothing lands in the
# developer's real ~/.ssh (removed in the EXIT trap). Based under /tmp (not
# $TMPDIR): macOS $TMPDIR lives under a long /var/folders/... path that, plus the
# `cm-root@localhost:PORT` socket name and the random suffix ssh appends while
# opening the master, overruns the ~104-char sun_path limit. The binary honours
# the ControlPath we inject below via HPC_COMPOSE_REMOTE_SSH_OPTS (those env opts
# come first and win ssh's first-value-wins order over its built-in default).
ssh_ctl_dir="$(mktemp -d /tmp/hcdc-ssh.XXXXXX)"

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

# Resolve a host hpc-compose binary (see header).
resolve_binary() {
  if [[ -n "${HPC_COMPOSE_BIN:-}" ]]; then
    [[ -x "$HPC_COMPOSE_BIN" ]] || fail "HPC_COMPOSE_BIN=$HPC_COMPOSE_BIN is not executable"
    printf '%s' "$HPC_COMPOSE_BIN"
    return 0
  fi
  local cand
  for cand in "$repo_root/target/release/hpc-compose" "$repo_root/target/debug/hpc-compose"; do
    if [[ -x "$cand" ]]; then
      printf '%s' "$cand"
      return 0
    fi
  done
  # Last resort: pull the image's (Linux) binary out. Runs on Linux CI runners;
  # on macOS the loop above already found the locally built binary.
  mkdir -p "$work_dir"
  if "$engine" cp "$container:/usr/local/bin/hpc-compose" "$work_dir/hpc-compose" 2>/dev/null \
    && [[ -x "$work_dir/hpc-compose" ]]; then
    printf '%s' "$work_dir/hpc-compose"
    return 0
  fi
  fail "no host hpc-compose binary; set HPC_COMPOSE_BIN or run 'cargo build'"
}

remote_jobid=""
case_ok=0
# up --remote multiplexes over the ControlPath we inject below; for root@localhost
# -p $ssh_port that expands to this socket. Close + remove it (and its dir) so no
# live master is left behind and nothing is written to the developer's ~/.ssh.
control_socket="$ssh_ctl_dir/cm-root@localhost:$ssh_port"
finish() {
  local current_jobs case_job_id
  # Snapshot the failure before cancellation and stage cleanup erase the most
  # useful scheduler and remote-state evidence.
  if [[ "${DEVCLUSTER_CASE_LOCAL_ONLY:-0}" == "1" && "$case_ok" != "1" ]]; then
    mkdir -p "$work_dir"
    inctr sinfo -a >"$work_dir/sinfo.txt" 2>&1 || true
    inctr squeue -a -o '%.18i %.12j %.10T %.20R' >"$work_dir/squeue.txt" 2>&1 || true
    inctr sacct -S now-1hour -X -P --format=JobIDRaw,JobName,State,ExitCode,Start,End \
      >"$work_dir/sacct.txt" 2>&1 || true
    "$engine" logs "$container" >"$work_dir/container.log" 2>&1 || true
    if inctr test -d /root/.hpc-compose-remote; then
      inctr tar -C /root/.hpc-compose-remote -czf - . \
        >"$work_dir/remote-stage.tar.gz" 2>/dev/null || true
    fi
    note "Failure diagnostics preserved at $work_dir"
  fi
  # Cancel a leaked remote job, drop the staged tree, and clean the work dir.
  if [[ -n "$remote_jobid" ]]; then
    inctr scancel "$remote_jobid" >/dev/null 2>&1 || true
  fi
  # The historical all-suite harness owns the otherwise-empty dev cluster and
  # retains its belt-and-suspenders cleanup. Selectable local cases must never
  # cancel a different local run, so they clean only the parsed job id above.
  if [[ "${DEVCLUSTER_CASE_LOCAL_ONLY:-0}" != "1" ]]; then
    inctr scancel --user=root >/dev/null 2>&1 || true
  elif [[ -f "$local_baseline_jobs" ]]; then
    current_jobs="$(inctr squeue -h -o '%A' 2>/dev/null | sort -u || true)"
    while IFS= read -r case_job_id; do
      [[ -n "$case_job_id" ]] || continue
      if ! grep -qxF "$case_job_id" "$local_baseline_jobs"; then
        inctr scancel "$case_job_id" >/dev/null 2>&1 || true
      fi
    done <<< "$current_jobs"
  fi
  if [[ -S "$control_socket" ]]; then
    ssh -o ControlPath="$control_socket" -O exit root@localhost >/dev/null 2>&1 || true
  fi
  rm -rf "$ssh_ctl_dir" 2>/dev/null || true
  if [[ "${DEVCLUSTER_CASE_LOCAL_ONLY:-0}" != "1" || "$case_ok" == "1" ]]; then
    rm -rf "$work_dir" 2>/dev/null || true
  fi
  inctr rm -f /root/.ssh/authorized_keys >/dev/null 2>&1 || true
  inctr rm -rf /root/.hpc-compose-remote >/dev/null 2>&1 || true
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

# --- 2. set up the SSH login-node stand-in ---------------------------------
mkdir -p "$work_dir"
key="$work_dir/id_devcluster"
rm -f "$key" "$key.pub"
ssh-keygen -t ed25519 -N '' -f "$key" -q
# Inject the public key as an authorized key for root in the container. No
# credentials are baked into the image; this is a throwaway per-run keypair.
# Needs `exec -i` so the key on stdin actually reaches the container process.
"$engine" exec -i "$container" sh -c 'mkdir -p /root/.ssh && chmod 700 /root/.ssh && cat > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys' < "$key.pub" \
  || fail "could not install the test public key in the container"

# Ad-hoc ssh options for a host not in ~/.ssh/config (port + key + no host-key
# prompt). hpc-compose's remote path appends these to every ssh/rsync it runs.
# The explicit ControlPath redirects the binary's ControlMaster socket into our
# per-run temp dir instead of ~/.ssh: these env opts come FIRST in the binary's
# arg vector and win ssh's first-value-wins precedence over its built-in default.
export HPC_COMPOSE_REMOTE_SSH_OPTS="-p $ssh_port -i $key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ControlPath=$ssh_ctl_dir/cm-%r@%h:%p"

note "Waiting for sshd to accept the key"
reachable=0
for _ in $(seq 1 30); do
  # shellcheck disable=SC2086
  if ssh $HPC_COMPOSE_REMOTE_SSH_OPTS -o BatchMode=yes root@localhost true >/dev/null 2>&1; then
    reachable=1
    break
  fi
  sleep 1
done
[[ "$reachable" == 1 ]] || fail "sshd on localhost:$ssh_port did not accept the test key"
pass "ssh login-node stand-in reachable on localhost:$ssh_port"

bin="$(resolve_binary)"
pass "using host binary: $bin"

# --- local-only case: remote read commands over one real staged run ---------
# This block is opt-in through scripts/devcluster_case.sh and is intentionally
# absent from the default/CI remote suite. Use the artifact spec so `pull` can
# resolve a genuine teardown manifest rather than a placeholder.
if [[ "${DEVCLUSTER_REMOTE_READS:-0}" == "1" ]]; then
  spec="$repo_root/dev-cluster/specs/artifacts.yaml"
  inctr squeue -h -o '%A' 2>/dev/null | sort -u >"$local_baseline_jobs" || true
  note "Submitting artifact-producing job for remote follow-up reads"
  out="$work_dir/up.out"
  err="$work_dir/up.err"
  remote_status=0
  "$bin" up --remote=root@localhost -f "$spec" >"$out" 2>"$err" || remote_status=$?
  [[ "$remote_status" == 0 ]] \
    || { sed 's/^/    | /' "$err" >&2; sed 's/^/    | /' "$out" >&2; fail "remote artifact run exited $remote_status"; }
  remote_jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
  [[ -n "$remote_jobid" ]] || fail "could not parse the remote artifact job id"
  grep -q 'final state: COMPLETED' "$out" || fail "remote artifact job did not reach COMPLETED"
  grep -q 'dev-cluster artifacts produced' "$out" || fail "remote artifact job output was not streamed back"
  pass "remote artifact job $remote_jobid completed"

  next_job_before="$(inctr scontrol show config 2>/dev/null \
    | awk '$1 == "NEXT_JOB_ID" { value=$3 } END { print value }')"
  [[ "$next_job_before" =~ ^[0-9]+$ ]] \
    || fail "could not read Slurm NEXT_JOB_ID before remote reads"

  run_remote_json() {
    local label="$1" contract="$2" attempts="$3"
    shift 3
    local json="$work_dir/$label.json" stderr="$work_dir/$label.err" ok=0
    for _ in $(seq 1 "$attempts"); do
      if "$@" >"$json" 2>"$stderr" \
        && python3 "$repo_root/scripts/devcluster_case_assert.py" \
          "$contract" "$json" --job-id "$remote_jobid"; then
        ok=1
        break
      fi
      sleep 1
    done
    if [[ "$ok" != 1 ]]; then
      sed 's/^/    | /' "$stderr" >&2 || true
      sed 's/^/    | /' "$json" >&2 || true
      fail "remote $label contract did not pass"
    fi
    pass "remote $label returned the tracked job contract"
  }

  run_remote_json status remote-status 3 \
    "$bin" status --remote=root@localhost -f "$spec" --job-id "$remote_jobid" --format json
  run_remote_json stats remote-stats 3 \
    "$bin" stats --remote=root@localhost -f "$spec" --job-id "$remote_jobid" \
      --accounting --format json

  logs_out="$work_dir/logs.out"
  logs_err="$work_dir/logs.err"
  "$bin" logs --remote=root@localhost -f "$spec" --job-id "$remote_jobid" \
    --service app --lines 100 \
    >"$logs_out" 2>"$logs_err" \
    || { sed 's/^/    | /' "$logs_err" >&2; fail "remote logs failed"; }
  grep -q 'dev-cluster artifacts produced' "$logs_out" \
    || fail "remote logs did not contain the artifact service output"
  pass "remote logs returned service output"

  run_remote_json score remote-score 10 \
    "$bin" score "$remote_jobid" --remote=root@localhost -f "$spec" --format json
  run_remote_json pull remote-pull 10 \
    "$bin" pull --remote=root@localhost -f "$spec" --job-id "$remote_jobid" --format json

  next_job_after="$(inctr scontrol show config 2>/dev/null \
    | awk '$1 == "NEXT_JOB_ID" { value=$3 } END { print value }')"
  [[ "$next_job_after" == "$next_job_before" ]] \
    || fail "remote reads advanced Slurm NEXT_JOB_ID ($next_job_before -> $next_job_after)"
  pass "remote reads created no scheduler allocations"
  case_ok=1
  note "Local dev-cluster case 'remote-reads' passed"
  exit 0
fi

# --- 3. drive `up --remote` and assert a real remote submission ------------
note "Delegating: hpc-compose up --remote=root@localhost -f hello.yaml"
out="$(mktemp)"
remote_status=0
if "$bin" up --remote=root@localhost -f "$spec" >"$out" 2>&1; then
  remote_status=0
else
  remote_status=$?
fi
sed 's/^/    | /' "$out"
[[ "$remote_status" == 0 ]] || fail "up --remote exited $remote_status"
pass "up --remote exited 0"

grep -q 'Submitted batch job' "$out" || fail "no remote sbatch submission found"
remote_jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
[[ -n "$remote_jobid" ]] || fail "could not parse the remote job id"
pass "remote submission via real sbatch (job $remote_jobid)"

grep -q 'final state: COMPLETED' "$out" || fail "watch did not report COMPLETED for the remote job"
grep -q 'hello from a real' "$out" || fail "expected remote service log output not streamed back"
pass "remote watch streamed back to COMPLETED with service logs"

# Authoritative terminal state straight from the remote accounting db.
state="$(inctr sacct -j "$remote_jobid" -n -P -X --format=State 2>/dev/null | head -n1 | awk '{print $1}')"
[[ "$state" == "COMPLETED" ]] || fail "sacct State=$state (expected COMPLETED) for remote job $remote_jobid"
pass "sacct confirms remote job $remote_jobid COMPLETED"

# The project really was staged over rsync (not run from a pre-existing mount).
inctr test -f "/root/.hpc-compose-remote/specs/hello.yaml" \
  || fail "the project was not rsync-staged into the remote stage dir"
pass "project was rsync-staged to the remote stage dir"
rm -f "$out"

# --- 4. remote-flag dry-run: local static preview, no second stage ----------
# `up --remote --dry-run` deliberately resolves to the same local static preview
# as ordinary `up --dry-run`: no SSH, rsync, remote install/probe, or submission.
# The focused fake-tool test proves no transport is invoked; this harness proves
# the remote stage and scheduler remain untouched after a real remote run.
note "Local static preview: up --remote=root@localhost --dry-run"
inctr rm -f /root/.hpc-compose-remote/specs/hpc-compose.sbatch >/dev/null 2>&1 || true
preview="$work_dir/remote-flag-static-preview.sbatch"
rm -f "$preview"
acct_before="$(inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' ')"
queue_before="$(inctr squeue -h 2>/dev/null | wc -l | tr -d ' ')"
dry_out="$(mktemp)"
dry_status=0
"$bin" --offline up --remote=root@localhost -f "$spec" --dry-run \
  --script-out "$preview" >"$dry_out" 2>&1 || dry_status=$?
sed 's/^/    | /' "$dry_out"
[[ "$dry_status" == 0 ]] || { rm -f "$dry_out"; fail "up --remote --dry-run exited $dry_status"; }
grep -q 'skipping sbatch submission' "$dry_out" \
  || { rm -f "$dry_out"; fail "remote dry-run did not report skipping submission"; }
if grep -q 'Submitted batch job' "$dry_out"; then
  rm -f "$dry_out"; fail "remote dry-run reported an sbatch submission"
fi
rm -f "$dry_out"
acct_after="$(inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' ')"
queue_after="$(inctr squeue -h 2>/dev/null | wc -l | tr -d ' ')"
# sacct rows are monotonic (a real submit always adds one), so this is the
# authoritative "submitted nothing" check. squeue can only shrink as a prior
# job drains, so assert it never GREW (a dry-run submission would grow it).
[[ "$acct_after" == "$acct_before" ]] \
  || fail "remote dry-run changed the accounting job count ($acct_before -> $acct_after)"
[[ "$queue_after" -le "$queue_before" ]] \
  || fail "remote dry-run enqueued a job ($queue_before -> $queue_after)"
pass "remote-flag dry-run submitted nothing (remote sacct=$acct_after, squeue=$queue_after unchanged)"
[[ -f "$preview" ]] || fail "remote-flag dry-run did not write the local preview"
grep -q '^#SBATCH ' "$preview" || fail "local preview has no #SBATCH directives"
if inctr test -f "/root/.hpc-compose-remote/specs/hpc-compose.sbatch"; then
  fail "remote-flag dry-run recreated a script in the remote stage"
fi
pass "remote-flag dry-run rendered locally and did not restage the project"

note "All dev-cluster remote-submit end-to-end checks passed"
# Stage cleanup and any requested teardown run in the EXIT trap (`finish`).
