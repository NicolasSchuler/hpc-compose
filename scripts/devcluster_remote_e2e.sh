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
# executor (and the remote --dry-run preview) against a real scheduler.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
work_dir="$repo_root/.tmp/devcluster-remote-e2e"
spec="$repo_root/dev-cluster/specs/hello.yaml"
ssh_port=2222

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
# up --remote always multiplexes over ~/.ssh/cm-%r@%h:%p, which for root@localhost
# -p 2222 is this socket; close + remove it so no live master is left on a laptop.
control_socket="$HOME/.ssh/cm-root@localhost:$ssh_port"
finish() {
  # Cancel a leaked remote job, drop the staged tree, and clean the work dir.
  if [[ -n "$remote_jobid" ]]; then
    inctr scancel "$remote_jobid" >/dev/null 2>&1 || true
  fi
  # Belt-and-suspenders for an interrupt before $remote_jobid was parsed: this
  # harness's only jobs are its own, so a blanket scancel can't strand others.
  inctr scancel --user=root >/dev/null 2>&1 || true
  if [[ -S "$control_socket" ]]; then
    ssh -o ControlPath="$control_socket" -O exit root@localhost >/dev/null 2>&1 || true
  fi
  rm -f "$control_socket" 2>/dev/null || true
  inctr rm -rf /root/.hpc-compose-remote >/dev/null 2>&1 || true
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
export HPC_COMPOSE_REMOTE_SSH_OPTS="-p $ssh_port -i $key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

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

# --- 4. remote dry-run: stage + render remotely, submit nothing -------------
# `up --remote --dry-run` is the safe preview for the laptop->login-node path: it
# rsyncs the project and renders the sbatch ON the login node, but submits no job.
# Prove it lands no job in the remote accounting db (stages-but-doesn't-submit).
note "Remote dry-run: up --remote=root@localhost --dry-run"
# Drop the sbatch the section-3 real submit rendered into the same staged path, so
# the "rendered a valid sbatch" assertion below proves the DRY-RUN produced it (a
# fresh render), not a stale leftover. (up --remote re-rsyncs with --delete, which
# also clears it, but removing it explicitly makes the assertion's intent clear.)
inctr rm -f /root/.hpc-compose-remote/specs/hpc-compose.sbatch >/dev/null 2>&1 || true
acct_before="$(inctr sacct -n -X -P --format=JobID 2>/dev/null | wc -l | tr -d ' ')"
queue_before="$(inctr squeue -h 2>/dev/null | wc -l | tr -d ' ')"
dry_out="$(mktemp)"
dry_status=0
"$bin" up --remote=root@localhost -f "$spec" --dry-run >"$dry_out" 2>&1 || dry_status=$?
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
pass "remote dry-run submitted nothing (remote sacct=$acct_after, squeue=$queue_after unchanged)"
# It stages-but-doesn't-submit: the project is rsynced and a VALID sbatch is
# rendered on the login node, ready for a real run, but no job was created.
inctr test -f "/root/.hpc-compose-remote/specs/hpc-compose.sbatch" \
  || fail "remote dry-run did not render the sbatch on the login node"
inctr grep -q '^#SBATCH ' "/root/.hpc-compose-remote/specs/hpc-compose.sbatch" \
  || fail "remote dry-run rendered a script with no #SBATCH directives"
pass "remote dry-run staged the project and rendered a valid sbatch (no submission)"

note "All dev-cluster remote-submit end-to-end checks passed"
# Stage cleanup and any requested teardown run in the EXIT trap (`finish`).
