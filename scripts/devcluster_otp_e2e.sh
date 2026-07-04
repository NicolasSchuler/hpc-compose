#!/usr/bin/env bash
# End-to-end proof of the ONE-OTP-PER-SESSION property for the laptop thin client.
#
# Real cluster login nodes demand an OTP/2FA passcode per SSH session. hpc-compose
# copes by reusing a single authenticated connection via SSH ControlMaster
# multiplexing (CONTROL_MASTER_SSH_OPTS in src/commands/runtime/ssh_hint.rs), so a
# whole laptop session prompts only once. The remote-submit harness
# (devcluster_remote_e2e.sh) proves a real `up --remote` lands, but its login-node
# stand-in is key-only and never exercises the OTP/ControlMaster lifecycle.
#
# This harness closes that gap. It flips the stand-in into an OTP-requiring mode
# (publickey + an interactive second factor, counted by a pam_exec hook; see
# dev-cluster/otp-sim.sh), then drives a realistic multi-command laptop session
# and asserts EXACTLY ONE authentication occurs across all of it -- corroborated
# by the live ControlMaster socket and `ssh -O check`.
#
#   scripts/devcluster_otp_e2e.sh        boot (build if needed), run, assert
#   DEVCLUSTER_SKIP_BUILD=1 ...           reuse an existing image (CI prebuilds it)
#   DEVCLUSTER_E2E_DOWN=1 ...             tear the cluster down when finished
#   HPC_COMPOSE_BIN=/path/to/hpc-compose use a specific host binary
#
# Same host-backend scope and privileged-container requirements as the other two
# dev-cluster harnesses.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
work_dir="$repo_root/.tmp/devcluster-otp-e2e"
spec="$repo_root/dev-cluster/specs/hello.yaml"
ssh_port="${DEVCLUSTER_SSH_PORT:-2222}"
# Per-run dir for the SSH ControlMaster socket, so the whole session multiplexes
# through our temp dir instead of the developer's real ~/.ssh (removed in the
# EXIT trap). Based under /tmp (not $TMPDIR): macOS $TMPDIR lives under a long
# /var/folders/... path that, plus the `cm-root@localhost:PORT` socket name and
# the random suffix ssh appends while opening the master, overruns the ~104-char
# sun_path limit. Below we inject `ControlPath=$ssh_ctl_dir/cm-%r@%h:%p` into
# HPC_COMPOSE_REMOTE_SSH_OPTS so the binary AND the manual corroboration probes
# share this exact one socket — the one-OTP-per-session assertion depends on
# every ssh invocation using an identical ControlPath.
ssh_ctl_dir="$(mktemp -d /tmp/hcdc-ssh.XXXXXX)"
# The control socket the binary opens: the injected ControlPath expands
# %r@%h:%p to root@localhost:$ssh_port under our per-run dir.
control_socket="$ssh_ctl_dir/cm-root@localhost:$ssh_port"

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

# Resolve a host hpc-compose binary (same strategy as devcluster_remote_e2e.sh).
resolve_binary() {
  if [[ -n "${HPC_COMPOSE_BIN:-}" ]]; then
    [[ -x "$HPC_COMPOSE_BIN" ]] || fail "HPC_COMPOSE_BIN=$HPC_COMPOSE_BIN is not executable"
    printf '%s' "$HPC_COMPOSE_BIN"
    return 0
  fi
  local cand
  for cand in "$repo_root/target/release/hpc-compose" "$repo_root/target/debug/hpc-compose"; do
    if [[ -x "$cand" ]]; then printf '%s' "$cand"; return 0; fi
  done
  mkdir -p "$work_dir"
  if "$engine" cp "$container:/usr/local/bin/hpc-compose" "$work_dir/hpc-compose" 2>/dev/null \
    && [[ -x "$work_dir/hpc-compose" ]]; then
    printf '%s' "$work_dir/hpc-compose"
    return 0
  fi
  fail "no host hpc-compose binary; set HPC_COMPOSE_BIN or run 'cargo build'"
}

otp_count() { inctr otp-sim count 2>/dev/null | tr -d '[:space:]'; }

finish() {
  # Close the laptop's ControlMaster, restore key-only sshd, and drop all state.
  if [[ -S "$control_socket" ]]; then
    ssh -o ControlPath="$control_socket" -O exit root@localhost >/dev/null 2>&1 || true
  fi
  rm -rf "$ssh_ctl_dir" 2>/dev/null || true
  inctr scancel --user=root >/dev/null 2>&1 || true
  inctr otp-sim disable >/dev/null 2>&1 || true
  inctr rm -rf /root/.hpc-compose-remote >/dev/null 2>&1 || true
  inctr rm -f /root/.ssh/authorized_keys >/dev/null 2>&1 || true
  rm -f "${out:-}" "${out2:-}" 2>/dev/null || true
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
  if inctr sinfo -h -o '%T' 2>/dev/null | grep -q '^idle'; then idle=1; break; fi
  sleep 1
done
[[ "$idle" == 1 ]] || fail "node did not reach 'idle' (check: scripts/devcluster.sh logs)"
pass "node is idle"

# --- 2. set up the OTP-requiring login-node stand-in -----------------------
mkdir -p "$work_dir"
key="$work_dir/id_devcluster"
rm -f "$key" "$key.pub" "$control_socket"
ssh-keygen -t ed25519 -N '' -f "$key" -q
"$engine" exec -i "$container" sh -c 'mkdir -p /root/.ssh && chmod 700 /root/.ssh && cat > /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys' < "$key.pub" \
  || fail "could not install the test public key in the container"

note "Enabling OTP/2FA simulation on the login-node stand-in"
inctr otp-sim enable >/dev/null || fail "otp-sim enable failed"
pass "sshd now requires publickey + an interactive (OTP) second factor"

# Connection opts the binary appends to every ssh/rsync (host not in ~/.ssh/config).
# The explicit ControlPath redirects the binary's ControlMaster socket into our
# per-run temp dir (keeping it out of ~/.ssh). It comes FIRST in the binary's arg
# vector, so it wins ssh's first-value-wins order over the built-in
# ~/.ssh/cm-%r@%h:%p default. Crucially it is the SAME socket the manual probes
# below reference, so all of them share ONE master — the one-OTP property still
# holds. ControlMaster/ControlPersist are left to the binary's built-in defaults.
export HPC_COMPOSE_REMOTE_SSH_OPTS="-p $ssh_port -i $key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ControlPath=$ssh_ctl_dir/cm-%r@%h:%p"
# Same connection opts as a bash array, for manual corroboration probes.
conn_opts=(-p "$ssh_port" -i "$key" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

note "Waiting for the OTP sshd to accept the interactive flow"
reachable=0
for _ in $(seq 1 30); do
  # ControlMaster=no + ControlPath=none: a throwaway one-off auth that neither
  # creates nor reuses any master socket (ControlPath=none also ignores a
  # developer ~/.ssh/config that matches localhost), so it can't be mistaken for
  # the session's first connection below.
  if ssh "${conn_opts[@]}" -o ControlMaster=no -o ControlPath=none root@localhost true >/dev/null 2>&1; then
    reachable=1; break
  fi
  sleep 1
done
[[ "$reachable" == 1 ]] || fail "OTP sshd did not accept the publickey+keyboard-interactive flow"
pass "OTP login flow succeeds (publickey + interactive factor)"

# Negative control: a key-only (BatchMode) login MUST be rejected, proving the
# second factor is genuinely required and not silently bypassed.
if ssh "${conn_opts[@]}" -o ControlMaster=no -o ControlPath=none -o BatchMode=yes root@localhost true >/dev/null 2>&1; then
  fail "publickey-only login succeeded; the OTP second factor is not being enforced"
fi
pass "publickey-only (no OTP) login is rejected — second factor enforced"

# --- 3. drive a multi-command laptop session over one ControlMaster --------
# Zero the counter AFTER readiness probing; the measured session starts clean.
inctr otp-sim reset >/dev/null
[[ ! -S "$control_socket" ]] || fail "a ControlMaster socket already exists before the session began"
pass "no ControlMaster socket yet (counter reset to $(otp_count))"

bin="$(resolve_binary)"
pass "using host binary: $bin"

# Command 1: a real `up --remote` submit (internally: mkdir + rsync + delegate =
# three SSH connections, which must share ONE authentication).
note "Command 1: up --remote (real submit; 3 internal SSH connections)"
out="$(mktemp)"
status=0
"$bin" up --remote=root@localhost -f "$spec" --watch-mode line >"$out" 2>&1 || status=$?
sed 's/^/    | /' "$out"
[[ "$status" == 0 ]] || fail "up --remote exited $status"
grep -q 'Submitted batch job' "$out" || fail "no remote sbatch submission found"
grep -q 'final state: COMPLETED' "$out" || fail "remote job did not reach COMPLETED"
grep -q 'only prompts on the first connection' "$out" \
  || fail "up --remote did not print the OTP/ControlMaster multiplexing note"
pass "up --remote submitted, tracked to COMPLETED, and printed the OTP note"

c1="$(otp_count)"
[[ "$c1" == 1 ]] || fail "expected exactly 1 OTP authentication after command 1, got $c1"
pass "exactly 1 OTP authentication after command 1 (3 connections, 1 auth)"
[[ -S "$control_socket" ]] || fail "ControlMaster socket was not created at $control_socket"
ssh "${conn_opts[@]}" -o ControlPath="$control_socket" -O check root@localhost >/dev/null 2>&1 \
  || fail "ssh -O check did not find a live master after command 1"
pass "ControlMaster socket is live ($control_socket)"

# Command 2: a second, distinct hpc-compose laptop command in the same session.
note "Command 2: up --remote --dry-run (second laptop command)"
out2="$(mktemp)"
status=0
"$bin" up --remote=root@localhost -f "$spec" --dry-run >"$out2" 2>&1 || status=$?
[[ "$status" == 0 ]] || { sed 's/^/    | /' "$out2" >&2; fail "up --remote --dry-run exited $status"; }
grep -q 'skipping sbatch submission' "$out2" || fail "dry-run did not report skipping submission"
! grep -q 'Submitted batch job' "$out2" || fail "dry-run unexpectedly submitted a job"
c2="$(otp_count)"
[[ "$c2" == 1 ]] || fail "command 2 re-authenticated (count now $c2); ControlMaster not reused"
pass "command 2 reused the master — still exactly 1 OTP authentication"

# Command 3: a pull/reach-style transfer over SSH (rsync -e 'ssh <mux opts>'),
# the exact shape `pull`/`experiment` emit. It must also reuse the one master.
note "Command 3: pull/reach-style rsync over the shared master"
ssh_e="ssh ${conn_opts[*]} -o ControlMaster=auto -o ControlPath=$control_socket -o ControlPersist=10m"
mkdir -p "$work_dir/pulled"
rsync -az -e "$ssh_e" root@localhost:/etc/hostname "$work_dir/pulled/" \
  || fail "pull-style rsync over the shared master failed"
[[ -f "$work_dir/pulled/hostname" ]] || fail "pull-style rsync produced no file"
c3="$(otp_count)"
[[ "$c3" == 1 ]] || fail "command 3 re-authenticated (count now $c3); ControlMaster not reused"
pass "command 3 (rsync) reused the master — still exactly 1 OTP authentication"

# --- 4. final assertion ----------------------------------------------------
final="$(otp_count)"
[[ "$final" == 1 ]] \
  || fail "expected EXACTLY ONE OTP authentication across the whole session, got $final"
note "One-OTP-per-session PROVEN: 3 laptop commands, $final authentication"
pass "the laptop session authenticated exactly once"

rm -f "$out" "$out2"
# Socket teardown, sshd restore, and any requested down run in the EXIT trap.
