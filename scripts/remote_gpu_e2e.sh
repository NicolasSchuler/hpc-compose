#!/usr/bin/env bash
# Opt-in REAL-GPU end-to-end check for the metrics pipeline against a real
# cluster (HAICORE). This is the ONLY place the GPU sampler is exercised against
# an actual NVIDIA device: the local dev-cluster harnesses run a GPU-less host
# backend, so gpu.jsonl there is always the "no GPU" path. This drives the thin
# laptop client (`up --remote`) end to end and asserts the collected metrics.
#
# It is DELIBERATELY not part of `just ci` and never runs in CI: it needs a live
# login node, a real GPU allocation, and one interactive OTP/2FA. Run it by hand
# when you want to prove the GPU/CPU metrics pipeline against real hardware.
#
#   scripts/remote_gpu_e2e.sh                 run against $HPC_REMOTE_HOST
#   HPC_REMOTE_HOST=haicore ...               override the login host / ssh alias
#   HPC_SLURM_ACCOUNT=kastel ...              override the account
#   HPC_SLURM_PARTITION=normal ...            override the partition
#   HPC_SLURM_GRES=gpu:1 ...                  override the GPU gres request
#   HPC_COMPOSE_BIN=/path/to/hpc-compose      use a specific host binary
#   HPC_REMOTE_EXTRA_SSH_OPTS="-i ~/.ssh/id"  extra ssh opts (appended)
#
# What it does (single OTP for the WHOLE session):
#   1. Opens ONE SSH ControlMaster to the login node (this is the only OTP
#      prompt; every later ssh/rsync/hpc-compose --remote reuses the master).
#   2. `up --remote` a tiny 1-GPU cuda-probe job (based on examples/cuda-probe.yaml)
#      and watches it to COMPLETED on the real scheduler.
#   3. `stats --remote --format json` and asserts the sampler surfaced cpu + gpu
#      nodes and a populated gpu_count.
#   4. Pulls (rsync over the shared master) the job-local gpu.jsonl / cpu.jsonl
#      and asserts non-null GPU utilization/memory and non-null CPU utilization.
#
# Safety / cleanup (EXIT trap, safe to re-run):
#   - Cancels ONLY the job id this run submitted (never a blanket scancel: this
#     is a shared cluster and the user may have unrelated production jobs).
#   - Removes this run's remote stage dir (~/.hpc-compose-remote/<stage>).
#   - Closes the ControlMaster and removes the per-run socket dir + local temp.
#
# Cluster facts baked in as defaults (from prior verified HAICORE runs):
#   account=kastel, partition=normal, gres=gpu:1 (gpu:full does NOT exist),
#   sinfo is denied on the login node and `test`-only runs are pessimistic, so
#   this submits a real job and reads the real sampler output instead.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Parameters (HAICORE defaults; override via env).
host="${HPC_REMOTE_HOST:-haicore}"
account="${HPC_SLURM_ACCOUNT:-kastel}"
partition="${HPC_SLURM_PARTITION:-normal}"
gres="${HPC_SLURM_GRES:-gpu:1}"
extra_ssh_opts="${HPC_REMOTE_EXTRA_SSH_OPTS:-}"

# Deterministic work dir: its basename becomes the remote stage name
# (~/.hpc-compose-remote/<basename>), so cleanup can target the exact remote dir.
stage_name="remote-gpu-e2e"
work_dir="$repo_root/.tmp/$stage_name"
spec="$work_dir/hpc-compose.yaml"
pulled_dir="$work_dir/pulled"

# Per-run dir for the SSH ControlMaster socket, so the whole session multiplexes
# through our temp dir instead of the developer's real ~/.ssh (removed in the
# EXIT trap). Based under /tmp (not $TMPDIR): macOS $TMPDIR lives under a long
# /var/folders/... path that, plus the `cm-%r@%h:%p` socket name and the random
# suffix ssh appends while opening the master, overruns the ~104-char sun_path
# limit. We inject ControlPath into HPC_COMPOSE_REMOTE_SSH_OPTS so the binary AND
# our manual ssh/rsync probes all share this exact one socket (one OTP).
ssh_ctl_dir="$(mktemp -d /tmp/hcge-ssh.XXXXXX)"
ctl_path="$ssh_ctl_dir/cm-%r@%h:%p"

red='\033[31m'; green='\033[32m'; bold='\033[1m'; reset='\033[0m'
note() { printf '%b==>%b %s\n' "$bold" "$reset" "$*"; }
pass() { printf '  %bok%b   %s\n' "$green" "$reset" "$*"; }
fail() { printf '  %bFAIL%b %s\n' "$red" "$reset" "$*" >&2; exit 1; }

# ssh options shared by every manual connection (master open, scancel, rsync -e,
# -O exit). ControlMaster=auto + our per-run ControlPath reuse the one master;
# RemoteCommand=none/RequestTTY=no neutralize an interactive Host alias (e.g. a
# `RemoteCommand tmux ...` block) so plain commands are not hijacked. User extra
# opts come FIRST so they win ssh's first-value-wins order.
read -r -a extra_opts_arr <<<"$extra_ssh_opts"
mux_opts=(
  "${extra_opts_arr[@]}"
  -o ControlMaster=auto
  -o ControlPath="$ctl_path"
  -o ControlPersist=10m
  -o RemoteCommand=none
  -o RequestTTY=no
)

# Resolve a host hpc-compose binary (same strategy as the dev-cluster harnesses).
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
  fail "no host hpc-compose binary; set HPC_COMPOSE_BIN or run 'cargo build --release'"
}

remote_jobid=""
finish() {
  # Cancel ONLY our own job id if we parsed one — never a blanket scancel, this
  # is a shared cluster with possibly-unrelated production jobs.
  if [[ -n "$remote_jobid" ]]; then
    # SC2029: $remote_jobid is a locally-parsed integer; client-side expansion
    # into the remote scancel is exactly what we want (and the only safe value).
    # shellcheck disable=SC2029
    ssh "${mux_opts[@]}" "$host" "scancel $remote_jobid" >/dev/null 2>&1 || true
  fi
  # Drop this run's remote stage dir (deterministic basename => exact target).
  # SC2029: $stage_name is a fixed local literal; expanding it client-side is
  # intentional, and ~ must expand remotely (the login node's home).
  # shellcheck disable=SC2029
  ssh "${mux_opts[@]}" "$host" "rm -rf ~/.hpc-compose-remote/$stage_name" >/dev/null 2>&1 || true
  # Close the ControlMaster (ssh expands the ControlPath template itself), then
  # remove the per-run socket dir and local temp.
  ssh "${mux_opts[@]}" -O exit "$host" >/dev/null 2>&1 || true
  rm -rf "$ssh_ctl_dir" 2>/dev/null || true
  rm -rf "$work_dir" 2>/dev/null || true
}
trap finish EXIT

# --- 0. preconditions ------------------------------------------------------
for tool in ssh rsync jq; do
  command -v "$tool" >/dev/null 2>&1 || fail "required tool '$tool' not found on PATH"
done
bin="$(resolve_binary)"
pass "using host binary: $bin"

# The binary appends these to every ssh/rsync it runs. We only override
# ControlPath (the binary adds ControlMaster/ControlPersist and the
# RemoteCommand/RequestTTY neutralizers itself); ours comes first and wins.
export HPC_COMPOSE_REMOTE_SSH_OPTS="-o ControlPath=$ctl_path $extra_ssh_opts"

# --- 1. open ONE ControlMaster (single OTP for the whole session) -----------
note "Opening one SSH ControlMaster to $host (this is the only OTP prompt)"
ssh "${mux_opts[@]}" "$host" true \
  || fail "could not open a ControlMaster to $host (check HPC_REMOTE_HOST / your ssh config / OTP)"
ssh "${mux_opts[@]}" -O check "$host" >/dev/null 2>&1 \
  || fail "ControlMaster is not live after opening it"
pass "ControlMaster live; every later ssh/rsync/--remote reuses it (no further OTP)"

# --- 2. generate the tiny 1-GPU spec ---------------------------------------
# Based on examples/cuda-probe.yaml: a tiny NVIDIA CUDA base image run as a
# one-shot Slurm job, with the HAICORE account/partition/gres baked in. The
# service loops nvidia-smi for ~30s so the 5s-interval sampler captures several
# real GPU rows (an idle GPU still yields non-null utilization/memory).
mkdir -p "$work_dir" "$pulled_dir"
cat >"$spec" <<YAML
name: remote-gpu-e2e

# Real-GPU metrics-pipeline probe. See scripts/remote_gpu_e2e.sh.
runtime:
  backend: pyxis

x-slurm:
  job_name: remote-gpu-e2e
  time: "00:15:00"
  cpus_per_task: 2
  mem: 8G
  account: "$account"
  partition: "$partition"
  gres: "$gres"
  metrics:
    enabled: true
    interval_seconds: 5

services:
  probe:
    image: nvidia/cuda:12.4.1-base-ubuntu22.04
    script: |
      set -eu
      echo "hostname=\$(hostname)"
      echo "CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-}"
      echo "SLURM_JOB_ID=\${SLURM_JOB_ID:-}"
      nvidia-smi -L
      # Keep the GPU allocated long enough for several sampler ticks.
      for i in \$(seq 1 6); do
        nvidia-smi --query-gpu=utilization.gpu,memory.used --format=csv,noheader
        sleep 5
      done
YAML
pass "generated 1-GPU spec (account=$account partition=$partition gres=$gres)"

# --- 3. up --remote and watch to COMPLETED ---------------------------------
note "Delegating: hpc-compose up --remote=$host (real 1-GPU submission)"
out="$work_dir/up.log"
status=0
"$bin" up --remote="$host" -f "$spec" --watch-mode line >"$out" 2>&1 || status=$?
sed 's/^/    | /' "$out"
[[ "$status" == 0 ]] || fail "up --remote exited $status"

grep -q 'Submitted batch job' "$out" || fail "no remote sbatch submission found"
remote_jobid="$(grep -oE 'Submitted batch job [0-9]+' "$out" | head -n1 | grep -oE '[0-9]+')"
[[ -n "$remote_jobid" ]] || fail "could not parse the remote job id"
pass "remote submission via real sbatch (job $remote_jobid)"

grep -q 'final state: COMPLETED' "$out" || fail "watch did not report COMPLETED for job $remote_jobid"
pass "remote job $remote_jobid tracked to COMPLETED"

# --- 4. stats --remote: assert the sampler surfaced cpu + gpu --------------
note "Reading: hpc-compose stats --remote=$host --format json"
stats_json="$work_dir/stats.json"
"$bin" stats --remote="$host" -f "$spec" --job-id "$remote_jobid" --format json >"$stats_json" 2>&1 \
  || { sed 's/^/    | /' "$stats_json" >&2; fail "stats --remote --format json failed"; }

jq -e '.sampler != null' "$stats_json" >/dev/null \
  || fail "stats JSON has no sampler node"
jq -e '.sampler.cpu != null' "$stats_json" >/dev/null \
  || fail "stats JSON sampler.cpu is missing/null"
pass "stats JSON carries sampler.cpu"

jq -e '(.sampler.gpu.nodes | length) >= 1' "$stats_json" >/dev/null \
  || fail "stats JSON sampler.gpu has no nodes"
jq -e 'any(.sampler.gpu.nodes[]; .gpu_count >= 1)' "$stats_json" >/dev/null \
  || fail "stats JSON has no gpu node with a populated gpu_count (>= 1)"
pass "stats JSON carries gpu nodes with a populated gpu_count"

# Where the job-local sampler wrote its jsonl on the remote (absolute path).
metrics_dir="$(jq -r '.metrics_dir // empty' "$stats_json")"
[[ -n "$metrics_dir" ]] || fail "stats JSON did not report a metrics_dir to pull from"
pass "remote metrics_dir: $metrics_dir"

# --- 5. pull the jsonl over the shared master and assert rows --------------
note "Pulling gpu.jsonl / cpu.jsonl over the shared ControlMaster (rsync)"
ssh_e="ssh ${mux_opts[*]}"
rsync -az -e "$ssh_e" \
  "$host:$metrics_dir/gpu.jsonl" "$host:$metrics_dir/cpu.jsonl" \
  "$pulled_dir/" \
  || fail "rsync of the metrics jsonl over the shared master failed"

gpu_jsonl="$pulled_dir/gpu.jsonl"
cpu_jsonl="$pulled_dir/cpu.jsonl"
[[ -s "$gpu_jsonl" ]] || fail "pulled gpu.jsonl is missing or empty"
[[ -s "$cpu_jsonl" ]] || fail "pulled cpu.jsonl is missing or empty"

# gpu.jsonl: at least one row with non-null utilization_gpu AND memory_used_mib.
gpu_rows="$(jq -s '[.[] | select(.utilization_gpu != null and .memory_used_mib != null)] | length' "$gpu_jsonl")"
[[ "${gpu_rows:-0}" -ge 1 ]] \
  || fail "gpu.jsonl has no row with non-null utilization_gpu and memory_used_mib"
pass "gpu.jsonl has $gpu_rows row(s) with non-null utilization_gpu + memory_used_mib"

# cpu.jsonl: at least one row with non-null cpu_util_pct.
cpu_rows="$(jq -s '[.[] | select(.cpu_util_pct != null)] | length' "$cpu_jsonl")"
[[ "${cpu_rows:-0}" -ge 1 ]] \
  || fail "cpu.jsonl has no row with non-null cpu_util_pct"
pass "cpu.jsonl has $cpu_rows row(s) with non-null cpu_util_pct"

note "All real-GPU metrics-pipeline end-to-end checks passed"
# Job cancellation (defensive), remote stage removal, and ControlMaster teardown
# run in the EXIT trap (`finish`).
