#!/usr/bin/env bash
# Collect a bounded, redacted diagnostic snapshot while the CI dev cluster is
# still running. Deliberately excludes container inspection, process
# environments, compose configuration, job records, rendered scripts, and job
# stdout/stderr: those surfaces can contain user secrets.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="hpc-compose-devcluster"
compose_file="$repo_root/dev-cluster/compose.yaml"

redact() {
  sed -E \
    -e 's#(Bearer[[:space:]]+)[A-Za-z0-9._~+/-]+=*#\1HPC_COMPOSE_REDACTED#g' \
    -e 's#gh[pousr]_[A-Za-z0-9_]+#HPC_COMPOSE_REDACTED#g' \
    -e 's#sk-[A-Za-z0-9_-]{8,}#HPC_COMPOSE_REDACTED#g' \
    -e 's#((TOKEN|token|PASSWORD|password|SECRET|secret|API[_-]?KEY|api[_-]?key)[[:space:]]*[:=][[:space:]]*)[^[:space:]]+#\1HPC_COMPOSE_REDACTED#g' \
    -e 's#(https?://)[^/@:[:space:]]+:[^/@[:space:]]+@#\1HPC_COMPOSE_REDACTED@#g'
}

# Narrow test/inspection seam: exercise the exact production redactor without
# probing a container engine or writing an artifact directory.
if [[ "${1:-}" == "--redact-stdin" ]]; then
  redact
  exit 0
fi

output_dir="${1:-$repo_root/.tmp/devcluster-failure-evidence}"
umask 077
mkdir -p "$output_dir"
scratch="$(mktemp "${TMPDIR:-/tmp}/hpc-compose-devcluster-evidence.XXXXXX")"
trap 'rm -f "$scratch"' EXIT

capture() {
  local output_name="$1"
  local status
  shift
  : > "$scratch"
  if "$@" > "$scratch" 2>&1; then
    :
  else
    status=$?
    printf '\n[collector: command failed with exit %s]\n' "$status" >> "$scratch"
  fi
  redact < "$scratch" > "$output_dir/$output_name"
  : > "$scratch"
}

{
  printf 'collected_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'runner=%s\n' "$(uname -srm)"
  docker --version 2>&1 || true
  docker compose version 2>&1 || true
} | redact > "$output_dir/collector-summary.txt"

# Bound raw daemon output by line count, then redact before it leaves scratch.
capture container.log docker logs --tail 2000 "$container"
capture compose-ps.txt docker compose -f "$compose_file" ps

# Scheduler control-plane summaries only: no job command lines, scripts, job
# environment, or job output paths are requested.
capture sinfo.txt "$repo_root/scripts/devcluster.sh" exec \
  sinfo --noheader --format '%P|%a|%l|%D|%t|%E'
capture squeue.txt "$repo_root/scripts/devcluster.sh" exec \
  squeue --noheader --format '%i|%j|%T|%M|%l|%R'
capture sacct.txt "$repo_root/scripts/devcluster.sh" exec \
  sacct --starttime today --noheader --parsable2 \
  --format JobIDRaw,JobName,State,ExitCode,Elapsed,Timelimit

printf 'devcluster: redacted failure evidence written to %s\n' "$output_dir"
