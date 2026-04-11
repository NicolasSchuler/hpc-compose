#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose_file=${HPC_COMPOSE_SMOKE_FILE:-"$repo_root/examples/minimal-batch.yaml"}
script_out=${HPC_COMPOSE_SMOKE_SCRIPT_OUT:-"${TMPDIR:-/tmp}/hpc-compose-smoke.sbatch"}

if [[ -z "${CACHE_DIR:-}" ]]; then
  echo "CACHE_DIR must point to shared storage visible from login and compute nodes" >&2
  exit 2
fi

cd "$repo_root"

cargo run --locked -- validate -f "$compose_file"
preflight_args=(preflight -f "$compose_file" --verbose)
if [[ "${HPC_COMPOSE_SMOKE_STRICT:-0}" == "1" ]]; then
  preflight_args+=(--strict)
fi
cargo run --locked -- "${preflight_args[@]}"
cargo run --locked -- render -f "$compose_file" --output "$script_out"

if [[ "${HPC_COMPOSE_SMOKE_SUBMIT:-0}" == "1" ]]; then
  cargo run --locked -- up -f "$compose_file" --watch
else
  echo "Rendered smoke script at $script_out"
  echo "Set HPC_COMPOSE_SMOKE_SUBMIT=1 to submit the smoke job."
fi
