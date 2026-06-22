#!/usr/bin/env bash
# Thin lifecycle wrapper for the local single-node Slurm dev cluster.
# Drives `docker compose`/`podman compose` and the in-container hpc-compose so
# you don't have to memorize the build + exec dance.
#
#   scripts/devcluster.sh up [--project DIR]   build + start the cluster
#   scripts/devcluster.sh run SPEC [ARGS...]   hpc-compose up -f SPEC inside it
#   scripts/devcluster.sh exec CMD [ARGS...]   run an arbitrary command inside it
#   scripts/devcluster.sh sinfo                show node/partition state
#   scripts/devcluster.sh logs                 follow the slurm daemon logs
#   scripts/devcluster.sh down                 stop + remove the cluster
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/dev-cluster/compose.yaml"
container="hpc-compose-devcluster"

die() { printf 'devcluster: %s\n' "$*" >&2; exit 1; }

# Pick a compose provider and matching engine CLI.
if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
  engine=docker
elif podman compose version >/dev/null 2>&1; then
  compose=(podman compose)
  engine=podman
else
  die "need 'docker compose' or 'podman compose' on PATH (is the engine running?)"
fi

usage() {
  sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

cmd="${1:-}"
[[ $# -gt 0 ]] && shift || true

case "$cmd" in
  up)
    project="$repo_root"
    if [[ "${1:-}" == "--project" ]]; then
      [[ -n "${2:-}" ]] || die "--project needs a directory"
      project="$(cd "$2" && pwd)"
      shift 2
    fi
    # DEVCLUSTER_SKIP_BUILD=1 reuses an existing hpc-compose-devcluster image
    # instead of rebuilding it — used by CI, which prebuilds the image with a
    # cached cargo layer before booting (see scripts/devcluster_e2e.sh).
    if [[ "${DEVCLUSTER_SKIP_BUILD:-0}" == "1" ]]; then
      # Fail fast: without --build, compose would silently fall back to a slow,
      # uncached build if the prebuilt image never loaded — defeating the point.
      if ! "$engine" image inspect hpc-compose-devcluster:latest >/dev/null 2>&1; then
        die "DEVCLUSTER_SKIP_BUILD=1 but image hpc-compose-devcluster:latest is absent; build it first"
      fi
      HPC_COMPOSE_PROJECT_DIR="$project" "${compose[@]}" -f "$compose_file" up -d "$@"
    else
      HPC_COMPOSE_PROJECT_DIR="$project" "${compose[@]}" -f "$compose_file" up --build -d "$@"
    fi
    printf 'devcluster: started; mounted %s at /workspace\n' "$project"
    ;;
  down)
    "${compose[@]}" -f "$compose_file" down "$@"
    ;;
  run)
    spec="${1:-}"
    [[ -n "$spec" ]] || die "usage: devcluster.sh run SPEC [ARGS...]"
    shift
    "$engine" exec -it "$container" hpc-compose up -f "$spec" "$@"
    ;;
  exec)
    [[ $# -gt 0 ]] || die "usage: devcluster.sh exec CMD [ARGS...]"
    "$engine" exec -it "$container" "$@"
    ;;
  sinfo)
    "$engine" exec -it "$container" sinfo "$@"
    ;;
  logs)
    "$engine" logs -f "$container"
    ;;
  ""|-h|--help|help)
    usage
    ;;
  *)
    die "unknown command '$cmd' (try: up, run, exec, sinfo, logs, down)"
    ;;
esac
