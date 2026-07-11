#!/usr/bin/env bash
# Run one opt-in local dev-cluster case without changing the CI E2E matrix.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/devcluster_case.sh --list
       scripts/devcluster_case.sh <case>

Cases:
  preemption   real USR1 checkpoint -> scontrol requeue -> resumed attempt
  fs-probes    real preflight --fs-probes sbatch --wait round trip
  remote-reads remote status/stats/logs/score/pull over the SSH stand-in

The cluster stays up by default so consecutive cases reuse it. Set
DEVCLUSTER_E2E_DOWN=1 to tear it down after the selected case.
EOF
}

case_name="${1:-}"
if [[ "$case_name" == "--list" ]]; then
  usage
  exit 0
fi
if [[ -z "$case_name" || $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

case "$case_name" in
  preemption|fs-probes|remote-reads) ;;
  *)
    printf 'unknown dev-cluster case: %s\n\n' "$case_name" >&2
    usage >&2
    exit 2
    ;;
esac

# The single-node cluster cannot isolate two mutating local case runs. A mkdir
# lock is portable across macOS/Linux and disappears through the EXIT trap.
lock_dir="$repo_root/.tmp/devcluster-case.lock"
mkdir -p "$repo_root/.tmp"
if ! mkdir "$lock_dir" 2>/dev/null; then
  printf 'another local dev-cluster case appears to be running (%s)\n' "$lock_dir" >&2
  printf 'remove the directory only after confirming that run is no longer active\n' >&2
  exit 1
fi
trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT

case "$case_name" in
  preemption|fs-probes)
    "$repo_root/scripts/devcluster_local_case.sh" "$case_name"
    ;;
  remote-reads)
    DEVCLUSTER_REMOTE_READS=1 DEVCLUSTER_CASE_LOCAL_ONLY=1 \
      "$repo_root/scripts/devcluster_remote_e2e.sh"
    ;;
esac
