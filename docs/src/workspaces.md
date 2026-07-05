# Manage Cluster Workspaces

Many HPC sites (for example KIT's HoreKa and HAICORE) manage scratch storage
with the [hpc-workspace](https://github.com/holgerBerger/hpc-workspace) tools:
`ws_allocate <name> [days]` creates an expiring directory, `ws_find <name>`
prints its path, `ws_extend <name> <days>` renews it, `ws_release <name>`
frees it, and `ws_list` shows workspaces with their remaining lifetime.
Without tooling, you hand-run these commands and paste the resulting path into
your cache configuration — and a forgotten `ws_extend` silently expires the
directory that holds your cache, datasets, and checkpoints.

The `workspace` command group makes that lifecycle a first-class part of
hpc-compose. Configure the workspace name once in settings, then let
`hpc-compose workspace ...` drive the site tools for you.

> **Phase 1 scope.** These commands run the `ws_*` tools locally — on the
> login node, or on a dev machine with fake tools. Deeper integration
> (up/preflight expiry checks, auto-allocate/auto-extend at submit time,
> `setup` prompting, and `--remote` support from a laptop) is planned as a
> follow-up; the `auto_allocate`, `auto_extend`, `warn_days_left`, and
> `queue_buffer_days` settings below are already defined for that phase.

## Configure the workspace

Add a `workspace` block to `.hpc-compose/settings.toml`, either under a
profile or under the shared defaults (a profile block overrides
`defaults.workspace` per field):

```toml
version = 1
default_profile = "haicore"

[profiles.haicore.workspace]
name = "hpc-compose-cache"   # passed to ws_allocate / ws_find / ...
duration_days = 30           # ws_allocate / ws_extend default

# Reserved for the submit-time integration phase:
# auto_allocate = true
# auto_extend = true
# warn_days_left = 7
# queue_buffer_days = 2
```

Only `name` (and optionally `duration_days`) matters for the commands on this
page. The workspace *path* is never stored in `settings.toml` or
`cluster.toml`; resolved facts land in the per-profile state file described
below.

## The four commands

| Command | What it does |
| --- | --- |
| `workspace status` | Read-only: `ws_find` + `ws_list`, refreshes the state file, prints name, path, remaining lifetime, and available extensions. |
| `workspace allocate [--duration-days <N>]` | Idempotent: reports an existing workspace as already allocated; otherwise runs `ws_allocate`, confirms with `ws_find`, and records the result. |
| `workspace extend [--days <N>]` | Runs `ws_extend` and refreshes the recorded expiry and remaining extensions. |
| `workspace release [--yes]` | Destructive: prompts for confirmation and refuses while tracked jobs keep cache or runtime state under the workspace. |

```bash
hpc-compose workspace status
hpc-compose workspace allocate
hpc-compose workspace extend --days 30
hpc-compose workspace release --yes
```

All four accept `--format json` (see
[JSON Output Stability](json-output-stability.md)) and the global
`--profile <name>` selector. The `ws_*` executables resolve from `PATH` by
default and can be overridden per invocation with `--ws-find-bin`,
`--ws-allocate-bin`, `--ws-extend-bin`, `--ws-release-bin`, and
`--ws-list-bin` — the same pattern as the Slurm `--sbatch-bin` overrides.

Notes on behavior:

* `allocate` always checks `ws_find` first and only runs `ws_allocate` for a
  missing workspace. Re-allocating an existing workspace errors on some
  hpc-workspace versions and silently extends on others, so hpc-compose never
  relies on either behavior.
* `status` treats `ws_list` as best-effort: expiry is computed from the
  `remaining time` field relative to now (locale-formatted expiration dates
  are carried as display text, not parsed), and a missing or unparsable
  field degrades to "unknown" instead of failing.
* `release` scans the tracked job records for the active compose context and
  refuses to free the workspace while any record's cache or runtime root lies
  under it — run `hpc-compose down --job-id <id>` or `hpc-compose clean`
  first.

## The state file

Resolved facts are persisted per profile in
`.hpc-compose/workspace-state.toml`, next to `settings.toml`:

```toml
version = 1

[profiles.haicore]
name = "hpc-compose-cache"
path = "/hkfs/work/workspace/scratch/ab1234-hpc-compose-cache"
expiry_epoch = 1785830400
extensions_remaining = 3
last_checked = 1783240000
```

Every `workspace` command refreshes this file from the `ws_*` tools; the
submit-time integration phase reads it to warn before a workspace expires
under a queued job. It is safe to delete — the next `workspace status`
rebuilds it.

## Related pages

* [HAICORE@KIT Guide](haicore-guide.md) — the site this workflow was built
  around, including which paths belong in a workspace.
* [Manage the Cache and Clean Up](cache-management.md) — what lives under
  `cache_dir` inside the workspace.
* [CLI Reference](cli-reference.md) — the full command surface.
