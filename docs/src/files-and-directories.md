# Files and Directories

`hpc-compose` writes to three independent on-disk roots, and keeping them separate is deliberate. Compose-level **metadata** lives next to the compose file so tracked records travel with your project; **per-job runtime state** lives under a per-job runtime root resolved at submit time; and the **cache** is a content-addressed store shared across jobs and visible from both the login node and the compute nodes. `src/tracked_paths.rs` is the single source of truth for every leaf name documented here, so the layout below matches what tooling reads and writes exactly.

## The three roots at a glance

| Root | Default location | Set with | Scope | Holds |
| --- | --- | --- | --- | --- |
| Metadata directory | `<compose-file-dir>/.hpc-compose/` | (always next to the compose file) | Per compose file | Tracked job records, latest pointers, sweep manifests |
| Per-job runtime root | `<submit-dir>/.hpc-compose/<job-id>/` | `x-slurm.runtime_root` | Per job | Logs, metrics, artifacts, allocation files, state |
| Cache directory | `$HOME/.cache/hpc-compose/` | `x-slurm.cache_dir` | Shared across jobs | Content-addressed images, enroot caches, rendezvous records |

The metadata directory and the *default* per-job runtime root share the same `.hpc-compose/` directory name, but they are addressed independently: the metadata root is anchored to the compose file's directory, while the runtime root is anchored to the submit directory (and is overridable). They coincide only when you submit from the directory that holds the compose file and leave `x-slurm.runtime_root` unset.

## Metadata directory

The metadata directory sits next to the compose file (`metadata_root_for` joins `.hpc-compose` onto the compose file's parent). It holds the durable record of every submission plus the latest-pointers that let follow-up commands reconnect without resubmitting.

```text
<compose-file-dir>/.hpc-compose/
├── latest.json              # most recent `up` (main) submission record
├── latest-run.json          # most recent `run` submission record
├── latest-canary.json       # most recent `germinate` canary record
├── latest-notebook.json     # most recent `notebook` server record
├── jobs/
│   └── <job-id>.json        # one tracked SubmissionRecord per submitted job
└── sweeps/
    ├── latest.json          # most recent sweep manifest pointer
    └── <sweep-id>/
        └── sweep.json       # per-sweep manifest
```

| Leaf | Kind | Contents |
| --- | --- | --- |
| `latest.json` | file | `SubmissionRecord` for the most recent `up` (main-kind) submission. |
| `latest-run.json` | file | `SubmissionRecord` for the most recent `run` submission. |
| `latest-canary.json` | file | `SubmissionRecord` for the most recent `germinate` canary submission. |
| `latest-notebook.json` | file | `SubmissionRecord` for the most recent tracked `notebook` submission. |
| `jobs/<job-id>.json` | file | The authoritative `SubmissionRecord` for one job, keyed by Slurm job id. |
| `sweeps/latest.json` | file | Pointer to the most recent sweep manifest. |
| `sweeps/<sweep-id>/sweep.json` | file | Manifest describing one sweep and its trials. |

A `SubmissionRecord` carries the paths the runtime root resolves to, including `runtime_root` (the resolved `x-slurm.runtime_root` override, present only when set), `batch_log`, `batch_log_managed`, and `service_logs` (the authoritative service-name to log-path map; see [Log lifecycle](#log-lifecycle)). The current `SubmissionRecord` schema version is `3`. Records written by schema 3 persist the `runtime_root` override when one was set; older records that lack the field fall back to the default `<submit-dir>/.hpc-compose` layout when read.

## Per-job runtime root

Each job gets its own runtime root: `<runtime-root>/<job-id>/`, where `<runtime-root>` defaults to `<submit-dir>/.hpc-compose` (`runtime_root_for`) and is overridable with `x-slurm.runtime_root` (`resolve_runtime_root`). The renderer resolves this to an **absolute path at submit time and bakes it into the rendered `JOB_ROOT`**, so a running job never depends on `$SLURM_SUBMIT_DIR` being set or shared-visible at compute-node runtime. A relative `x-slurm.runtime_root` resolves against the submit directory; an absolute one is used as-is.

```text
<runtime-root>/
├── logs/
│   ├── hpc-compose-%j.out        # default batch log (job-id, Slurm-expanded)
│   └── <service-token>.log       # one log per service (see Log lifecycle)
└── <job-id>/
    ├── state.json                # job state snapshot (latest view)
    ├── logs/
    │   └── <service-token>.log   # per-service logs, latest attempt
    ├── metrics/
    │   ├── meta.json
    │   ├── gpu.jsonl
    │   ├── gpu_processes.jsonl
    │   ├── slurm.jsonl
    │   ├── diagnostics/
    │   └── gpu-node-samples/
    ├── artifacts/
    │   ├── manifest.json
    │   └── payload/
    ├── allocation/
    │   ├── primary_node
    │   ├── nodes.txt
    │   ├── service-nodelists/
    │   ├── mpi-hostfiles/
    │   └── distributed-hostfiles/
    ├── service-exits/
    ├── hooks/
    └── attempts/                 # resume-aware runs only
        └── <n>/                  # logs/, metrics/, artifacts/, state.json per attempt
```

| Leaf (under `<job-id>/`) | Kind | Contents |
| --- | --- | --- |
| `state.json` | file | Latest-view job state snapshot used by `status` and friends. |
| `logs/<service-token>.log` | file | One log per service for the latest attempt; the filename is encoded (see below). |
| `metrics/meta.json` | file | Metrics collection metadata. |
| `metrics/gpu.jsonl` | file | Per-sample GPU metrics. |
| `metrics/gpu_processes.jsonl` | file | Per-sample GPU process attribution. |
| `metrics/slurm.jsonl` | file | Slurm step statistics samples. |
| `metrics/diagnostics/` | dir | Collected diagnostic artifacts. |
| `metrics/gpu-node-samples/` | dir | Per-node GPU sample files. |
| `artifacts/manifest.json` | file | Manifest describing exported artifacts. |
| `artifacts/payload/` | dir | The exported artifact payload tree. |
| `allocation/primary_node` | file | Hostname of the primary allocation node. |
| `allocation/nodes.txt` | file | The full allocation node list. |
| `allocation/service-nodelists/` | dir | Per-service node lists. |
| `allocation/mpi-hostfiles/` | dir | Generated MPI hostfiles. |
| `allocation/distributed-hostfiles/` | dir | Generated distributed (torchrun-style) hostfiles. |
| `service-exits/` | dir | Per-service exit markers (`<service>.jsonl`). |
| `hooks/` | dir | Materialized prologue/epilogue/event hook scripts and their manifest. |
| `attempts/<n>/` | dir | Per-attempt copies of `logs/`, `metrics/`, `artifacts/`, and `state.json` for resume-aware runs. These per-attempt `state.json` files are the data source for `hpc-compose checkpoints` attempt/requeue history. |

The batch script keeps the root-level `logs/`, `metrics/`, `artifacts/`, and `state.json` as the "latest" view (it updates them to point at the most recent attempt) so status and export commands read the latest attempt without reconstructing shell logic.

### Default batch log location

When you do not set `x-slurm.output`, real submissions get a baked `--output` directive at `<runtime-root>/logs/hpc-compose-%j.out`. Note that this parent is **job-id-free** (`<runtime-root>/logs/`, not under `<runtime-root>/<job-id>/`), because Slurm opens `--output` before the script body runs, so the CLI pre-creates that directory host-side before `sbatch`. The default basename deliberately avoids `%x` so a raw job name can never become a path component; `%j` is expanded by Slurm. Setting `x-slurm.output` replaces this default entirely. Dry-run previews (`inspect`, `spec render`) keep the portable Slurm default instead of a baked absolute path so committed example renders stay machine-independent.

## Cache directory

The cache directory defaults to `$HOME/.cache/hpc-compose/` and is set with `x-slurm.cache_dir` (resolved with the precedence documented in [Spec Reference](spec-reference.md)). It must be visible from both the login node and the compute nodes. Image artifacts are **content-addressed**: the filename embeds a short hash of the cache key, so identical inputs reuse the same artifact across jobs and machines.

```text
<cache_dir>/
├── base/
│   ├── <hash>-<label>.sqsh        # imported base image
│   ├── <hash>-<label>.sqsh.json   # manifest sidecar
│   └── <hash>-<label>.sqsh.json.lock  # advisory-lock sidecar
├── prepared/
│   ├── <hash>-<name>.sqsh         # prepared runtime image
│   └── <hash>-<name>.sqsh.json    # manifest sidecar
├── enroot/                        # login-node shared enroot store
│   ├── cache/
│   ├── data/
│   └── tmp/
├── runtime/
│   └── <job-id>/                  # per-job compute-node enroot runtime cache
│       ├── cache/                 # ENROOT_CACHE_PATH
│       ├── data/                  # ENROOT_DATA_PATH
│       └── tmp/                   # ENROOT_TEMP_PATH
└── rendezvous/
    └── <name>/
        ├── latest.json            # current provider for this rendezvous name
        └── <token>.json           # historical per-registration records
```

| Leaf | Kind | Contents |
| --- | --- | --- |
| `base/<hash>-<label>.sqsh` | file | A base image imported from a remote reference, named by `<short-hash>-<label>`. |
| `base/<hash>-<label>.sqsh.json` | file | Manifest tracking the cache entry. |
| `base/<hash>-<label>.sqsh.json.lock` | file | Advisory-lock sidecar that serializes concurrent manifest read-modify-write. |
| `prepared/<hash>-<name>.sqsh` | file | A prepared runtime image derived from a base image plus prepare steps, named by `<short-hash>-<service-name>`. |
| `prepared/<hash>-<name>.sqsh.json` | file | Manifest tracking the prepared entry. |
| `enroot/cache/`, `enroot/data/`, `enroot/tmp/` | dir | The shared login-node enroot store used during host-side prepare. |
| `runtime/<job-id>/{cache,data,tmp}/` | dir | The per-job compute-node enroot runtime cache; the renderer exports `ENROOT_CACHE_PATH`/`ENROOT_DATA_PATH`/`ENROOT_TEMP_PATH` at these paths (`enroot_runtime_job_dir`). Namespaced by job id so removing it never touches the shared cache root. |
| `rendezvous/<name>/latest.json` | file | The current provider record for one rendezvous name (atomic latest pointer). |
| `rendezvous/<name>/<token>.json` | file | Historical per-registration records, retained until TTL expiry or owner cleanup. |

Manifest `.lock` sidecars carry no data and only serialize writers; the manifest JSON next to each artifact is the persisted record. See [Connect Jobs Across Allocations](cross-job-rendezvous.md) for how rendezvous records are produced and resolved.

## Environment variables that affect paths

`hpc-compose` both reads some path-affecting variables from your environment and sets others into the running job. The table below consolidates the relevant ones.

| Variable | Direction | Effect |
| --- | --- | --- |
| `HOME` | Read from environment | Anchors the default cache directory (`$HOME/.cache/hpc-compose`) when `x-slurm.cache_dir` is unset. |
| `SLURM_SUBMIT_DIR` | Read from environment | Now only a **preview fallback**: dry-run renders use `${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose` for `JOB_ROOT`. Real submissions bake an absolute runtime root, so the running job no longer depends on it. |
| `SLURM_JOB_ID` | Read from environment (set by Slurm) | Selects the per-job runtime root (`JOB_ROOT/<job-id>`) and the per-job enroot runtime dir (`runtime/<job-id>`); expanded into `%j` in the default batch log. |
| `ENROOT_CACHE_PATH` | Set by hpc-compose | Exported to `<cache_dir>/runtime/<job-id>/cache` in the rendered batch script. |
| `ENROOT_DATA_PATH` | Set by hpc-compose | Exported to `<cache_dir>/runtime/<job-id>/data`. |
| `ENROOT_TEMP_PATH` | Set by hpc-compose | Exported to `<cache_dir>/runtime/<job-id>/tmp`. |
| `HPC_COMPOSE_BACKEND_OVERRIDE` | Read from environment | Selects the runtime backend used by the batch script (defaults to `slurm`). |
| `HPC_COMPOSE_DEV_CONTROL_DIR` | Read from environment | When set, enables the dev control directory used for live restart requests during local smoke-tests. |
| `HPC_COMPOSE_SERVICE_LOG` | Set by hpc-compose | Points each service and its hooks at the in-container path of that service's log file. |
| `HPC_COMPOSE_RESUME_DIR` | Set by hpc-compose | The in-container path of the resume directory for resume-aware runs. |

During login-node prepare the same enroot variables are pointed at the shared `<cache_dir>/enroot/{cache,data,tmp}` store rather than the per-job `runtime/<job-id>` store. The full set of `HPC_COMPOSE_*` runtime variables injected into services (distributed, rendezvous, MPI, scratch, and hook variables) is described in [Monitor a Run](runtime-observability.md) and the feature guides.

## Cleanup scope

Different commands reap different subsets of these roots. The table is precise about what each one deletes and what it leaves intact.

| Command / mechanism | Deletes | Preserves |
| --- | --- | --- |
| `down` (a.k.a. `cancel`) | The job's tracked record `jobs/<job-id>.json`, the per-job runtime root `<runtime-root>/<job-id>/`, the hpc-compose-managed default batch log when `x-slurm.output` was not set, the per-job enroot dir `<cache_dir>/runtime/<job-id>/`, and this job's owned rendezvous records. Repairs the latest pointers afterward. | Other jobs' records and runtime roots, user-pinned `x-slurm.output` files, the shared cache root, `base/`/`prepared/` artifacts, and other jobs' rendezvous records. |
| `jobs clean` | The same per-job state as `down` for each reaped record (tracked record, per-job runtime root, managed default batch log, per-job enroot dir, owned rendezvous records), selected by `--age DAYS` or `--all` (all except the latest). | The retained records and their runtime roots, user-pinned `x-slurm.output` files, the shared cache root, and content-addressed artifacts. |
| Batch teardown trap (`x-slurm.cleanup.runtime_cache`) | Only the per-job enroot runtime cache (`ENROOT_CACHE_PATH`/`DATA_PATH`/`TEMP_PATH` under `runtime/<job-id>/`), and only when the policy opts in. Default is `never`; `on_success` runs only on exit code 0; `always` runs on every clean exit. | Everything else. Because cancelled or crashed jobs never run the trap, host-side `down`/`jobs clean` are the reliable reapers of `runtime/<job-id>`. |
| `cache prune` (`--age DAYS` or `--all-unused`) | Content-addressed artifacts (`base/` and `prepared/` entries plus their manifest/lock sidecars) that are expired or no longer referenced, and now-empty parent directories left behind. | The cache root itself (never removed), still-referenced artifacts, and non-empty parent directories. |
| `down --purge-cache` | In addition to the per-job teardown above, the cached artifacts attributed to this submission. | The shared cache root and artifacts belonging to other jobs. |
| `sweep` cleanup | Tracked sweep trial records and per-trial runtime state, consistent with `jobs clean`. | The sweep manifest history under `sweeps/` unless explicitly removed, and the cache. |
| `rendezvous prune` | Expired rendezvous records (latest and historical) across all names. | Live `latest.json` pointers and other jobs' unexpired records. |

Two things to keep in mind: tracked metadata records live next to the compose file while the managed default batch log lives under `<runtime-root>/logs/`, so cleanup uses the persisted record to remove only the log hpc-compose owns; and the per-job enroot dir is namespaced by job id, so reaping it can never touch the shared cache root or another job's runtime cache.

## Log lifecycle

The **default batch log** (sbatch stdout/stderr) is `<runtime-root>/logs/hpc-compose-%j.out` unless you set `x-slurm.output` (see [Default batch log location](#default-batch-log-location)).

**Service logs** are written one-per-service under `<job-id>/logs/`. The filename is produced by a reversible token encoding of the service name: each non-alphanumeric byte becomes an `_x{hh}_` hex sequence. For example, `db.primary` (the `.` is byte `0x2e`) becomes `db_x2e_primary.log`. Do not parse these filenames by hand; the authoritative service-name to log-path map is `SubmissionRecord.service_logs`, which `logs`, `watch`, and `replay` read.

For **resume-aware runs**, each attempt's logs and state are preserved under `attempts/<n>/`, while the root-level `logs/`/`state.json` track the latest attempt.

Automatic size-based log rotation is **not yet implemented**. There is no `x-slurm.logs` key; cap log volume from inside your service command (for example by limiting verbosity or rotating within your own process) if a long-running service can produce unbounded output.

## Related Docs

- [Spec Reference](spec-reference.md)
- [Architecture for Contributors](architecture.md)
- [Monitor a Run](runtime-observability.md)
- [Manage the Cache and Clean Up](cache-management.md)
- [Operate a Real Cluster Run](runbook.md)
