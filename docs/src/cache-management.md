# Manage the Cache and Clean Up

The resolved cache directory stores imported and prepared runtime artifacts. It comes from explicit `x-slurm.cache_dir`, then profile/default settings, then `$HOME/.cache/hpc-compose`. For real cluster runs, it must be visible from both the submission host and compute nodes; see [Execution Model](execution-model.md) for why prepared artifacts must live on shared storage.

## Choose A Cache Path

Use a project scratch, work, or shared filesystem path:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
```

You can record that path in project settings instead of every compose file:

```bash
hpc-compose setup --profile-name dev --cache-dir "$CACHE_DIR" --default-profile dev --non-interactive
```

Do not use `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`. Validation may accept those strings, but `preflight` reports them as unsafe because compute nodes must reuse artifacts prepared before submission.

## Inspect Cache State

```bash
hpc-compose cache list
hpc-compose cache inspect -f compose.yaml
hpc-compose cache inspect -f compose.yaml --service app
```

Use `cache inspect` to answer:

- which artifact is being reused
- whether a prepared image came from a cached manifest
- whether a service rebuilds on every prepare because prepare mounts are present

## Staged-Input Cache (Datasets/Models)

Staged datasets and models live in a content-addressed store under the same shared cache root, at `cache_dir/datasets/<key>` and `cache_dir/models/<key>`. The key is derived from the input spec (its source URI and pinned revision), so identical staged inputs are materialized once and reused on every later run. Each staged directory carries a sidecar manifest (`<key>.dataset.json` or `<key>.model.json`) so `cache list` and `cache prune` cover staged inputs alongside image artifacts.

The store itself never fetches anything: it is a pure on-disk store, and the actual fetch and materialization (network) is approval-gated and introduced by the `hf://` stage-in work, not run automatically by `cache`, `plan`, or `prepare`.

## Prune Cache Entries

Prune old entries by age:

```bash
hpc-compose --profile dev cache prune --age 14 --yes
```

Prune artifacts not referenced by the current plan:

```bash
hpc-compose cache prune --all-unused -f compose.yaml --yes
```

Prune one cache directory directly:

```bash
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>' --yes
```

`--age` and `--all-unused` are mutually exclusive.

## Rendezvous Records

Cross-job rendezvous records live under the same shared cache root and are pruned separately (`rendezvous list`, `rendezvous prune`). See [Cross-Job Rendezvous](cross-job-rendezvous.md) for placement, TTL, and ownership rules.

## Clean Up Old Tracked Runs

Tracked job metadata and logs accumulate in `.hpc-compose/`. Preview disk usage and cleanup before deleting:

```bash
hpc-compose jobs list --disk-usage
hpc-compose clean -f compose.yaml --age 7 --dry-run
hpc-compose clean -f compose.yaml --age 7
```

For long-lived project directories, use deep cleanup to audit the residue that is
not covered by ordinary tracked-job selection alone:

```bash
hpc-compose clean -f compose.yaml --age 7 --deep --dry-run --disk-usage
hpc-compose clean -f compose.yaml --age 7 --deep --yes
```

`--deep` keeps the same tracked-job selection (`--age DAYS` or `--all`), and
adds expired rendezvous records plus unreferenced per-job enroot runtime dirs
under `cache_dir/runtime/<job-id>`. It does not prune content-addressed cache
artifacts; use `cache prune` for `base/`, `prepared/`, dataset, and model cache
entries.

## After Upgrading

Cache keys include the tool version, so upgrading `hpc-compose` invalidates existing cached artifacts. Expect a full rebuild on the next `prepare` or `up`, then optionally prune old entries:

```bash
hpc-compose cache prune --age 0 --yes
```

## Related Docs

- [Operate a Real Cluster Run](runbook.md)
- [Monitor a Run](runtime-observability.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Execution Model](execution-model.md)
- [Cross-Job Rendezvous](cross-job-rendezvous.md)
- [CLI Reference](cli-reference.md#cache-maintenance)
