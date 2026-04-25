# Cache Management

`x-slurm.cache_dir` stores imported and prepared runtime artifacts. It must be visible from both the submission host and compute nodes.

## Choose A Cache Path

Use a project scratch, work, or shared filesystem path:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$CACHE_DIR"
test -w "$CACHE_DIR"
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

## Prune Cache Entries

Prune old entries by age:

```bash
hpc-compose --profile dev cache prune --age 14
```

Prune artifacts not referenced by the current plan:

```bash
hpc-compose cache prune --all-unused -f compose.yaml
```

Prune one cache directory directly:

```bash
hpc-compose cache prune --age 7 --cache-dir '<shared-cache-dir>'
```

`--age` and `--all-unused` are mutually exclusive.

## After Upgrading

Cache keys include the tool version, so upgrading `hpc-compose` invalidates existing cached artifacts. Expect a full rebuild on the next `prepare` or `up`, then optionally prune old entries:

```bash
hpc-compose cache prune --age 0
```

## Related Docs

- [Quickstart](quickstart.md#4-pick-and-test-cache_dir)
- [Runbook](runbook.md)
- [CLI Reference](cli-reference.md#cache-maintenance)
