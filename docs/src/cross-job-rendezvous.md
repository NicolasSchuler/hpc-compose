# Cross-Job Rendezvous

`hpc-compose` rendezvous lets independent Slurm jobs coordinate through the shared cache directory. A provider job registers an address under `<cache_dir>/rendezvous/<name>/latest.json`; a later client job resolves that record and receives stable `HPC_COMPOSE_RDZV_*` environment variables.

This is same-cluster shared-storage discovery. It does not create DNS, tunnels, authentication, authorization, or a service mesh. Use it only inside a same-user or trusted shared-project cache boundary.

## Provider

```yaml
name: model-server

x-slurm:
  cache_dir: ${CACHE_DIR}

services:
  model:
    image: python:3.12-slim
    command: python -m http.server 8000
    readiness:
      type: tcp
      port: 8000
    x-slurm:
      rendezvous:
        register:
          name: model-server
          port: 8000
          protocol: http
          path: /
          ttl_seconds: 3600
```

Provider registration is declarative. If readiness is configured, the rendered script registers after the readiness check succeeds. On cleanup, it removes `latest.json` only when the current job still owns the latest record.

## Client

```yaml
name: model-client

x-slurm:
  cache_dir: ${CACHE_DIR}
  rendezvous: model-server

services:
  client:
    image: curlimages/curl:8.10.1
    command: curl -fsS "$HPC_COMPOSE_RDZV_MODEL_SERVER_URL"
```

Clients receive generic variables such as `HPC_COMPOSE_RDZV_URL`, plus name-scoped variables such as `HPC_COMPOSE_RDZV_MODEL_SERVER_URL`, `HPC_COMPOSE_RDZV_MODEL_SERVER_HOST`, and `HPC_COMPOSE_RDZV_MODEL_SERVER_PORT`.

## Debugging CLI

```bash
hpc-compose rendezvous list --cache-dir "$CACHE_DIR"
hpc-compose rendezvous resolve model-server --cache-dir "$CACHE_DIR"
hpc-compose rendezvous register model-server --host node01 --port 8000 --job-id 12345 --cache-dir "$CACHE_DIR"
hpc-compose rendezvous prune --cache-dir "$CACHE_DIR"
```

`register` is mainly for debugging and custom workflows. Normal provider jobs should use `services.<name>.x-slurm.rendezvous.register`.

## TTL and Staleness

Records have a TTL. Resolution ignores expired records, and `prune` removes expired latest and historical JSON files. If the provider job exits cleanly, cleanup removes the latest pointer only if it still points at that job, so a newer provider is not deregistered by an older job finishing later.

## Requirements

- `x-slurm.cache_dir` must point to storage visible from the login node and compute nodes.
- Provider and client jobs must use the same cache directory.
- Names are single safe path components: ASCII letters, digits, `.`, `_`, and `-`.

See [`examples/rendezvous-model-server.yaml`](example-source.md#rendezvous-model-server) and [`examples/rendezvous-client.yaml`](example-source.md#rendezvous-client) for a runnable pair.

## Related Docs

- [Cache Management](cache-management.md)
- [Runtime Observability](runtime-observability.md)
- [Spec Reference](spec-reference.md)
- [Examples](examples.md)
