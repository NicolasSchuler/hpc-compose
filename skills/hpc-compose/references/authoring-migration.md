# Authoring and migration

Read this reference for new specs, repository adaptation, Docker Compose or sbatch migration, topology, dependencies, readiness, and static repair.

## Source-of-truth order

1. Installed CLI help and `hpc-compose --offline docs`.
2. Installed authoring and output schemas from `hpc-compose --offline schema`.
3. Repository evidence named by the probe.
4. Published `/raw/*.md` guidance when installed docs are unavailable.

Do not infer a shipped field or command from a newer skill or `main` checkout when the installed binary differs.

## Choose a starting point

Pass the probe's derived `workload_phrases` to:

```bash
hpc-compose --offline examples recommend "<phrases>" --format json
```

Use the returned registry metadata to choose a built-in template or checked-in example. Do not hard-code an example from framework keywords alone. Confirm the actual entrypoint, image, data paths, topology, runtime, CPU/GPU/memory, walltime, readiness, and completion behavior.

Keep application intent in the spec and user/site policy in settings, a selected profile, environment inputs, or cluster documentation. Never commit secrets, personal absolute paths, or account values without an explicit request.

## Preserve the execution model

One spec becomes one Slurm allocation and one launcher script. Services become steps within that allocation. Use one spec when services share lifecycle and allocation resources. Use separate jobs plus rendezvous or dependencies when lifecycles or allocations differ.

For multi-node work, identify whether one distributed service spans the allocation or multiple services have explicit placement. Confirm the framework's launcher and cluster fabric; do not turn a single-node service into multi-node merely by raising `nodes`.

## Docker Compose migration

Preserve compatible intent: `image`, `command`, `entrypoint`, `environment`, `env_file`, bind `volumes`, `working_dir`, and supported `depends_on` conditions.

Translate deliberately:

| Source | hpc-compose treatment |
| --- | --- |
| `build:` | Immutable `image:` plus backend-neutral `x-runtime.prepare.commands` when preparation is needed. |
| `ports:` | Remove; services share allocation networking. Derive user access separately. |
| service-name DNS | Use `127.0.0.1` only for same-node helpers; use allocation metadata for distributed peers. |
| `healthcheck:` | Use supported `readiness` when downstream startup truly depends on health. |
| `restart:` | Model finite completion or explicit service failure policy. |
| `deploy:` resources | Use first-class top-level or service-level `x-slurm` fields. |
| networks/network_mode | Remove unsupported container-orchestration networking. |

Prefer first-class Slurm fields over `submit_args` and `extra_srun_args`. Raw flags require a verified site-specific gap.

## sbatch migration

Extract evidence before translating:

- `#SBATCH` account, partition/QOS, walltime, topology, CPU, memory, GPU/GRES, signal, array, and output paths;
- module/runtime setup and exported environment;
- the true workload command and whether `srun` or a framework launcher owns fanout;
- stage-in, checkpoint, artifact, and recovery paths.

Keep scheduler policy in `x-slurm`, runtime setup in backend or setup fields, and application commands in services. Preserve shell semantics explicitly; do not paste a large sbatch script into one opaque service command.

## Dependencies and readiness

Plain dependency order means a service started, not that it is healthy. Use supported readiness only when another service needs a stable TCP/HTTP/log/sleep signal. Use successful-completion dependencies for finite preprocessing or postprocessing stages. Bound all readiness and finite-test waits.

## Static repair loop

Inspect each output schema before parsing fields, then use redacted JSON:

```bash
hpc-compose --offline validate -f compose.hpc.yaml --format json
hpc-compose --offline lint -f compose.hpc.yaml --format json
hpc-compose --offline plan -f compose.hpc.yaml --format json
hpc-compose --offline inspect -f compose.hpc.yaml --format json
hpc-compose --offline explain -f compose.hpc.yaml --format json
```

Change one coherent cause at a time. `validate` proves legal shape and semantics, `lint` identifies risky but legal choices, `plan` exposes planned execution, `inspect` exposes normalized topology and mounts, and `explain` gives script provenance without dumping the full script.

Stop after static verification unless the next policy tier is authorized.
