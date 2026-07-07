# FAQ

## Is hpc-compose a Docker Compose replacement?

No. `hpc-compose` uses a Compose-like YAML shape for the subset that maps cleanly to one Slurm allocation and one generated batch script. It intentionally leaves out Docker Compose features such as `build:`, custom networks, `ports`, `restart`, and `deploy`.

Use [Why hpc-compose](why-hpc-compose.md) and [Slurm Capability Scope](slurm-capability-scope.md) to decide whether the model fits your workload.

## What can I run before touching Slurm?

Use static authoring commands first:

```bash
hpc-compose validate -f compose.yaml
hpc-compose plan -f compose.yaml
hpc-compose plan --show-script -f compose.yaml
hpc-compose inspect -f compose.yaml
```

Those commands are meant for local authoring and do not submit jobs. Before the first real run on a login node, use `hpc-compose debug -f compose.yaml --preflight` to check cluster prerequisites.

## Why must the cache directory be shared storage?

Image preparation happens on the submission host, while services run on compute nodes. The resolved cache directory therefore needs to be visible from both places. Use a shared project, work, or scratch path instead of node-local paths such as `/tmp`, `/var/tmp`, `/private/tmp`, or `/dev/shm`.

See [Manage the Cache and Clean Up](cache-management.md) and [Execution Model](execution-model.md) for the full flow.

## Which runtime backend should I choose?

Use the backend your cluster actually supports:

- Pyxis/Enroot when `srun` exposes container-image support and Enroot is available on the submission host.
- Apptainer or Singularity when that is the site-supported container runtime.
- Host runtime when the workload should run against site modules or already-installed software.

Check [Runtime Backends](runtime-backends.md) and [Support Matrix](support-matrix.md) before assuming a release build is enough for end-to-end runtime support.

## Can hpc-compose run multi-node jobs?

Yes, for constrained Slurm jobs where the allocation is still one application and one generated script. This fits cases such as one distributed service spanning the allocation with explicit placement. It does not try to provide dynamic scheduler-style placement across arbitrary nodes.

For the supported model, start with [Execution Model](execution-model.md), [Slurm Capability Scope](slurm-capability-scope.md), and [Spec Reference](spec-reference.md).

## Can I use an existing docker-compose.yaml directly?

Usually not unchanged. `hpc-compose` can reuse the familiar `services` shape, but unsupported Docker Compose keys need to be removed or translated. Replace `build:` with an `image:` plus `x-runtime.prepare.commands`, and move networking expectations into Slurm-appropriate service readiness and localhost/allocation metadata.

Use [Migrate a docker-compose.yaml](docker-compose-migration.md) for the migration path.

## Where do logs, artifacts, and state live?

Tracked job state lives under `.hpc-compose/` by default. `hpc-compose status`, `ps`, `logs`, `stats`, and `artifacts` read that tracked state so you do not have to reconstruct scheduler paths by hand.

See [Monitor a Run](runtime-observability.md), [Artifacts and Resume](artifacts-and-resume.md), and [Files and Directories](files-and-directories.md).

## How should I report bugs or adoption friction?

Use the issue form that matches the situation:

- [Bug report](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=bug_report.yml) for reproducible CLI, docs, packaging, or runtime bugs.
- [Feature request](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=feature_request.yml) for a proposed workflow, Compose subset, backend, docs, or ergonomics change.
- [Adoption feedback](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) when the tool did or did not fit a real cluster workflow.

For sensitive security issues, use the private reporting process in [SECURITY.md](https://github.com/NicolasSchuler/hpc-compose/blob/main/SECURITY.md) instead of a public issue.

## Related Docs

- [Overview](./)
- [Why hpc-compose](why-hpc-compose.md)
- [Support Matrix](support-matrix.md)
- [Runtime Backends](runtime-backends.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
- [Roadmap and Non-Goals](roadmap.md)
