# Roadmap and Non-Goals

This roadmap is intentionally short. `hpc-compose` is not trying to become a general-purpose orchestrator.

The [living product backlog](backlog.md) records item-level `shipped`,
`candidate`, `rejected`, and `superseded` state with evidence. This page owns
only the durable strategic direction and non-goals.

## Authoring Ergonomics

- make the supported Compose subset easier to discover from examples and docs
- keep `validate`, `inspect`, `config`, and `render` as the fast path for authoring confidence
- keep refining starter templates and example selection (now surfaced through `examples recommend`, `search`, and `coverage`) before adding more surface area

## Runtime Visibility

- make tracked jobs easier to reconnect to and reason about
- keep improving `status`, `ps`, `watch`, `stats`, and artifact export for real cluster debugging
- prefer inspectable generated state over hidden orchestration behavior

## Cluster Compatibility

- expand confidence on more Linux cluster environments before broadening scope
- keep support policy explicit through the support matrix
- improve docs and examples around shared storage, Pyxis, and Enroot expectations

If your workflow falls outside this roadmap, that is useful feedback. Open an [adoption feedback issue](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) with your cluster type, workload type, and main friction point.

## Heterogeneous Jobs

`hpc-compose` submits **one** `sbatch` allocation and runs each service as an `srun` step inside it. Partition, account, and QOS are job-level attributes fixed at submission for that single allocation, so they cannot be routed per service. Setting `services.<name>.x-slurm.partition`, `.qos`, or `.account` is therefore a validation error today — set those fields at the top-level `x-slurm` block, which applies to the whole allocation.

Slurm's native mechanism for running components in different partitions, accounts, or QOS within one submission is a **heterogeneous job** (hetjob): `#SBATCH hetjob`-separated components launched with `srun --het-group=…`, each with its own partition/account/qos/nodes/gres. Supporting hetjobs would let a spec bind services to distinct allocation components (for example, a GPU trainer in a `gpu` partition alongside a CPU data loader in a `cpu` partition).

This is a planned epic, not a shipped feature. It reshapes the planner (which currently assumes a single allocation and node set), the `#SBATCH` header renderer, and placement/MPI/rendezvous — so it needs its own design pass. Until then, per-service partition/account/qos routing is intentionally rejected rather than silently ignored.

## Related Docs

- [Full Example Specs](example-source.md)
- [Living Product Backlog](backlog.md)
- [Glossary](glossary.md)
- [Support Matrix](support-matrix.md)
- [Architecture for Contributors](architecture.md)
