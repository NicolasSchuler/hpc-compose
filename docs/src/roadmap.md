# Roadmap

This roadmap is intentionally short. `hpc-compose` is not trying to become a general-purpose orchestrator.

## Authoring Ergonomics

- make the supported Compose subset easier to discover from examples and docs
- keep `validate`, `inspect`, `config`, and `render` as the fast path for authoring confidence
- improve starter templates and example selection before adding more surface area

## Runtime Visibility

- make tracked jobs easier to reconnect to and reason about
- keep improving `status`, `ps`, `watch`, `stats`, and artifact export for real cluster debugging
- prefer inspectable generated state over hidden orchestration behavior

## Cluster Compatibility

- expand confidence on more Linux cluster environments before broadening scope
- keep support policy explicit through the support matrix
- improve docs and examples around shared storage, Pyxis, and Enroot expectations

If your workflow falls outside this roadmap, that is useful feedback. Open an [adoption feedback issue](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) with your cluster type, workload type, and main friction point.
