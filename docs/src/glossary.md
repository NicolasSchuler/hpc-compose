# Glossary

Core `hpc-compose` terms, in one place. The short version of this list also appears on the Overview page; this page is the fuller reference.

| Term | Meaning |
| --- | --- |
| allocation | The single Slurm job allocation where all of an application's services run. `hpc-compose` compiles one spec into one allocation. |
| artifact bundle | A named group of output paths declared under `x-slurm.artifacts.paths` and exported with `hpc-compose artifacts`. The bundle name `default` is reserved for the top-level `paths`. |
| canary | A short, minimized probe run submitted by `hpc-compose germinate` to gather fresh resource evidence. It writes `latest-canary.json` and leaves normal `latest.json` untouched. |
| cache directory | Shared storage for imported and prepared runtime images, resolved from `x-slurm.cache_dir`, profile/default settings, or `$HOME/.cache/hpc-compose`. For real runs it must be visible from both the submission host and the compute nodes. |
| compose file / spec | The YAML file describing services, runtime backend, and Slurm settings. "Spec" and "compose file" refer to the same thing. |
| context | The resolved view of settings, selected profile, binaries, interpolation variables, and runtime paths for an invocation. Inspect it with `hpc-compose context`. |
| failure policy | Per-service restart behavior under `services.<name>.x-slurm.failure_policy` (`fail_job`, `ignore`, or `restart_on_failure` with bounded retries and a rolling crash-loop window). |
| local mode | Running a plan on the current Linux host through the local Pyxis/Enroot supervisor (`up --local`, `test --local`, `dev`, `tmux`) instead of submitting to Slurm. Single-host and Pyxis-only. |
| preflight | Checks that inspect local tools, paths, backend support, and optional cluster profiles before a run. Run them with `hpc-compose preflight`. |
| prepare | The login-node phase that imports base images and builds prepared runtime artifacts, reused later by `up` and `run`. |
| profile | A named settings block in `.hpc-compose/settings.toml` (compose path, env files, env vars, binaries, cache). Select one with `--profile <name>`. |
| readiness | A gate that holds a dependent service until a probe passes. Types are `sleep`, `tcp`, `http`, and `log`. |
| rendezvous | Same-cluster service discovery through JSON records under the shared cache directory, letting a provider job publish an endpoint that a later client job resolves. Not DNS, auth, or a service mesh. |
| resume | Resume-aware reruns backed by a shared `x-slurm.resume.path` and attempt-aware state, distinct from exported artifact bundles. |
| right-sizing | Comparing requested versus observed resource usage to suggest conservative reductions (`inspect --rightsize`), and the related 0-100 efficiency grade from `hpc-compose score`. |
| runtime backend | The mechanism used to launch services: Pyxis/Enroot, Apptainer, Singularity, or host software. Selected with `runtime.backend`. |
| service | One container or host process in the allocation, defined under `services.<name>`. (`steps` is an accepted alias for `services`.) |
| smoke test | A finite end-to-end run (`hpc-compose test`) where every service must start, pass readiness, and complete successfully. |
| sweep | An embedded `sweep` block expanded by `hpc-compose sweep submit` into many independent tracked allocations, one per trial. |
| tracked job | Metadata under `.hpc-compose/<job-id>/` that lets `status`, `ps`, `watch`, `logs`, `stats`, and `artifacts` reconnect to a run later. |
| `x-runtime.prepare` | The spec block for image-preparation commands and mounts. `x-enroot.prepare` is an accepted legacy alias. |
| `x-slurm` | The spec section for Slurm settings and `hpc-compose` runtime extensions, available at the top level and per service. |

## Related Docs

- [Spec Reference](spec-reference.md)
- [CLI Reference](cli-reference.md)
- [Execution Model](execution-model.md)
- [Quickstart](quickstart.md)
