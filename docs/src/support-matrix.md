# Support Matrix

This page separates what `hpc-compose` can build, what CI currently exercises, and what is officially supported for real workflows.

## Support levels

| Level | Meaning |
| --- | --- |
| Officially supported | Maintained target for user-facing workflows and issue triage |
| CI-tested | Exercised in the repository's automated checks today |
| Release-built | Prebuilt archive is published, but that is not a promise of full runtime support |

## Officially supported

| Platform | Scope | Notes |
| --- | --- | --- |
| Linux `x86_64` | Full CLI and runtime workflows | Requires Slurm client tools plus at least one supported runtime backend: Pyxis/Enroot, Apptainer, Singularity, or host software modules |
| Linux `arm64` | Full CLI and runtime workflows | Same cluster requirements as Linux `x86_64` |
| macOS `x86_64` | Authoring and local non-runtime commands | Suitable for project-local authoring flows such as `new`, `setup`, `context`, `plan`, `validate`, `inspect`, `render`, and `completions`; not for Slurm/Enroot runtime commands |
| macOS `arm64` | Authoring and local non-runtime commands | Same scope as macOS `x86_64` |

## CI-tested

| Platform | What is tested today |
| --- | --- |
| Ubuntu 24.04 `x86_64` | formatting, clippy, unit/integration tests, docs build, link checks, installer smoke tests, and coverage |
| Ubuntu 24.04 `arm64` | native arm64 authoring-focused tests plus validate, render, and schema command smoke tests |
| macOS `arm64` | authoring-focused tests, validate/render/schema smoke tests, installer smoke tests, and Homebrew smoke tests |
| macOS `x86_64` | authoring-focused tests, validate/render/schema smoke tests, and Homebrew smoke tests |

### Linux arm64 evidence limits

Linux arm64 remains officially supported and has two concrete distribution
signals: a native `linux-arm64-authoring` CI job runs on `ubuntu-24.04-arm`, and
the release workflow builds a Linux arm64 archive. Runtime behavior is
principally exercised through the portable fake-tool suites used across the
project. There is currently **no real arm64 Slurm/backend end-to-end CI lane**;
the privileged local Slurm dev-cluster lane runs on Linux x86_64. Do not cite
the authoring job or release archive as evidence of real arm64 cluster runtime
coverage.

Current CI validates full runtime-facing behavior on Ubuntu x86_64,
authoring-focused behavior on Linux arm64, and authoring/distribution behavior
on macOS. Other published builds should be treated as lower-confidence until
corresponding CI coverage exists.

## Release-built

| Platform | Status |
| --- | --- |
| Linux `x86_64` | Release archive published |
| Linux `arm64` | Release archive published |
| macOS `x86_64` | Release archive published |
| macOS `arm64` | Release archive published |
| Windows `x86_64` | Release archive published, but runtime workflows are not officially supported |

## Windows status

Windows archives are published so users can inspect the CLI surface or experiment with non-runtime commands, but Windows is currently **release-built only**:

- Slurm plus HPC runtime workflows are not an officially supported Windows target.
- Issues that are specific to Windows runtime execution may be closed as out of scope until the support policy changes.

## Cluster assumptions for full support

For full runtime support on Linux, the target environment should provide:

- `sbatch`, `srun`, and related Slurm client tools on the submission host
- one supported runtime path:
  - Pyxis container support in `srun` plus Enroot on the submission host,
  - Apptainer on the submission host and compute nodes,
  - Singularity on the submission host and compute nodes,
  - or module/vendor software available on the host runtime path
- shared storage for the resolved cache directory

Use [Runtime Backends](runtime-backends.md), [Runbook](runbook.md), and [Execution Model](execution-model.md) before adapting a real workload to a cluster.

## Related Docs

- [Installation](installation.md)
- [Quickstart](quickstart.md)
- [Why hpc-compose](why-hpc-compose.md)
- [Runtime Backends](runtime-backends.md)
- [Runbook](runbook.md)
