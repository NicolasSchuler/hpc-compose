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
| Linux `x86_64` | Full CLI and runtime workflows | Requires Slurm client tools plus Enroot and Pyxis on the submission host/cluster |
| Linux `arm64` | Full CLI and runtime workflows | Same cluster requirements as Linux `x86_64` |
| macOS `x86_64` | Authoring and local non-runtime commands | Suitable for project-local authoring flows such as `new`, `setup`, `context`, `validate`, `inspect`, `render`, and `completions`; not for Slurm/Enroot runtime commands |
| macOS `arm64` | Authoring and local non-runtime commands | Same scope as macOS `x86_64` |

## CI-tested

| Platform | What is tested today |
| --- | --- |
| Ubuntu 24.04 `x86_64` | formatting, clippy, unit/integration tests, docs build, link checks, installer smoke tests, and coverage |

Current CI validates project behavior on Ubuntu. Other published builds should be treated as lower-confidence until corresponding CI coverage exists.

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

- Slurm + Enroot + Pyxis runtime workflows are not an officially supported Windows target.
- Issues that are specific to Windows runtime execution may be closed as out of scope until the support policy changes.

## Cluster assumptions for full support

For full runtime support on Linux, the target environment should provide:

- `sbatch`, `srun`, and related Slurm client tools on the submission host
- Pyxis container support in `srun`
- Enroot on the submission host for image import and prepare steps
- shared storage for `x-slurm.cache_dir`

Use [Runbook](runbook.md) and [Execution Model](execution-model.md) before adapting a real workload to a cluster.
