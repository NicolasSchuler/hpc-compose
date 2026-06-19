# Governance

This document describes, honestly, how `hpc-compose` is maintained today and how
that can change as adoption grows.

## Current model

`hpc-compose` is currently a single-maintainer project. Nicolas Schuler reviews
and merges changes, cuts releases, and triages issues. Decisions are made in the
open through GitHub issues and pull requests. There is no formal steering body.

This keeps the project simple, but it also means review and release cadence are
bounded by one person's availability. The scope is intentionally narrow (see the
README "Scope" section), which helps keep the maintenance surface manageable.

## How HPC facilities can participate

The project is built for HPC and research-computing teams, and contributions from
those teams are welcome:

- **Co-maintainers.** If your facility relies on `hpc-compose` and is willing to
  share review and release work, open an issue proposing co-maintainership. We can
  add reviewers and codeowners for the areas your team supports.
- **Cluster-specific feedback.** Adoption-feedback issues that describe a real
  cluster, workload, and friction point directly shape priorities.
- **Forking.** The project is MIT-licensed. If your facility needs a divergent
  policy, a faster release cadence, or site-specific behavior, you are free to
  fork and adapt it. Upstreaming useful, general changes is appreciated but not
  required.

## Security and supported versions

Security fixes target the latest published release and the current `main` branch.
Older releases may receive guidance but should not be assumed to receive
backported fixes. See [SECURITY.md](SECURITY.md) for the reporting process.

## Sustainability

Because the project is single-maintainer today, the most important sustainability
levers are: keeping scope narrow, keeping the dependency set minimal, keeping the
build reproducible, and documenting enough that a facility could co-maintain or
fork with low ramp-up cost. Contributions that reduce maintenance burden are
weighted accordingly.
