# Launch Checklist

Use one message across every channel:

> `hpc-compose` turns a Compose-like spec into one Slurm job for multi-service HPC and research ML workflows.

Supporting line:

> Docker-Compose-like ergonomics on Slurm without adding Kubernetes or a custom control plane.

## Before the Release

- confirm the README opening still matches the current positioning
- confirm the four promoted examples are the right funnel:
  - `minimal-batch.yaml`
  - `app-redis-worker.yaml`
  - `llm-curl-workflow-workdir.yaml`
  - `training-resume.yaml`
- confirm the support matrix still reflects what is actually supported
- update the release notes using `.github/RELEASE_TEMPLATE.md`
- choose one primary link to reuse everywhere:
  - docs overview
  - examples page
  - or `running-compose-style-workflows-on-slurm.md`

## Lightweight Launch Round

Do these in order, not all at once:

1. publish the GitHub release with polished notes
2. post one technical launch thread to Hacker News or Lobsters
3. post one community-specific version to an HPC / Slurm / research computing forum
4. optionally reuse the same message on LinkedIn, X, or Mastodon if already active there

## What To Emphasize

- Slurm-first scope
- one inspectable generated job
- multi-service workflows inside one allocation
- explicit product boundary
- examples and quickstart, not just reference docs

## What Not To Claim

- full Docker Compose compatibility
- general orchestration
- cluster-side control plane behavior
- arbitrary multi-node service placement

## Track After Posting

- README and docs traffic
- release downloads
- new issue or discussion threads from real users
- repeated adoption patterns from feedback:
  - cluster type
  - workload type
  - friction point
