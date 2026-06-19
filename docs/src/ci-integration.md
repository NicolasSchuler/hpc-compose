# CI Integration

`hpc-compose` ships fast, authoring-time commands (`validate`, `lint`) that are well-suited to pre-commit hooks and CI. This page covers three drop-in integrations: a [pre-commit](https://pre-commit.com) hook, a reusable GitHub Actions workflow, and a GitLab CI snippet.

All integrations require the `hpc-compose` binary to be installed first (see [Installation](installation.md)):

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Pin `RELEASE_TAG` to a release from the [GitHub Releases](https://github.com/NicolasSchuler/hpc-compose/releases) page.

## Pre-commit

The repository ships a `.pre-commit-hooks.yaml` defining two local hooks that run `hpc-compose validate` and `hpc-compose lint` against `compose.yaml`. Because `hpc-compose` is not distributed via `pip`, the hooks use `language: system` and require the binary to already be on `PATH`.

Add this repo to your `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/NicolasSchuler/hpc-compose
    rev: v0.1.45  # pin to a release tag
    hooks:
      - id: hpc-compose-validate
      - id: hpc-compose-lint
```

By default the hooks run when `compose.yaml` is staged. To point at a different filename, override `entry` and `files`:

```yaml
      - id: hpc-compose-lint
        entry: hpc-compose lint -f deploy/compose.yaml --allow-warnings
        files: ^deploy/compose\.yaml$
```

- `hpc-compose-validate` fails on any spec error.
- `hpc-compose-lint` passes with `--allow-warnings` (warnings are advisory). Use `hpc-compose-validate` plus a strict CI lint (below) to enforce both.

## GitHub Actions

### Reusable workflow

The simplest integration calls the maintained reusable workflow, which installs a pinned release and runs validate + lint:

```yaml
jobs:
  hpc-compose:
    uses: NicolasSchuler/hpc-compose/.github/workflows/hpc-compose-lint.yml@v0.1.45
    with:
      compose-file: compose.yaml
      version: v0.1.45
      strict: true
```

Set `strict: true` to fail on lint warnings, or `strict: false` (default) to allow warnings. Pin both the `uses:` ref and `version` to the same release tag.

### Inline snippet

For repos that prefer an inline step:

```yaml
jobs:
  lint:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - name: Install hpc-compose
        run: |
          set -euo pipefail
          curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/v0.1.45/install.sh" \
            | env HPC_COMPOSE_VERSION="v0.1.45" sh
          echo "$HOME/.local/bin" >> "$GITHUB_PATH"
      - run: hpc-compose validate -f compose.yaml
      - run: hpc-compose lint -f compose.yaml --allow-warnings
```

## GitLab CI

GitLab runners typically do not provide `hpc-compose`, so install it inside the job first:

```yaml
hpc-compose-lint:
  image: alpine:3.20
  rules:
    - changes: [compose.yaml]
  variables:
    HPC_COMPOSE_VERSION: v0.1.45
  before_script:
    - apk add --no-cache curl ca-certificates
    - |
      curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${HPC_COMPOSE_VERSION}/install.sh" \
        | env HPC_COMPOSE_VERSION="${HPC_COMPOSE_VERSION}" sh
    - export PATH="${HOME}/.local/bin:${PATH}"
  script:
    - hpc-compose validate -f compose.yaml
    - hpc-compose lint -f compose.yaml --allow-warnings
```

## Strict vs. warnings

`validate` always fails on structural spec errors. `lint` emits advisory findings (`HPC001`–`HPC006`, `HPC900`); by default these fail the command, so add `--allow-warnings` for advisory-only runs. A common setup is:

- **pre-commit / local:** `lint --allow-warnings` (fast feedback, advisory).
- **CI (merge gate):** `lint` without `--allow-warnings`, or `strict: true` (enforce).

See [Spec Reference](spec-reference.md) for the full lint rule table and [Notebook Sessions](notebook.md), [Troubleshooting](troubleshooting.md) for related workflows.

## Related Docs

- [Spec Reference](spec-reference.md)
- [CLI Reference](cli-reference.md)
- [Installation](installation.md)
