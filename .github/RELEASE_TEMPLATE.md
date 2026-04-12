# hpc-compose {{TAG}}

{{CHANGELOG_NOTES}}

## Install

Version-pinned installer:

```bash
RELEASE_TAG={{TAG}}
curl -fsSL "https://raw.githubusercontent.com/NicolasSchuler/hpc-compose/${RELEASE_TAG}/install.sh" \
  | env HPC_COMPOSE_VERSION="${RELEASE_TAG}" sh
```

Homebrew:

```bash
brew install NicolasSchuler/hpc-compose/hpc-compose
```

## Verify

```bash
gh release verify {{TAG}} -R {{REPO}}
gh release verify-asset {{TAG}} ./<downloaded-asset> -R {{REPO}}
gh attestation verify ./<downloaded-asset> \
  --repo {{REPO}} \
  --signer-workflow {{REPO}}/.github/workflows/release.yml
```

Published releases ship `SHA256SUMS` plus per-asset `.sha256` sidecars. Treat the attestations as the primary authenticity check and the checksums as corruption or mirroring checks.

## Assets

{{ASSET_LIST}}

See the installation guide for manual downloads, verification steps, and internal mirror notes:

- https://nicolasschuler.github.io/hpc-compose/installation.html
