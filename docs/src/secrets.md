# Secrets

`hpc-compose` resolves named secrets from local files or environment variables and feeds them into the interpolation map as first-class, redacted values. This keeps secrets out of the rendered batch script's `environment:` block authoring surface and ensures they are hidden in `config`/`context`/inspect output.

## Declaring secrets

Add a top-level `secrets:` block mapping a secret name to exactly one source:

```yaml
secrets:
  hf_token:
    file: ./secrets/hf.txt       # value = file contents (trimmed)
  db_password:
    env: DB_PASSWORD             # value = named environment variable
```

- `file:` reads the value from a file relative to the compose file directory.
- `env:` reads the value from the named variable in the resolved environment (process env, `.env`, or settings `env_files`).
- Each secret must set **exactly one** of `file` or `env`.

## Using secrets

Reference a secret anywhere interpolation works — most commonly in a service `environment:` block:

```yaml
services:
  trainer:
    image: pytorch/pytorch:2.3.1-cuda12.1-cudnn9-runtime
    environment:
      HF_TOKEN: ${hf_token}
      DB_PASSWORD: ${db_password}
    command: python -m train
```

The resolved value flows through the normal `environment:` render path (`--container-env=` + the launcher env array) into the container. No new mount machinery is required.

## Redaction

A value resolved through `secrets:` is tagged as a secret source. It is **always redacted** in diagnostic output regardless of its name:

```text
$ hpc-compose config -f compose.yaml
...
    environment:
      HF_TOKEN: <redacted>
      MODEL: llama
```

Name-based redaction (e.g. `API_KEY`, `PASSWORD`, `TOKEN`) also still applies to any sensitive-named value. Pass `--show-values` on `config` or `context` to reveal secrets when you have a legitimate need:

```bash
hpc-compose config -f compose.yaml --show-values
hpc-compose context   # shows interpolation vars, secrets tagged (Secret) and redacted
```

The raw secret value never appears in `config`, `context`, or `inspect` output by default. `inspect` does not expose a `--show-values` escape hatch; use `config --show-values` or `context --show-values` for trusted local diagnostics.

## Resolution order

Secrets are resolved after process environment variables and declared with the `secret` source. Declaring a secret is authoritative for its name; an explicit declaration overrides a same-named variable from a lower-precedence source. For `env:` sources, the named variable is read from the full resolved environment (including `.env` and settings `env_files`).

## What is not included

This v1 ships local `file:` and `env:` sources only. Backend integrations (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager) are intentionally deferred — they would require either shelling out to the `vault`/`gcloud` CLIs or adding a client crate, which conflicts with the project's minimal-dependency stance. You can bridge to them today by writing the fetched value into a file or exporting it as an environment variable, then referencing it through `secrets:`.

File-mount injection to `/run/secrets/<name>` (Docker Compose semantics) is also deferred; env-var injection through `environment:` covers the common case.

## Related Docs

- [Spec Reference](spec-reference.md)
- [CLI Reference](cli-reference.md)
- [Troubleshooting](troubleshooting.md)
