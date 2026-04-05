# Quickstart

This is the shortest end-to-end path from checkout to a submitted job.

## Cadence

| Step | When to do it |
| --- | --- |
| Build or install `hpc-compose` | once per checkout or upgrade |
| `init` or copy an example | once per new spec |
| `submit --watch` | normal run, every time you want to launch the stack |
| `validate`, `inspect`, `preflight`, `prepare` | first-time setup or troubleshooting |

## 1. Build the binary

```bash
cargo build --release
```

## 2. Initialize a starter spec

```bash
target/release/hpc-compose init \
  --template dev-python-app \
  --name my-app \
  --cache-dir /shared/$USER/hpc-compose-cache \
  --output /tmp/compose.yaml
```

If you already know the closest shipped example, you can copy it directly instead.

## 3. Usual run: submit and watch

```bash
target/release/hpc-compose submit --watch -f /tmp/compose.yaml
```

`submit --watch` is the normal fast path. It runs preflight, prepares missing artifacts, renders the batch script, submits it through `sbatch`, then follows scheduler state and tracked logs.

## 4. Optional first-time checks

```bash
target/release/hpc-compose validate -f /tmp/compose.yaml
target/release/hpc-compose inspect --verbose -f /tmp/compose.yaml
```

Use `inspect` to confirm:

- service order
- normalized image references
- cache artifact paths
- whether prepare steps will rebuild every submit

## 5. Optional troubleshooting commands

```bash
target/release/hpc-compose preflight -f /tmp/compose.yaml
target/release/hpc-compose prepare -f /tmp/compose.yaml
```

Run these separately when you want to debug login-node prerequisites, inspect cache reuse, or isolate image preparation from job submission.

## 6. Revisit a tracked run later

```bash
target/release/hpc-compose status -f /tmp/compose.yaml
target/release/hpc-compose stats -f /tmp/compose.yaml
target/release/hpc-compose logs -f /tmp/compose.yaml --follow
```

## When to leave the quickstart

- Use the [Runbook](runbook.md) when adapting a real workload to a real cluster.
- Use the [Spec Reference](spec-reference.md) when changing fields or validation-sensitive values.
- Use the [Examples](examples.md) page when you want the closest known-good template.
