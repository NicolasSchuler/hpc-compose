# Runtime Observability

After a submission, `hpc-compose` records tracked metadata under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/
```

That directory lets follow-up commands reconnect without resubmitting.

## Common Commands

```bash
hpc-compose status -f compose.yaml
hpc-compose ps -f compose.yaml
hpc-compose watch -f compose.yaml
hpc-compose logs -f compose.yaml --follow
hpc-compose stats -f compose.yaml
```

| Command | Use it for |
| --- | --- |
| `status` | Scheduler state, batch log path, runtime paths, and failure-policy state. |
| `ps` | Stable per-service snapshot with readiness, status, restart counters, and log path. |
| `watch` | Live terminal UI; falls back to line-oriented output on non-interactive terminals. |
| `logs` | Text log output, optionally focused on one service. |
| `stats` | Tracked metrics and Slurm step statistics. |

Use `--format json` on non-streaming commands when automation needs stable fields. `stats` also supports `--format csv` and `--format jsonl`.

## Logs

Runtime logs live under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/logs/<service>.log
```

Slurm may also write a top-level batch log such as `slurm-<jobid>.out`, or to the path configured with `x-slurm.output`. Check the batch log first when a job fails before any service log appears.

Service names containing non-alphanumeric characters are encoded in log filenames. Prefer `[a-zA-Z0-9_-]` in service names for readability.

## Metrics

When `x-slurm.metrics` is enabled, sampler files are written under:

```text
${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/${SLURM_JOB_ID}/metrics/
  meta.json
  gpu.jsonl
  gpu_processes.jsonl
  slurm.jsonl
  diagnostics/
```

The sampler can collect GPU snapshots through `nvidia-smi` and job-step CPU/memory snapshots through `sstat`. Collector failures are best-effort: missing `nvidia-smi`, missing `sstat`, or unsupported queries do not fail the batch job itself.

## Related Docs

- [Runbook](runbook.md)
- [Troubleshooting](troubleshooting.md)
- [Artifacts and Resume](artifacts-and-resume.md)
