# Exit Codes

`hpc-compose` maps its failures onto a small, stable set of process exit codes so that scripts and CI can branch on *what* failed — an invalid spec, an unreachable cluster, or lint findings — without scraping stderr. The set is deliberately minimal: **every code is a contract and will not be repurposed.**

## The catalog

| Code | Meaning | Emitted when |
| --- | --- | --- |
| `0` | Success | The command completed successfully. |
| `1` | Generic failure | An unexpected error, local I/O failure, or an external tool reporting an error, with no more specific category. |
| `2` | Usage or validation error | An invalid flag or argument combination, or an invalid spec. |
| `3` | Preflight / environment not ready | A readiness check failed or the cluster is unreachable. |
| `4` | Lint findings present | `lint` found findings that failed the gate. |
| *child* | Propagated child status | A direct-execution command's child process exited nonzero. |

Codes `1`–`4` are what `hpc-compose` emits for its *own* failures. Direct-execution commands instead surface the status of a child process they ran on your behalf (see [Child process status](#child-process-status)).

## Which command produces which code

### Code 1 — deliberate gates

Most `1` exits are unexpected failures, but a few commands use it as a deliberate, scriptable "check failed" signal:

- `diff --against-spec --fail-on-change` exits `1` when the current compose file's effective config differs from the tracked run's recorded snapshot, and `0` when there is no drift — so `hpc-compose diff --against-spec --fail-on-change && hpc-compose up` submits only an unchanged spec.
- `up` exits `1` when resume config drift is detected without `--allow-resume-changes`.

### Code 2 — usage and validation

- Parse-level usage errors — an unknown flag, a missing argument — are reported by the argument parser, which exits `2` before any command runs.
- Command-level usage errors — semantic flag or argument combinations that must be checked after parsing — also exit `2`.
- `validate`, and any command that loads a malformed `compose.yaml`, exits `2`. This covers semantic planning failures too — an undefined `depends_on` target, a service without an `image`, or an unsatisfiable allocation geometry all exit `2`.

### Code 3 — preflight and environment

- `preflight` exits `3` when it finds errors, or warnings under `--strict`.
- The inline preflight gate that `up`, `alloc`, `run`, `shell`, `notebook`, `germinate`, `dev`, `when`, `debug`, and `sweep submit` run before launching also exits `3` on errors, matching the standalone command.
- `doctor` exits `3` when a smoke or readiness probe fails.
- `up --remote` and the remote follow-up commands (`status`/`stats`/`logs`/`score`/`pull --remote`) exit `3` when the login node is unreachable — the `ssh`, `rsync`, or version probe connection fails.

### Code 4 — lint findings

`lint` exits `4` when findings fail the gate: any error-level finding, or a warning-level finding without `--allow-warnings`. Pass `--allow-warnings` to treat warnings as advisory, which exits `0` when there are no error-level findings.

### Child process status

Direct-execution commands — `run`, `alloc`, `shell`, `notebook`, `reach`, `exec` — exec a child process on your behalf and propagate its exit status verbatim, so a test runner's `2` stays distinguishable from its `5`. A propagated status can coincide with a reserved code above; that is expected, and matches how `env(1)`, `timeout(1)`, and shells behave. A child that "failed" while reporting `0` is surfaced as `1`, so a failure never exits `0`.

## Branch on the code in CI

Inspect `$?` immediately after the command:

```bash
hpc-compose validate -f compose.yaml
case "$?" in
  0) echo "spec is valid" ;;
  2) echo "spec is invalid — fix compose.yaml" >&2; exit 2 ;;
  *) echo "hpc-compose failed for another reason" >&2; exit 1 ;;
esac
```

A common gate runs `validate` then `lint` and treats them differently — a broken spec stops the pipeline, while lint findings are reported on their own code:

```bash
hpc-compose validate -f compose.yaml || exit   # exit 2 on an invalid spec
hpc-compose lint -f compose.yaml                # exit 4 on findings, 0 when clean
```

## Related Docs

- [Wire Up CI](ci-integration.md)
- [CLI Reference](cli-reference.md)
- [Troubleshoot a Failed Run](troubleshooting.md)
