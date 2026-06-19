# Notebook Sessions

`hpc-compose notebook` launches a tracked interactive server — JupyterLab or VS Code (`code tunnel`) — as a single-service Slurm job, waits for it to become ready, and prints the connection URL. The session is a normal tracked job: manage it with `hpc-compose status` and stop it with `hpc-compose cancel`.

Use it when you want an interactive IDE or notebook on a compute node (for example, on a GPU partition) without hand-writing `sbatch` glue.

## Kinds

| `--kind` | Default image | Connection |
| --- | --- | --- |
| `jupyter` (default) | `jupyter/scipy-notebook:latest` | Local URL + SSH tunnel hint; you forward the port from your laptop. |
| `vscode` | none (requires `--image`) | A `https://vscode.dev/tunnel/...` link. VS Code tunnels outbound, so no port forwarding is needed. |

## Quickstart

```bash
# JupyterLab on one GPU, with your project mounted
hpc-compose notebook --kind jupyter --gpus 1 \
  --volume ./project:/workspace --working-dir /workspace

# VS Code tunnel (supply an image containing the `code` CLI)
hpc-compose notebook --kind vscode --image ghcr.io/example/code:1 --gpus 1
```

After readiness, `hpc-compose` prints the URL. For Jupyter on Slurm it also prints a ready-to-copy SSH command:

```text
Open: http://127.0.0.1:8888/lab?token=<generated>

On your laptop, forward the port:
  ssh -L 8888:<compute-node>:8888 <login-node>
then open the URL above in your browser.
```

For VS Code, open the printed `vscode.dev` link directly in a browser — no tunnel is required.

## Local mode

`--local` runs the server on the current host (login node or workstation) through the same local supervisor used by `dev`. The printed URL points at `127.0.0.1` directly:

```bash
hpc-compose notebook --kind jupyter --local --volume ./src:/workspace
```

Local mode requires a Linux host with Pyxis-compatible Enroot tooling, like the rest of the local-development command layer.

## Managing the session

The notebook is a tracked job, so the standard commands work:

```bash
hpc-compose status -f <compose>          # scheduler + service state
hpc-compose logs -f <compose> --follow   # tail the notebook log
hpc-compose cancel -f <compose>          # stop and release the allocation
```

By default `notebook` detaches after printing the URL (the job keeps running). Pass `--follow` to stream logs in the foreground instead.

## Security

For Jupyter, `hpc-compose` generates a random auth token and embeds it in the printed URL, so the link is unguessable but self-contained. Override it with `--token` if you prefer. Do not share the printed URL: it grants access to the notebook session.

For VS Code, `code tunnel` performs GitHub device-flow authentication the first time; `--accept-server-license-terms` is passed automatically.

## Authoring notes

- **Images and users.** `jupyter/scipy-notebook` runs as the non-root `jovyan` user. Bind-mounted host directories must be writable by that user (typically uid 1000). Use `--working-dir` to point at your mounted workspace and adjust ownership on the host if needed.
- **VS Code images.** There is no universal default `code` image; supply one with `--image` that contains the VS Code CLI.
- **Readiness.** `hpc-compose` waits for a log pattern (`/lab?token=` for Jupyter, `vscode.dev/tunnel/` for VS Code) before printing the URL. Use `--ready-timeout` (default `10m`) to bound the wait; first-run image pulls happen during `prepare`, before the readiness clock starts.
- **Declarative counterpart.** The same workflow is available as a compose file via the `jupyter` template (`hpc-compose new --template jupyter`), so you can commit it to a repo and launch with `hpc-compose up`.

## Related Docs

- [Examples](examples.md)
- [Development Workflow](development-workflow.md)
- [CLI Reference](cli-reference.md)
- [Runbook](runbook.md)
