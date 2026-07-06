# Run a Notebook or IDE Session

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
  ssh -N -o ControlMaster=auto -o ControlPath=~/.ssh/cm-%r@%h:%p -o ControlPersist=10m -L 8888:<compute-node>:8888 <login-node>
then open the URL above in your browser.
The ControlMaster options reuse one authenticated connection, so a login node that requires an OTP/2FA only prompts on the first connection within ControlPersist.
```

The printed command already carries the SSH connection-multiplexing options (the
same ones `reach`, `pull`, and `experiment` emit), so a login node that requires
an OTP/2FA prompts only on the first connection of your session.

For VS Code, open the printed `vscode.dev` link directly in a browser — no tunnel is required.

### Login nodes that require an OTP / 2FA

If your login node demands a one-time password on every SSH session, keep SSH
connection multiplexing enabled so you authenticate **once** and every later
tunnel (and `rsync`/`scp`) reuses the master connection. The printed Jupyter
tunnel command already includes these options; the equivalent persistent
`~/.ssh/config` form is:

```text
# ~/.ssh/config
Host <login-node>
    ControlMaster auto
    ControlPath ~/.ssh/cm-%r@%h:%p
    ControlPersist 10m
```

Establish the master once (entering the OTP), then the forward runs without
re-authenticating until `ControlPersist` expires:

```sh
ssh -fN <login-node>                          # OTP entered here, once
ssh -L 8888:<compute-node>:8888 <login-node>  # reuses the master — no OTP
```

`hpc-compose` only prints the tunnel command; it never opens a connection or
stores credentials, so the OTP step stays entirely under your control.

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

## Promote a Notebook to Batch

After prototyping interactively, convert a tracked notebook session plus an
`.ipynb` into a normal batch compose spec:

```bash
hpc-compose notebook promote notebooks/train.ipynb \
  --requirements requirements.txt \
  --param SEED=1 \
  --output train-batch.yaml
```

`notebook promote` is a static authoring command. It reads the latest tracked
notebook session from `.hpc-compose/latest-notebook.json` by default, writes a
compose file, and does not submit, contact Slurm, SSH, run preflight/prepare, or
execute Papermill. The promoted service runs `python -m papermill` against the
notebook, and `--param NAME=DEFAULT` exposes Papermill parameters through
compose interpolation such as `${NAME:-DEFAULT}`.

Dependencies should be declared explicitly. Pass `--requirements
requirements.txt` or repeated `--prepare-command` flags to render package setup
under `x-runtime.prepare.commands`; this keeps slower-changing dependencies in
the prepared runtime image instead of hiding them in interactive notebook cells.
Promotion scans the notebook for obvious `%pip install`, `!pip install`,
`conda install`, or `mamba install` cells and warns when it finds them.

Existing notebook records may not contain a full submit-time config snapshot.
When that metadata is missing, promotion reconstructs the spec from persisted
record fields plus explicit overrides such as `--image`, `--volume`, and
`--working-dir`, then prints a warning so you can inspect the generated YAML
before launching it.

## Security

For Jupyter, `hpc-compose` generates a random auth token and embeds it in the printed URL, so the link is unguessable but self-contained. Override it with `--token` if you prefer. Do not share the printed URL: it grants access to the notebook session.

For VS Code, `code tunnel` performs GitHub device-flow authentication the first time; `--accept-server-license-terms` is passed automatically.

## Authoring notes

- **Images and users.** `jupyter/scipy-notebook` runs as the non-root `jovyan` user. Bind-mounted host directories must be writable by that user (typically uid 1000). Use `--working-dir` to point at your mounted workspace and adjust ownership on the host if needed.
- **VS Code images.** There is no universal default `code` image; supply one with `--image` that contains the VS Code CLI.
- **Readiness.** `hpc-compose` waits for a log pattern (`/lab?token=` for Jupyter, `vscode.dev/tunnel/` for VS Code) before printing the URL. Use `--timeout` (default `10m`) to bound the wait; first-run image pulls happen during `prepare`, before the readiness clock starts.
- **Declarative counterpart.** The same workflow is available as a compose file via the `jupyter` template (`hpc-compose new --template jupyter`), so you can commit it to a repo and launch with `hpc-compose up`.

## Related Docs

- [Use Secrets](secrets.md)
- [Development Workflow](development-workflow.md)
- [Examples](examples.md)
- [CLI Reference](cli-reference.md)
