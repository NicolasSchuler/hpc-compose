# LLM Curl Workflow

This example is the smallest end-to-end `hpc-compose` LLM workflow:

- the login node prepares a small Debian-based `curl` client image,
- the Slurm job starts `llama-server` on the compute node,
- a second service sends one request with `curl`,
- and the stack exits cleanly after the response is written to the client log.

## Prerequisites

- a supported login node with `enroot`, `srun`, and `sbatch`,
- `CACHE_DIR` set to a shared path visible from the submission host and compute nodes,
- a GGUF model at [`../models/model.gguf`](../models/model.gguf) for the repo-local example, or at `$HOME/models/model.gguf` for the home-directory example.

## Normal run

For a real cluster, start with the home-directory variant:

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
mkdir -p "$HOME/models"
# Copy the real GGUF file, not just a symlink whose target lives elsewhere.
cp /path/to/your/model.gguf "$HOME/models/model.gguf"
hpc-compose up -f examples/llm-curl-workflow-workdir.yaml
```

This is the lowest-overhead path because it does not require `HPC_COMPOSE_HOME`, a helper script, or a separate request file.

## Repo-local variant

```bash
export CACHE_DIR=/cluster/shared/hpc-compose-cache
hpc-compose up -f examples/llm-curl-workflow.yaml
```

`up` already runs preflight, prepares missing images, renders the batch script, calls `sbatch`, then follows the tracked job output.

## Debugging flow

```bash
hpc-compose validate -f examples/llm-curl-workflow.yaml
hpc-compose inspect -f examples/llm-curl-workflow.yaml
hpc-compose preflight -f examples/llm-curl-workflow.yaml
hpc-compose prepare -f examples/llm-curl-workflow.yaml
hpc-compose up -f examples/llm-curl-workflow.yaml
```

If you are using the home-directory variant, replace `examples/llm-curl-workflow.yaml` with `examples/llm-curl-workflow-workdir.yaml` in the commands above.

## What to look for

- `llm.log` shows `llama-server` starting and serving the request.
- `curl_client.log` contains the JSON response from `/v1/chat/completions`.
- If the job fails before either service starts, check Slurm's batch log such as `slurm-<jobid>.out`.

## Built-in job scratch

Every service automatically sees the per-job directory at `/hpc-compose/job`.

The example uses that shared mount to:

- wait for `curl_client` to finish,
- signal the `llm` service to stop,
- and avoid any extra host-side workflow directory.

## Startup gating

The example waits for `llama.cpp` to report that the model is ready before launching the client request:

- `readiness.type: log`
- `readiness.pattern: "main: model loaded"`
- `readiness.timeout_seconds: 300`

This is intentional. `llama-server` can bind its TCP port before the model is fully ready, which can cause early `503` responses from the client.

## Adjusting the prompt

Edit the inline JSON body in `curl_client.command` inside:

- [`../llm-curl-workflow.yaml`](../llm-curl-workflow.yaml), or
- [`../llm-curl-workflow-workdir.yaml`](../llm-curl-workflow-workdir.yaml)

You can change:

- the system or user message,
- generation settings such as `temperature` or `max_tokens`,
- or the request shape entirely.

The home-directory variant:

- uses `$HOME/models:/models`,
- defaults `x-slurm.cache_dir` to `/cluster/shared/hpc-compose-cache` and still lets you override it with `CACHE_DIR`,
- and no longer needs `HPC_COMPOSE_HOME`, `run-request.sh`, or `request.json`.

Logs land under:

```text
.hpc-compose/<job-id>/logs/
```
