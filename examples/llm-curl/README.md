# LLM Curl Workflow

This example is the smallest end-to-end `hpc-compose` LLM workflow:

- the login node prepares a tiny `curl` client image,
- the Slurm job starts `llama-server` on the compute node,
- a second service sends one request with `curl`,
- and the stack exits cleanly after the response is written to the client log.

## Prerequisites

- a supported login node with `enroot`, `srun`, and `sbatch`,
- a shared `x-slurm.cache_dir`,
- a GGUF model at [`../models/model.gguf`](../models/model.gguf).
- a request payload at [`request.json`](request.json).

## Login-node flow

```bash
hpc-compose validate -f examples/llm-curl-workflow.yaml
hpc-compose inspect -f examples/llm-curl-workflow.yaml
hpc-compose preflight -f examples/llm-curl-workflow.yaml
hpc-compose prepare -f examples/llm-curl-workflow.yaml
hpc-compose submit -f examples/llm-curl-workflow.yaml
```

## What to look for

- `llm.log` shows `llama-server` starting and serving the request.
- `curl_client.log` contains the JSON response from `/v1/chat/completions`.

## Adjusting the prompt

Edit [`request.json`](request.json) to change:

- the system or user message,
- generation settings such as `temperature` or `max_tokens`,
- or the request shape entirely.

The `curl` script posts that file directly, so you can make the request payload as simple or as custom as you want.

## Running from an arbitrary work directory

If you want the same workflow rooted in a login-node directory such as `/home/kastel/vy3326`, use [`../llm-curl-workflow-workdir.yaml`](../llm-curl-workflow-workdir.yaml) and set:

```bash
export HPC_COMPOSE_HOME=/home/kastel/vy3326
```

Then create:

- `$HPC_COMPOSE_HOME/models/model.gguf`
- `$HPC_COMPOSE_HOME/llm-curl/run-request.sh`
- `$HPC_COMPOSE_HOME/llm-curl/request.json`

Logs land under:

```text
.hpc-compose/<job-id>/logs/
```
