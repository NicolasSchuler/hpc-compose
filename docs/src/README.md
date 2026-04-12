# hpc-compose

<div class="hpc-compose-hero">
  <img src="logo.png" alt="hpc-compose logo">
  <p><code>hpc-compose</code> turns a Compose-like spec into one Slurm job for multi-service HPC and research ML workflows.</p>
  <div class="hpc-compose-links">
    <a href="quickstart.html">Quickstart</a>
    <a href="examples.html">Examples</a>
    <a href="support-matrix.html">Support Matrix</a>
    <a href="task-guide.html">Task Guide</a>
    <a href="cli-reference.html">CLI Reference</a>
    <a href="execution-model.html">Execution Model</a>
    <a href="runbook.html">Runbook</a>
    <a href="spec-reference.html">Spec Reference</a>
    <a href="supported-slurm-model.html">Supported Slurm Model</a>
    <a href="running-compose-style-workflows-on-slurm.html">Why Compose on Slurm</a>
    <a href="roadmap.html">Roadmap</a>
  </div>
</div>

It is for teams who want Docker-Compose-like ergonomics on Slurm without adding Kubernetes or a custom control plane.

Start with the [Support Matrix](support-matrix.md) before planning a real runtime workflow. Linux is the maintained runtime target; macOS is intended for authoring, validation, rendering, and inspection.

## Why This Exists

- Multi-service Slurm jobs are awkward to author and debug with plain `sbatch` scripts alone.
- Docker Compose is familiar, but its networking and orchestration assumptions do not map cleanly to one Slurm allocation.
- `hpc-compose` keeps the scope narrow so you can validate, inspect, render, and submit one generated job instead of introducing a cluster-side control plane.

## Who It Is For

- research engineers and ML practitioners running jobs on Slurm clusters
- HPC platform or tooling owners who support those users
- teams that want one inspectable batch job instead of a long-running orchestrator

## Used For

- model serving plus helper services inside one allocation
- data and ETL pipelines with startup ordering and shared job-local state
- training jobs with checkpoint export, artifact tracking, and resume-aware reruns

## Start Here Examples

These four examples form the intended adoption funnel.

### 1. `minimal-batch.yaml`

- Demonstrates: one service, no dependencies, no prepare step
- Prerequisites: any machine for `validate` and `inspect`; Slurm and Enroot for `up`
- Run: `hpc-compose up -f examples/minimal-batch.yaml`
- Success signal: the batch log prints `Hello from Slurm!`

### 2. `app-redis-worker.yaml`

- Demonstrates: multi-service startup ordering, TCP readiness, and one helper service depending on another
- Prerequisites: a normal Slurm + Enroot submission host and shared `CACHE_DIR`
- Run: `hpc-compose up -f examples/app-redis-worker.yaml`
- Success signal: `worker.log` shows a successful `PING` and repeated `INCR jobs` calls after Redis becomes healthy

### 3. `llm-curl-workflow-workdir.yaml`

- Demonstrates: one GPU-backed LLM service plus one client service inside the same job
- Prerequisites: one visible GGUF file at `$HOME/models/model.gguf`, a GPU-capable Slurm target, and shared `CACHE_DIR`
- Run: `hpc-compose up -f examples/llm-curl-workflow-workdir.yaml`
- Success signal: `curl_client.log` contains a JSON response from `/v1/chat/completions`

### 4. `training-resume.yaml`

- Demonstrates: checkpoint export, resume-aware reruns, and attempt-aware state
- Prerequisites: shared storage for `x-slurm.resume.path` plus shared `CACHE_DIR`
- Run: `hpc-compose up -f examples/training-resume.yaml`
- Success signal: `results/<job-id>/` contains exported checkpoints and later attempts continue from the previous epoch

The full example matrix lives in [Examples](examples.md).

## Golden Path

If you are evaluating `hpc-compose` from a workstation first, use the authoring path on the promoted minimal example:

```bash
hpc-compose validate -f examples/minimal-batch.yaml
hpc-compose inspect -f examples/minimal-batch.yaml
hpc-compose up --dry-run --skip-prepare --no-preflight \
  --script-out /tmp/hpc-compose-demo.sbatch \
  -f examples/minimal-batch.yaml
```

Success looks like:

- `validate` prints `spec is valid`
- `inspect` shows `service order: app`
- `up --dry-run` writes a script path and skips `sbatch`

Download the [asciinema-style quickstart demo cast](quickstart-demo.cast).

## Execution Model at a Glance

```text
compose.yaml
    |
    +--> validate / inspect / render on the submission host
    |
    +--> generate one batch script
              |
              v
        one Slurm allocation
              |
              +--> primary-node helper services
              +--> optional allocation-wide distributed service
              +--> shared /hpc-compose/job scratch for coordination
```

For the exact boundary, read [Execution Model](execution-model.md) and [Supported Slurm Model](supported-slurm-model.md).

## Comparison

| Approach | Best at | Weakness for this problem |
| --- | --- | --- |
| Plain `sbatch` scripts | total control and site-specific tuning | multi-service coordination, validation, and repeatability remain ad hoc |
| Docker Compose | familiar service authoring on one machine | networking, restart, and orchestration assumptions do not fit one Slurm allocation cleanly |
| `hpc-compose` | Compose-like authoring for one inspectable Slurm job | intentionally narrow scope; not a general orchestrator or full Compose runtime |

## What It Does Not Support

- Compose `build:`
- `ports`
- `networks` / `network_mode`
- Compose `restart` as a Docker key
- `deploy`
- arbitrary multi-node orchestration or partial-node service placement

## When Not To Use `hpc-compose`

- You need custom container networking.
- You need broad Docker Compose compatibility.
- You want a long-running orchestration control plane.
- You need arbitrary cross-node service placement beyond one distributed service plus primary-node helpers.

## Roadmap

The near-term roadmap stays short:

- [Authoring ergonomics](roadmap.md#authoring-ergonomics)
- [Runtime visibility](roadmap.md#runtime-visibility)
- [Cluster compatibility](roadmap.md#cluster-compatibility)

## Feedback

If you try `hpc-compose`, open an [adoption feedback issue](https://github.com/NicolasSchuler/hpc-compose/issues/new?template=adoption-feedback.yml) with:

- cluster type
- workload type
- the main failure or friction point

## Read Next

1. [Quickstart](quickstart.md) for the shortest install-and-run path
2. [Examples](examples.md) for the four promoted workflows plus the broader matrix
3. [Running Compose-Style Multi-Service Workflows on Slurm](running-compose-style-workflows-on-slurm.md) for the canonical explainer
4. [Support Matrix](support-matrix.md) before assuming runtime support on a specific machine
5. [Task Guide](task-guide.md) when you already know the job you want to run
