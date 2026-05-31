//! Example metadata used by CLI discovery and documentation coverage.

use std::fmt;

/// How a shipped example can be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExampleAvailability {
    /// The example can be rendered by `hpc-compose new --template`.
    BuiltInTemplate,
    /// The example is a repository YAML file users copy directly.
    RepositoryFile,
}

impl ExampleAvailability {
    /// Returns the human-readable label used in text and markdown output.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::BuiltInTemplate => "Built-in template",
            Self::RepositoryFile => "Repository file",
        }
    }
}

impl fmt::Display for ExampleAvailability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// A shipped runnable example or starter template.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct ExampleInfo {
    /// Stable example id without the `.yaml` suffix.
    pub name: &'static str,
    /// Repository path to the YAML file.
    pub path: &'static str,
    /// Whether `hpc-compose new` can scaffold this example directly.
    pub availability: ExampleAvailability,
    /// Broad workflow category used for grouping.
    pub category: &'static str,
    /// Short description of the feature or workflow demonstrated.
    pub demonstrates: &'static str,
    /// User-facing guidance for when this is a good starting point.
    pub start_when: &'static str,
    /// Searchable tags.
    pub tags: &'static [&'static str],
}

impl ExampleInfo {
    /// Returns true when this example has the requested tag.
    #[must_use]
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(&tag)
    }

    /// Returns true when this example should match a free-text query.
    #[must_use]
    pub fn matches_query(&self, query: &str) -> bool {
        let query = query.trim().to_ascii_lowercase();
        if query.is_empty() {
            return true;
        }
        query.split_whitespace().all(|term| {
            self.name.contains(term)
                || self.path.contains(term)
                || self.category.contains(term)
                || self.demonstrates.to_ascii_lowercase().contains(term)
                || self.start_when.to_ascii_lowercase().contains(term)
                || self.tags.iter().any(|tag| tag.contains(term))
        })
    }
}

/// Returns all shipped examples in display order.
#[must_use]
pub fn examples() -> &'static [ExampleInfo] {
    EXAMPLES
}

/// Finds example metadata by id with or without `.yaml`.
#[must_use]
pub fn find_example(name: &str) -> Option<&'static ExampleInfo> {
    let normalized = name.trim().trim_end_matches(".yaml");
    EXAMPLES.iter().find(|example| example.name == normalized)
}

/// Returns the category for an example or template id.
#[must_use]
pub fn example_category(name: &str) -> Option<&'static str> {
    find_example(name).map(|example| example.category)
}

/// Returns true when the example is a built-in template.
#[must_use]
pub fn is_built_in_template(name: &str) -> bool {
    find_example(name)
        .is_some_and(|example| example.availability == ExampleAvailability::BuiltInTemplate)
}

const EXAMPLES: &[ExampleInfo] = &[
    ExampleInfo {
        name: "minimal-batch",
        path: "examples/minimal-batch.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "basics",
        demonstrates: "Smallest single-service batch job.",
        start_when: "You are new to hpc-compose and want the smallest possible file.",
        tags: &["beginner", "batch", "single-service"],
    },
    ExampleInfo {
        name: "dev-python-app",
        path: "examples/dev-python-app.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "basics",
        demonstrates: "Mounted source code plus x-runtime.prepare.commands for dependencies.",
        start_when: "You want an iterative source-mounted development workflow.",
        tags: &["dev", "python", "prepare", "hot-reload"],
    },
    ExampleInfo {
        name: "dev-python-smoke",
        path: "examples/dev-python-smoke.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "basics",
        demonstrates: "Finite test variant of the source-mounted Python app.",
        start_when: "You want to test a development spec without a long-running process.",
        tags: &["test", "python", "dev", "finite"],
    },
    ExampleInfo {
        name: "app-redis-worker",
        path: "examples/app-redis-worker.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "basics",
        demonstrates: "Multiple services with startup ordering and TCP readiness.",
        start_when: "Your workload depends on multi-service startup ordering.",
        tags: &["multi-service", "readiness", "redis", "tcp"],
    },
    ExampleInfo {
        name: "restart-policy",
        path: "examples/restart-policy.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "basics",
        demonstrates: "Bounded restart_on_failure with rolling-window crash-loop guards.",
        start_when: "You need transient-failure retries without letting a service spin forever.",
        tags: &["failure-policy", "restart", "resilience"],
    },
    ExampleInfo {
        name: "llm-curl-workflow",
        path: "examples/llm-curl-workflow.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "Repo-local LLM service with a dependent curl client.",
        start_when: "You want the smallest concrete inference workflow under the repository tree.",
        tags: &["llm", "curl", "inference", "readiness"],
    },
    ExampleInfo {
        name: "llm-curl-workflow-workdir",
        path: "examples/llm-curl-workflow-workdir.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "Home-directory LLM workflow for direct login-node use.",
        start_when: "You want the smallest real-cluster inference workflow.",
        tags: &["llm", "curl", "inference", "workdir"],
    },
    ExampleInfo {
        name: "llama-app",
        path: "examples/llama-app.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "GPU-backed service, mounted model files, and dependent app service.",
        start_when: "You need accelerator resources or a model-serving pattern.",
        tags: &["llm", "gpu", "model-serving", "readiness"],
    },
    ExampleInfo {
        name: "llama-uv-worker",
        path: "examples/llama-uv-worker.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "llama.cpp serving plus a source-mounted Python worker run through uv.",
        start_when: "You want the GGUF server plus mounted worker pattern.",
        tags: &["llm", "uv", "worker", "python", "llama"],
    },
    ExampleInfo {
        name: "vllm-openai",
        path: "examples/vllm-openai.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "vLLM serving with an in-job Python client.",
        start_when: "You want vLLM-based inference instead of llama.cpp.",
        tags: &["llm", "vllm", "openai", "gpu"],
    },
    ExampleInfo {
        name: "vllm-uv-worker",
        path: "examples/vllm-uv-worker.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "llm",
        demonstrates: "vLLM serving plus a source-mounted Python worker run through uv.",
        start_when: "You want a common LLM stack with mounted app code.",
        tags: &["llm", "vllm", "uv", "worker", "python"],
    },
    ExampleInfo {
        name: "training-checkpoints",
        path: "examples/training-checkpoints.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "training",
        demonstrates: "GPU training with checkpoints exported to shared storage.",
        start_when: "You need durable checkpoint outputs but not automatic resume semantics.",
        tags: &["training", "gpu", "checkpoints", "artifacts"],
    },
    ExampleInfo {
        name: "training-resume",
        path: "examples/training-resume.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "training",
        demonstrates: "GPU training with a shared resume directory and attempt-aware checkpoints.",
        start_when: "The run should resume from shared storage across retries or later submissions.",
        tags: &["training", "gpu", "resume", "checkpoints"],
    },
    ExampleInfo {
        name: "training-sweep",
        path: "examples/training-sweep.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "training",
        demonstrates: "Embedded sweep parameters with interpolation defaults.",
        start_when: "You want many independent trial allocations from one sweep block.",
        tags: &["training", "sweep", "hyperparameters"],
    },
    ExampleInfo {
        name: "fairseq-preprocess",
        path: "examples/fairseq-preprocess.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "training",
        demonstrates: "CPU-heavy NLP data preprocessing with parallel workers.",
        start_when: "You need a CPU-bound data preprocessing pipeline.",
        tags: &["training", "nlp", "cpu", "preprocess"],
    },
    ExampleInfo {
        name: "canary-right-size",
        path: "examples/canary-right-size.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "training",
        demonstrates: "Deliberately over-requested training probe for germinate.",
        start_when: "Your first question is whether a large GPU or memory request is justified.",
        tags: &["training", "canary", "rightsize", "metrics"],
    },
    ExampleInfo {
        name: "mpi-hello",
        path: "examples/mpi-hello.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "MPI hello world using service-level x-slurm.mpi.",
        start_when: "You need a small first-class MPI workload.",
        tags: &["distributed", "mpi", "hello"],
    },
    ExampleInfo {
        name: "mpi-pmix-v4-host-mpi",
        path: "examples/mpi-pmix-v4-host-mpi.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Versioned PMIx launch plus host MPI bind/env configuration.",
        start_when: "Your site requires a host MPI stack inside containers.",
        tags: &["distributed", "mpi", "pmix", "host-mpi"],
    },
    ExampleInfo {
        name: "multi-node-mpi",
        path: "examples/multi-node-mpi.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Primary-node helper plus one allocation-wide distributed MPI step.",
        start_when: "You want a minimal multi-node MPI pattern without extra orchestration.",
        tags: &["distributed", "mpi", "multi-node"],
    },
    ExampleInfo {
        name: "multi-node-partitioned",
        path: "examples/multi-node-partitioned.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "distributed",
        demonstrates: "Disjoint node ranges, fractional selection, and explicit co-location.",
        start_when: "Multiple distributed roles need explicit node ranges or share_with co-location.",
        tags: &["distributed", "multi-node", "placement", "partitioned"],
    },
    ExampleInfo {
        name: "multi-node-torchrun",
        path: "examples/multi-node-torchrun.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Allocation-wide torchrun launch using the primary node as rendezvous.",
        start_when: "You want a multi-node GPU training starting point.",
        tags: &["distributed", "torchrun", "gpu", "training"],
    },
    ExampleInfo {
        name: "multi-node-deepspeed",
        path: "examples/multi-node-deepspeed.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "DeepSpeed no-SSH multi-node training with generated rendezvous env.",
        start_when: "You want distributed fine-tuning without hand-written rendezvous setup.",
        tags: &["distributed", "deepspeed", "gpu", "training"],
    },
    ExampleInfo {
        name: "multi-node-accelerate",
        path: "examples/multi-node-accelerate.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Hugging Face Accelerate multi-machine launch.",
        start_when: "You want an Accelerate-based training or fine-tuning starting point.",
        tags: &["distributed", "accelerate", "hugging-face", "training"],
    },
    ExampleInfo {
        name: "multi-node-horovod",
        path: "examples/multi-node-horovod.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Horovod rank-per-GPU launch through Slurm MPI.",
        start_when: "You want Horovod without SSH fanout.",
        tags: &["distributed", "horovod", "mpi", "gpu"],
    },
    ExampleInfo {
        name: "multi-node-jax",
        path: "examples/multi-node-jax.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "JAX distributed training with generated coordinator env.",
        start_when: "You want a JAX distributed starting point.",
        tags: &["distributed", "jax", "gpu", "training"],
    },
    ExampleInfo {
        name: "nccl-tests",
        path: "examples/nccl-tests.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "MPI-backed NCCL all-reduce test job for GPU fabric debugging.",
        start_when: "You need to debug NCCL, InfiniBand, UCX, or OFI before real training.",
        tags: &["distributed", "nccl", "mpi", "gpu", "fabric"],
    },
    ExampleInfo {
        name: "ray-symmetric",
        path: "examples/ray-symmetric.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Ray symmetric-run across one Slurm allocation.",
        start_when: "You want a modern Ray-on-Slurm starting point without an autoscaler.",
        tags: &["distributed", "ray", "symmetric"],
    },
    ExampleInfo {
        name: "ray-head-workers",
        path: "examples/ray-head-workers.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Ray head plus workers inside one Slurm allocation.",
        start_when: "You need explicit Ray head/worker control for an older or site-specific setup.",
        tags: &["distributed", "ray", "workers"],
    },
    ExampleInfo {
        name: "dask-scheduler-workers",
        path: "examples/dask-scheduler-workers.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Dask scheduler on the primary node plus allocation workers.",
        start_when: "You want Dask CLI deployment inside one Slurm allocation.",
        tags: &["distributed", "dask", "workers"],
    },
    ExampleInfo {
        name: "spark-standalone",
        path: "examples/spark-standalone.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Spark standalone master, workers, and app submission inside one allocation.",
        start_when: "You need a conservative Spark standalone pattern without external cluster management.",
        tags: &["distributed", "spark", "workers"],
    },
    ExampleInfo {
        name: "flux-nested",
        path: "examples/flux-nested.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "distributed",
        demonstrates: "Nested Flux instance launched inside a Slurm allocation.",
        start_when: "You want Flux scheduling inside an existing Slurm allocation.",
        tags: &["distributed", "flux", "nested"],
    },
    ExampleInfo {
        name: "postgres-etl",
        path: "examples/postgres-etl.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "workflow",
        demonstrates: "PostgreSQL plus a Python data processing job.",
        start_when: "You need a database-backed batch pipeline.",
        tags: &["workflow", "postgres", "etl", "python"],
    },
    ExampleInfo {
        name: "nextflow-bridge",
        path: "examples/nextflow-bridge.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "workflow",
        demonstrates: "Nextflow command wrapper inside one hpc-compose allocation.",
        start_when: "You want hpc-compose tracking around a workflow-engine run.",
        tags: &["workflow", "nextflow", "bridge"],
    },
    ExampleInfo {
        name: "snakemake-bridge",
        path: "examples/snakemake-bridge.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "workflow",
        demonstrates: "Snakemake command wrapper inside one hpc-compose allocation.",
        start_when: "You want hpc-compose tracking around a Snakemake run.",
        tags: &["workflow", "snakemake", "bridge"],
    },
    ExampleInfo {
        name: "multi-stage-pipeline",
        path: "examples/multi-stage-pipeline.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "workflow",
        demonstrates: "Two-stage data pipeline coordinating through the shared job mount.",
        start_when: "You need file-based stage-to-stage handoff.",
        tags: &["workflow", "pipeline", "artifacts"],
    },
    ExampleInfo {
        name: "pipeline-dag",
        path: "examples/pipeline-dag.yaml",
        availability: ExampleAvailability::BuiltInTemplate,
        category: "workflow",
        demonstrates: "One-shot preprocess -> train -> postprocess DAG with completion dependencies.",
        start_when: "You need stage completion, not service readiness, to gate downstream work.",
        tags: &["workflow", "dag", "pipeline", "depends-on"],
    },
    ExampleInfo {
        name: "rendezvous-model-server",
        path: "examples/rendezvous-model-server.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "workflow",
        demonstrates: "Provider job that registers a model-server endpoint in the shared cache.",
        start_when: "One Slurm allocation should publish a service for later jobs.",
        tags: &["workflow", "rendezvous", "model-serving"],
    },
    ExampleInfo {
        name: "rendezvous-client",
        path: "examples/rendezvous-client.yaml",
        availability: ExampleAvailability::RepositoryFile,
        category: "workflow",
        demonstrates: "Separate client job resolving HPC_COMPOSE_RDZV_MODEL_SERVER_URL.",
        start_when: "A later job should discover a provider through shared storage.",
        tags: &["workflow", "rendezvous", "client"],
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn metadata_names_are_unique() {
        let mut names = BTreeSet::new();
        for example in examples() {
            assert!(names.insert(example.name), "duplicate {}", example.name);
            assert!(example.path.ends_with(".yaml"));
            assert!(!example.tags.is_empty(), "{} has no tags", example.name);
        }
    }

    #[test]
    fn query_matches_all_terms() {
        let vllm = find_example("vllm-uv-worker").expect("vllm example");
        assert!(vllm.matches_query("vllm worker"));
        assert!(!vllm.matches_query("vllm mpi"));
    }
}
