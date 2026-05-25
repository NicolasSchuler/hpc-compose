#!/usr/bin/env python3
"""Probe a repository for hpc-compose adaptation clues.

The script is intentionally heuristic. It does not validate an hpc-compose spec;
it gives Codex a compact starting inventory for migration decisions.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable


SKIP_DIRS = {
    ".cache",
    ".cargo",
    ".git",
    ".github",
    ".hpc-compose",
    ".hg",
    ".mypy_cache",
    ".nox",
    ".pytest_cache",
    ".ruff_cache",
    ".svn",
    ".tmp",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "docs",
    "examples",
    "fixtures",
    "node_modules",
    "target",
    "test",
    "tests",
    "vendor",
    "venv",
}

TEXT_SUFFIXES = {
    ".Dockerfile",
    ".cfg",
    ".conf",
    ".ini",
    ".json",
    ".md",
    ".py",
    ".sh",
    ".slurm",
    ".sbatch",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}

PACKAGE_FILES = {
    "requirements.txt",
    "pyproject.toml",
    "environment.yml",
    "environment.yaml",
    "conda.yml",
    "conda.yaml",
    "package.json",
    "Cargo.toml",
    "Project.toml",
    "Manifest.toml",
    "Makefile",
    "Snakefile",
    "nextflow.config",
}


def iter_files(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        kept_dirs = []
        for dirname in dirnames:
            child = Path(dirpath) / dirname
            if dirname in SKIP_DIRS:
                continue
            if dirname.startswith("."):
                continue
            if (child / "SKILL.md").exists() and (child / "agents").is_dir():
                continue
            kept_dirs.append(dirname)
        dirnames[:] = kept_dirs
        for filename in filenames:
            path = Path(dirpath) / filename
            try:
                if path.stat().st_size > 1_000_000:
                    continue
            except OSError:
                continue
            yield path


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def looks_text(path: Path) -> bool:
    if path.name in PACKAGE_FILES:
        return True
    if path.name.startswith("Dockerfile"):
        return True
    if path.suffix in TEXT_SUFFIXES:
        return True
    return False


def read_lower(path: Path) -> str:
    if not looks_text(path):
        return ""
    try:
        return path.read_text(errors="ignore").lower()
    except OSError:
        return ""


def collect(root: Path) -> dict[str, object]:
    files = list(iter_files(root))
    rels = [rel(p, root) for p in files]
    lower_by_file = {rel(p, root): read_lower(p) for p in files}
    signal_text = "\n".join(
        text
        for filename, text in lower_by_file.items()
        if Path(filename).suffix != ".md" and Path(filename).name != "hpc_compose_repo_probe.py"
    )

    docker_compose = [
        f
        for f in rels
        if Path(f).name in {"docker-compose.yml", "docker-compose.yaml", "compose.yaml", "compose.yml"}
        or Path(f).name.startswith("docker-compose.")
    ]
    dockerfiles = [f for f in rels if Path(f).name.startswith("Dockerfile")]
    hpc_specs = [f for f in rels if "hpc" in Path(f).name.lower() and Path(f).suffix in {".yml", ".yaml"}]
    slurm_scripts = [
        f
        for f, text in lower_by_file.items()
        if f.endswith((".slurm", ".sbatch", ".job", ".sh")) and "#sbatch" in text
    ]
    package_files = [f for f in rels if Path(f).name in PACKAGE_FILES]

    signals = {
        "docker_compose": docker_compose,
        "dockerfiles": dockerfiles,
        "existing_hpc_like_specs": hpc_specs,
        "slurm_scripts": slurm_scripts,
        "package_files": package_files,
        "mentions_torch": "torch" in signal_text or "pytorch" in signal_text,
        "mentions_deepspeed": "deepspeed" in signal_text,
        "mentions_accelerate": "accelerate" in signal_text,
        "mentions_jax": "jax" in signal_text,
        "mentions_mpi": "mpi" in signal_text or "mpirun" in signal_text or "srun" in signal_text,
        "mentions_vllm_or_llama": "vllm" in signal_text or "llama" in signal_text or "gguf" in signal_text,
        "mentions_snakemake": "snakemake" in signal_text or any(Path(f).name == "Snakefile" for f in rels),
        "mentions_nextflow": "nextflow" in signal_text or any(Path(f).name == "nextflow.config" for f in rels),
        "mentions_redis_or_db": any(word in signal_text for word in ["redis", "postgres", "mysql", "mongodb"]),
        "mentions_cuda_or_gpu": any(word in signal_text for word in ["cuda", "gpu", "nvidia", "nccl"]),
    }
    return signals


def recommend(signals: dict[str, object]) -> tuple[list[str], list[str], list[str]]:
    observations: list[str] = []
    hypotheses: list[str] = []
    recommendations: list[str] = []

    if signals["docker_compose"]:
        observations.append(f"Found Docker Compose files: {', '.join(signals['docker_compose'])}")
        recommendations.append("Read docker-compose-migration.md and create a separate compose.hpc.yaml.")
    if signals["dockerfiles"]:
        observations.append(f"Found Dockerfiles: {', '.join(signals['dockerfiles'])}")
        recommendations.append("Replace Docker Compose build steps with image plus x-runtime.prepare.commands.")
    if signals["slurm_scripts"]:
        observations.append(f"Found Slurm scripts: {', '.join(signals['slurm_scripts'])}")
        recommendations.append("Map existing #SBATCH resources into first-class x-slurm fields before raw submit_args.")
    if signals["existing_hpc_like_specs"]:
        observations.append(f"Found existing HPC-like YAML specs: {', '.join(signals['existing_hpc_like_specs'])}")
    if signals["package_files"]:
        observations.append(f"Found package/runtime files: {', '.join(signals['package_files'])}")

    if signals["mentions_vllm_or_llama"]:
        hypotheses.append("The repository may fit an LLM serving/client hpc-compose example.")
        recommendations.append("Compare against llm-curl-workflow-workdir, llama-app, vllm-openai, or vllm-uv-worker.")
    if signals["mentions_deepspeed"]:
        hypotheses.append("The workload may need a DeepSpeed distributed template.")
        recommendations.append("Compare against multi-node-deepspeed and verify cluster fabric settings.")
    elif signals["mentions_accelerate"]:
        hypotheses.append("The workload may need a Hugging Face Accelerate distributed template.")
        recommendations.append("Compare against multi-node-accelerate.")
    elif signals["mentions_jax"]:
        hypotheses.append("The workload may need a JAX distributed template.")
        recommendations.append("Compare against multi-node-jax.")
    elif signals["mentions_torch"]:
        hypotheses.append("The workload may be a PyTorch training or inference job.")
        recommendations.append("Compare against training-resume, training-checkpoints, or multi-node-torchrun.")
    if signals["mentions_mpi"]:
        hypotheses.append("The workload or existing scripts mention MPI or srun.")
        recommendations.append("Check srun --mpi=list and compare against multi-node-mpi or mpi-hello.")
    if signals["mentions_snakemake"]:
        hypotheses.append("The repository may be a Snakemake workflow.")
        recommendations.append("Consider snakemake-bridge when hpc-compose should wrap tracking around the workflow engine.")
    if signals["mentions_nextflow"]:
        hypotheses.append("The repository may be a Nextflow workflow.")
        recommendations.append("Consider nextflow-bridge when hpc-compose should wrap tracking around the workflow engine.")
    if signals["mentions_redis_or_db"]:
        hypotheses.append("The workload may have a same-allocation helper service.")
        recommendations.append("Use readiness and 127.0.0.1 for same-node helper services.")
    if signals["mentions_cuda_or_gpu"]:
        hypotheses.append("The workload likely needs GPU resources.")
        recommendations.append("Map requested GPUs through x-slurm.gres or x-slurm.gpus and verify site-specific GRES syntax.")

    if not observations:
        observations.append("No obvious Docker Compose, Slurm, or package-manager entrypoint was found in the scanned files.")
        recommendations.append("Ask for the intended run command, container image, resource needs, and target cluster.")

    recommendations.append("Run hpc-compose validate and plan before any real Slurm submission.")
    return observations, hypotheses, recommendations


def markdown_report(root: Path, signals: dict[str, object]) -> str:
    observations, hypotheses, recommendations = recommend(signals)
    open_questions = [
        "What is the exact command or service entrypoint to run on the cluster?",
        "Which cluster, partition, account/QOS, and walltime should be used?",
        "Which shared filesystem path should hold x-slurm.cache_dir?",
        "Is Pyxis/Enroot available, or should the spec use Apptainer/Singularity/host?",
        "Is a real Slurm submission approved, or should work stop at validate/plan/preflight?",
    ]

    lines = [f"# hpc-compose repository probe: {root}", ""]
    for title, items in [
        ("Observation", observations),
        ("Hypothesis", hypotheses or ["No strong workload-specific hypothesis yet."]),
        ("Recommendation", recommendations),
        ("Open question", open_questions),
    ]:
        lines.append(f"## {title}")
        lines.extend(f"- {item}" for item in items)
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe a repository for hpc-compose adaptation clues.")
    parser.add_argument("repo", nargs="?", default=".", help="Repository path to scan.")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    args = parser.parse_args()

    root = Path(args.repo).resolve()
    if not root.exists() or not root.is_dir():
        parser.error(f"repo path is not a directory: {root}")

    signals = collect(root)
    if args.format == "json":
        observations, hypotheses, recommendations = recommend(signals)
        print(
            json.dumps(
                {
                    "root": str(root),
                    "signals": signals,
                    "observations": observations,
                    "hypotheses": hypotheses,
                    "recommendations": recommendations,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(markdown_report(root, signals), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
