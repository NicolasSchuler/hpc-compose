#!/usr/bin/env python3
"""Emit bounded, evidence-only JSON signals for hpc-compose adaptation."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 1
MAX_FILES = 25_000
MAX_TOTAL_TEXT_BYTES = 64 * 1024 * 1024
MAX_FILE_TEXT_BYTES = 256 * 1024
MAX_EVIDENCE_PATHS = 20

SKIP_DIRS = {
    ".cache",
    ".cargo",
    ".git",
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
    ".cfg",
    ".conf",
    ".ini",
    ".jl",
    ".json",
    ".py",
    ".r",
    ".rs",
    ".sh",
    ".slurm",
    ".sbatch",
    ".toml",
    ".yaml",
    ".yml",
}

PACKAGE_FILES = {
    "Cargo.toml",
    "Manifest.toml",
    "Makefile",
    "Project.toml",
    "Snakefile",
    "conda.yaml",
    "conda.yml",
    "environment.yaml",
    "environment.yml",
    "nextflow.config",
    "package.json",
    "pyproject.toml",
    "requirements.txt",
}

SECRET_NAMES = {
    ".env",
    ".npmrc",
    ".pypirc",
    "credentials.json",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
}


@dataclass(frozen=True)
class ScanLimits:
    max_files: int = MAX_FILES
    max_total_text_bytes: int = MAX_TOTAL_TEXT_BYTES
    max_file_text_bytes: int = MAX_FILE_TEXT_BYTES
    max_evidence_paths: int = MAX_EVIDENCE_PATHS


@dataclass
class ScanMetadata:
    limits: dict[str, int]
    files_seen: int = 0
    text_files_read: int = 0
    text_bytes_read: int = 0
    skipped_symlinks: int = 0
    skipped_hidden_dirs: int = 0
    skipped_sensitive_files: int = 0
    skipped_binary_files: int = 0
    per_file_truncations: int = 0
    truncated: bool = False
    truncation_reasons: list[str] = field(default_factory=list)

    def truncate(self, reason: str) -> None:
        self.truncated = True
        if reason not in self.truncation_reasons:
            self.truncation_reasons.append(reason)


@dataclass
class Signal:
    name: str
    confidence: str
    workload_phrase: str
    evidence_paths: list[str] = field(default_factory=list)
    evidence_truncated: bool = False


@dataclass(frozen=True)
class ContentRule:
    name: str
    workload_phrase: str
    pattern: re.Pattern[str]
    confidence: str = "high"


CONTENT_RULES = (
    ContentRule(
        "pytorch",
        "PyTorch training or inference",
        re.compile(r"\b(?:import\s+torch|from\s+torch\b|pytorch\b|torchrun\b)", re.I),
    ),
    ContentRule("deepspeed", "distributed DeepSpeed training", re.compile(r"\bdeepspeed\b", re.I)),
    ContentRule(
        "accelerate",
        "distributed Hugging Face Accelerate training",
        re.compile(r"\b(?:accelerate\s+launch|from\s+accelerate|import\s+accelerate)\b", re.I),
    ),
    ContentRule("jax", "distributed JAX workload", re.compile(r"\b(?:import\s+jax|from\s+jax)\b", re.I)),
    ContentRule(
        "mpi",
        "multi-node MPI workload",
        re.compile(
            r"\b(?:from\s+mpi4py|import\s+mpi4py|mpirun|mpiexec|openmpi|pmix|srun\s+[^\n]*--mpi(?:=|\s))\b",
            re.I,
        ),
    ),
    ContentRule(
        "gpu",
        "GPU or CUDA workload",
        re.compile(r"\b(?:cuda|nvidia-smi|nccl|gres\s*:\s*gpu|gpus?_per_node)\b", re.I),
    ),
    ContentRule("redis", "multi-service application with Redis", re.compile(r"\bredis(?:-server)?\b", re.I)),
    ContentRule("snakemake", "Snakemake workflow", re.compile(r"\bsnakemake\b", re.I)),
    ContentRule("nextflow", "Nextflow workflow", re.compile(r"\bnextflow\b", re.I)),
    ContentRule("vllm", "LLM serving with vLLM", re.compile(r"\bvllm\b", re.I)),
)


class SignalSet:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self._signals: dict[str, Signal] = {}

    def add(self, name: str, confidence: str, workload_phrase: str, path: str) -> None:
        signal = self._signals.setdefault(name, Signal(name, confidence, workload_phrase))
        if confidence_rank(confidence) > confidence_rank(signal.confidence):
            signal.confidence = confidence
        if path in signal.evidence_paths:
            return
        if len(signal.evidence_paths) < self.limit:
            signal.evidence_paths.append(path)
            signal.evidence_paths.sort()
        else:
            signal.evidence_truncated = True

    def values(self) -> list[Signal]:
        return [self._signals[name] for name in sorted(self._signals)]


def confidence_rank(value: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}[value]


def is_sensitive_file(path: Path) -> bool:
    name = path.name.lower()
    return (
        name in SECRET_NAMES
        or name.startswith(".env.")
        or name.startswith("credentials.")
        or name.startswith("secrets.")
        or path.suffix.lower() in {".key", ".pem", ".p12", ".pfx"}
    )


def looks_text(path: Path) -> bool:
    return (
        path.name in PACKAGE_FILES
        or path.name.startswith("Dockerfile")
        or path.suffix.lower() in TEXT_SUFFIXES
    )


def iter_files(root: Path, metadata: ScanMetadata, limits: ScanLimits) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        current = Path(dirpath)
        kept_dirs: list[str] = []
        for dirname in sorted(dirnames):
            child = current / dirname
            # Hidden directories are tool/configuration state, not workload
            # evidence. Browser and agent capture directories can contain
            # arbitrary copied page text that must not influence recommendations.
            if dirname.startswith("."):
                metadata.skipped_hidden_dirs += 1
                continue
            if dirname in SKIP_DIRS:
                continue
            if (child / "SKILL.md").is_file() and (child / "agents").is_dir():
                continue
            if child.is_symlink():
                metadata.skipped_symlinks += 1
                continue
            kept_dirs.append(dirname)
        dirnames[:] = kept_dirs

        for filename in sorted(filenames):
            path = current / filename
            if path.is_symlink():
                metadata.skipped_symlinks += 1
                continue
            if metadata.files_seen >= limits.max_files:
                metadata.truncate("file_limit")
                return
            metadata.files_seen += 1
            if path.is_file():
                yield path


def relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def code_text(path: Path, text: str) -> str:
    """Remove prose and comment-only lines before token-aware workload matching."""
    if path.suffix.lower() not in TEXT_SUFFIXES or path.suffix.lower() in {".json"}:
        return text
    kept: list[str] = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#") and not stripped.upper().startswith("#SBATCH"):
            continue
        if stripped.startswith("//"):
            continue
        kept.append(line)
    return "\n".join(kept)


def record_path_signals(path: Path, rel: str, signals: SignalSet) -> None:
    name = path.name
    lowered = name.lower()
    if lowered in {"compose.yml", "compose.yaml", "docker-compose.yml", "docker-compose.yaml"} or lowered.startswith(
        "docker-compose."
    ):
        signals.add("docker-compose", "high", "Docker Compose migration", rel)
    if name.startswith("Dockerfile"):
        signals.add("dockerfile", "high", "containerized workload with image preparation", rel)
    if "hpc" in lowered and path.suffix.lower() in {".yaml", ".yml"}:
        signals.add("hpc-compose-spec", "medium", "existing HPC compose specification", rel)
    if name in PACKAGE_FILES:
        signals.add("package-manifest", "high", "application with managed dependencies", rel)
    if name == "Snakefile":
        signals.add("snakemake", "high", "Snakemake workflow", rel)
    if name == "nextflow.config":
        signals.add("nextflow", "high", "Nextflow workflow", rel)


def read_bounded_text(
    path: Path, metadata: ScanMetadata, limits: ScanLimits
) -> str | None:
    if is_sensitive_file(path):
        metadata.skipped_sensitive_files += 1
        return None
    if not looks_text(path):
        return None
    if metadata.text_bytes_read >= limits.max_total_text_bytes:
        metadata.truncate("total_text_byte_limit")
        return None
    try:
        size = path.stat(follow_symlinks=False).st_size
    except OSError:
        return None
    remaining = limits.max_total_text_bytes - metadata.text_bytes_read
    read_limit = min(limits.max_file_text_bytes, remaining)
    try:
        with path.open("rb") as handle:
            data = handle.read(read_limit)
    except OSError:
        return None
    metadata.text_files_read += 1
    metadata.text_bytes_read += len(data)
    if size > limits.max_file_text_bytes:
        metadata.per_file_truncations += 1
        metadata.truncate("per_file_text_byte_limit")
    if size > remaining:
        metadata.truncate("total_text_byte_limit")
    if b"\x00" in data:
        metadata.skipped_binary_files += 1
        return None
    return data.decode("utf-8", errors="replace")


def scan_repository(root: Path, limits: ScanLimits = ScanLimits()) -> dict[str, object]:
    metadata = ScanMetadata(
        limits={
            "max_files": limits.max_files,
            "max_total_text_bytes": limits.max_total_text_bytes,
            "max_file_text_bytes": limits.max_file_text_bytes,
            "max_evidence_paths_per_signal": limits.max_evidence_paths,
        }
    )
    signals = SignalSet(limits.max_evidence_paths)
    for path in iter_files(root, metadata, limits):
        rel = relative(path, root)
        record_path_signals(path, rel, signals)
        text = read_bounded_text(path, metadata, limits)
        if text is None:
            continue
        searchable = code_text(path, text)
        if path.suffix.lower() in {".slurm", ".sbatch", ".job", ".sh"} and re.search(
            r"(?im)^\s*#SBATCH\b", text
        ):
            signals.add("slurm-script", "high", "migration from an existing Slurm script", rel)
        for rule in CONTENT_RULES:
            if rule.pattern.search(searchable):
                signals.add(rule.name, rule.confidence, rule.workload_phrase, rel)

    signal_values = signals.values()
    phrases = sorted({signal.workload_phrase for signal in signal_values})
    return {
        "schema_version": SCHEMA_VERSION,
        "root": str(root),
        "scan": asdict(metadata),
        "signals": [asdict(signal) for signal in signal_values],
        "workload_phrases": phrases,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", nargs="?", default=".", help="Repository path to scan")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.repo).resolve()
    if not root.is_dir():
        raise SystemExit(f"repository path is not a directory: {root}")
    print(json.dumps(scan_repository(root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
