#!/usr/bin/env python3
"""Validate and package the version-matched hpc-compose Agent Skill."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import re
import sys
import tarfile
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills/hpc-compose"
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
SKILL_PAYLOAD = {
    "SKILL.md",
    "VERSION",
    "agents/openai.yaml",
    "references/authoring-migration.md",
    "references/cluster-setup.md",
    "references/command-safety.md",
    "references/operations-recovery.md",
    "scripts/hpc_compose_repo_probe.py",
}


class SkillError(RuntimeError):
    pass


def crate_version() -> str:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    return manifest["package"]["version"]


def parse_frontmatter(text: str) -> dict[str, str]:
    lines = text.splitlines()
    if not lines or lines[0] != "---":
        raise SkillError("SKILL.md must start with YAML frontmatter")
    try:
        end = lines.index("---", 1)
    except ValueError as error:
        raise SkillError("SKILL.md frontmatter is not closed") from error
    fields: dict[str, str] = {}
    for line in lines[1:end]:
        if not line.strip():
            continue
        if ":" not in line:
            raise SkillError(f"unsupported frontmatter line: {line}")
        key, value = line.split(":", 1)
        fields[key.strip()] = value.strip().strip('"\'')
    return fields


def validate_skill(version: str) -> list[Path]:
    skill_file = SKILL / "SKILL.md"
    text = skill_file.read_text(encoding="utf-8")
    fields = parse_frontmatter(text)
    if set(fields) != {"name", "description"}:
        raise SkillError("SKILL.md frontmatter must contain exactly name and description")
    if fields["name"] != SKILL.name:
        raise SkillError("SKILL.md name must match its directory")
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", fields["name"]):
        raise SkillError("SKILL.md name is not Agent Skills compatible")
    if len(fields["name"]) > 64:
        raise SkillError("SKILL.md name exceeds the Agent Skills limit of 64 characters")
    if not 1 <= len(fields["description"]) <= 1024:
        raise SkillError("SKILL.md description must contain 1-1024 characters")
    if len(text.splitlines()) > 150:
        raise SkillError("SKILL.md exceeds the project limit of 150 lines")
    if len(text.split()) > 1500:
        raise SkillError("SKILL.md exceeds the project limit of 1500 words")

    recorded_version = (SKILL / "VERSION").read_text(encoding="utf-8").strip()
    if recorded_version != version:
        raise SkillError(
            f"skills/hpc-compose/VERSION is {recorded_version!r}, expected {version!r}"
        )

    for match in LINK_RE.finditer(text):
        target = match.group(1)
        if "://" in target or target.startswith("#"):
            continue
        path = (SKILL / target).resolve()
        try:
            path.relative_to(SKILL)
        except ValueError as error:
            raise SkillError(f"SKILL.md reference escapes the skill: {target}") from error
        if not path.exists():
            raise SkillError(f"SKILL.md reference does not exist: {target}")

    manifest = (SKILL / "agents/openai.yaml").read_text(encoding="utf-8")
    if not manifest.startswith("interface:\n"):
        raise SkillError("agents/openai.yaml must define a top-level interface mapping")
    display_match = re.search(r'^\s*display_name:\s*"([^"]+)"\s*$', manifest, re.M)
    short_match = re.search(r'^\s*short_description:\s*"([^"]+)"\s*$', manifest, re.M)
    prompt_match = re.search(r'^\s*default_prompt:\s*"([^"]+)"\s*$', manifest, re.M)
    if display_match is None or not 1 <= len(display_match.group(1)) <= 64:
        raise SkillError("agents/openai.yaml display_name must contain 1-64 characters")
    if short_match is None or not 25 <= len(short_match.group(1)) <= 64:
        raise SkillError("agents/openai.yaml short_description must contain 25-64 characters")
    if prompt_match is None or "$hpc-compose" not in prompt_match.group(1):
        raise SkillError("agents/openai.yaml default_prompt must mention $hpc-compose")
    if (SKILL / "agents/README.md").exists():
        raise SkillError("the skill must not contain an auxiliary agent README")

    symlinks = [path for path in SKILL.rglob("*") if path.is_symlink()]
    if symlinks:
        names = ", ".join(path.relative_to(SKILL).as_posix() for path in symlinks)
        raise SkillError(f"the packaged skill must not contain symlinks: {names}")

    files = [
        path
        for path in SKILL.rglob("*")
        if path.is_file()
        and "tests" not in path.relative_to(SKILL).parts
        and "__pycache__" not in path.relative_to(SKILL).parts
    ]
    relative_files = {path.relative_to(SKILL).as_posix() for path in files}
    if relative_files != SKILL_PAYLOAD:
        missing = sorted(SKILL_PAYLOAD - relative_files)
        unexpected = sorted(relative_files - SKILL_PAYLOAD)
        raise SkillError(
            f"skill payload differs from allowlist; missing={missing}, unexpected={unexpected}"
        )
    return sorted(files, key=lambda path: path.relative_to(SKILL).as_posix())


def archive_bytes(files: list[Path]) -> bytes:
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in files:
            relative = path.relative_to(SKILL)
            data = path.read_bytes()
            info = tarfile.TarInfo((Path("hpc-compose") / relative).as_posix())
            info.size = len(data)
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mode = 0o755 if path.parent.name == "scripts" else 0o644
            archive.addfile(info, io.BytesIO(data))
    return gzip.compress(tar_buffer.getvalue(), compresslevel=9, mtime=0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Validate without writing an archive")
    parser.add_argument("--version", help="Release version without a leading v")
    parser.add_argument("--output-dir", type=Path, default=Path("dist"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    version = (args.version or crate_version()).removeprefix("v")
    try:
        files = validate_skill(version)
        payload = archive_bytes(files)
    except (OSError, SkillError, tomllib.TOMLDecodeError) as error:
        print(f"skill packaging failed: {error}", file=sys.stderr)
        return 1
    if args.check:
        print(f"validated hpc-compose skill v{version} ({len(files)} files)")
        return 0

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    name = f"hpc-compose-skill-v{version}.tar.gz"
    archive = output_dir / name
    archive.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    (output_dir / f"{name}.sha256").write_text(f"{digest}  {name}\n", encoding="utf-8")
    print(f"wrote {archive} and {archive.name}.sha256")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
