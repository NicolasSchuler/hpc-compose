#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render release notes from the checked-in template."
    )
    parser.add_argument("--template", required=True, help="Path to the release template.")
    parser.add_argument("--tag", required=True, help="Release tag, for example v0.1.25.")
    parser.add_argument("--repo", required=True, help="Repository in owner/name form.")
    parser.add_argument(
        "--assets",
        required=True,
        help="Path to a newline-delimited list of release asset filenames.",
    )
    parser.add_argument(
        "--notes-file",
        required=True,
        help="Path to a file containing curated release summary text.",
    )
    parser.add_argument("--output", required=True, help="Path to write the rendered notes.")
    return parser.parse_args()


def render_asset_list(path: Path) -> str:
    entries = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return "\n".join(f"- `{entry}`" for entry in entries)


def render_notes(path: Path) -> str:
    notes = path.read_text(encoding="utf-8").strip()
    return notes or "See the tagged commit history for the user-visible changes in this release."


def main() -> None:
    args = parse_args()
    template = Path(args.template).read_text(encoding="utf-8")
    rendered = (
        template.replace("{{TAG}}", args.tag)
        .replace("{{REPO}}", args.repo)
        .replace("{{ASSET_LIST}}", render_asset_list(Path(args.assets)))
        .replace("{{CHANGELOG_NOTES}}", render_notes(Path(args.notes_file)))
    )
    Path(args.output).write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
