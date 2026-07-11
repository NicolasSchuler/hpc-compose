#!/usr/bin/env python3
"""Generate a pa11y-ci URL list from the built mdBook site.

Uses `.pa11yci.json` as the settings template and discovers rendered pages
recursively, so nested chapters are checked without rewriting the tracked
template. mdBook helper pages such as print output and the sidebar table of
contents are skipped.

Run after `mdbook build docs`, normally with `--output target/pa11y-ci.json`.
"""

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import quote

BUILD_DIR = Path("target/mdbook")
CONFIG = Path(".pa11yci.json")
BASE_URL = "http://127.0.0.1:3000"
SKIP = {"404.html", "print.html", "toc.html"}


def rendered_pages(build_dir: Path) -> list[str]:
    return sorted(
        path.relative_to(build_dir).as_posix()
        for path in build_dir.rglob("*.html")
        if path.relative_to(build_dir).as_posix() not in SKIP
    )


def generated_config(config_path: Path, build_dir: Path, base_url: str) -> dict[str, object]:
    pages = rendered_pages(build_dir)
    if not pages:
        raise ValueError(
            f"no built HTML pages found under {build_dir}; run `mdbook build docs` first"
        )

    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    config["urls"] = [
        f"{base_url.rstrip('/')}/{quote(name, safe='/')}" for name in pages
    ]
    return config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=BUILD_DIR)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path, default=CONFIG)
    parser.add_argument("--base-url", default=BASE_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = generated_config(args.config, args.build_dir, args.base_url)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(error, file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")

    print(f"pa11y URL list: {len(config['urls'])} pages -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
