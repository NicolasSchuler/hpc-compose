#!/usr/bin/env python3
"""Regenerate the pa11y-ci URL list from the built mdBook site.

Keeps `.pa11yci.json` accessibility coverage in sync with `docs/src/SUMMARY.md`
automatically: every rendered content page is checked, while mdBook helper pages
such as the sidebar table of contents are skipped.

Run after `mdbook build docs`.
"""

import glob
import json
import os
import sys

BUILD_DIR = "target/mdbook"
CONFIG = ".pa11yci.json"
BASE_URL = "http://127.0.0.1:3000"
SKIP = {"404.html", "print.html", "toc.html"}


def main() -> int:
    pages = sorted(
        os.path.basename(path)
        for path in glob.glob(os.path.join(BUILD_DIR, "*.html"))
        if os.path.basename(path) not in SKIP
    )
    if not pages:
        print(
            f"no built HTML pages found under {BUILD_DIR}; run `mdbook build docs` first",
            file=sys.stderr,
        )
        return 1

    with open(CONFIG, encoding="utf-8") as handle:
        config = json.load(handle)
    config["urls"] = [f"{BASE_URL}/{name}" for name in pages]
    with open(CONFIG, "w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")

    print(f"pa11y URL list: {len(pages)} pages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
