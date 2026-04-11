#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


TEMPLATE = """class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "{version}"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v{version}/hpc-compose-v{version}-aarch64-apple-darwin.tar.gz"
    sha256 "{arm64_sha256}"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v{version}/hpc-compose-v{version}-x86_64-apple-darwin.tar.gz"
    sha256 "{x86_64_sha256}"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{{bin}}/hpc-compose --version")
  end
end
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the Homebrew formula for the latest published hpc-compose release."
    )
    parser.add_argument("--version", required=True, help="Release version without the leading v.")
    parser.add_argument("--arm64-sha256", required=True, help="SHA-256 of the arm64 macOS tarball.")
    parser.add_argument(
        "--x86-64-sha256",
        dest="x86_64_sha256",
        required=True,
        help="SHA-256 of the x86_64 macOS tarball.",
    )
    parser.add_argument(
        "--output",
        default="Formula/hpc-compose.rb",
        help="Path to the formula file to write.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    rendered = TEMPLATE.format(
        version=args.version,
        arm64_sha256=args.arm64_sha256,
        x86_64_sha256=args.x86_64_sha256,
    )
    output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
