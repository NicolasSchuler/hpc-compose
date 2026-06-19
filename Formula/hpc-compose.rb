class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.46"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.46/hpc-compose-v0.1.46-aarch64-apple-darwin.tar.gz"
    sha256 "24bdf503406f92313e28473d2b172f20d453ffd48617b1a545bcadc0b56bdd81"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.46/hpc-compose-v0.1.46-x86_64-apple-darwin.tar.gz"
    sha256 "ab0d246a2e64558cad9a4c6ee1ff8bec85ee84493b14714ba74fab5cfd07a300"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
