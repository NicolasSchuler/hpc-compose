class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.51"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.51/hpc-compose-v0.1.51-aarch64-apple-darwin.tar.gz"
    sha256 "ad8d86c157598c1703e4fafb51a290e7d9f3c3bc85df83bba05c8edf3bbdf773"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.51/hpc-compose-v0.1.51-x86_64-apple-darwin.tar.gz"
    sha256 "489ac7d78515143169581cb4a3798f6726ebe8a74584a5bad353fd14a1b860f4"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
