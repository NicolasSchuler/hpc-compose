class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.39"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.39/hpc-compose-v0.1.39-aarch64-apple-darwin.tar.gz"
    sha256 "158ca131e5ade40eee5ef38b239aa0726e7ecd931e4ac77fd4e54086502fb51f"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.39/hpc-compose-v0.1.39-x86_64-apple-darwin.tar.gz"
    sha256 "349adfbbc012826d8a9a240bed025462d2f1b69935aef69213b4617f822b9c89"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
