class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.38"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.38/hpc-compose-v0.1.38-aarch64-apple-darwin.tar.gz"
    sha256 "3f871176f3fe399216dddef057484d50b6383e0d53f84783329e655395e638b6"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.38/hpc-compose-v0.1.38-x86_64-apple-darwin.tar.gz"
    sha256 "91faaffc9a73d2f874417936e2f52c36e2d6bf512d5ceb2c893aca5c66c56173"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
