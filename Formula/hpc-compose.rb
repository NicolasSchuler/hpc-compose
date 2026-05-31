class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.44"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.44/hpc-compose-v0.1.44-aarch64-apple-darwin.tar.gz"
    sha256 "0e8ea4345742636c33483e0e467bed54482c69ad95de96947fcb95f0749632f2"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.44/hpc-compose-v0.1.44-x86_64-apple-darwin.tar.gz"
    sha256 "9b2e557a7a9e21c867165c8e40da9c9509efc85946719c30882ae23cddcd83f3"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
