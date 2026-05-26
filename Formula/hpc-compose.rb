class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.41"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.41/hpc-compose-v0.1.41-aarch64-apple-darwin.tar.gz"
    sha256 "2f3ce71c226de2ed720bdc4ea2b4f49599481f4088ebf4a338d6ca09d5afcee0"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.41/hpc-compose-v0.1.41-x86_64-apple-darwin.tar.gz"
    sha256 "3e37fea20074144b0722ff0b07ff6f84a32555fdc55311079291f73104a2898e"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
