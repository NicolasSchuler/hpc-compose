class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.45"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.45/hpc-compose-v0.1.45-aarch64-apple-darwin.tar.gz"
    sha256 "45680875fa0ee4db334f637d5f953e8bb77dd60b229fb83829ce16800acb48bc"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.45/hpc-compose-v0.1.45-x86_64-apple-darwin.tar.gz"
    sha256 "3a924e8690e1927e2711534456a247bd9721965082f250beec6331e3541b8275"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
