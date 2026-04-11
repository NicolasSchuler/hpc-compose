class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.23"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.23/hpc-compose-v0.1.23-aarch64-apple-darwin.tar.gz"
    sha256 "c91a16e6a87edf11b43a12778f6d2387ea7822a050fef70e3a90e0b103627132"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.23/hpc-compose-v0.1.23-x86_64-apple-darwin.tar.gz"
    sha256 "3bb06faba75e9d6471ce324349281bf716ae44163ccc15fa82c7a9c751792379"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
