class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.36"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.36/hpc-compose-v0.1.36-aarch64-apple-darwin.tar.gz"
    sha256 "5081dd8b9c650e02c8d9a6d169dc789f6f29b0e2f55f4b49174f0937d715472c"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.36/hpc-compose-v0.1.36-x86_64-apple-darwin.tar.gz"
    sha256 "27b9dc11d922bf671c0340ba435602b6276589cd9e593c926b188276d6dd7672"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
