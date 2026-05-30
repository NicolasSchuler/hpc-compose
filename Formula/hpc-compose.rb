class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.42"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.42/hpc-compose-v0.1.42-aarch64-apple-darwin.tar.gz"
    sha256 "d0e06db138f437861d81e899b9b469adab7847d4e8871a73266c4ef43dd2b523"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.42/hpc-compose-v0.1.42-x86_64-apple-darwin.tar.gz"
    sha256 "10c2246143524997ae9636904d7be8ae00470722bb999bae9b49340f9d2c14dc"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
