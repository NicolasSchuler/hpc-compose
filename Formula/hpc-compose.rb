class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.49"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.49/hpc-compose-v0.1.49-aarch64-apple-darwin.tar.gz"
    sha256 "70b26cc3bf34cc5670b18df0d17af68307b77313099e25c2a39b26558078421c"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.49/hpc-compose-v0.1.49-x86_64-apple-darwin.tar.gz"
    sha256 "111828975e2d273ddab96e2745d75e6227ddda90a273d4bab304ce4b9ced9a92"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
