class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.52"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.52/hpc-compose-v0.1.52-aarch64-apple-darwin.tar.gz"
    sha256 "d2cd761bb51a3551b707a5baf34cdfd7d7b899ccf7dacd9f592a8ba056003168"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.52/hpc-compose-v0.1.52-x86_64-apple-darwin.tar.gz"
    sha256 "36e401bd81c8a7507e7b679fd91dcc320f69cb717ede8b79da9eba351e638196"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
