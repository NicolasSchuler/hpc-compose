class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.47"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.47/hpc-compose-v0.1.47-aarch64-apple-darwin.tar.gz"
    sha256 "f08eaf0b6e4bf83689af2dbc98fc1d02883e5ca2ca93be301876a94d94474049"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.47/hpc-compose-v0.1.47-x86_64-apple-darwin.tar.gz"
    sha256 "4191cfb29ae8244ef0b7550598047fabbfff93849a9dcc6949538d181fede685"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
