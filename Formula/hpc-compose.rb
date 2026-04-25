class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.31"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.31/hpc-compose-v0.1.31-aarch64-apple-darwin.tar.gz"
    sha256 "60bf62b6ddd136424100466efcc0e8976357ca64b0ca897bf936c793d3215174"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.31/hpc-compose-v0.1.31-x86_64-apple-darwin.tar.gz"
    sha256 "502f35accf307e13596783d62e3b85d1e00004a0b919f27708b597248e1b98ab"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
