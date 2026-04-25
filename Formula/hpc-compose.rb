class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.32"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.32/hpc-compose-v0.1.32-aarch64-apple-darwin.tar.gz"
    sha256 "2bdb90b956d62723c0df892a1c7ba9f861a2ded3dc16acd071f28bcdc0f4e7c8"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.32/hpc-compose-v0.1.32-x86_64-apple-darwin.tar.gz"
    sha256 "a8d1a2d01928ebe98c134d9568e340fbe1b65632ad2f228f78579fecb4694378"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
