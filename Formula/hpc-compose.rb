class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.34"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.34/hpc-compose-v0.1.34-aarch64-apple-darwin.tar.gz"
    sha256 "679fcd5aaa4199d31ef11e8cf3dbe98ad219b4d638d0b1f4c7ca44761d647b5d"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.34/hpc-compose-v0.1.34-x86_64-apple-darwin.tar.gz"
    sha256 "73dfc1e7eb3ee6e6d82aeaf1c9736261c7ceb2a4241dcf181eab9f570f379563"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
