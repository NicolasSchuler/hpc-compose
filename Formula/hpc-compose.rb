class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs with Enroot and Pyxis"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.33"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.33/hpc-compose-v0.1.33-aarch64-apple-darwin.tar.gz"
    sha256 "9cd10ad86bec4106b567aebfad07577819a387f3a71fad715dc038f9745948a1"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.33/hpc-compose-v0.1.33-x86_64-apple-darwin.tar.gz"
    sha256 "67c65ec343969b5cc60419f3dbbdfb8a59a7c8b989532192aaa2f9c9a0a1b18a"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
