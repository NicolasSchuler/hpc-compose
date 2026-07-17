class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.2.3"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.2.3/hpc-compose-v0.2.3-aarch64-apple-darwin.tar.gz"
    sha256 "b1ee75d85b03a11c11e7a7104f0514372ac1630a984e5fd71cab3523fc545b91"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.2.3/hpc-compose-v0.2.3-x86_64-apple-darwin.tar.gz"
    sha256 "d788b1211ab2a9cb26292baf200c18c9f10a487589f83e4035a782915daa1527"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
