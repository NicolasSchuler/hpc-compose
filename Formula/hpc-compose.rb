class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.37"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.37/hpc-compose-v0.1.37-aarch64-apple-darwin.tar.gz"
    sha256 "12a16ac148c7a553238e0afb0dbda29e26a51875a65dbe52b4c46d6ed582cf7f"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.37/hpc-compose-v0.1.37-x86_64-apple-darwin.tar.gz"
    sha256 "4a0d706458d99bd78cbbec3bc64b491db80e60e12cc8c5c02e1ffed0eeceeb78"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
