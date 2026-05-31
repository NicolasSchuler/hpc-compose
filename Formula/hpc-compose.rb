class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.43"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.43/hpc-compose-v0.1.43-aarch64-apple-darwin.tar.gz"
    sha256 "a48d4a8c021c89e5dcfca64022e81dda6d793867698a76a2b09f4e75f1cdb957"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.43/hpc-compose-v0.1.43-x86_64-apple-darwin.tar.gz"
    sha256 "f42b1107332845f101fbefb59fe4850800d22392986e64e35e1ac64c8535dbe1"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
