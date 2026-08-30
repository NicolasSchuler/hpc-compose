class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.2.4"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.2.4/hpc-compose-v0.2.4-aarch64-apple-darwin.tar.gz"
    sha256 "9bb71de7c3e3052488d8bd781b6c40b9e3fbfcf2fdf5c2e07adfa66639696195"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.2.4/hpc-compose-v0.2.4-x86_64-apple-darwin.tar.gz"
    sha256 "e2c465cb61753d12672b352c6c88c544af8351c2d1e45802e03f919aab7dec0c"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
