class HpcCompose < Formula
  desc "Compose-like specs for single-allocation Slurm jobs across HPC runtime backends"
  homepage "https://github.com/NicolasSchuler/hpc-compose"
  license "MIT"
  version "0.1.48"

  on_arm do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.48/hpc-compose-v0.1.48-aarch64-apple-darwin.tar.gz"
    sha256 "77e82cad9db44cc5d94185a694b85476dc8961926a70248977a3d1c218efd196"
  end

  on_intel do
    url "https://github.com/NicolasSchuler/hpc-compose/releases/download/v0.1.48/hpc-compose-v0.1.48-x86_64-apple-darwin.tar.gz"
    sha256 "bd0eefe52fab41a161227c90d444b855ea8da305eacd997a897778d14b9a5ffc"
  end

  def install
    bin.install "hpc-compose"
    man1.install Dir["share/man/man1/*.1"]
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/hpc-compose --version")
  end
end
