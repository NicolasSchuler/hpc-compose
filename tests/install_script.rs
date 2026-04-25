use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use sha2::{Digest, Sha256};
use tempfile::tempdir;

const BINARY_NAME: &str = "hpc-compose";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn installer_path() -> PathBuf {
    repo_root().join("install.sh")
}

fn built_binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_hpc-compose"))
}

fn host_release_target() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "aarch64-unknown-linux-musl",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        (os, arch) => panic!("unsupported test platform: {os} {arch}"),
    }
}

fn copy_file(src: &Path, dst: &Path) {
    fs::copy(src, dst).unwrap_or_else(|err| {
        panic!(
            "failed to copy {} to {}: {err}",
            src.display(),
            dst.display()
        )
    });
}

fn build_fake_release_asset(dist_dir: &Path, version: &str) -> String {
    let stage_dir = dist_dir
        .parent()
        .expect("dist directory should have parent")
        .join("stage");
    fs::create_dir_all(stage_dir.join("share/man/man1")).expect("create staged manpage directory");

    let staged_binary = stage_dir.join(BINARY_NAME);
    copy_file(&built_binary_path(), &staged_binary);
    let mut perms = fs::metadata(&staged_binary)
        .expect("staged binary metadata")
        .permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&staged_binary, perms).expect("chmod staged binary");

    copy_file(&repo_root().join("README.md"), &stage_dir.join("README.md"));

    for entry in fs::read_dir(repo_root().join("man/man1")).expect("read man/man1") {
        let entry = entry.expect("manpage entry");
        let src = entry.path();
        if src.is_file() {
            copy_file(
                &src,
                &stage_dir.join("share/man/man1").join(entry.file_name()),
            );
        }
    }

    let asset = format!("{BINARY_NAME}-{version}-{}.tar.gz", host_release_target());
    let archive_path = dist_dir.join(&asset);

    let status = Command::new("tar")
        .arg("-C")
        .arg(&stage_dir)
        .arg("-czf")
        .arg(&archive_path)
        .arg(BINARY_NAME)
        .arg("README.md")
        .arg("share/man/man1")
        .status()
        .expect("run tar to create staged release archive");
    assert!(
        status.success(),
        "tar failed creating {}",
        archive_path.display()
    );

    asset
}

fn sha256_hex(path: &Path) -> String {
    let bytes =
        fs::read(path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn write_checksum_file(dist_dir: &Path, asset: &str, include_dist_prefix: bool) {
    let archive_path = dist_dir.join(asset);
    let digest = sha256_hex(&archive_path);
    let listed_name = if include_dist_prefix {
        format!("dist/{asset}")
    } else {
        asset.to_string()
    };
    let checksum_content = format!("{digest}  {listed_name}\n");
    fs::write(dist_dir.join(format!("{asset}.sha256")), checksum_content)
        .expect("write checksum file");
}

fn run_installer(dist_dir: &Path, version: &str, install_dir: &Path) -> Output {
    Command::new("sh")
        .arg(installer_path())
        .env(
            "HPC_COMPOSE_BASE_URL",
            format!("file://{}", dist_dir.display()),
        )
        .env("HPC_COMPOSE_VERSION", version)
        .env("HPC_COMPOSE_INSTALL_DIR", install_dir)
        .output()
        .expect("run install.sh")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "installer failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn setup_release_fixture(include_dist_prefix: bool) -> (tempfile::TempDir, PathBuf, String) {
    let temp = tempdir().expect("create tempdir");
    let dist_dir = temp.path().join("dist");
    fs::create_dir_all(&dist_dir).expect("create dist directory");

    let version = "v0.0.0-installer-test".to_string();
    let asset = build_fake_release_asset(&dist_dir, &version);
    write_checksum_file(&dist_dir, &asset, include_dist_prefix);

    (temp, dist_dir, version)
}

#[test]
fn installer_places_binary_in_requested_directory_and_makes_it_executable() {
    let (_temp, dist_dir, version) = setup_release_fixture(false);

    let install_dir = dist_dir
        .parent()
        .expect("dist parent")
        .join("install-root/bin");
    fs::create_dir_all(&install_dir).expect("create install directory");

    let output = run_installer(&dist_dir, &version, &install_dir);
    assert_success(&output);

    let installed_binary = install_dir.join(BINARY_NAME);
    assert!(
        installed_binary.exists(),
        "expected installed binary at {}",
        installed_binary.display()
    );
    let mode = fs::metadata(&installed_binary)
        .expect("installed binary metadata")
        .permissions()
        .mode();
    assert_ne!(
        mode & 0o111,
        0,
        "expected {} to be executable, mode={mode:o}",
        installed_binary.display()
    );

    let version_output = Command::new(&installed_binary)
        .arg("--version")
        .output()
        .expect("run installed binary --version");
    assert!(
        version_output.status.success(),
        "installed binary --version failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&version_output.stdout),
        String::from_utf8_lossy(&version_output.stderr)
    );

    let man_root = install_dir
        .parent()
        .expect("install root")
        .join("share/man/man1");
    assert!(
        man_root.join("hpc-compose.1").exists(),
        "expected installed manpage at {}",
        man_root.join("hpc-compose.1").display()
    );
    assert!(
        man_root.join("hpc-compose-up.1").exists(),
        "expected installed subcommand manpage at {}",
        man_root.join("hpc-compose-up.1").display()
    );
}

#[test]
fn installer_accepts_checksums_that_list_dist_prefixed_paths() {
    let (_temp, dist_dir, version) = setup_release_fixture(true);

    let install_dir = dist_dir
        .parent()
        .expect("dist parent")
        .join("install-root/bin");
    fs::create_dir_all(&install_dir).expect("create install directory");

    let output = run_installer(&dist_dir, &version, &install_dir);
    assert_success(&output);

    assert!(
        install_dir.join(BINARY_NAME).exists(),
        "expected installed binary at {}",
        install_dir.join(BINARY_NAME).display()
    );
}
