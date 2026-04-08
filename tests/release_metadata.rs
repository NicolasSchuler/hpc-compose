use std::fs;
use std::path::Path;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn cargo_package_version() -> String {
    let cargo_toml = fs::read_to_string(repo_root().join("Cargo.toml")).expect("read Cargo.toml");
    let mut in_package = false;

    for line in cargo_toml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(']') {
            in_package = trimmed == "[package]";
            continue;
        }

        if in_package && trimmed.starts_with("version = ") {
            return trimmed
                .split('"')
                .nth(1)
                .expect("Cargo package version")
                .to_string();
        }
    }

    panic!("failed to locate [package].version in Cargo.toml");
}

#[test]
fn release_metadata_matches_cargo_package_version() {
    let version = cargo_package_version();

    let citation = fs::read_to_string(repo_root().join("CITATION.cff")).expect("read CITATION.cff");
    assert!(
        citation.contains(&format!("version: \"{version}\"")),
        "CITATION.cff version should match Cargo.toml version {version}"
    );

    let readme = fs::read_to_string(repo_root().join("README.md")).expect("read README.md");
    assert!(
        readme.contains(&format!("version = {{{version}}}")),
        "README citation snippet should match Cargo.toml version {version}"
    );

    let manpage = fs::read_to_string(repo_root().join("man/man1/hpc-compose.1"))
        .expect("read hpc-compose manpage");
    assert!(
        manpage.contains(&format!("hpc\\-compose {version}")),
        "checked-in manpage should match Cargo.toml version {version}"
    );
}
