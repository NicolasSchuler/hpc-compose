use std::fs;
use std::path::Path;

use toml::Value;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn cargo_manifest() -> Value {
    fs::read_to_string(repo_root().join("Cargo.toml"))
        .expect("read Cargo.toml")
        .parse::<Value>()
        .expect("parse Cargo.toml")
}

fn cargo_package_table() -> toml::value::Table {
    cargo_manifest()["package"]
        .as_table()
        .expect("[package] table")
        .clone()
}

fn cargo_package_string(key: &str) -> String {
    cargo_package_table()[key]
        .as_str()
        .unwrap_or_else(|| panic!("Cargo package {key} should be a string"))
        .to_string()
}

fn has_deb_asset(assets: &[Value], source: &str, dest: &str, mode: &str) -> bool {
    assets.iter().any(|asset| {
        asset.as_array().is_some_and(|entries| {
            entries.len() == 3
                && entries[0].as_str() == Some(source)
                && entries[1].as_str() == Some(dest)
                && entries[2].as_str() == Some(mode)
        })
    })
}

fn has_rpm_asset(assets: &[Value], source: &str, dest: &str, mode: &str) -> bool {
    assets.iter().any(|asset| {
        asset.as_table().is_some_and(|entry| {
            entry.get("source").and_then(Value::as_str) == Some(source)
                && entry.get("dest").and_then(Value::as_str) == Some(dest)
                && entry.get("mode").and_then(Value::as_str) == Some(mode)
        })
    })
}

#[test]
fn release_metadata_matches_cargo_package_version() {
    let version = cargo_package_string("version");

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

#[test]
fn linux_package_metadata_matches_release_layout() {
    let manifest = cargo_manifest();
    let package = manifest["package"].as_table().expect("[package] table");
    let package_name = package["name"].as_str().expect("package.name");
    let package_license = package["license"].as_str().expect("package.license");
    let package_homepage = package["homepage"].as_str().expect("package.homepage");
    let package_description = package["description"]
        .as_str()
        .expect("package.description");

    let deb = manifest["package"]["metadata"]["deb"]
        .as_table()
        .expect("[package.metadata.deb] table");
    assert_eq!(deb.get("depends").and_then(Value::as_str), Some("$auto"));
    let deb_assets = deb["assets"].as_array().expect("deb assets array");
    assert!(
        has_deb_asset(deb_assets, "target/release/hpc-compose", "usr/bin/", "755"),
        "deb metadata should package the release binary into usr/bin"
    );
    assert!(
        has_deb_asset(
            deb_assets,
            "README.md",
            "usr/share/doc/hpc-compose/README.md",
            "644"
        ),
        "deb metadata should package README into the doc directory"
    );
    assert!(
        has_deb_asset(deb_assets, "man/man1/*.1", "usr/share/man/man1/", "644"),
        "deb metadata should package manpages into usr/share/man/man1"
    );

    let rpm = manifest["package"]["metadata"]["generate-rpm"]
        .as_table()
        .expect("[package.metadata.generate-rpm] table");
    assert_eq!(
        rpm.get("name"),
        None,
        "rpm metadata should reuse package.name"
    );
    assert_eq!(
        rpm.get("version"),
        None,
        "rpm metadata should reuse package.version"
    );
    assert_eq!(
        rpm.get("license"),
        None,
        "rpm metadata should reuse package.license"
    );
    assert_eq!(
        rpm.get("url"),
        None,
        "rpm metadata should reuse package.homepage"
    );
    assert_eq!(
        rpm.get("summary").and_then(Value::as_str),
        Some(package_description)
    );
    assert_eq!(rpm.get("require-sh").and_then(Value::as_bool), Some(false));
    let rpm_assets = rpm["assets"].as_array().expect("rpm assets array");
    assert!(
        has_rpm_asset(
            rpm_assets,
            "target/release/hpc-compose",
            "/usr/bin/hpc-compose",
            "755"
        ),
        "rpm metadata should package the release binary into /usr/bin"
    );
    assert!(
        has_rpm_asset(
            rpm_assets,
            "README.md",
            "/usr/share/doc/hpc-compose/README.md",
            "644"
        ),
        "rpm metadata should package README into the doc directory"
    );
    assert!(
        has_rpm_asset(rpm_assets, "man/man1/*.1", "/usr/share/man/man1/", "644"),
        "rpm metadata should package manpages into /usr/share/man/man1"
    );

    assert_eq!(package_name, "hpc-compose");
    assert_eq!(package_license, "MIT");
    assert_eq!(
        package_homepage,
        "https://github.com/NicolasSchuler/hpc-compose"
    );
}

#[test]
fn release_workflow_publishes_checksum_manifest_and_rendered_notes() {
    let workflow = fs::read_to_string(repo_root().join(".github/workflows/release.yml"))
        .expect("read release workflow");
    assert!(
        workflow.contains("dist/SHA256SUMS"),
        "release workflow should publish an aggregate checksum manifest"
    );
    assert!(
        workflow.contains("scripts/render_release_notes.py"),
        "release workflow should render release notes from the checked-in template"
    );
    assert!(
        workflow.contains("homebrew-formula-refresh"),
        "release workflow should refresh the Homebrew formula after publishing assets"
    );
    assert!(
        workflow.contains("hpc-compose-up.1"),
        "release workflow should smoke-test a current subcommand manpage"
    );
    assert!(
        workflow.contains("require_manpage"),
        "release workflow should validate native package manpages through the helper"
    );
    assert!(
        workflow.contains("\".gz\"") && workflow.contains("\".zst\""),
        "native package manpage checks should allow common package-manager compression"
    );
    assert!(
        workflow.contains("cargo generate-rpm --target"),
        "release workflow should pass the matrix target to cargo-generate-rpm"
    );
    assert!(
        workflow.contains("--payload-compress gzip"),
        "release workflow should build rpm payloads readable by the smoke-test tooling"
    );
    assert!(
        !workflow.contains("hpc-compose-submit.1"),
        "release workflow should not reference removed submit manpages"
    );
}

#[test]
fn release_template_mentions_verification_commands() {
    let template = fs::read_to_string(repo_root().join(".github/RELEASE_TEMPLATE.md"))
        .expect("read release template");
    assert!(
        template.contains("gh release verify {{TAG}} -R {{REPO}}"),
        "release template should include release verification guidance"
    );
    assert!(
        template.contains("gh release verify-asset {{TAG}} ./<downloaded-asset> -R {{REPO}}"),
        "release template should include asset verification guidance"
    );
    assert!(
        template.contains("gh attestation verify ./<downloaded-asset>"),
        "release template should include attestation verification guidance"
    );
    assert!(
        template.contains("SHA256SUMS"),
        "release template should mention the aggregate checksum manifest"
    );
}
