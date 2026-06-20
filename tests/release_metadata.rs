use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value as JsonValue;
use toml::{Table, Value};

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn cargo_manifest() -> Table {
    fs::read_to_string(repo_root().join("Cargo.toml"))
        .expect("read Cargo.toml")
        .parse::<Table>()
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

fn homebrew_formula_version() -> String {
    let formula =
        fs::read_to_string(repo_root().join("Formula/hpc-compose.rb")).expect("read formula");
    formula
        .lines()
        .map(str::trim)
        .find_map(|line| {
            line.strip_prefix("version \"")
                .and_then(|rest| rest.strip_suffix('"'))
        })
        .unwrap_or_else(|| panic!("Formula/hpc-compose.rb should declare version \"...\""))
        .to_string()
}

fn parse_version_triplet(version: &str) -> (u64, u64, u64) {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("invalid major version in {version}"));
    let minor = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("invalid minor version in {version}"));
    let patch = parts
        .next()
        .and_then(|part| part.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("invalid patch version in {version}"));
    assert!(
        parts.next().is_none(),
        "version {version} should have exactly three components"
    );
    (major, minor, patch)
}

fn rust_string_array_const(source: &str, name: &str) -> BTreeSet<String> {
    let marker = format!("const {name}: &[&str] = &[");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("missing {name} allow-list"))
        + marker.len();
    let rest = &source[start..];
    let end = rest
        .find("];")
        .unwrap_or_else(|| panic!("{name} allow-list should end with ];"));
    rest[..end]
        .split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

fn json_object_keys(schema: &JsonValue, pointer: &str) -> BTreeSet<String> {
    schema
        .pointer(pointer)
        .unwrap_or_else(|| panic!("schema missing {pointer}"))
        .as_object()
        .unwrap_or_else(|| panic!("schema {pointer} should be an object"))
        .keys()
        .cloned()
        .collect()
}

fn git_tracks_path(path: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo_root())
        .args(["ls-files", "--error-unmatch", path])
        .status()
        .is_ok_and(|status| status.success())
}

fn workflow_files() -> Vec<PathBuf> {
    let mut files = fs::read_dir(repo_root().join(".github/workflows"))
        .expect("read workflows directory")
        .map(|entry| entry.expect("workflow entry").path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|ext| ext.to_str()),
                Some("yml" | "yaml")
            )
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn is_sha_pinned_action_reference(reference: &str) -> bool {
    let Some((_, revision)) = reference.rsplit_once('@') else {
        return false;
    };
    revision.len() == 40 && revision.chars().all(|ch| ch.is_ascii_hexdigit())
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
fn homebrew_formula_tracks_published_release_or_pending_refresh() {
    let version = cargo_package_string("version");
    let formula_version = homebrew_formula_version();
    let formula =
        fs::read_to_string(repo_root().join("Formula/hpc-compose.rb")).expect("read formula");

    let cargo_triplet = parse_version_triplet(&version);
    let formula_triplet = parse_version_triplet(&formula_version);
    let formula_is_current = formula_version == version;
    let formula_is_previous_patch = formula_triplet.0 == cargo_triplet.0
        && formula_triplet.1 == cargo_triplet.1
        && formula_triplet.2 + 1 == cargo_triplet.2;
    assert!(
        formula_is_current || formula_is_previous_patch,
        "Homebrew formula version {formula_version} should match Cargo.toml {version} or lag by exactly one patch while release automation opens the checksum-refresh PR"
    );
    assert!(
        formula.contains(&format!("/releases/download/v{formula_version}/")),
        "Homebrew formula URLs should point at v{formula_version} release assets"
    );
    assert!(
        formula.contains(&format!(
            "hpc-compose-v{formula_version}-aarch64-apple-darwin.tar.gz"
        )),
        "Homebrew formula should reference the arm64 macOS tarball for its declared version"
    );
    assert!(
        formula.contains(&format!(
            "hpc-compose-v{formula_version}-x86_64-apple-darwin.tar.gz"
        )),
        "Homebrew formula should reference the x86_64 macOS tarball for its declared version"
    );
}

#[test]
fn changelog_documents_current_release_and_submit_to_up_rename() {
    let version = cargo_package_string("version");
    let changelog =
        fs::read_to_string(repo_root().join("CHANGELOG.md")).expect("read CHANGELOG.md");
    assert!(
        changelog.contains(&format!("## [{version}]")),
        "CHANGELOG.md should contain a release entry for v{version}"
    );
    assert!(
        changelog.contains("submit` -> `up") || changelog.contains("submit -> up"),
        "CHANGELOG.md should document the submit-to-up breaking rename"
    );
}

#[test]
fn schema_allowed_keys_match_spec_validation_allowlists() {
    let validation =
        fs::read_to_string(repo_root().join("src/spec/validation.rs")).expect("read validation.rs");
    let schema: JsonValue = serde_json::from_str(
        &fs::read_to_string(repo_root().join("schema/hpc-compose.schema.json"))
            .expect("read schema"),
    )
    .expect("parse schema");

    assert_eq!(
        json_object_keys(&schema, "/properties"),
        rust_string_array_const(&validation, "ROOT_ALLOWED_KEYS"),
        "root schema properties should match ROOT_ALLOWED_KEYS"
    );
    assert_eq!(
        json_object_keys(&schema, "/definitions/service/properties"),
        rust_string_array_const(&validation, "SERVICE_ALLOWED_KEYS"),
        "service schema properties should match SERVICE_ALLOWED_KEYS"
    );
}

#[test]
fn root_generated_sbatch_artifacts_stay_out_of_the_repo() {
    let artifact = repo_root().join("hpc-compose-run.sbatch");
    assert!(
        !(artifact.exists() && git_tracks_path("hpc-compose-run.sbatch")),
        "generated root hpc-compose-run.sbatch artifact should not exist as a tracked file"
    );
    let gitignore = fs::read_to_string(repo_root().join(".gitignore")).expect("read .gitignore");
    assert!(
        gitignore.lines().any(|line| line.trim() == "/*.sbatch"),
        ".gitignore should ignore generated root-level sbatch scripts"
    );
}

#[test]
fn coverage_gate_ignores_only_declarative_shells() {
    let justfile = fs::read_to_string(repo_root().join("justfile")).expect("read justfile");
    assert!(
        justfile.contains(
            "--ignore-filename-regex '(^|/)commands/mod\\.rs$|(^|/)cli/commands\\.rs$|(^|/)main\\.rs$|(^|/)job/model\\.rs$'"
        ),
        "coverage gate should only ignore thin declarative shell files"
    );
    for broad_pattern in [
        "commands/|",
        "output/mod\\.rs",
        "watch_ui\\.rs",
        "term\\.rs",
        "progress\\.rs",
        "manpages\\.rs",
        "cli/|",
    ] {
        assert!(
            !justfile.contains(broad_pattern),
            "coverage gate should not broadly ignore {broad_pattern}"
        );
    }
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
        workflow.contains("Unable to create Homebrew formula PR")
            && workflow.contains("/pull/new/${branch}"),
        "Homebrew formula refresh should degrade gracefully when Actions cannot create PRs"
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
        workflow.contains("require_rpm_entry"),
        "release workflow should validate rpm contents through rpm query output"
    );
    assert!(
        !workflow.contains("rpm2cpio"),
        "release workflow should not depend on rpm2cpio extraction for rpm smoke checks"
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

#[test]
fn workflow_action_references_are_sha_pinned() {
    for path in workflow_files() {
        let workflow = fs::read_to_string(&path).expect("read workflow");
        for (line_index, line) in workflow.lines().enumerate() {
            let trimmed = line.trim();
            if let Some(reference) = trimmed.strip_prefix("uses: ") {
                assert!(
                    is_sha_pinned_action_reference(reference),
                    "{}:{} should pin GitHub Actions by full commit SHA, found '{}'",
                    path.display(),
                    line_index + 1,
                    reference
                );
            }
        }
    }
}

#[test]
fn ci_docs_qa_tools_are_version_pinned() {
    let workflow =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    assert!(
        workflow.contains("LYCHEE_VERSION: \"0.23.0\"")
            && workflow.contains("cargo install lychee --locked --version"),
        "CI should pin lychee so docs link checks remain reproducible"
    );
    assert!(
        workflow.contains("PA11Y_CI_VERSION: \"4.0.1\"")
            && workflow.contains("pa11y-ci@${PA11Y_CI_VERSION}"),
        "CI should pin pa11y-ci so accessibility checks remain reproducible"
    );
    assert!(
        workflow.contains("TYPOS_VERSION: \"1.28.4\"")
            && workflow.contains("cargo install typos-cli --locked --version"),
        "CI should pin typos so spell checks remain reproducible"
    );
    assert!(
        workflow.contains("MARKDOWNLINT_CLI2_VERSION: \"0.14.0\"")
            && workflow.contains("markdownlint-cli2@${MARKDOWNLINT_CLI2_VERSION}"),
        "CI should pin markdownlint-cli2 so markdown lint remains reproducible"
    );
}

#[test]
fn ci_runs_actionlint_and_uses_explicit_rust_cache_keys() {
    let workflow =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    assert!(
        workflow.contains("ACTIONLINT_VERSION: \"1.7.12\"")
            && workflow.contains("workflow-lint:")
            && workflow.contains("sha256sum -c -")
            && workflow.contains("actionlint -color"),
        "CI should install verified actionlint and lint GitHub workflows"
    );

    let setup_count = workflow
        .matches("actions-rust-lang/setup-rust-toolchain@")
        .count();
    let cache_key_count = workflow.matches("cache-key: ${{ github.job }}").count();
    assert!(
        setup_count > 0,
        "CI should use actions-rust-lang/setup-rust-toolchain"
    );
    assert_eq!(
        cache_key_count, setup_count,
        "every Rust toolchain setup step in CI should define an explicit cache key"
    );
}

#[test]
fn justfile_exposes_bootstrap_and_workflow_lint_recipes() {
    let justfile = fs::read_to_string(repo_root().join("justfile")).expect("read justfile");
    for expected in [
        "MDBOOK_VERSION := \"0.5.2\"",
        "LYCHEE_VERSION := \"0.23.0\"",
        "PA11Y_CI_VERSION := \"4.0.1\"",
        "ACTIONLINT_VERSION := \"1.7.12\"",
        "bootstrap-docs-tools:",
        "cargo install mdbook --locked --version",
        "cargo install lychee --locked --version",
        "pa11y-ci@{{PA11Y_CI_VERSION}}",
        "workflow-check:",
        "actionlint -color",
        "check: workflow-check",
    ] {
        assert!(
            justfile.contains(expected),
            "justfile should contain local QA tooling detail '{expected}'"
        );
    }
}

#[test]
fn just_clean_preserves_local_runtime_state() {
    let justfile = fs::read_to_string(repo_root().join("justfile")).expect("read justfile");
    let mut lines = justfile.lines().skip_while(|line| line.trim() != "clean:");
    assert_eq!(lines.next().map(str::trim), Some("clean:"));
    let clean_recipe = lines
        .take_while(|line| line.trim().is_empty() || line.starts_with(char::is_whitespace))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !clean_recipe.contains(".hpc-compose"),
        "just clean must not delete .hpc-compose runtime/config state"
    );
}

#[test]
fn pre_commit_hooks_file_advertises_validate_and_lint() {
    let hooks = fs::read_to_string(repo_root().join(".pre-commit-hooks.yaml"))
        .expect("read .pre-commit-hooks.yaml");
    let parsed: Vec<BTreeMap<String, serde_norway::Value>> =
        serde_norway::from_str(&hooks).expect("parse .pre-commit-hooks.yaml");
    let ids = parsed
        .iter()
        .filter_map(|hook| hook.get("id").and_then(serde_norway::Value::as_str))
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec!["hpc-compose-validate", "hpc-compose-lint"],
        ".pre-commit-hooks.yaml must be a list of the shipped hook entries"
    );
    for expected in [
        "id: hpc-compose-validate",
        "id: hpc-compose-lint",
        "language: system",
        "pass_filenames: false",
        "hpc-compose validate -f compose.yaml",
        "hpc-compose lint -f compose.yaml",
        "files: ^compose\\.(yaml|yml)$",
    ] {
        assert!(
            hooks.contains(expected),
            ".pre-commit-hooks.yaml should declare '{expected}'"
        );
    }
}

#[test]
fn reusable_lint_workflow_exists_and_is_sha_pinned() {
    let workflow = fs::read_to_string(repo_root().join(".github/workflows/hpc-compose-lint.yml"))
        .expect("read reusable lint workflow");
    assert!(
        workflow.contains("on:") && workflow.contains("workflow_call:"),
        "reusable workflow must be callable"
    );
    assert!(
        workflow.contains("hpc-compose validate") && workflow.contains("hpc-compose lint"),
        "reusable workflow must run validate and lint"
    );
    // Reuse the same SHA-pinning rule as every other workflow.
    for (line_index, line) in workflow.lines().enumerate() {
        let trimmed = line.trim();
        if let Some(reference) = trimmed.strip_prefix("uses: ") {
            assert!(
                is_sha_pinned_action_reference(reference),
                "hpc-compose-lint.yml:{} must pin actions by SHA, found '{}'",
                line_index + 1,
                reference
            );
        }
    }
}
