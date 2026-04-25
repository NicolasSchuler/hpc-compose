use std::fs;
use std::path::{Path, PathBuf};

use hpc_compose::cli::build_cli_command;
use serde_json::Value;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn example_yaml_files() -> Vec<String> {
    let mut files = fs::read_dir(repo_root().join("examples"))
        .expect("read examples directory")
        .filter_map(|entry| {
            let path: PathBuf = entry.expect("read examples entry").path();
            let is_yaml = path.extension().and_then(|ext| ext.to_str()) == Some("yaml");
            is_yaml.then(|| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .expect("example filename should be UTF-8")
                    .to_string()
            })
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn examples_guide_mentions_every_repository_yaml_example() {
    let examples_guide =
        fs::read_to_string(repo_root().join("docs/src/examples.md")).expect("read examples guide");
    let example_source = fs::read_to_string(repo_root().join("docs/src/example-source.md"))
        .expect("read example source appendix");

    for file in example_yaml_files() {
        assert!(
            examples_guide.contains(&file),
            "docs/src/examples.md should mention examples/{file}"
        );
        assert!(
            example_source.contains(&format!("../../examples/{file}")),
            "docs/src/example-source.md should include examples/{file}"
        );
    }
}

fn collect_public_command_paths(
    command: &clap::Command,
    prefix: Vec<String>,
    paths: &mut Vec<String>,
) {
    for subcommand in command.get_subcommands() {
        if subcommand.is_hide_set() {
            continue;
        }
        let mut path = prefix.clone();
        path.push(subcommand.get_name().to_string());
        paths.push(path.join(" "));
        collect_public_command_paths(subcommand, path, paths);
    }
}

#[test]
fn cli_reference_mentions_every_public_command_path() {
    let cli_reference =
        fs::read_to_string(repo_root().join("docs/src/cli-reference.md")).expect("cli reference");
    let mut command_paths = Vec::new();
    collect_public_command_paths(&build_cli_command(), Vec::new(), &mut command_paths);
    command_paths.sort();
    command_paths.dedup();

    for path in command_paths {
        let command_name = path.split_whitespace().next().expect("command name");
        assert!(
            cli_reference.contains(&format!("`{path}`"))
                || cli_reference.contains(&format!("hpc-compose {path}"))
                || cli_reference.contains(&format!("`{command_name} "))
                || cli_reference.contains(&format!("hpc-compose {command_name} ")),
            "docs/src/cli-reference.md should mention public command path '{path}'"
        );
    }
}

#[test]
fn spec_reference_mentions_top_level_schema_properties() {
    let spec_reference =
        fs::read_to_string(repo_root().join("docs/src/spec-reference.md")).expect("spec reference");
    let schema: Value = serde_json::from_str(
        &fs::read_to_string(repo_root().join("schema/hpc-compose.schema.json"))
            .expect("schema json"),
    )
    .expect("parse schema");
    let properties = schema["properties"]
        .as_object()
        .expect("top-level schema properties");

    for property in properties.keys() {
        assert!(
            spec_reference.contains(&format!("`{property}`")),
            "docs/src/spec-reference.md should mention top-level schema property '{property}'"
        );
    }
}

#[test]
fn support_matrix_reflects_ci_tested_platforms() {
    let support_matrix =
        fs::read_to_string(repo_root().join("docs/src/support-matrix.md")).expect("support matrix");
    for expected in [
        "Ubuntu 24.04 `x86_64`",
        "macOS `x86_64`",
        "macOS `arm64`",
        "authoring",
        "installer smoke",
        "Homebrew smoke",
    ] {
        assert!(
            support_matrix.contains(expected),
            "docs/src/support-matrix.md should mention CI-tested support detail '{expected}'"
        );
    }
}

#[test]
fn ci_docs_link_check_excludes_generated_edit_urls() {
    let workflow =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    assert!(
        workflow
            .contains("--exclude '^https://github\\.com/NicolasSchuler/hpc-compose/edit/main/'"),
        "lychee should ignore mdBook edit links because GitHub can transiently reject them"
    );
}
