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

fn collect_global_long_flags(command: &clap::Command) -> Vec<String> {
    let mut flags = command
        .get_arguments()
        .filter(|arg| arg.is_global_set() && !arg.is_hide_set())
        .filter_map(|arg| arg.get_long().map(|long| format!("--{long}")))
        .collect::<Vec<_>>();
    flags.sort();
    flags.dedup();
    flags
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
fn cli_reference_mentions_every_public_global_flag() {
    let cli_reference =
        fs::read_to_string(repo_root().join("docs/src/cli-reference.md")).expect("cli reference");
    for flag in collect_global_long_flags(&build_cli_command()) {
        assert!(
            cli_reference.contains(&format!("`{flag}"))
                || cli_reference.contains(&format!("`{flag} ")),
            "docs/src/cli-reference.md should mention public global flag '{flag}'"
        );
    }
}

#[test]
fn cli_reference_documents_accessibility_and_automation_flags() {
    let cli_reference =
        fs::read_to_string(repo_root().join("docs/src/cli-reference.md")).expect("cli reference");
    for expected in [
        "`--color auto|always|never`",
        "`--color never`",
        "`--quiet`",
        "`--watch-mode auto|tui|line`",
        "`--no-tui`",
        "Accessible and Automation-Friendly Output",
        "hpc-compose logs -f compose.yaml --service app --follow",
        "hpc-compose status -f compose.yaml --format json",
    ] {
        assert!(
            cli_reference.contains(expected),
            "docs/src/cli-reference.md should mention CLI accessibility/automation detail '{expected}'"
        );
    }
}

#[test]
fn contributing_documents_local_quality_gates_and_bootstrap() {
    let contributing =
        fs::read_to_string(repo_root().join("CONTRIBUTING.md")).expect("contributing docs");
    for expected in [
        "just bootstrap-docs-tools",
        "`actionlint`",
        "| Fast Rust and workflow check | `just check` |",
        "| Documentation | `just docs-check` |",
        "| Examples and shell output | `just examples-check` |",
        "| Release metadata and coverage | `just release-check` |",
        "| Full local CI mirror | `just ci` |",
    ] {
        assert!(
            contributing.contains(expected),
            "CONTRIBUTING.md should mention local quality gate detail '{expected}'"
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

#[test]
fn runtime_observability_documents_watch_ui_controls_and_line_mode() {
    let runtime_observability =
        fs::read_to_string(repo_root().join("docs/src/runtime-observability.md"))
            .expect("runtime observability docs");
    for expected in [
        "Keybindings:",
        "`j`, `Down`, `Tab`",
        "`/`",
        "`q`",
        "--watch-mode line",
        "--no-tui",
        "screen reader",
    ] {
        assert!(
            runtime_observability.contains(expected),
            "runtime observability docs should mention watch UI detail '{expected}'"
        );
    }
}
