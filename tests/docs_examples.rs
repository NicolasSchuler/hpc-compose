use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use hpc_compose::cli::build_cli_command;
use hpc_compose::evolve;
use hpc_compose::examples::{ExampleAvailability, examples};
use hpc_compose::init::templates;
use regex::Regex;
use serde_json::Value;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn build_cli_command_for_test() -> clap::Command {
    std::thread::Builder::new()
        .name("build-cli-command".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(build_cli_command)
        .expect("spawn CLI command builder")
        .join()
        .expect("CLI command builder should not panic")
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

#[test]
fn examples_readme_links_every_repository_yaml_example() {
    let readme = fs::read_to_string(repo_root().join("examples/README.md"))
        .expect("read examples/README.md");
    for file in example_yaml_files() {
        assert!(
            readme.contains(&format!("({file})")),
            "examples/README.md should link examples/{file} (the hand-maintained list drifted from the on-disk specs)"
        );
    }
}

#[test]
fn example_registry_covers_repository_examples_and_templates() {
    let registry = examples();
    for file in example_yaml_files() {
        let name = file.trim_end_matches(".yaml");
        let matches = registry
            .iter()
            .filter(|example| example.name == name)
            .collect::<Vec<_>>();
        assert_eq!(
            matches.len(),
            1,
            "registry should contain one entry for {file}"
        );
        assert_eq!(matches[0].path, format!("examples/{file}"));
        assert!(!matches[0].tags.is_empty(), "{file} should have tags");
    }

    for template in templates() {
        let example = registry
            .iter()
            .find(|example| example.name == template.name)
            .unwrap_or_else(|| panic!("template {} should have example metadata", template.name));
        assert_eq!(
            example.availability,
            ExampleAvailability::BuiltInTemplate,
            "template {} should be marked built-in",
            template.name
        );
    }
}

#[test]
fn examples_docs_include_registry_tags_and_availability() {
    let examples_guide =
        fs::read_to_string(repo_root().join("docs/src/examples.md")).expect("read examples guide");
    for example in examples() {
        let row = examples_guide
            .lines()
            .find(|line| {
                line.starts_with("| [") && line.contains(&format!("{}.yaml", example.name))
            })
            .unwrap_or_else(|| {
                panic!(
                    "examples docs should contain a matrix row for {}",
                    example.name
                )
            });
        assert!(
            row.contains(&format!("{}.yaml", example.name)),
            "examples docs should mention {}.yaml",
            example.name
        );
        assert!(
            row.contains(example.availability.label()),
            "examples docs should mention availability {}",
            example.availability.label()
        );
        for tag in example.tags {
            assert!(
                row.contains(&format!("`{tag}`")),
                "examples docs should mention tag `{tag}` for {}",
                example.name
            );
        }
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
    collect_public_command_paths(
        &build_cli_command_for_test(),
        Vec::new(),
        &mut command_paths,
    );
    command_paths.sort();
    command_paths.dedup();

    for path in command_paths {
        let command_name = path.split_whitespace().next().expect("command name");
        let exact = cli_reference.contains(&format!("`{path}`"))
            || cli_reference.contains(&format!("hpc-compose {path}"));
        let top_level_fallback = !path.contains(' ')
            && (cli_reference.contains(&format!("`{command_name} "))
                || cli_reference.contains(&format!("hpc-compose {command_name} ")));
        assert!(
            exact || top_level_fallback,
            "docs/src/cli-reference.md should mention public command path '{path}'"
        );
    }
}

#[test]
fn cli_reference_mentions_every_public_global_flag() {
    let cli_reference =
        fs::read_to_string(repo_root().join("docs/src/cli-reference.md")).expect("cli reference");
    for flag in collect_global_long_flags(&build_cli_command_for_test()) {
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
        "Accessible and Automation-Friendly Output",
        "hpc-compose logs -f compose.yaml --service app --follow",
        "hpc-compose status -f compose.yaml --format json",
        "hpc-compose doctor readiness -f compose.yaml --service api",
        "hpc-compose examples list --tag mpi --format json",
        "hpc-compose examples recommend 'multi-node training' --tag gpu",
    ] {
        assert!(
            cli_reference.contains(expected),
            "docs/src/cli-reference.md should mention CLI accessibility/automation detail '{expected}'"
        );
    }
}

#[test]
fn docs_mention_every_shipped_evolve_lesson_and_step() {
    let mut docs = String::new();
    for path in [
        "docs/src/SUMMARY.md",
        "docs/src/evolve.md",
        "docs/src/quickstart.md",
        "docs/src/examples.md",
        "docs/src/task-guide.md",
        "docs/src/cli-reference.md",
    ] {
        docs.push_str(&fs::read_to_string(repo_root().join(path)).expect("read evolve docs"));
        docs.push('\n');
    }

    for lesson in evolve::lessons() {
        assert!(
            docs.contains(lesson.id()),
            "docs should mention evolve lesson id '{}'",
            lesson.id()
        );
        for step in lesson.steps() {
            assert!(
                docs.contains(step.id()),
                "docs should mention evolve step id '{}'",
                step.id()
            );
        }
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
fn docs_link_checks_exclude_generated_edit_urls() {
    let workflow =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    let justfile = fs::read_to_string(repo_root().join("justfile")).expect("read justfile");
    let exclude = "--exclude '^https://github\\.com/NicolasSchuler/hpc-compose/edit/main/'";
    assert!(
        workflow.contains(exclude),
        "CI lychee should ignore mdBook edit links because GitHub can transiently reject them"
    );
    assert!(
        justfile.contains(exclude),
        "local docs-check lychee should ignore mdBook edit links because GitHub can transiently reject them"
    );
    for (surface, content) in [
        ("CI", workflow.as_str()),
        ("local docs-check", justfile.as_str()),
    ] {
        assert!(
            content.contains("--include-fragments=anchor-only"),
            "{surface} lychee must validate local and published heading anchors"
        );
    }
}

#[test]
fn onboarding_docs_keep_one_minimal_cluster_smoke_contract() {
    let landing =
        fs::read_to_string(repo_root().join("docs/src/README.md")).expect("read docs landing");
    let quickstart =
        fs::read_to_string(repo_root().join("docs/src/quickstart.md")).expect("read quickstart");
    let faq = fs::read_to_string(repo_root().join("docs/src/faq.md")).expect("read FAQ");
    let root_readme = fs::read_to_string(repo_root().join("README.md")).expect("read root README");
    let task_guide =
        fs::read_to_string(repo_root().join("docs/src/task-guide.md")).expect("read task guide");

    for expected in [
        "canonical `minimal-batch` cluster smoke",
        "return to [Examples](examples.md)",
        "selects the default, `pyxis`",
    ] {
        assert!(
            quickstart.contains(expected),
            "Quickstart should preserve the minimal-smoke contract detail '{expected}'"
        );
    }
    assert!(
        landing.contains("examples.html#app-redis-workeryaml"),
        "the multi-service landing path should route through the annotated example guide"
    );
    assert!(
        !landing.contains("example-source.html#app-redis-worker"),
        "the landing page should not bypass example selection for the raw source appendix"
    );
    assert!(
        !faq.contains("plan --show-script") && !root_readme.contains("plan --show-script"),
        "onboarding should route to the canonical redacted/offline authoring flow instead of a sensitive script preview"
    );
    assert!(
        task_guide.contains("runtime-backends.md#host-runtime-notes"),
        "host-backend selection should link a shipped guide instead of a phantom example"
    );
}

#[test]
fn published_docs_are_tied_to_the_verified_commit_and_describe_version_skew() {
    let workflow = fs::read_to_string(repo_root().join(".github/workflows/docs-pages.yml"))
        .expect("read Pages workflow");
    let ai_setup = fs::read_to_string(repo_root().join("docs/src/ai-agent-setup.md"))
        .expect("read AI setup docs");

    assert!(
        workflow.contains("ref: ${{ github.event.workflow_run.head_sha }}"),
        "Pages must build the exact commit whose CI result triggered deployment"
    );
    assert!(
        !workflow.contains("workflow_dispatch"),
        "Pages deployment must not bypass the full CI docs gate through manual dispatch"
    );
    for expected in [
        "CI-verified snapshot of the `main`",
        "newer than the latest GitHub release",
        "Older releases may",
        "VERSION=\"$(hpc-compose --version | awk",
    ] {
        assert!(
            ai_setup.contains(expected),
            "AI setup docs should explain version skew and matching assets: '{expected}'"
        );
    }
    assert!(
        !ai_setup.contains("VERSION=0.2.0"),
        "AI setup docs must not hard-code an unreleased or stale skill version"
    );
}

#[test]
fn rustdoc_gate_denies_documentation_warnings() {
    let workflow =
        fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).expect("read CI workflow");
    let justfile = fs::read_to_string(repo_root().join("justfile")).expect("read justfile");

    assert!(
        workflow.contains("RUSTDOCFLAGS: -D warnings"),
        "CI must deny rustdoc warnings with RUSTDOCFLAGS"
    );
    assert!(
        justfile.contains("RUSTDOCFLAGS=\"-D warnings\" cargo doc --locked --no-deps"),
        "local docs-check must deny rustdoc warnings with RUSTDOCFLAGS"
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
        "`Space`",
        "`PgUp` / `PgDn`",
        "`a`",
        "`q`",
        "--hold-on-exit never|failure|always",
        "--watch-mode line",
        "best-effort",
        "service-exits",
        "metrics",
        "speed",
        "screen reader",
    ] {
        assert!(
            runtime_observability.contains(expected),
            "runtime observability docs should mention watch UI detail '{expected}'"
        );
    }
}

fn docs_src_markdown_files() -> Vec<(String, String)> {
    let mut files = Vec::new();
    for entry in fs::read_dir(repo_root().join("docs/src")).expect("read docs/src") {
        let path = entry.expect("read docs/src entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("doc filename should be UTF-8")
            .to_string();
        let content = fs::read_to_string(&path).expect("read doc file");
        files.push((name, content));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

fn markdown_heading_anchors(content: &str) -> BTreeSet<String> {
    content
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let level = trimmed.chars().take_while(|ch| *ch == '#').count();
            if level == 0
                || level > 6
                || !trimmed
                    .as_bytes()
                    .get(level)
                    .is_some_and(u8::is_ascii_whitespace)
            {
                return None;
            }
            let mut heading = trimmed[level..].trim().trim_end_matches('#').trim();
            if let Some(without_brace) = heading.strip_suffix('}')
                && let Some(marker) = without_brace.rfind(" {#")
            {
                let anchor = &without_brace[marker + 3..];
                if !anchor.is_empty() {
                    return Some(anchor.to_string());
                }
                heading = heading[..marker].trim();
            }

            let mut anchor = String::new();
            let mut previous_dash = false;
            for ch in heading.chars().flat_map(char::to_lowercase) {
                if ch.is_ascii_alphanumeric() {
                    anchor.push(ch);
                    previous_dash = false;
                } else if (ch.is_whitespace() || matches!(ch, '-' | '_' | '/' | ':'))
                    && !anchor.is_empty()
                    && !previous_dash
                {
                    anchor.push('-');
                    previous_dash = true;
                }
            }
            while anchor.ends_with('-') {
                anchor.pop();
            }
            (!anchor.is_empty()).then_some(anchor)
        })
        .collect()
}

#[test]
fn published_manual_fragment_links_match_local_headings() {
    let source = fs::read_to_string(repo_root().join("docs/plans/2026-07-feature-brainstorm.md"))
        .expect("read living backlog");
    let links = Regex::new(
        r"https://nicolasschuler\.github\.io/hpc-compose/([a-z0-9-]+)\.html#([a-z0-9_-]+)",
    )
    .expect("same-site docs link regex");
    let mut checked = 0;

    for captures in links.captures_iter(&source) {
        let page = captures.get(1).expect("page capture").as_str();
        let fragment = captures.get(2).expect("fragment capture").as_str();
        let content = fs::read_to_string(repo_root().join(format!("docs/src/{page}.md")))
            .unwrap_or_else(|error| panic!("read docs page {page}: {error}"));
        assert!(
            markdown_heading_anchors(&content).contains(fragment),
            "published link {page}.html#{fragment} does not match a local heading"
        );
        checked += 1;
    }
    assert!(
        checked >= 10,
        "expected to verify the backlog's evidence links"
    );
}

#[test]
fn docs_do_not_reference_removed_migration_v2_page() {
    for (name, content) in docs_src_markdown_files() {
        assert!(
            !content.contains("migration-v2"),
            "{name} references the removed spec-v2 placeholder page (migration-v2.md)"
        );
    }
}

#[test]
fn docs_avoid_phantom_spec_v2_phrasing() {
    // Design boundaries are intentional, not temporary "v1" limits awaiting a "v2".
    // Describe them in the present tense instead (see the CONTRIBUTING docs conventions).
    for (name, content) in docs_src_markdown_files() {
        for phrase in ["in v1", "rejected in v1", "V1 "] {
            assert!(
                !content.contains(phrase),
                "{name} uses version-coupled phrasing '{phrase}'; \
                 describe deliberate boundaries in the present tense"
            );
        }
    }
}

#[test]
fn content_pages_have_navigation_footer() {
    const SKIP: [&str; 4] = [
        "README.md",
        "SUMMARY.md",
        "brand-assets.md",
        "example-source.md",
    ];
    for (name, content) in docs_src_markdown_files() {
        if SKIP.contains(&name.as_str()) {
            continue;
        }
        assert!(
            content.contains("## Related Docs") || content.contains("## Read Next"),
            "{name} should end with a '## Related Docs' (or '## Read Next') navigation footer"
        );
    }
}

#[test]
fn generated_site_guides_are_current() {
    let status = Command::new("python3")
        .arg(repo_root().join("scripts/generate_site_guides.py"))
        .arg("--check")
        .current_dir(repo_root())
        .status()
        .expect("run site-guide generator check");
    assert!(
        status.success(),
        "generated site guides should match their validated JSON facts and template"
    );
}

#[test]
fn generated_site_guide_facts_keep_verification_and_primary_sources() {
    let facts_path = repo_root().join("docs/site-guides/sites/haicore.json");
    let facts: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&facts_path).expect("read HAICORE site facts"))
            .expect("parse HAICORE site facts");

    let verified_on = facts["verification"]["verified_on"]
        .as_str()
        .expect("verification date");
    assert_eq!(
        verified_on.split('-').map(str::len).collect::<Vec<_>>(),
        vec![4, 2, 2],
        "site verification date should use ISO YYYY-MM-DD"
    );

    let sources = facts["sources"].as_array().expect("primary sources");
    assert!(
        !sources.is_empty(),
        "site facts should cite primary sources"
    );
    assert!(
        sources.iter().all(|source| source["url"]
            .as_str()
            .is_some_and(|url| url.starts_with("https://www.nhr.kit.edu/"))),
        "HAICORE facts should cite the official NHR@KIT documentation"
    );
}

#[test]
fn living_backlog_has_one_source_and_auditable_rows() {
    let source = fs::read_to_string(repo_root().join("docs/plans/2026-07-feature-brainstorm.md"))
        .expect("read living backlog");
    let published =
        fs::read_to_string(repo_root().join("docs/src/backlog.md")).expect("read backlog include");

    assert!(
        published.starts_with("{{#include ../plans/2026-07-feature-brainstorm.md}}"),
        "manual backlog should include, not duplicate, the living source"
    );
    for required in ["**Last reviewed:**", "**Current release baseline:**"] {
        assert!(
            source.contains(required),
            "backlog should declare {required}"
        );
    }
    for removed in ["Top Five", "Runners-up", "Agreed roadmap"] {
        assert!(
            !source.contains(removed),
            "backlog should not retain duplicated section '{removed}'"
        );
    }

    let allowed = ["shipped", "candidate", "rejected", "superseded"];
    let mut ids = std::collections::BTreeSet::new();
    let mut rows = 0;
    for line in source.lines().filter(|line| line.starts_with("| ")) {
        let cells = line.split('|').map(str::trim).collect::<Vec<_>>();
        if cells.len() < 7 || !cells[1].contains('-') || !cells[3].starts_with('`') {
            continue;
        }
        let id = cells[1];
        let status = cells[3].trim_matches('`');
        assert!(
            allowed.contains(&status),
            "{id} has unsupported status {status}"
        );
        assert!(ids.insert(id), "backlog ID {id} appears more than once");
        assert!(
            !cells[4].is_empty(),
            "{id} should have evidence or a decision link"
        );
        assert!(!cells[5].is_empty(), "{id} should have a next condition");
        if status == "shipped" {
            assert!(
                cells[4].contains("nicolasschuler.github.io/hpc-compose")
                    || cells[4].contains("/docs/src/"),
                "shipped row {id} should link user docs"
            );
            assert!(
                cells[4].contains("test"),
                "shipped row {id} should link tests"
            );
            assert!(
                cells[4].contains("manpage"),
                "shipped row {id} should link a manpage"
            );
        }
        rows += 1;
    }
    assert!(
        rows >= 40,
        "expected the complete living backlog, found {rows} rows"
    );
}

#[test]
fn docs_use_canonical_job_id_placeholder() {
    for (name, content) in docs_src_markdown_files() {
        for placeholder in [".hpc-compose/12345/", "{job_id}", "{JOB_ID}", "<JOB_ID>"] {
            assert!(
                !content.contains(placeholder),
                "{name} uses a non-canonical job-id placeholder '{placeholder}'; use <job-id>"
            );
        }
    }
}

#[test]
fn ai_agent_setup_links_canonical_generated_contracts() {
    let ai_setup = fs::read_to_string(repo_root().join("docs/src/ai-agent-setup.md"))
        .expect("read AI agent setup docs");
    let llms = fs::read_to_string(repo_root().join("llms.txt")).expect("read root llms.txt");

    for expected in [
        "llms.txt",
        "agent-command-safety.md",
        "agent-command-policy.json",
        "hpc-compose --offline docs",
        "hpc-compose-skill-v${VERSION}.tar.gz",
    ] {
        assert!(
            ai_setup.contains(expected),
            "docs/src/ai-agent-setup.md should reference canonical agent artifact {expected}"
        );
    }
    for expected in [
        "/raw/ai-agent-setup.md",
        "/raw/agent-command-safety.md",
        "/agent-command-policy.json",
    ] {
        assert!(
            llms.contains(expected),
            "root llms.txt should publish canonical agent artifact {expected}"
        );
    }
}

#[test]
fn openai_agent_manifest_has_required_interface_fields() {
    let manifest = repo_root().join("skills/hpc-compose/agents/openai.yaml");
    assert!(manifest.exists(), "OpenAI agent manifest should exist");
    let manifest = fs::read_to_string(manifest).expect("read OpenAI agent manifest");
    for expected in [
        "interface:",
        "display_name:",
        "short_description:",
        "default_prompt:",
        "$hpc-compose",
    ] {
        assert!(
            manifest.contains(expected),
            "OpenAI agent manifest should contain {expected}"
        );
    }
}

#[test]
fn vscode_yaml_schema_settings_match_schemastore_entry() {
    let settings: Value = serde_json::from_str(
        &fs::read_to_string(repo_root().join(".vscode/settings.json"))
            .expect("read VS Code settings"),
    )
    .expect("parse VS Code settings");
    let catalog: Value = serde_json::from_str(
        &fs::read_to_string(repo_root().join("schema/schemastore-catalog-entry.json"))
            .expect("read SchemaStore catalog entry"),
    )
    .expect("parse SchemaStore catalog entry");

    let url = catalog["url"].as_str().expect("catalog url");
    let expected_matches = catalog["fileMatch"]
        .as_array()
        .expect("catalog fileMatch")
        .iter()
        .map(|value| value.as_str().expect("fileMatch string"))
        .collect::<BTreeSet<_>>();
    let configured_matches = settings["yaml.schemas"][url]
        .as_array()
        .expect("yaml.schemas entry for published schema")
        .iter()
        .map(|value| value.as_str().expect("yaml schema glob"))
        .collect::<BTreeSet<_>>();

    assert_eq!(
        configured_matches, expected_matches,
        ".vscode/settings.json should mirror the SchemaStore hpc-compose globs"
    );
    assert!(
        !configured_matches.contains("compose.yaml"),
        "generic compose.yaml should not be claimed by the hpc-compose schema"
    );
}

fn collect_bin_flag_commands(
    command: &clap::Command,
    prefix: Vec<String>,
    out: &mut BTreeMap<String, BTreeSet<String>>,
) {
    for subcommand in command.get_subcommands() {
        if subcommand.is_hide_set() {
            continue;
        }
        let mut path = prefix.clone();
        path.push(subcommand.get_name().to_string());
        let path_str = path.join(" ");
        for arg in subcommand.get_arguments() {
            if arg.is_hide_set() {
                continue;
            }
            if let Some(long) = arg.get_long().filter(|long| long.ends_with("-bin")) {
                out.entry(format!("--{long}"))
                    .or_default()
                    .insert(path_str.clone());
            }
        }
        collect_bin_flag_commands(subcommand, path, out);
    }
}

fn parse_tool_override_matrix(cli_reference: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut matrix = BTreeMap::new();
    for line in cli_reference.lines() {
        let line = line.trim();
        if !line.starts_with("| `--") || !line.contains("-bin` |") {
            continue;
        }
        let cols = line
            .trim_matches('|')
            .split('|')
            .map(|col| col.trim())
            .collect::<Vec<_>>();
        if cols.len() != 3 {
            continue;
        }
        let flag = cols[0].trim_matches('`').to_string();
        let commands = cols[2]
            .split(',')
            .map(|command| command.trim().trim_matches('`').to_string())
            .filter(|command| !command.is_empty())
            .collect::<BTreeSet<_>>();
        matrix.insert(flag, commands);
    }
    matrix
}

#[test]
fn tool_override_matrix_matches_cli_reference() {
    let cli_reference =
        fs::read_to_string(repo_root().join("docs/src/cli-reference.md")).expect("cli reference");
    let mut clap_matrix = BTreeMap::new();
    collect_bin_flag_commands(&build_cli_command_for_test(), Vec::new(), &mut clap_matrix);
    assert!(
        !clap_matrix.is_empty(),
        "expected at least one --*-bin tool override flag in the CLI"
    );
    let documented = parse_tool_override_matrix(&cli_reference);
    for (flag, commands) in &clap_matrix {
        let doc_commands = documented.get(flag).cloned().unwrap_or_default();
        for command in commands {
            assert!(
                doc_commands.contains(command),
                "docs/src/cli-reference.md tool-override matrix should list `{command}` under `{flag}`"
            );
        }
    }
}
