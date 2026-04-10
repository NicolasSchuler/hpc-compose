//! Generated manpage support for the `hpc-compose` CLI.
#![allow(missing_docs)]

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use clap::{Arg, Command};

use crate::cli::{build_cli_command, examples_for_path};

pub const DEFAULT_MANPAGE_DIR: &str = "man/man1";

const SOURCE_LABEL: &str = concat!("hpc-compose ", env!("CARGO_PKG_VERSION"));
const MANUAL_LABEL: &str = "User Commands";
const FILES_SECTION: &[(&str, &str)] = &[
    (
        "compose.yaml",
        "Fallback compose specification file when -f or --file is omitted and the active context does not provide one.",
    ),
    (
        "<compose-file-dir>/hpc-compose.sbatch",
        "Default rendered batch script path written by submit when --script-out is not set.",
    ),
    (
        "${SLURM_SUBMIT_DIR:-$PWD}/.hpc-compose/",
        "Tracked job metadata, logs, metrics, and artifact state written after submission.",
    ),
    (
        "~/.cache/hpc-compose",
        "Default cache root for imported and prepared image artifacts when x-slurm.cache_dir is unset.",
    ),
];
const ENVIRONMENT_SECTION: &[(&str, &str)] = &[
    (
        "HOME",
        "Used to resolve the default cache path and default configuration locations.",
    ),
    (
        "PATH",
        "Used to resolve external commands such as enroot, sbatch, srun, squeue, sacct, sstat, and scancel.",
    ),
    (
        "ENROOT_CONFIG_PATH",
        "Consulted during preflight when checking Enroot configuration discovery.",
    ),
    (
        "XDG_CONFIG_HOME",
        "Consulted during preflight when checking Enroot configuration discovery.",
    ),
    (
        "SLURM_SUBMIT_DIR",
        "Used by rendered jobs when resolving the tracked runtime directory on the host.",
    ),
    (
        "SLURM_JOB_ID",
        "Used in tracked runtime paths and in artifact export directory expansion after submission.",
    ),
    (
        "HPC_COMPOSE_PRIMARY_NODE and related HPC_COMPOSE_* runtime variables",
        "Injected into services inside submitted jobs to describe allocation metadata and resume state.",
    ),
];
const EXIT_STATUS_SECTION: &[(&str, &str)] = &[
    ("0", "The command completed successfully."),
    (
        "1",
        "The command failed because validation, local I/O, or an external tool reported an error.",
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedManPage {
    pub file_name: String,
    pub contents: String,
}

pub fn render_manpages() -> Vec<RenderedManPage> {
    let mut root = build_cli_command();
    root.build();
    let root_for_refs = root.clone();
    let mut pages = Vec::new();
    collect_pages(root, Vec::new(), &root_for_refs, &mut pages);
    pages
}

pub fn write_manpages(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;

    let pages = render_manpages();
    let expected: BTreeSet<_> = pages.iter().map(|page| page.file_name.as_str()).collect();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry =
            entry.with_context(|| format!("failed to read entry under {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(OsStr::to_str) == Some("1")
            && let Some(name) = path.file_name().and_then(OsStr::to_str)
            && !expected.contains(name)
        {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove stale manpage {}", path.display()))?;
        }
    }

    for page in pages {
        let path = dir.join(&page.file_name);
        fs::write(&path, page.contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

pub fn check_manpages(dir: &Path) -> Result<()> {
    let pages = render_manpages();
    let mut stale = Vec::new();
    let mut missing = Vec::new();

    for page in &pages {
        let path = dir.join(&page.file_name);
        match fs::read_to_string(&path) {
            Ok(existing) if existing == page.contents => {}
            Ok(_) => stale.push(page.file_name.clone()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                missing.push(page.file_name.clone())
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to read {}", path.display()));
            }
        }
    }

    let expected: BTreeSet<_> = pages.iter().map(|page| page.file_name.as_str()).collect();
    let mut unexpected = Vec::new();
    if dir.exists() {
        for entry in
            fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry under {}", dir.display()))?;
            let path = entry.path();
            if path.extension().and_then(OsStr::to_str) == Some("1")
                && let Some(name) = path.file_name().and_then(OsStr::to_str)
                && !expected.contains(name)
            {
                unexpected.push(name.to_string());
            }
        }
    }

    if stale.is_empty() && missing.is_empty() && unexpected.is_empty() {
        return Ok(());
    }

    let mut message = String::from("manpages are out of date");
    if !missing.is_empty() {
        message.push_str(&format!("\nmissing: {}", missing.join(", ")));
    }
    if !stale.is_empty() {
        message.push_str(&format!("\nstale: {}", stale.join(", ")));
    }
    if !unexpected.is_empty() {
        message.push_str(&format!("\nunexpected: {}", unexpected.join(", ")));
    }
    bail!("{message}");
}

fn collect_pages(
    command: Command,
    path: Vec<String>,
    root: &Command,
    pages: &mut Vec<RenderedManPage>,
) {
    pages.push(RenderedManPage {
        file_name: format!("{}.1", page_stem(&path)),
        contents: render_page(&command, &path, root),
    });

    for subcommand in visible_subcommands(&command) {
        let mut next_path = path.clone();
        next_path.push(subcommand.get_name().to_string());
        collect_pages(subcommand.clone(), next_path, root, pages);
    }
}

fn render_page(command: &Command, path: &[String], root: &Command) -> String {
    let stem = page_stem(path);
    let title = stem.to_ascii_uppercase();
    let summary = command_summary(command);
    let description = command_description(command);
    let examples = examples_for_path(&path.iter().map(String::as_str).collect::<Vec<_>>());
    let subcommands = visible_subcommands(command);
    let options = visible_arguments(command);
    let related = related_pages(path, command, root);

    let mut out = String::new();
    out.push_str(
        ".\\\" DO NOT EDIT. Generated by `cargo run --features manpage-bin --bin gen-manpages`\n",
    );
    out.push_str(&format!(
        ".TH \"{}\" \"1\" \"\" \"{}\" \"{}\"\n",
        escape_roff(&title),
        escape_roff(SOURCE_LABEL),
        escape_roff(MANUAL_LABEL)
    ));
    out.push_str(".SH NAME\n");
    out.push_str(&format!(
        "{} \\- {}\n",
        escape_roff(&stem),
        escape_roff(&summary)
    ));
    out.push_str(".SH SYNOPSIS\n");
    write_literal_block(&mut out, &usage_lines(command));
    out.push_str(".SH DESCRIPTION\n");
    if description.is_empty() {
        out.push_str(".PP\n");
        out.push_str(&format!("{}\n", escape_roff(&summary)));
    } else {
        for paragraph in description {
            out.push_str(".PP\n");
            out.push_str(&format!("{}\n", escape_roff(&paragraph)));
        }
    }

    if !subcommands.is_empty() {
        out.push_str(".SH SUBCOMMANDS\n");
        for subcommand in subcommands {
            let mut child_path = path.to_vec();
            child_path.push(subcommand.get_name().to_string());
            let child_ref = format!("{}(1)", page_stem(&child_path));
            out.push_str(".TP\n");
            out.push_str(&format!("\\fB{}\\fR\n", escape_roff(subcommand.get_name())));
            out.push_str(&format!(
                "{}. See {}.\n",
                escape_roff(&command_summary(subcommand)),
                escape_roff(&child_ref)
            ));
        }
    }

    out.push_str(".SH OPTIONS\n");
    for arg in options {
        out.push_str(".TP\n");
        out.push_str(&format!("{}\n", argument_term(arg)));
        out.push_str(&format!("{}\n", argument_description(arg)));
    }

    if path.is_empty() {
        out.push_str(".SH FILES\n");
        write_definition_list(&mut out, FILES_SECTION);
        out.push_str(".SH ENVIRONMENT\n");
        write_definition_list(&mut out, ENVIRONMENT_SECTION);
        out.push_str(".SH EXIT STATUS\n");
        write_definition_list(&mut out, EXIT_STATUS_SECTION);
    }

    out.push_str(".SH EXAMPLES\n");
    if examples.is_empty() {
        out.push_str(".PP\n");
        out.push_str("No examples are documented for this command.\n");
    } else {
        write_literal_block(&mut out, examples);
    }

    out.push_str(".SH SEE ALSO\n");
    out.push_str(&format!("{}\n", escape_roff(&related.join(", "))));

    out
}

fn write_definition_list(out: &mut String, items: &[(&str, &str)]) {
    for (term, description) in items {
        out.push_str(".TP\n");
        out.push_str(&format!("\\fB{}\\fR\n", escape_roff(term)));
        out.push_str(&format!("{}\n", escape_roff(description)));
    }
}

fn write_literal_block(out: &mut String, lines: &[impl AsRef<str>]) {
    out.push_str(".nf\n");
    for line in lines {
        out.push_str(&format!("{}\n", escape_roff(line.as_ref())));
    }
    out.push_str(".fi\n");
}

fn usage_lines(command: &Command) -> Vec<String> {
    let mut usage_command = command.clone();
    let usage = usage_command.render_usage().to_string();
    usage.lines().map(strip_usage_prefix).collect()
}

fn strip_usage_prefix(line: &str) -> String {
    line.strip_prefix("Usage: ")
        .unwrap_or(line)
        .trim_end()
        .to_string()
}

fn command_summary(command: &Command) -> String {
    command
        .get_about()
        .or_else(|| command.get_long_about())
        .map(|text| text.to_string())
        .unwrap_or_else(|| "Command reference".to_string())
}

fn command_description(command: &Command) -> Vec<String> {
    let text = command
        .get_long_about()
        .or_else(|| command.get_about())
        .map(|text| text.to_string())
        .unwrap_or_default();
    split_paragraphs(&text)
}

fn split_paragraphs(text: &str) -> Vec<String> {
    text.split("\n\n")
        .map(str::trim)
        .filter(|paragraph| !paragraph.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn visible_subcommands(command: &Command) -> Vec<&Command> {
    command
        .get_subcommands()
        .filter(|subcommand| !subcommand.is_hide_set() && subcommand.get_name() != "help")
        .collect()
}

fn visible_arguments(command: &Command) -> Vec<&Arg> {
    command
        .get_arguments()
        .filter(|arg| !arg.is_hide_set())
        .collect()
}

fn argument_term(arg: &Arg) -> String {
    if arg.is_positional() {
        return format!("\\fI{}\\fR", escape_roff(&value_name(arg)));
    }

    let mut flags = Vec::new();
    if let Some(short) = arg.get_short() {
        flags.push(format!("\\fB-{}\\fR", escape_roff(&short.to_string())));
    }
    if let Some(long) = arg.get_long() {
        flags.push(format!("\\fB--{}\\fR", escape_roff(long)));
    }
    let mut term = flags.join(", ");
    if takes_values(arg) {
        term.push(' ');
        term.push_str(&format!("\\fI{}\\fR", escape_roff(&value_name(arg))));
    }
    term
}

fn argument_description(arg: &Arg) -> String {
    let mut parts = Vec::new();
    let help = arg
        .get_long_help()
        .or_else(|| arg.get_help())
        .map(|text| text.to_string())
        .unwrap_or_else(|| "No description available.".to_string());
    parts.push(ensure_sentence(&help));

    let defaults: Vec<_> = arg
        .get_default_values()
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect();
    if takes_values(arg) && !defaults.is_empty() {
        parts.push(format!("Default: {}.", defaults.join(", ")));
    }

    let values: Vec<_> = arg
        .get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_string())
        .collect();
    if !values.is_empty() {
        parts.push(format!("Possible values: {}.", values.join(", ")));
    }

    escape_roff(&parts.join(" "))
}

fn takes_values(arg: &Arg) -> bool {
    arg.is_positional()
        || arg
            .get_num_args()
            .expect("built clap command")
            .takes_values()
}

fn ensure_sentence(text: &str) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if matches!(trimmed.chars().last(), Some('.' | '!' | '?')) {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    }
}

fn value_name(arg: &Arg) -> String {
    arg.get_value_names()
        .and_then(|names| names.first())
        .map(|name| name.to_string())
        .unwrap_or_else(|| normalize_value_name(arg.get_id().as_str()))
}

fn normalize_value_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn related_pages(path: &[String], command: &Command, root: &Command) -> Vec<String> {
    let mut refs = BTreeSet::new();

    if !path.is_empty() {
        refs.insert(page_reference(&[]));
    }
    if path.len() > 1 {
        refs.insert(page_reference(&path[..path.len() - 1]));
    }

    for subcommand in visible_subcommands(command) {
        let mut child_path = path.to_vec();
        child_path.push(subcommand.get_name().to_string());
        refs.insert(page_reference(&child_path));
    }

    if !path.is_empty()
        && let Some(parent) = find_command(root, &path[..path.len() - 1])
    {
        for sibling in visible_subcommands(parent) {
            if sibling.get_name() == path[path.len() - 1] {
                continue;
            }
            let mut sibling_path = path[..path.len() - 1].to_vec();
            sibling_path.push(sibling.get_name().to_string());
            refs.insert(page_reference(&sibling_path));
        }
    }

    refs.into_iter().collect()
}

fn find_command<'a>(command: &'a Command, path: &[String]) -> Option<&'a Command> {
    let mut current = command;
    for segment in path {
        current = current
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == segment)?;
    }
    Some(current)
}

fn page_reference(path: &[String]) -> String {
    format!("{}(1)", page_stem(path))
}

fn page_stem(path: &[String]) -> String {
    if path.is_empty() {
        "hpc-compose".to_string()
    } else {
        format!("hpc-compose-{}", path.join("-"))
    }
}

fn escape_roff(text: &str) -> String {
    let escaped = text.replace('\\', r"\\").replace('-', r"\-");
    escaped
        .lines()
        .map(|line| {
            if line.starts_with('.') || line.starts_with('\'') {
                format!(r"\&{line}")
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use clap::{Arg, Command};

    use super::{
        argument_description, argument_term, check_manpages, command_description, command_summary,
        ensure_sentence, escape_roff, find_command, normalize_value_name, page_reference,
        page_stem, related_pages, render_manpages, render_page, split_paragraphs,
        strip_usage_prefix, usage_lines, value_name, visible_arguments, visible_subcommands,
        write_manpages,
    };

    #[test]
    fn rendered_pages_include_expected_files() {
        let pages = render_manpages();
        let names: Vec<_> = pages.iter().map(|page| page.file_name.as_str()).collect();
        assert!(names.contains(&"hpc-compose.1"));
        assert!(names.contains(&"hpc-compose-jobs.1"));
        assert!(names.contains(&"hpc-compose-jobs-list.1"));
        assert!(names.contains(&"hpc-compose-submit.1"));
        assert!(names.contains(&"hpc-compose-cache-prune.1"));
    }

    #[test]
    fn rendered_pages_contain_required_sections() {
        let pages = render_manpages();
        let top = pages
            .iter()
            .find(|page| page.file_name == "hpc-compose.1")
            .expect("top-level page");
        for section in [
            ".SH NAME",
            ".SH SYNOPSIS",
            ".SH DESCRIPTION",
            ".SH OPTIONS",
            ".SH SUBCOMMANDS",
            ".SH FILES",
            ".SH ENVIRONMENT",
            ".SH EXIT STATUS",
            ".SH EXAMPLES",
            ".SH SEE ALSO",
        ] {
            assert!(
                top.contents.contains(section),
                "missing top-level section {section}"
            );
        }

        let submit = pages
            .iter()
            .find(|page| page.file_name == "hpc-compose-submit.1")
            .expect("submit page");
        for section in [
            ".SH NAME",
            ".SH SYNOPSIS",
            ".SH DESCRIPTION",
            ".SH OPTIONS",
            ".SH EXAMPLES",
            ".SH SEE ALSO",
        ] {
            assert!(
                submit.contents.contains(section),
                "missing submit section {section}"
            );
        }
        assert!(submit.contents.contains(r"\fB--watch\fR"));
    }

    #[test]
    fn hidden_compatibility_flags_are_omitted_from_rendered_pages() {
        let pages = render_manpages();
        let inspect = pages
            .iter()
            .find(|page| page.file_name == format!("{}.1", page_stem(&["inspect".to_string()])))
            .expect("inspect page");
        assert!(!inspect.contents.contains("--json"));
    }

    #[test]
    fn write_and_check_manpages_refresh_generated_directory() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let dir = tmpdir.path();
        fs::write(dir.join("obsolete.1"), "stale").expect("obsolete manpage");
        fs::write(dir.join("notes.txt"), "keep me").expect("non-manpage helper");

        write_manpages(dir).expect("write manpages");
        assert!(dir.join("hpc-compose.1").exists());
        assert!(!dir.join("obsolete.1").exists());
        assert_eq!(
            fs::read_to_string(dir.join("notes.txt")).expect("notes"),
            "keep me"
        );
        check_manpages(dir).expect("fresh manpages");

        let top = dir.join("hpc-compose.1");
        fs::write(&top, "outdated").expect("stale rewrite");
        fs::remove_file(dir.join("hpc-compose-submit.1")).expect("remove page");
        fs::write(dir.join("unexpected.1"), "extra").expect("unexpected page");

        let err = check_manpages(dir).expect_err("drift should fail");
        let message = err.to_string();
        assert!(message.contains("manpages are out of date"));
        assert!(message.contains("missing: hpc-compose-submit.1"));
        assert!(message.contains("stale: hpc-compose.1"));
        assert!(message.contains("unexpected: unexpected.1"));

        write_manpages(dir).expect("rewrite manpages");
        assert!(!dir.join("unexpected.1").exists());
        check_manpages(dir).expect("rewritten manpages");
    }

    #[test]
    fn helper_renderers_cover_fallbacks_and_hidden_entries() {
        let command = Command::new("demo")
            .about("Demo summary")
            .long_about("First paragraph.\n\nSecond paragraph")
            .arg(Arg::new("input-path").help("input spec"))
            .arg(
                Arg::new("config-path")
                    .short('c')
                    .long("config-path")
                    .help("config file")
                    .default_value("compose.yaml")
                    .value_parser(["compose.yaml", "other.yaml"])
                    .num_args(1),
            )
            .arg(Arg::new("hidden-flag").long("hidden-flag").hide(true))
            .subcommand(Command::new("child").about("Child summary"))
            .subcommand(Command::new("sibling").about("Sibling summary"))
            .subcommand(
                Command::new("hidden-child")
                    .about("Hidden summary")
                    .hide(true),
            );
        let mut built = command.clone();
        built.build();

        assert_eq!(strip_usage_prefix("Usage: demo child"), "demo child");
        assert_eq!(strip_usage_prefix("demo child"), "demo child");
        assert_eq!(ensure_sentence(""), "");
        assert_eq!(ensure_sentence("Done"), "Done.");
        assert_eq!(ensure_sentence("Done!"), "Done!");
        assert_eq!(normalize_value_name("config-path"), "CONFIG_PATH");
        assert_eq!(split_paragraphs(" one \n\n two "), vec!["one", "two"]);
        assert_eq!(command_summary(&built), "Demo summary");
        assert_eq!(
            command_description(&built),
            vec!["First paragraph.", "Second paragraph"]
        );
        assert_eq!(
            usage_lines(&built),
            vec!["demo [OPTIONS] [input-path] [COMMAND]"]
        );

        let visible_args = visible_arguments(&built);
        let visible_arg_ids: Vec<_> = visible_args
            .iter()
            .map(|arg| arg.get_id().as_str())
            .collect();
        assert!(visible_arg_ids.contains(&"input-path"));
        assert!(visible_arg_ids.contains(&"config-path"));
        assert!(!visible_arg_ids.contains(&"hidden-flag"));
        let positional = visible_args
            .iter()
            .find(|arg| arg.get_id().as_str() == "input-path")
            .expect("positional arg");
        let option = visible_args
            .iter()
            .find(|arg| arg.get_id().as_str() == "config-path")
            .expect("option arg");
        assert_eq!(argument_term(positional), r"\fIINPUT_PATH\fR");
        assert_eq!(
            argument_term(option),
            r"\fB-c\fR, \fB--config\-path\fR \fICONFIG_PATH\fR"
        );
        let description = argument_description(option);
        assert!(description.contains("config file."));
        assert!(description.contains("Default: compose.yaml."));
        assert!(description.contains("Possible values: compose.yaml, other.yaml."));
        assert_eq!(value_name(option), "CONFIG_PATH");

        let visible_subcommands = visible_subcommands(&built);
        assert_eq!(
            visible_subcommands
                .iter()
                .map(|subcommand| subcommand.get_name())
                .collect::<Vec<_>>(),
            vec!["child", "sibling"]
        );
        let child_path = vec!["child".to_string()];
        let refs = related_pages(&child_path, visible_subcommands[0], &built);
        assert!(refs.contains(&page_reference(&[])));
        assert!(refs.contains(&page_reference(&["sibling".to_string()])));
        assert!(find_command(&built, &child_path).is_some());
        assert!(find_command(&built, &["missing".to_string()]).is_none());

        let escaped = escape_roff(".dash-leading\n'quoted\nslash\\value");
        assert!(escaped.contains(r"\&.dash\-leading"));
        assert!(escaped.contains(r"\&'quoted"));
        assert!(escaped.contains(r"slash\\value"));
    }

    #[test]
    fn render_page_uses_fallback_summary_when_description_and_examples_are_absent() {
        let command = Command::new("empty");
        let path = vec!["mystery".to_string()];
        let page = render_page(&command, &path, &command);

        assert!(page.contains(".SH DESCRIPTION"));
        assert!(page.contains(".PP\nCommand reference\n"));
        assert!(page.contains("No examples are documented for this command."));
        assert!(page.contains(".SH SEE ALSO"));
        assert!(page.contains(r"hpc\-compose(1)"));
    }
}
