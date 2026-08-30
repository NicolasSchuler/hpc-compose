//! Shared CLI definitions for the runtime binary, completion generation, and
//! generated manpages.
#![allow(missing_docs)]

use std::ffi::{OsStr, OsString};

use clap::{ColorChoice, CommandFactory, FromArgMatches, ValueEnum};

mod commands;
mod help;

pub use crate::term::ColorPolicy;
pub use commands::{
    CacheCommands, Cli, Commands, CompletionValueKind, DoctorCommands, ExamplesCommands,
    ExperimentCommands, JobsCommands, NotebookCommands, RendezvousCommands, RuntimeLaunchArgs,
    SweepCommands, WorkspaceCommands, WorkspaceToolArgs,
};
pub use help::examples_for_path;

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum FeedbackKind {
    Bug,
    Feature,
    Adoption,
    Question,
}

/// Interactive server preset for the `notebook` command.
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum NotebookKindArg {
    /// JupyterLab notebook server.
    Jupyter,
    /// VS Code remote tunnel (`code tunnel`).
    Vscode,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum ExamplesOutputFormat {
    Text,
    Json,
    Markdown,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum StatsOutputFormat {
    Text,
    Json,
    Csv,
    Jsonl,
}

/// Output format for tabular commands that additionally support CSV, i.e.
/// `sweep results` (row-per-run) and the N-way `diff --across`/`--jobs` matrix
/// (column-per-run). CSV is table-specific and cannot live on the shared
/// [`OutputFormat`] (which is Text/Json only); both commands share the exact
/// same Text/Json/Csv variant set, so they share one enum.
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum CsvOutputFormat {
    Text,
    Json,
    Csv,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum DependencyOutputFormat {
    Text,
    Dot,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum WatchMode {
    Auto,
    Tui,
    Line,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum HoldOnExit {
    Never,
    Failure,
    Always,
}

/// Controls whether `up --remote` bootstraps/upgrades `hpc-compose` on the login
/// node before delegating.
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum RemoteInstallMode {
    /// Install the newest release only when the login node's `hpc-compose` is
    /// missing or older than the local version.
    Auto,
    /// Never install; fail with a clear error if the login node's binary is
    /// missing or too old (use on locked-down/air-gapped login nodes).
    Never,
    /// Always (re)install the newest release before delegating.
    Force,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum SchemaKind {
    Compose,
    Settings,
}

/// Parses process arguments into the top-level CLI struct.
pub fn parse_cli() -> Cli {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    parse_cli_from(&raw_args)
}

/// Configures the presentation policy needed before Clap parses the command
/// line. This keeps explicit `--color` behavior consistent for help, parse
/// failures, and runtime diagnostics.
pub fn init_early_presentation(raw_args: &[OsString]) -> ColorPolicy {
    let policy = color_policy_from_raw_args(raw_args);
    crate::term::init_color(policy);
    crate::diagnostics::install_miette_handler();
    policy
}

/// Parses the supplied process arguments after applying their explicit color
/// policy to Clap itself.
pub fn parse_cli_from(raw_args: &[OsString]) -> Cli {
    let policy = init_early_presentation(raw_args);
    let matches = Cli::command()
        .color(match policy {
            ColorPolicy::Auto => ColorChoice::Auto,
            ColorPolicy::Always => ColorChoice::Always,
            ColorPolicy::Never => ColorChoice::Never,
        })
        .get_matches_from(raw_args);
    Cli::from_arg_matches(&matches).expect("Clap matches must construct the derived CLI")
}

fn color_policy_from_raw_args(raw_args: &[OsString]) -> ColorPolicy {
    let mut policy = ColorPolicy::Auto;
    let mut index = 1;
    while index < raw_args.len() {
        let arg = &raw_args[index];
        // Everything after `--` belongs to a child command for the CLI's
        // trailing-argv surfaces (`run`, `alloc`, and `notebook`). Treating a
        // forwarded `--color=...` as our global flag would make the early Clap
        // and Miette renderers disagree with the `Cli` value parsed below.
        if arg == OsStr::new("--") {
            break;
        }
        if arg == OsStr::new("--color") {
            if let Some(value) = raw_args.get(index + 1).and_then(|value| value.to_str()) {
                policy = parse_early_color_value(value).unwrap_or(policy);
                index += 1;
            }
        } else if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--color="))
        {
            policy = parse_early_color_value(value).unwrap_or(policy);
        }
        index += 1;
    }
    policy
}

fn parse_early_color_value(value: &str) -> Option<ColorPolicy> {
    match value {
        "auto" => Some(ColorPolicy::Auto),
        "always" => Some(ColorPolicy::Always),
        "never" => Some(ColorPolicy::Never),
        _ => None,
    }
}

/// Builds the Clap command tree used by the binary and manpage generator.
#[must_use]
pub fn build_cli_command() -> clap::Command {
    Cli::command()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn early_color_policy_accepts_global_forms_and_last_value() {
        assert_eq!(
            color_policy_from_raw_args(&args(&[
                "hpc-compose",
                "plan",
                "--color=always",
                "--color",
                "never",
            ])),
            ColorPolicy::Never
        );
        assert_eq!(
            color_policy_from_raw_args(&args(&["hpc-compose", "--color", "always", "--help"])),
            ColorPolicy::Always
        );
        assert_eq!(
            color_policy_from_raw_args(&args(&["hpc-compose", "--help"])),
            ColorPolicy::Auto
        );
    }

    #[test]
    fn invalid_early_color_value_is_left_for_clap_to_report() {
        assert_eq!(
            color_policy_from_raw_args(&args(&["hpc-compose", "--color=surprise", "--help"])),
            ColorPolicy::Auto
        );
    }

    #[test]
    fn early_color_policy_stops_at_the_forwarded_argv_separator() {
        assert_eq!(
            color_policy_from_raw_args(&args(&[
                "hpc-compose",
                "--color",
                "always",
                "run",
                "service",
                "--",
                "child",
                "--color=never",
            ])),
            ColorPolicy::Always
        );
        assert_eq!(
            color_policy_from_raw_args(&args(&[
                "hpc-compose",
                "run",
                "service",
                "--",
                "child",
                "--color=always",
            ])),
            ColorPolicy::Auto
        );
    }
}
