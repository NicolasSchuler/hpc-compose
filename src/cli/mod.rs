//! Shared CLI definitions for the runtime binary, completion generation, and
//! generated manpages.
#![allow(missing_docs)]

use clap::{CommandFactory, Parser, ValueEnum};

mod commands;
mod help;

pub use crate::term::ColorPolicy;
pub use commands::{
    CacheCommands, Cli, Commands, DoctorCommands, ExamplesCommands, ExperimentCommands,
    JobsCommands, RendezvousCommands, RuntimeLaunchArgs, SweepCommands,
};
pub use help::examples_for_path;

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
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
    Cli::parse()
}

/// Builds the Clap command tree used by the binary and manpage generator.
#[must_use]
pub fn build_cli_command() -> clap::Command {
    Cli::command()
}
