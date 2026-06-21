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

/// Output format for `sweep results`. CSV is sweep-results-specific and cannot
/// live on the shared [`OutputFormat`] (which is Text/Json only).
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum SweepResultsFormat {
    Text,
    Json,
    Csv,
}

/// Output format for the N-way `diff --across`/`--jobs` matrix. CSV is
/// matrix-specific (column-per-run) and cannot live on the shared
/// [`OutputFormat`] (which is Text/Json only).
#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum DiffMatrixFormat {
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
