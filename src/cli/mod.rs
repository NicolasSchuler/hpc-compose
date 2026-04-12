//! Shared CLI definitions for the runtime binary, completion generation, and
//! generated manpages.
#![allow(missing_docs)]

use clap::{CommandFactory, Parser, ValueEnum};

mod commands;
mod help;

pub use commands::{CacheCommands, Cli, Commands, JobsCommands};
pub use help::examples_for_path;

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum StatsOutputFormat {
    Text,
    Json,
    Csv,
    Jsonl,
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
