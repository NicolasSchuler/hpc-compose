mod commands;
mod output;

use anyhow::Result;
use hpc_compose::cli::parse_cli;

fn main() -> Result<()> {
    let cli = parse_cli();
    commands::run_command(cli.command)
}
