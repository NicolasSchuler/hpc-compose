mod commands;
mod output;

use anyhow::Result;
use hpc_compose::cli::parse_cli;

fn main() -> Result<()> {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let cli = parse_cli();
    commands::run_cli(cli, &raw_args)
}
