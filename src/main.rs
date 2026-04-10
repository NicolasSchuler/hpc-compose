use anyhow::Result;
use hpc_compose::cli::parse_cli;
use hpc_compose::commands::run_cli;

fn main() -> Result<()> {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let cli = parse_cli();
    run_cli(cli, &raw_args)
}
