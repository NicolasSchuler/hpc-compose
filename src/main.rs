use hpc_compose::cli::parse_cli;
use hpc_compose::cli_error_report;
use hpc_compose::commands::run_cli;

fn main() {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let cli = parse_cli();
    if let Err(err) = run_cli(cli, &raw_args) {
        eprintln!("{:?}", cli_error_report(err));
        std::process::exit(1);
    }
}
