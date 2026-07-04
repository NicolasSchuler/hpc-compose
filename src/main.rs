use hpc_compose::cli::parse_cli;
use hpc_compose::cli_error_report;
use hpc_compose::commands::run_cli;
use hpc_compose::exit::ExitCodeError;

fn main() {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let cli = parse_cli();
    if let Err(err) = run_cli(cli, &raw_args) {
        // main.rs is the only exit site: derive the process code from the typed
        // error layer's stable catalog (see `hpc_compose::exit`). This maps a
        // command's failure onto 1 (generic), 2 (usage/spec), 3 (environment),
        // 4 (lint), or a child process's own status.
        let code = hpc_compose::exit::exit_code_for(&err);
        // A direct-execution command's child has already written its own output,
        // so surface its status silently rather than rendering a diagnostic.
        // Every other failure gets a rendered diagnostic.
        if err.downcast_ref::<ExitCodeError>().is_none() {
            eprintln!("{:?}", cli_error_report(err));
        }
        std::process::exit(code);
    }
}
