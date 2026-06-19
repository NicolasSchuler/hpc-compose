use hpc_compose::cli::parse_cli;
use hpc_compose::cli_error_report;
use hpc_compose::commands::run_cli;
use hpc_compose::exit::ExitCodeError;

fn main() {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let cli = parse_cli();
    if let Err(err) = run_cli(cli, &raw_args) {
        // Preserve a child process's real exit code for direct-execution
        // commands (run/alloc/shell/notebook) instead of collapsing every
        // failure to 1. The child has already written its own output, so we
        // exit silently with its status rather than rendering a diagnostic.
        if let Some(exit) = err.downcast_ref::<ExitCodeError>() {
            let code = exit.code();
            std::process::exit(if code == 0 { 1 } else { code });
        }
        eprintln!("{:?}", cli_error_report(err));
        std::process::exit(1);
    }
}
