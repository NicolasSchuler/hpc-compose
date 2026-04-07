use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use hpc_compose::manpages::{DEFAULT_MANPAGE_DIR, check_manpages, write_manpages};

#[derive(Debug, Parser)]
#[command(about = "Generate checked-in manpages for hpc-compose")]
struct Args {
    #[arg(long, help = "Fail if checked-in manpages are stale")]
    check: bool,
    #[arg(
        long,
        value_name = "DIR",
        default_value = DEFAULT_MANPAGE_DIR,
        help = "Directory where section-1 manpages are written"
    )]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.check {
        check_manpages(&args.output_dir)
    } else {
        write_manpages(&args.output_dir)
    }
}
