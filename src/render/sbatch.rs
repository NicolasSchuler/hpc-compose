use std::fmt::Display;
use std::fmt::Write as _;

pub(super) fn push_directive(out: &mut String, name: &str, value: impl Display) {
    writeln!(out, "#SBATCH --{name}={value}").expect("writing to String cannot fail");
}

pub(super) fn push_raw_directive(out: &mut String, arg: &str) {
    writeln!(out, "#SBATCH {arg}").expect("writing to String cannot fail");
}
