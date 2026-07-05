use std::fmt::Display;
use std::fmt::Write as _;

use super::annotate::Annotations;

pub(super) fn push_directive(
    out: &mut String,
    ann: &mut Annotations,
    source: &str,
    name: &str,
    value: impl Display,
) {
    ann.field(out, source, |out| {
        writeln!(out, "#SBATCH --{name}={value}").expect("writing to String cannot fail");
    });
}

pub(super) fn push_raw_directive(out: &mut String, ann: &mut Annotations, source: &str, arg: &str) {
    ann.field(out, source, |out| {
        writeln!(out, "#SBATCH {arg}").expect("writing to String cannot fail");
    });
}

pub(super) fn push_bare_directive(
    out: &mut String,
    ann: &mut Annotations,
    source: &str,
    name: &str,
) {
    ann.field(out, source, |out| {
        writeln!(out, "#SBATCH --{name}").expect("writing to String cannot fail");
    });
}
