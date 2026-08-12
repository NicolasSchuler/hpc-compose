//! Text presentation for embedded documentation search results.

use std::io::{self, Write};

use hpc_compose::docs_search::DocsSearchOutput;
use hpc_compose::term;

pub(crate) fn write_search_results(
    writer: &mut impl Write,
    report: &DocsSearchOutput,
) -> io::Result<()> {
    writeln!(
        writer,
        "{}",
        term::styled_section_header(&format!("Docs matches for `{}`", report.query))
    )?;
    writeln!(
        writer,
        "Static offline search over the bundled manual; no settings, SSH, Slurm, or browser access."
    )?;
    writeln!(writer)?;

    if report.matches.is_empty() {
        writeln!(
            writer,
            "No docs matched. Try a command, field, or symptom such as `cache`, `--offline`, or `readiness`."
        )?;
        return Ok(());
    }

    for (index, hit) in report.matches.iter().enumerate() {
        writeln!(
            writer,
            "{}. {} ({})",
            index + 1,
            term::styled_bold(&hit.title),
            hit.location()
        )?;
        if let Some(heading) = &hit.heading {
            writeln!(writer, "   Section: {heading}")?;
        }
        writeln!(writer, "   {}", hit.snippet)?;
        writeln!(writer)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hpc_compose::docs_search::search_docs;

    #[test]
    fn empty_search_results_preserve_exact_bytes() {
        crate::term::with_test_color_policy(crate::term::ColorPolicy::Never, || {
            let report = search_docs("", 2);
            let mut output = Vec::new();

            write_search_results(&mut output, &report).expect("write docs results");

            assert_eq!(
                String::from_utf8(output).expect("UTF-8"),
                "Docs matches for ``\nStatic offline search over the bundled manual; no settings, SSH, Slurm, or browser access.\n\nNo docs matched. Try a command, field, or symptom such as `cache`, `--offline`, or `readiness`.\n"
            );
        });
    }

    #[test]
    fn ranked_search_results_preserve_exact_excerpts_and_order() {
        crate::term::with_test_color_policy(crate::term::ColorPolicy::Never, || {
            let report = search_docs("x-slurm.cache_dir", 2);
            let mut output = Vec::new();

            write_search_results(&mut output, &report).expect("write docs results");

            assert_eq!(
                String::from_utf8(output).expect("UTF-8"),
                "Docs matches for `x-slurm.cache_dir`\nStatic offline search over the bundled manual; no settings, SSH, Slurm, or browser access.\n\n1. Spec Reference (spec-reference.md#x-slurmcache-dir)\n   Section: `x-slurm.cache_dir`\n   `x-slurm.cache_dir` - Shape: string - Default precedence: explicit `x-slurm.cache_dir`, then `[profiles.<name>.cache].dir`, then `[defaults.cache].dir`, then `$HOME/.cache/hpc-compose`. - Notes: - Relative paths and...\n\n2. CLI Reference (cli-reference.md#authoring-and-setup)\n   Section: Authoring and Setup\n   ...templates before writing a file. `--cache-dir` is optional and writes an explicit `x-slurm.cache_dir`. | | `examples` | Search and recommend shipped examples and starter templates | Use `examples recommend` for a...\n\n"
            );
        });
    }
}
