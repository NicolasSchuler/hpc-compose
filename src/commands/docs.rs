use anyhow::Result;
use hpc_compose::cli::OutputFormat;
use hpc_compose::docs_search::{DocsSearchOutput, search_docs};
use hpc_compose::term;

use crate::output;

pub(crate) fn search(
    query_parts: Vec<String>,
    limit: usize,
    format: Option<OutputFormat>,
) -> Result<()> {
    let query = query_parts.join(" ");
    let report = search_docs(&query, limit);
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        OutputFormat::Text => print_text(&report),
    }
    Ok(())
}

fn print_text(report: &DocsSearchOutput) {
    println!(
        "{}",
        term::styled_section_header(&format!("Docs matches for `{}`", report.query))
    );
    println!(
        "Static offline search over the bundled manual; no settings, SSH, Slurm, or browser access."
    );
    println!();

    if report.matches.is_empty() {
        println!(
            "No docs matched. Try a command, field, or symptom such as `cache`, `--offline`, or `readiness`."
        );
        return;
    }

    for (index, hit) in report.matches.iter().enumerate() {
        println!(
            "{}. {} ({})",
            index + 1,
            term::styled_bold(&hit.title),
            hit.location()
        );
        if let Some(heading) = &hit.heading {
            println!("   Section: {heading}");
        }
        println!("   {}", hit.snippet);
        println!();
    }
}
