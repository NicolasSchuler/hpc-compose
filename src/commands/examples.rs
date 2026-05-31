use anyhow::Result;
use hpc_compose::cli::ExamplesOutputFormat;
use hpc_compose::examples::{ExampleInfo, examples};
use hpc_compose::term;
use serde::Serialize;

pub(crate) fn list(tag: Option<String>, format: Option<ExamplesOutputFormat>) -> Result<()> {
    let entries = examples()
        .iter()
        .copied()
        .filter(|example| tag.as_deref().is_none_or(|tag| example.has_tag(tag)))
        .collect::<Vec<_>>();
    print_examples(&entries, format.unwrap_or(ExamplesOutputFormat::Text))
}

pub(crate) fn search(query: String, format: Option<ExamplesOutputFormat>) -> Result<()> {
    let entries = examples()
        .iter()
        .copied()
        .filter(|example| example.matches_query(&query))
        .collect::<Vec<_>>();
    print_examples(&entries, format.unwrap_or(ExamplesOutputFormat::Text))
}

pub(crate) fn coverage(format: Option<ExamplesOutputFormat>) -> Result<()> {
    print_examples(examples(), format.unwrap_or(ExamplesOutputFormat::Markdown))
}

fn print_examples(entries: &[ExampleInfo], format: ExamplesOutputFormat) -> Result<()> {
    match format {
        ExamplesOutputFormat::Text => print_text(entries),
        ExamplesOutputFormat::Json => {
            #[derive(Serialize)]
            struct Output<'a> {
                examples: &'a [ExampleInfo],
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&Output { examples: entries })?
            );
        }
        ExamplesOutputFormat::Markdown => {
            println!("{}", markdown_table(entries));
        }
    }
    Ok(())
}

fn print_text(entries: &[ExampleInfo]) {
    for category in ["basics", "llm", "training", "distributed", "workflow"] {
        let grouped = entries
            .iter()
            .filter(|example| example.category == category)
            .collect::<Vec<_>>();
        if grouped.is_empty() {
            continue;
        }
        println!("{}:", term::styled_section_header(category));
        for example in grouped {
            println!(
                "  {}\t{} | {} | tags: {}",
                term::styled_bold(example.name),
                example.availability.label(),
                example.start_when,
                example.tags.join(", ")
            );
        }
        println!();
    }
}

fn markdown_table(entries: &[ExampleInfo]) -> String {
    let mut out = String::new();
    out.push_str(
        "| Example | Availability | Tags | What it demonstrates | When to start from it |\n",
    );
    out.push_str("| --- | --- | --- | --- | --- |\n");
    for example in entries {
        out.push_str(&format!(
            "| [`{}.yaml`](example-source.md#{}) | {} | `{}` | {} | {} |\n",
            example.name,
            example.name,
            example.availability.label(),
            example.tags.join("`, `"),
            markdown_escape(example.demonstrates),
            markdown_escape(example.start_when)
        ));
    }
    out
}

fn markdown_escape(value: &str) -> String {
    value.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_table_contains_tags_and_examples() {
        let table = markdown_table(examples());
        assert!(table.contains("| Example | Availability | Tags |"));
        assert!(table.contains("minimal-batch.yaml"));
        assert!(table.contains("`mpi`"));
    }

    #[test]
    fn markdown_output_accepts_list_entries() {
        assert!(print_examples(&examples()[0..1], ExamplesOutputFormat::Markdown).is_ok());
    }
}
