use anyhow::Result;
use hpc_compose::cli::{ExamplesOutputFormat, OutputFormat};
use hpc_compose::examples::{ExampleInfo, ExampleRecommendation, examples, recommend_examples};
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

pub(crate) fn recommend(
    query: Option<String>,
    tags: Vec<String>,
    limit: usize,
    format: Option<OutputFormat>,
) -> Result<()> {
    let recommendations = recommend_examples(query.as_deref(), &tags, limit);
    print_recommendations(
        query.as_deref(),
        &tags,
        &recommendations,
        format.unwrap_or(OutputFormat::Text),
    )
}

/// `examples list` / `search` / `coverage` JSON output.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExamplesListOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) examples: &'a [ExampleInfo],
}

/// `examples recommend` JSON output.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExamplesRecommendOutput<'a> {
    pub(crate) schema_version: u32,
    pub(crate) query: Option<&'a str>,
    pub(crate) required_tags: &'a [String],
    pub(crate) safe_authoring_note: &'static str,
    pub(crate) recommendations: &'a [ExampleRecommendation],
}

fn print_examples(entries: &[ExampleInfo], format: ExamplesOutputFormat) -> Result<()> {
    match format {
        ExamplesOutputFormat::Text => print_text(entries),
        ExamplesOutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&ExamplesListOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                    examples: entries,
                })?
            );
        }
        ExamplesOutputFormat::Markdown => {
            println!("{}", markdown_table(entries));
        }
    }
    Ok(())
}

fn print_text(entries: &[ExampleInfo]) {
    if entries.is_empty() {
        println!(
            "No examples matched. Try `hpc-compose examples list` or `hpc-compose examples recommend <description>`."
        );
        return;
    }
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

fn print_recommendations(
    query: Option<&str>,
    tags: &[String],
    recommendations: &[ExampleRecommendation],
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Text => {
            print_recommendations_text(query, tags, recommendations);
        }
        OutputFormat::Json => {
            println!(
                "{}",
                crate::output::to_pretty_json(&ExamplesRecommendOutput {
                    schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                    query,
                    required_tags: tags,
                    safe_authoring_note: "Recommendation commands only copy or scaffold a spec and run static plan checks; they do not contact Slurm.",
                    recommendations,
                })?
            );
        }
    }
    Ok(())
}

fn print_recommendations_text(
    query: Option<&str>,
    tags: &[String],
    recommendations: &[ExampleRecommendation],
) {
    let label = recommendation_label(query, tags);
    println!("{}", term::styled_section_header(&label));
    println!(
        "Safe authoring path only: these commands copy or scaffold a spec and run plan checks without contacting Slurm."
    );
    println!();

    if recommendations.is_empty() {
        println!("No examples matched. Try `hpc-compose examples list` or broaden the query.");
        return;
    }

    for (index, recommendation) in recommendations.iter().enumerate() {
        let example = recommendation.example;
        println!(
            "{}. {} ({}, {})",
            index + 1,
            term::styled_bold(example.name),
            example.availability.label(),
            example.path
        );
        println!("   Demonstrates: {}", example.demonstrates);
        println!("   Start when: {}", example.start_when);
        println!("   Why: {}", recommendation.reasons.join("; "));
        println!(
            "   Prerequisites to review: {}",
            recommendation.prerequisites.join("; ")
        );
        println!("   Safe next commands:");
        for command in &recommendation.next_commands {
            println!("     {command}");
        }
        println!();
    }
}

fn recommendation_label(query: Option<&str>, tags: &[String]) -> String {
    let query = query.unwrap_or_default().trim();
    match (query.is_empty(), tags.is_empty()) {
        (true, true) => "Recommended starting examples".to_string(),
        (false, true) => format!("Recommended examples for `{query}`"),
        (true, false) => format!("Recommended examples for tags `{}`", tags.join("`, `")),
        (false, false) => format!(
            "Recommended examples for `{query}` with tags `{}`",
            tags.join("`, `")
        ),
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
    fn coverage_table_matches_examples_doc() {
        let generated = markdown_table(examples());
        let doc = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/src/examples.md"),
        )
        .expect("read docs/src/examples.md");
        assert!(
            doc.contains(&generated),
            "docs/src/examples.md coverage table is out of sync with the example registry; \
             regenerate it with `hpc-compose examples coverage --format markdown`"
        );
    }

    #[test]
    fn markdown_output_accepts_list_entries() {
        assert!(print_examples(&examples()[0..1], ExamplesOutputFormat::Markdown).is_ok());
    }

    #[test]
    fn recommendation_text_contains_safe_authoring_commands() {
        let recommendations = recommend_examples(Some("vllm worker"), &[], 2);
        assert!(print_recommendations(None, &[], &recommendations, OutputFormat::Text).is_ok());
        assert_eq!(recommendations[0].example.name, "vllm-uv-worker");
        assert!(
            recommendations[0]
                .next_commands
                .iter()
                .any(|command| command.contains("hpc-compose plan -f compose.yaml"))
        );
    }
}
