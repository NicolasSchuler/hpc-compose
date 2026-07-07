use anyhow::{Context, Result};
use hpc_compose::cli::{FeedbackKind, OutputFormat};
use serde::Serialize;

use crate::output;

const REPOSITORY: &str = env!("CARGO_PKG_REPOSITORY");

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct FeedbackOutput {
    pub(crate) schema_version: u32,
    pub(crate) kind: String,
    pub(crate) issue_url: String,
    pub(crate) report: FeedbackReport,
    pub(crate) telemetry_sent: bool,
}

#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub(crate) struct FeedbackReport {
    pub(crate) package: String,
    pub(crate) version: String,
    pub(crate) repository: String,
    pub(crate) build_rev: Option<String>,
    pub(crate) build_dirty: bool,
    pub(crate) os: String,
    pub(crate) arch: String,
}

pub(crate) fn feedback(kind: FeedbackKind, format: Option<OutputFormat>) -> Result<()> {
    let output = build_feedback_output(kind);
    match output::common::resolve_output_format(format) {
        OutputFormat::Text => print_feedback(&output),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize feedback output")?
            );
        }
    }
    Ok(())
}

fn build_feedback_output(kind: FeedbackKind) -> FeedbackOutput {
    let report = FeedbackReport {
        package: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        repository: REPOSITORY.to_string(),
        build_rev: option_env!("HPC_COMPOSE_BUILD_REV")
            .filter(|rev| !rev.is_empty())
            .map(str::to_string),
        build_dirty: option_env!("HPC_COMPOSE_BUILD_DIRTY") == Some("1"),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    };
    FeedbackOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        kind: kind.as_str().to_string(),
        issue_url: issue_url(kind, &report),
        report,
        telemetry_sent: false,
    }
}

fn issue_url(kind: FeedbackKind, report: &FeedbackReport) -> String {
    let template = match kind {
        FeedbackKind::Bug => "bug_report.yml",
        FeedbackKind::Feature => "feature_request.yml",
        FeedbackKind::Adoption => "adoption-feedback.yml",
        FeedbackKind::Question => "",
    };
    let title = match kind {
        FeedbackKind::Bug => "[bug] ",
        FeedbackKind::Feature => "[feature] ",
        FeedbackKind::Adoption => "[adoption] ",
        FeedbackKind::Question => "[question] ",
    };
    let body = format!(
        "hpc-compose feedback report\n\nversion: {}\nbuild_rev: {}\nbuild_dirty: {}\nos: {}\narch: {}\n\nNo telemetry was sent; this report was generated locally.",
        report.version,
        report.build_rev.as_deref().unwrap_or("unknown"),
        report.build_dirty,
        report.os,
        report.arch
    );
    if template.is_empty() {
        format!(
            "{REPOSITORY}/issues/new?title={}&body={}",
            url_encode(title),
            url_encode(&body)
        )
    } else {
        format!(
            "{REPOSITORY}/issues/new?template={template}&title={}&body={}",
            url_encode(title),
            url_encode(&body)
        )
    }
}

fn print_feedback(output: &FeedbackOutput) {
    println!("hpc-compose feedback ({})", output.kind);
    println!("Issue link: {}", output.issue_url);
    println!();
    println!("Local report:");
    println!("  package: {}", output.report.package);
    println!("  version: {}", output.report.version);
    println!(
        "  build_rev: {}",
        output.report.build_rev.as_deref().unwrap_or("unknown")
    );
    println!("  build_dirty: {}", output.report.build_dirty);
    println!("  os: {}", output.report.os);
    println!("  arch: {}", output.report.arch);
    println!();
    println!("No telemetry was sent. Paste this report into the issue if it helps.");
}

fn url_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b' ' => encoded.push_str("%20"),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

impl FeedbackKind {
    fn as_str(self) -> &'static str {
        match self {
            FeedbackKind::Bug => "bug",
            FeedbackKind::Feature => "feature",
            FeedbackKind::Adoption => "adoption",
            FeedbackKind::Question => "question",
        }
    }
}
