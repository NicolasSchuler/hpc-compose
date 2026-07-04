use anyhow::Result;
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::ResolvedBinaries;
use hpc_compose::weather::{GpuModelSummary, NodeSummary, WeatherOptions, WeatherReport};

use crate::output;

pub(crate) fn weather(format: Option<OutputFormat>, binaries: &ResolvedBinaries) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let report = hpc_compose::weather::collect_weather(&WeatherOptions {
        binaries,
        cwd: &cwd,
    })?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => print_weather_report(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output::contract::WeatherOutput::new(report))?
            );
        }
    }
    Ok(())
}

fn print_weather_report(report: &WeatherReport) {
    println!(
        "CLUSTER WEATHER: {}",
        report.cluster.as_deref().unwrap_or("unknown")
    );
    println!("Condition: {}", report.condition.label());
    if let Some(nodes) = &report.nodes {
        print_node_summary(nodes);
    } else {
        println!("Nodes: unavailable");
    }
    if let Some(queue) = &report.queue {
        let wait = queue
            .average_pending_wait_seconds
            .map(format_duration)
            .map(|value| format!("; avg pending wait ~{value}"))
            .unwrap_or_default();
        println!(
            "Queue: {} running, {} pending, {} other{}",
            queue.running_jobs, queue.pending_jobs, queue.other_jobs, wait
        );
    } else {
        println!("Queue: unavailable");
    }
    println!(
        "Your jobs: {} running, {} pending, {} other",
        report.user.running_jobs, report.user.pending_jobs, report.user.other_jobs
    );
    if let Some(fairshare) = &report.fairshare {
        println!(
            "Fairshare: account={} value={}",
            fairshare.account.as_deref().unwrap_or("unknown"),
            fairshare
                .fairshare
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    if let Some(priority) = &report.priority {
        println!(
            "Priority: {} pending job(s), top={} priority={}",
            priority.pending_jobs,
            priority.top_job_id.as_deref().unwrap_or("unknown"),
            priority
                .highest_priority
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );
    }
    if report.maintenance.is_empty() {
        println!("Maintenance/unavailable: none reported");
    } else {
        println!("Maintenance/unavailable:");
        for note in &report.maintenance {
            println!(
                "  {}: {} node(s) in {}{}",
                note.partition.as_deref().unwrap_or("unknown"),
                note.nodes,
                note.state,
                note.reason
                    .as_ref()
                    .map(|reason| format!(" ({reason})"))
                    .unwrap_or_default()
            );
        }
    }
    if !report.warnings.is_empty() {
        println!("Warnings:");
        for warning in &report.warnings {
            println!("  {warning}");
        }
    }
}

fn print_node_summary(nodes: &NodeSummary) {
    println!(
        "Nodes: {}/{} free ({} unavailable)",
        nodes.free_nodes, nodes.total_nodes, nodes.unavailable_nodes
    );
    println!(
        "CPU nodes: {}/{} free",
        nodes.cpu.free_nodes, nodes.cpu.total_nodes
    );
    let models = format_gpu_models(&nodes.gpu.models);
    println!(
        "GPU nodes: {}/{} free{}",
        nodes.gpu.free_nodes, nodes.gpu.total_nodes, models
    );
}

fn format_gpu_models(models: &[GpuModelSummary]) -> String {
    if models.is_empty() {
        return String::new();
    }
    let labels = models
        .iter()
        .map(|model| {
            format!(
                "{}: {}/{} nodes free",
                model.model.to_ascii_uppercase(),
                model.free_nodes,
                model.total_nodes
            )
        })
        .collect::<Vec<_>>();
    format!(" ({})", labels.join(", "))
}

fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{} min", div_round(seconds, 60))
    } else if seconds < 86_400 {
        format!("{} h", div_round(seconds, 3_600))
    } else {
        format!("{} d", div_round(seconds, 86_400))
    }
}

fn div_round(value: u64, divisor: u64) -> u64 {
    (value + divisor / 2) / divisor
}
