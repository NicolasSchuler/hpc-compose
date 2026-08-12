use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::{OutputFormat, RendezvousCommands};
use hpc_compose::rendezvous::{self, RendezvousRegisterRequest};

use crate::output;
pub(crate) use crate::output::runtime::RendezvousRegisterOutput;

pub(crate) fn explicit_cache_dir(command: &RendezvousCommands) -> Option<&Path> {
    match command {
        RendezvousCommands::Register { cache_dir, .. }
        | RendezvousCommands::Resolve { cache_dir, .. }
        | RendezvousCommands::List { cache_dir, .. }
        | RendezvousCommands::Prune { cache_dir, .. } => cache_dir.as_deref(),
    }
}

pub(crate) fn run(command: RendezvousCommands, cache_dir: PathBuf) -> Result<()> {
    match command {
        RendezvousCommands::Register {
            name,
            host,
            port,
            job_id,
            service,
            protocol,
            path,
            ttl_seconds,
            cache_dir: _,
            format,
        } => {
            let job_id = job_id
                .or_else(|| env::var("SLURM_JOB_ID").ok())
                .ok_or_else(|| {
                    hpc_compose::exit::UsageError::new(
                        "rendezvous register requires --job-id outside a Slurm job",
                    )
                })?;
            rendezvous_register(
                cache_dir,
                name,
                job_id,
                service,
                host,
                port,
                protocol,
                path,
                ttl_seconds,
                format,
            )
        }
        RendezvousCommands::Resolve {
            name,
            cache_dir: _,
            format,
        } => rendezvous_resolve(cache_dir, name, format),
        RendezvousCommands::List {
            cache_dir: _,
            format,
        } => rendezvous_list(cache_dir, format),
        RendezvousCommands::Prune {
            cache_dir: _,
            format,
        } => rendezvous_prune(cache_dir, format),
    }
}

#[allow(clippy::too_many_arguments)]
fn rendezvous_register(
    cache_dir: PathBuf,
    name: String,
    job_id: String,
    service: Option<String>,
    host: String,
    port: u16,
    protocol: String,
    path: Option<String>,
    ttl_seconds: u64,
    format: Option<OutputFormat>,
) -> Result<()> {
    let now = rendezvous::unix_timestamp_now();
    let record = rendezvous::build_record(
        &cache_dir,
        RendezvousRegisterRequest {
            name,
            job_id,
            service,
            host,
            port,
            protocol,
            path,
            ttl_seconds,
            metadata: BTreeMap::new(),
        },
        now,
    )?;
    let record_path = rendezvous::register(&cache_dir, &record)?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("registered rendezvous: {}", record.name);
            println!("url: {}", record.url);
            println!("job id: {}", record.job_id);
            println!("record: {}", record_path.display());
        }
        OutputFormat::Json => println!(
            "{}",
            crate::output::to_pretty_json(&RendezvousRegisterOutput {
                schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                cache_dir,
                record_path,
                record,
            })
            .context("failed to serialize rendezvous register output")?
        ),
    }
    Ok(())
}

fn rendezvous_resolve(
    cache_dir: PathBuf,
    name: String,
    format: Option<OutputFormat>,
) -> Result<()> {
    let Some(record) = rendezvous::resolve(&cache_dir, &name, rendezvous::unix_timestamp_now())?
    else {
        bail!(
            "no live rendezvous record named '{}' found under {}",
            name,
            rendezvous::root_dir(&cache_dir).display()
        );
    };
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("name: {}", record.name);
            println!("url: {}", record.url);
            println!("host: {}", record.host);
            println!("port: {}", record.port);
            println!("job id: {}", record.job_id);
            if let Some(service) = &record.service {
                println!("service: {service}");
            }
            println!(
                "expires in: {}s",
                record.ttl_seconds.saturating_sub(
                    rendezvous::unix_timestamp_now().saturating_sub(record.registered_at)
                )
            );
        }
        OutputFormat::Json => println!(
            "{}",
            crate::output::to_pretty_json(&record)
                .context("failed to serialize rendezvous resolve output")?
        ),
    }
    Ok(())
}

fn rendezvous_list(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
    let records = rendezvous::list(&cache_dir, rendezvous::unix_timestamp_now())?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            if records.is_empty() {
                println!(
                    "no live rendezvous records found under {}",
                    rendezvous::root_dir(&cache_dir).display()
                );
            } else {
                for record in records {
                    println!("{} {} job={}", record.name, record.url, record.job_id);
                }
            }
        }
        OutputFormat::Json => println!(
            "{}",
            crate::output::to_pretty_json(&records)
                .context("failed to serialize rendezvous list output")?
        ),
    }
    Ok(())
}

fn rendezvous_prune(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
    let report = rendezvous::prune(&cache_dir, rendezvous::unix_timestamp_now())?;
    match output::resolve_output_format(format) {
        OutputFormat::Text => {
            println!("removed {} rendezvous record(s)", report.removed.len());
            for path in &report.removed {
                println!("  {}", path.display());
            }
        }
        OutputFormat::Json => println!(
            "{}",
            crate::output::to_pretty_json(&output::contract::RendezvousPruneOutput::new(report))
                .context("failed to serialize rendezvous prune output")?
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::RendezvousRegisterOutput;

    #[test]
    fn rendezvous_register_output_preserves_exact_pretty_json_bytes() {
        let output = RendezvousRegisterOutput {
            schema_version: 1,
            cache_dir: PathBuf::from("cache"),
            record_path: PathBuf::from("cache/rendezvous/model-server.json"),
            record: hpc_compose::rendezvous::RendezvousRecord {
                schema_version: 1,
                name: "model-server".to_string(),
                job_id: "42".to_string(),
                service: None,
                host: "node01".to_string(),
                port: 8000,
                protocol: "http".to_string(),
                path: None,
                url: "http://node01:8000".to_string(),
                registered_at: 100,
                ttl_seconds: 300,
                cache_dir: PathBuf::from("cache"),
                metadata: BTreeMap::new(),
            },
        };

        let actual =
            crate::output::to_pretty_json(&output).expect("serialize rendezvous register fixture");
        let expected = r#"{
  "schema_version": 1,
  "cache_dir": "cache",
  "record_path": "cache/rendezvous/model-server.json",
  "record": {
    "schema_version": 1,
    "name": "model-server",
    "job_id": "42",
    "service": null,
    "host": "node01",
    "port": 8000,
    "protocol": "http",
    "path": null,
    "url": "http://node01:8000",
    "registered_at": 100,
    "ttl_seconds": 300,
    "cache_dir": "cache",
    "metadata": {}
  }
}"#;
        assert_eq!(actual, expected);
        assert!(!actual.ends_with('\n'));
    }
}
