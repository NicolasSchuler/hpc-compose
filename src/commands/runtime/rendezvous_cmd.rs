use super::*;

#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct RendezvousRegisterOutput {
    pub(crate) schema_version: u32,
    cache_dir: PathBuf,
    record_path: PathBuf,
    record: hpc_compose::rendezvous::RendezvousRecord,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn rendezvous_register(
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
            serde_json::to_string_pretty(&RendezvousRegisterOutput {
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

pub(crate) fn rendezvous_resolve(
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
            serde_json::to_string_pretty(&record)
                .context("failed to serialize rendezvous resolve output")?
        ),
    }
    Ok(())
}

pub(crate) fn rendezvous_list(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
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
            serde_json::to_string_pretty(&records)
                .context("failed to serialize rendezvous list output")?
        ),
    }
    Ok(())
}

pub(crate) fn rendezvous_prune(cache_dir: PathBuf, format: Option<OutputFormat>) -> Result<()> {
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
            serde_json::to_string_pretty(&output::contract::RendezvousPruneOutput::new(report))
                .context("failed to serialize rendezvous prune output")?
        ),
    }
    Ok(())
}
