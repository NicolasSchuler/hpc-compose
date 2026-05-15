use std::collections::BTreeMap;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use super::model::{SubmissionBackend, SubmissionRecord};
use super::scheduler::{command_unavailable_detail, command_unavailable_error};
use super::stats::{find_tres_value, parse_tres_map};

/// Slurm accounting data returned by `stats --accounting`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountingSnapshot {
    pub available: bool,
    pub reason: Option<String>,
    pub source: String,
    pub summary: Option<AccountingSummary>,
    pub rows: Vec<AccountingRow>,
}

/// Grant-report friendly accounting rollup.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountingSummary {
    pub allocated_cpu_hours: Option<f64>,
    pub total_cpu_hours: Option<f64>,
    pub allocated_gpu_hours: Option<f64>,
    pub allocated_memory_byte_seconds: Option<f64>,
    pub max_rss_bytes: Option<u64>,
    pub memory_basis: String,
}

/// One parsed `sacct` row.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountingRow {
    pub job_id_raw: String,
    pub job_name: String,
    pub state: String,
    pub exit_code: String,
    pub elapsed_raw_seconds: Option<u64>,
    pub alloc_cpus: Option<u64>,
    pub cpu_time_raw_seconds: Option<u64>,
    pub total_cpu_seconds: Option<u64>,
    pub alloc_tres: String,
    pub req_tres: String,
    pub alloc_tres_map: BTreeMap<String, String>,
    pub req_tres_map: BTreeMap<String, String>,
    pub max_rss_bytes: Option<u64>,
    pub tres_usage_in_tot: String,
    pub tres_usage_in_tot_map: BTreeMap<String, String>,
    pub nnodes: Option<u64>,
    pub account: Option<String>,
    pub qos: Option<String>,
    pub partition: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
}

const SACCT_ACCOUNTING_FORMAT: &str = "JobIDRaw,JobName,State,ExitCode,ElapsedRaw,AllocCPUS,CPUTimeRAW,TotalCPU,AllocTRES,ReqTRES,MaxRSS,TRESUsageInTot,NNodes,Account,QOS,Partition,Start,End";

pub(super) fn build_accounting_snapshot(
    job_id: &str,
    record: Option<&SubmissionRecord>,
    sacct_bin: &str,
) -> Result<AccountingSnapshot> {
    if record.is_some_and(|record| record.backend == SubmissionBackend::Local) {
        return Ok(AccountingSnapshot {
            available: false,
            reason: Some("Slurm accounting is unavailable for locally launched jobs".to_string()),
            source: "local".to_string(),
            summary: None,
            rows: Vec::new(),
        });
    }

    let output = match Command::new(sacct_bin)
        .args([
            "-n",
            "-j",
            job_id,
            "--parsable2",
            "--noconvert",
            &format!("--format={SACCT_ACCOUNTING_FORMAT}"),
        ])
        .output()
    {
        Ok(output) => output,
        Err(err) if command_unavailable_error(&err) => {
            return Ok(AccountingSnapshot {
                available: false,
                reason: Some(command_unavailable_detail("sacct", sacct_bin, &err)),
                source: "sacct".to_string(),
                summary: None,
                rows: Vec::new(),
            });
        }
        Err(err) => return Err(err).with_context(|| format!("failed to execute '{sacct_bin}'")),
    };
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("sacct accounting query failed for job {job_id}");
        }
        bail!("sacct accounting query failed for job {job_id}: {detail}");
    }

    let rows = parse_sacct_accounting_output(&String::from_utf8_lossy(&output.stdout))?;
    if rows.is_empty() {
        return Ok(AccountingSnapshot {
            available: false,
            reason: Some("sacct returned no accounting rows for this job".to_string()),
            source: "sacct".to_string(),
            summary: None,
            rows,
        });
    }
    let summary = summarize_accounting_rows(job_id, &rows);
    Ok(AccountingSnapshot {
        available: true,
        reason: None,
        source: "sacct".to_string(),
        summary: Some(summary),
        rows,
    })
}

pub(super) fn parse_sacct_accounting_output(stdout: &str) -> Result<Vec<AccountingRow>> {
    let mut rows = Vec::new();
    for (index, raw_line) in stdout.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields
            .first()
            .is_some_and(|field| field.eq_ignore_ascii_case("JobIDRaw"))
        {
            continue;
        }
        if fields.len() != 18 {
            bail!(
                "malformed sacct accounting output on line {}: expected 18 fields, found {}",
                index + 1,
                fields.len()
            );
        }
        let alloc_tres_map = parse_tres_map(fields[8])
            .context(format!("failed to parse AllocTRES for row '{}'", fields[0]))?;
        let req_tres_map = parse_tres_map(fields[9])
            .context(format!("failed to parse ReqTRES for row '{}'", fields[0]))?;
        let tres_usage_in_tot_map = parse_tres_map(fields[11]).context(format!(
            "failed to parse TRESUsageInTot for row '{}'",
            fields[0]
        ))?;
        rows.push(AccountingRow {
            job_id_raw: fields[0].to_string(),
            job_name: fields[1].to_string(),
            state: fields[2].to_string(),
            exit_code: fields[3].to_string(),
            elapsed_raw_seconds: parse_optional_u64(fields[4]),
            alloc_cpus: parse_optional_u64(fields[5]),
            cpu_time_raw_seconds: parse_optional_u64(fields[6]),
            total_cpu_seconds: parse_slurm_accounting_duration(fields[7]),
            alloc_tres: fields[8].to_string(),
            req_tres: fields[9].to_string(),
            alloc_tres_map,
            req_tres_map,
            max_rss_bytes: parse_memory_bytes(fields[10]),
            tres_usage_in_tot: fields[11].to_string(),
            tres_usage_in_tot_map,
            nnodes: parse_optional_u64(fields[12]),
            account: optional_string(fields[13]),
            qos: optional_string(fields[14]),
            partition: optional_string(fields[15]),
            start: optional_string(fields[16]),
            end: optional_string(fields[17]),
        });
    }
    Ok(rows)
}

fn summarize_accounting_rows(job_id: &str, rows: &[AccountingRow]) -> AccountingSummary {
    let allocation_rows = rows
        .iter()
        .filter(|row| is_allocation_row(job_id, &row.job_id_raw))
        .collect::<Vec<_>>();
    let primary_rows = if allocation_rows.is_empty() {
        rows.iter().collect::<Vec<_>>()
    } else {
        allocation_rows
    };

    let allocated_cpu_seconds = sum_optional_f64(primary_rows.iter().filter_map(|row| {
        match (row.alloc_cpus, row.elapsed_raw_seconds) {
            (Some(cpus), Some(elapsed)) => Some((cpus as f64) * (elapsed as f64)),
            _ => row.cpu_time_raw_seconds.map(|seconds| seconds as f64),
        }
    }));
    let total_cpu_seconds = sum_optional_f64(primary_rows.iter().filter_map(|row| {
        row.cpu_time_raw_seconds
            .or(row.total_cpu_seconds)
            .map(|seconds| seconds as f64)
    }));
    let allocated_gpu_seconds = sum_optional_f64(primary_rows.iter().filter_map(|row| {
        let elapsed = row.elapsed_raw_seconds?;
        let gpus =
            tres_gpu_count(&row.alloc_tres_map).or_else(|| tres_gpu_count(&row.req_tres_map))?;
        Some((gpus as f64) * (elapsed as f64))
    }));
    let mut memory_basis = "allocation_tres".to_string();
    let mut memory_values = primary_rows
        .iter()
        .filter_map(|row| {
            let elapsed = row.elapsed_raw_seconds?;
            let bytes = tres_memory_bytes(&row.alloc_tres_map)?;
            Some((bytes as f64) * (elapsed as f64))
        })
        .collect::<Vec<_>>();
    if memory_values.is_empty() {
        memory_basis = "requested_tres".to_string();
        memory_values = primary_rows
            .iter()
            .filter_map(|row| {
                let elapsed = row.elapsed_raw_seconds?;
                let bytes = tres_memory_bytes(&row.req_tres_map)?;
                Some((bytes as f64) * (elapsed as f64))
            })
            .collect::<Vec<_>>();
    }
    if memory_values.is_empty() {
        memory_basis = "unavailable".to_string();
    }
    let allocated_memory_byte_seconds = sum_optional_f64(memory_values);
    let max_rss_bytes = rows.iter().filter_map(|row| row.max_rss_bytes).max();

    AccountingSummary {
        allocated_cpu_hours: allocated_cpu_seconds.map(seconds_to_hours),
        total_cpu_hours: total_cpu_seconds.map(seconds_to_hours),
        allocated_gpu_hours: allocated_gpu_seconds.map(seconds_to_hours),
        allocated_memory_byte_seconds,
        max_rss_bytes,
        memory_basis,
    }
}

fn is_allocation_row(job_id: &str, row_job_id: &str) -> bool {
    row_job_id == job_id || (!row_job_id.contains('.') && !row_job_id.contains('_'))
}

fn sum_optional_f64(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let mut seen = false;
    let mut total = 0.0;
    for value in values {
        seen = true;
        total += value;
    }
    seen.then_some(total)
}

fn seconds_to_hours(seconds: f64) -> f64 {
    seconds / 3_600.0
}

fn tres_gpu_count(values: &BTreeMap<String, String>) -> Option<u64> {
    find_tres_value(values, "gres/gpu")
        .or_else(|| find_tres_value(values, "gpu"))
        .and_then(|value| parse_optional_u64(&value))
}

fn tres_memory_bytes(values: &BTreeMap<String, String>) -> Option<u64> {
    values
        .get("mem")
        .or_else(|| values.get("memory"))
        .and_then(|value| parse_memory_bytes(value))
}

fn parse_optional_u64(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

fn parse_slurm_accounting_duration(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(seconds);
    }
    let without_fraction = trimmed.split('.').next().unwrap_or(trimmed);
    let (days, time) = match without_fraction.split_once('-') {
        Some((days, time)) => (days.parse::<u64>().ok()?, time),
        None => (0, without_fraction),
    };
    let parts = time
        .split(':')
        .map(|part| part.parse::<u64>().ok())
        .collect::<Option<Vec<_>>>()?;
    let seconds = match parts.as_slice() {
        [minutes, seconds] => minutes.saturating_mul(60).saturating_add(*seconds),
        [hours, minutes, seconds] => hours
            .saturating_mul(3_600)
            .saturating_add(minutes.saturating_mul(60))
            .saturating_add(*seconds),
        _ => return None,
    };
    Some(days.saturating_mul(86_400).saturating_add(seconds))
}

fn parse_memory_bytes(raw: &str) -> Option<u64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("unknown") {
        return None;
    }
    let number_end = trimmed
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_ascii_digit()).then_some(index))
        .unwrap_or(trimmed.len());
    let value = trimmed[..number_end].parse::<u64>().ok()?;
    let unit = trimmed[number_end..].trim().to_ascii_uppercase();
    let multiplier = match unit.as_str() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => 1_024,
        "M" | "MB" | "MIB" => 1_024_u64.pow(2),
        "G" | "GB" | "GIB" => 1_024_u64.pow(3),
        "T" | "TB" | "TIB" => 1_024_u64.pow(4),
        "P" | "PB" | "PIB" => 1_024_u64.pow(5),
        _ => return None,
    };
    Some(value.saturating_mul(multiplier))
}

fn optional_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("unknown")).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sacct_accounting_output_and_rollup() {
        let rows = parse_sacct_accounting_output(
            "\
12345|demo|COMPLETED|0:0|120|4|240|00:04:00|cpu=4,mem=8G,gres/gpu=2|cpu=4,mem=8G|512M|cpu=00:04:00,mem=512M|1|acct|normal|gpu|2026-01-01T00:00:00|2026-01-01T00:02:00
12345.0|app|COMPLETED|0:0|100|4|200|00:03:20|cpu=4,mem=8G,gres/gpu=2|cpu=4,mem=8G|1G|cpu=00:03:20,mem=1G|1|acct|normal|gpu|2026-01-01T00:00:10|2026-01-01T00:01:50
",
        )
        .expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].alloc_tres_map["gres/gpu"], "2");
        assert_eq!(rows[1].max_rss_bytes, Some(1_024_u64.pow(3)));

        let summary = summarize_accounting_rows("12345", &rows);
        assert_eq!(summary.allocated_cpu_hours, Some(480.0 / 3_600.0));
        assert_eq!(summary.allocated_gpu_hours, Some(240.0 / 3_600.0));
        assert_eq!(
            summary.allocated_memory_byte_seconds,
            Some((8.0 * 1_024.0 * 1_024.0 * 1_024.0) * 120.0)
        );
        assert_eq!(summary.max_rss_bytes, Some(1_024_u64.pow(3)));
        assert_eq!(summary.memory_basis, "allocation_tres");
    }
}
