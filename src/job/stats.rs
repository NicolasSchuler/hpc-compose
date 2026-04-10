use super::runtime_state::load_runtime_state;
use super::scheduler::build_local_scheduler_status;
use super::*;

/// Builds the tracked metrics snapshot used by `hpc-compose stats`.
pub fn build_stats_snapshot(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &StatsOptions,
) -> Result<StatsSnapshot> {
    let (job_id, record) = match job_id {
        Some(job_id) => (
            job_id.to_string(),
            load_submission_record(spec_path, Some(job_id)).ok(),
        ),
        None => {
            let record = load_submission_record(spec_path, None)?;
            (record.job_id.clone(), Some(record))
        }
    };
    let runtime_state = record.as_ref().and_then(load_runtime_state);
    let raw_scheduler = match record.as_ref().map(|record| record.backend) {
        Some(SubmissionBackend::Local) => build_local_scheduler_status(runtime_state.as_ref()),
        _ => probe_scheduler_status(&job_id, &options.scheduler),
    };
    let scheduler = if let Some(record) = &record {
        match record.backend {
            SubmissionBackend::Slurm => reconcile_scheduler_status(
                raw_scheduler,
                record.submitted_at,
                None,
                unix_timestamp_now(),
            ),
            SubmissionBackend::Local => raw_scheduler,
        }
    } else {
        raw_scheduler
    };
    let metrics_dir = record.as_ref().map(metrics_dir_for_record);
    let SamplerLoadOutcome { sampler, mut notes } = if let Some(metrics_dir) = metrics_dir.as_ref()
    {
        load_sampler_snapshot(metrics_dir)
    } else {
        SamplerLoadOutcome::default()
    };

    let mut steps = sampler
        .as_ref()
        .and_then(|snapshot| snapshot.slurm.as_ref())
        .map(|snapshot| snapshot.steps.clone())
        .unwrap_or_default();
    let sampler_contributed = sampler.as_ref().is_some_and(|snapshot| {
        snapshot.gpu.is_some()
            || snapshot
                .slurm
                .as_ref()
                .is_some_and(|slurm| !slurm.steps.is_empty())
    });
    let used_sampler_steps = !steps.is_empty();
    let mut used_live_sstat = false;

    if steps.is_empty()
        && record.as_ref().map(|record| record.backend) != Some(SubmissionBackend::Local)
    {
        match probe_step_stats(&job_id, &options.sstat_bin) {
            Ok(probed_steps) => {
                steps = probed_steps;
                used_live_sstat = !steps.is_empty();
            }
            Err(err) if sampler_contributed => {
                notes.push(format!(
                    "live sstat fallback failed while reading sampler-backed stats: {err}"
                ));
            }
            Err(err) => return Err(err),
        }
    }

    let available = !steps.is_empty()
        || sampler
            .as_ref()
            .and_then(|snapshot| snapshot.gpu.as_ref())
            .is_some();
    if record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local) {
        notes.push("Slurm step statistics are unavailable for locally launched jobs".to_string());
    }
    if available
        && sampler
            .as_ref()
            .and_then(|snapshot| snapshot.gpu.as_ref())
            .is_none()
        && !steps.is_empty()
        && steps.iter().all(|step| !step.has_live_gpu_metrics())
    {
        notes.push(
            "GPU accounting metrics are unavailable for this job; this cluster may not expose GPU TRES accounting via sstat".to_string(),
        );
    }
    let source = if record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local) {
        "sampler"
    } else if sampler_contributed && (used_live_sstat || (!used_sampler_steps && !steps.is_empty()))
    {
        "sampler+sstat"
    } else if sampler_contributed {
        "sampler"
    } else {
        "sstat"
    };
    let reason = if !available
        && record.as_ref().map(|record| record.backend) == Some(SubmissionBackend::Local)
    {
        Some("runtime metrics are not available because no local sampler data has been collected yet".to_string())
    } else {
        (!available).then(|| stats_unavailable_reason(&scheduler))
    };

    Ok(StatsSnapshot {
        job_id,
        record,
        metrics_dir,
        scheduler: scheduler.clone(),
        available,
        reason,
        source: source.to_string(),
        notes,
        sampler,
        steps,
        attempt: runtime_state.as_ref().and_then(|state| state.attempt),
        is_resume: runtime_state.as_ref().and_then(|state| state.is_resume),
        resume_dir: runtime_state
            .as_ref()
            .and_then(|state| state.resume_dir.clone()),
    })
}

/// Returns the tracked metrics directory for a submission record.
pub fn metrics_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::latest_metrics_dir(&tracked_paths::runtime_job_root(
        &record.submit_dir,
        &record.job_id,
    ))
}

pub(crate) fn load_sampler_snapshot(metrics_dir: &Path) -> SamplerLoadOutcome {
    if !metrics_dir.is_dir() {
        return SamplerLoadOutcome::default();
    }

    let meta_path = metrics_dir.join("meta.json");
    let meta: SamplerMetaFile = match read_json(&meta_path) {
        Ok(meta) => meta,
        Err(err) => {
            return SamplerLoadOutcome {
                sampler: None,
                notes: vec![format!(
                    "failed to parse metrics sampler metadata at {}: {err}",
                    meta_path.display()
                )],
            };
        }
    };

    let mut notes = meta
        .collectors
        .iter()
        .filter(|collector| collector.enabled)
        .filter_map(|collector| {
            collector
                .note
                .as_ref()
                .map(|note| format!("metrics collector '{}': {note}", collector.name))
        })
        .collect::<Vec<_>>();

    let gpu = if collector_enabled(&meta.collectors, "gpu") {
        match load_gpu_snapshot(metrics_dir) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                notes.push(format!(
                    "failed to parse GPU sampler data under {}: {err}",
                    metrics_dir.display()
                ));
                None
            }
        }
    } else {
        None
    };

    let slurm = if collector_enabled(&meta.collectors, "slurm") {
        match load_slurm_sampler_snapshot(metrics_dir) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                notes.push(format!(
                    "failed to parse Slurm sampler data under {}: {err}",
                    metrics_dir.display()
                ));
                None
            }
        }
    } else {
        None
    };

    SamplerLoadOutcome {
        sampler: Some(SamplerSnapshot {
            interval_seconds: meta.interval_seconds,
            collectors: meta.collectors,
            gpu,
            slurm,
        }),
        notes,
    }
}

fn collector_enabled(collectors: &[CollectorStatus], name: &str) -> bool {
    collectors
        .iter()
        .find(|collector| collector.name == name)
        .is_some_and(|collector| collector.enabled)
}

fn load_gpu_snapshot(metrics_dir: &Path) -> Result<Option<GpuSnapshot>> {
    let gpu_path = metrics_dir.join("gpu.jsonl");
    let Some((sampled_at, devices)) = load_latest_gpu_devices(&gpu_path)? else {
        return Ok(None);
    };
    let processes =
        load_gpu_processes_for_timestamp(&metrics_dir.join("gpu_processes.jsonl"), &sampled_at)?;
    Ok(Some(GpuSnapshot {
        sampled_at,
        gpus: devices,
        processes,
    }))
}

fn load_latest_gpu_devices(path: &Path) -> Result<Option<(String, Vec<GpuDeviceSample>)>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    let mut latest_sampled_at: Option<String> = None;
    let mut devices = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: GpuDeviceSampleRow = serde_json::from_str(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        match latest_sampled_at.as_deref() {
            None => {
                latest_sampled_at = Some(row.sampled_at.clone());
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            Some(current) if row.sampled_at.as_str() > current => {
                latest_sampled_at = Some(row.sampled_at.clone());
                devices.clear();
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            Some(current) if row.sampled_at == current => {
                devices.push(GpuDeviceSample {
                    index: row.index,
                    uuid: row.uuid,
                    name: row.name,
                    utilization_gpu: row.utilization_gpu,
                    utilization_memory: row.utilization_memory,
                    memory_used_mib: row.memory_used_mib,
                    memory_total_mib: row.memory_total_mib,
                    temperature_c: row.temperature_c,
                    power_draw_w: row.power_draw_w,
                    power_limit_w: row.power_limit_w,
                });
            }
            _ => {}
        }
    }

    match latest_sampled_at {
        Some(sampled_at) => Ok(Some((sampled_at, devices))),
        None => Ok(None),
    }
}

fn load_gpu_processes_for_timestamp(
    path: &Path,
    sampled_at: &str,
) -> Result<Vec<GpuProcessSample>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path).context(format!("failed to read {}", path.display()))?;
    let mut processes = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: GpuProcessSampleRow = serde_json::from_str(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        if row.sampled_at != sampled_at {
            continue;
        }
        processes.push(GpuProcessSample {
            gpu_uuid: row.gpu_uuid,
            pid: row.pid,
            process_name: row.process_name,
            used_memory_mib: row.used_memory_mib,
        });
    }

    Ok(processes)
}

fn load_slurm_sampler_snapshot(metrics_dir: &Path) -> Result<Option<SlurmSamplerSnapshot>> {
    let path = metrics_dir.join("slurm.jsonl");
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path).context(format!("failed to read {}", path.display()))?;
    let mut latest_sampled_at: Option<String> = None;
    let mut steps = Vec::new();

    for (index, raw_line) in raw.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let row: SlurmSampleRow = serde_json::from_str(line).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        let sampled_at = row.sampled_at.clone();
        let step = step_from_slurm_sample_row(row).context(format!(
            "failed to parse {} line {}",
            path.display(),
            index + 1
        ))?;
        match latest_sampled_at.as_deref() {
            None => {
                latest_sampled_at = Some(sampled_at);
                steps.push(step);
            }
            Some(current) if sampled_at.as_str() > current => {
                latest_sampled_at = Some(sampled_at);
                steps.clear();
                steps.push(step);
            }
            Some(current) if sampled_at == current => {
                steps.push(step);
            }
            _ => {}
        }
    }

    match latest_sampled_at {
        Some(sampled_at) => Ok(Some(SlurmSamplerSnapshot { sampled_at, steps })),
        None => Ok(None),
    }
}

pub(crate) fn step_from_slurm_sample_row(row: SlurmSampleRow) -> Result<StepStats> {
    let step_id = required_json_string("step_id", row.step_id)?;
    let alloc_tres = row.alloc_tres.unwrap_or_default();
    let tres_usage_in_ave = row.tres_usage_in_ave.unwrap_or_default();
    let alloc_tres_map = parse_tres_map(&alloc_tres)
        .context(format!("failed to parse AllocTRES for step '{step_id}'"))?;
    let usage_tres_in_ave_map = parse_tres_map(&tres_usage_in_ave).context(format!(
        "failed to parse TRESUsageInAve for step '{step_id}'"
    ))?;

    Ok(StepStats {
        step_id,
        ntasks: row.ntasks.unwrap_or_default(),
        ave_cpu: row.ave_cpu.unwrap_or_default(),
        ave_rss: row.ave_rss.unwrap_or_default(),
        max_rss: row.max_rss.unwrap_or_default(),
        alloc_tres: alloc_tres.clone(),
        tres_usage_in_ave: tres_usage_in_ave.clone(),
        gpu_count: find_tres_value(&alloc_tres_map, "gres/gpu"),
        gpu_util: find_tres_value(&usage_tres_in_ave_map, "gres/gpuutil"),
        gpu_mem: find_tres_value(&usage_tres_in_ave_map, "gres/gpumem"),
        alloc_tres_map,
        usage_tres_in_ave_map,
    })
}

fn required_json_string(field: &str, value: Option<String>) -> Result<String> {
    value.context(format!("missing required field '{field}'"))
}

pub(crate) fn probe_step_stats(job_id: &str, binary: &str) -> Result<Vec<StepStats>> {
    let output = Command::new(binary)
        .args([
            "--allsteps",
            "--jobs",
            job_id,
            "--parsable2",
            "--noconvert",
            "--format=JobID,NTasks,AveCPU,AveRSS,MaxRSS,AllocTRES,TRESUsageInAve",
        ])
        .output()
        .context(format!("failed to execute '{binary}'"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let detail = if !stderr.is_empty() { stderr } else { stdout };
        if detail.is_empty() {
            bail!("sstat failed for job {job_id}");
        }
        bail!("sstat failed for job {job_id}: {detail}");
    }

    parse_sstat_output(job_id, &String::from_utf8_lossy(&output.stdout))
}

pub(crate) fn parse_sstat_output(job_id: &str, stdout: &str) -> Result<Vec<StepStats>> {
    let mut steps = Vec::new();

    for (index, raw_line) in stdout.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let fields = line.split('|').map(str::trim).collect::<Vec<_>>();
        if fields
            .first()
            .is_some_and(|field| field.eq_ignore_ascii_case("JobID"))
        {
            continue;
        }
        if fields.len() != 7 {
            bail!(
                "malformed sstat output on line {}: expected 7 fields, found {}",
                index + 1,
                fields.len()
            );
        }

        let step_id = fields[0];
        if !is_numbered_step(job_id, step_id) {
            continue;
        }

        let alloc_tres_map = parse_tres_map(fields[5])
            .context(format!("failed to parse AllocTRES for step '{step_id}'"))?;
        let usage_tres_in_ave_map = parse_tres_map(fields[6]).context(format!(
            "failed to parse TRESUsageInAve for step '{step_id}'"
        ))?;
        steps.push(StepStats {
            step_id: step_id.to_string(),
            ntasks: fields[1].to_string(),
            ave_cpu: fields[2].to_string(),
            ave_rss: fields[3].to_string(),
            max_rss: fields[4].to_string(),
            alloc_tres: fields[5].to_string(),
            tres_usage_in_ave: fields[6].to_string(),
            gpu_count: find_tres_value(&alloc_tres_map, "gres/gpu"),
            gpu_util: find_tres_value(&usage_tres_in_ave_map, "gres/gpuutil"),
            gpu_mem: find_tres_value(&usage_tres_in_ave_map, "gres/gpumem"),
            alloc_tres_map,
            usage_tres_in_ave_map,
        });
    }

    Ok(steps)
}

fn parse_tres_map(raw: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for segment in raw.split(',') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (key, value) = segment
            .split_once('=')
            .context(format!("invalid TRES entry '{segment}'"))?;
        values.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(values)
}

fn find_tres_value(values: &BTreeMap<String, String>, key: &str) -> Option<String> {
    if let Some(value) = values.get(key) {
        return Some(value.clone());
    }
    let prefix = format!("{key}:");
    for (candidate, value) in values {
        if candidate.starts_with(&prefix) {
            return Some(value.clone());
        }
    }
    None
}

fn is_numbered_step(job_id: &str, step_id: &str) -> bool {
    let Some(suffix) = step_id
        .strip_prefix(job_id)
        .and_then(|rest| rest.strip_prefix('.'))
    else {
        return false;
    };
    !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampler_helpers_cover_latest_rows_and_missing_files() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = load_sampler_snapshot(&tmpdir.path().join("missing"));
        assert!(missing.sampler.is_none());
        assert!(missing.notes.is_empty());

        let metrics_dir = tmpdir.path().join("metrics");
        fs::create_dir_all(&metrics_dir).expect("metrics dir");
        fs::write(
            metrics_dir.join("meta.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "interval_seconds": 5,
                "collectors": [
                    {"name": "gpu", "enabled": true, "available": true, "note": "nvidia-smi", "last_sampled_at": "2026-04-10T10:00:00Z"},
                    {"name": "slurm", "enabled": true, "available": true, "note": null, "last_sampled_at": "2026-04-10T10:00:00Z"}
                ]
            }))
            .expect("meta json"),
        )
        .expect("write meta");
        fs::write(
            metrics_dir.join("gpu.jsonl"),
            concat!(
                "\n",
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"index\":\"0\",\"uuid\":\"gpu-old\",\"name\":\"A100\",\"utilization_gpu\":\"10\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"index\":\"0\",\"uuid\":\"gpu-new-0\",\"name\":\"A100\",\"utilization_gpu\":\"80\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"index\":\"1\",\"uuid\":\"gpu-new-1\",\"name\":\"A100\",\"utilization_gpu\":\"75\"}\n"
            ),
        )
        .expect("write gpu");
        fs::write(
            metrics_dir.join("gpu_processes.jsonl"),
            concat!(
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"gpu_uuid\":\"gpu-old\",\"pid\":\"1\",\"process_name\":\"old\",\"used_memory_mib\":\"64\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"gpu_uuid\":\"gpu-new-0\",\"pid\":\"42\",\"process_name\":\"python\",\"used_memory_mib\":\"512\"}\n"
            ),
        )
        .expect("write gpu processes");
        fs::write(
            metrics_dir.join("slurm.jsonl"),
            concat!(
                "\n",
                "{\"sampled_at\":\"2026-04-10T09:59:00Z\",\"step_id\":\"123.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:01\",\"alloc_tres\":\"cpu=1\",\"tres_usage_in_ave\":\"cpu=00:00:01\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"123.0\",\"ntasks\":\"1\",\"ave_cpu\":\"00:00:02\",\"alloc_tres\":\"cpu=1,gres/gpu:tesla=2\",\"tres_usage_in_ave\":\"cpu=00:00:02,gres/gpuutil:tesla=80\"}\n",
                "{\"sampled_at\":\"2026-04-10T10:00:00Z\",\"step_id\":\"123.1\",\"ntasks\":\"2\",\"ave_cpu\":\"00:00:03\",\"alloc_tres\":\"cpu=2\",\"tres_usage_in_ave\":\"cpu=00:00:03\"}\n"
            ),
        )
        .expect("write slurm");

        let loaded = load_sampler_snapshot(&metrics_dir);
        let sampler = loaded.sampler.expect("sampler");
        assert_eq!(sampler.interval_seconds, 5);
        assert!(loaded.notes.iter().any(|note| note.contains("nvidia-smi")));
        let gpu = sampler.gpu.expect("gpu snapshot");
        assert_eq!(gpu.sampled_at, "2026-04-10T10:00:00Z");
        assert_eq!(gpu.gpus.len(), 2);
        assert_eq!(gpu.processes.len(), 1);
        let slurm = sampler.slurm.expect("slurm snapshot");
        assert_eq!(slurm.sampled_at, "2026-04-10T10:00:00Z");
        assert_eq!(slurm.steps.len(), 2);
    }

    #[test]
    fn stats_parser_helpers_cover_error_and_prefix_paths() {
        let mut tres = BTreeMap::new();
        tres.insert("gres/gpu:tesla".to_string(), "2".to_string());
        assert_eq!(find_tres_value(&tres, "gres/gpu").as_deref(), Some("2"));
        assert!(!is_numbered_step("123", "123.batch"));
        assert!(is_numbered_step("123", "123.0"));

        let parsed = parse_tres_map(" , cpu=1 ,, gres/gpumem:tesla=8192M ").expect("tres");
        assert_eq!(parsed.get("cpu").map(String::as_str), Some("1"));
        assert!(
            parse_tres_map("broken-entry")
                .expect_err("invalid tres")
                .to_string()
                .contains("invalid TRES entry")
        );

        let row = SlurmSampleRow {
            sampled_at: "2026-04-10T10:00:00Z".into(),
            step_id: Some("123.0".into()),
            ntasks: None,
            ave_cpu: None,
            ave_rss: None,
            max_rss: None,
            alloc_tres: Some("gres/gpu:tesla=2".into()),
            tres_usage_in_ave: Some("gres/gpumem:tesla=8192M".into()),
        };
        let step = step_from_slurm_sample_row(row).expect("step");
        assert_eq!(step.gpu_count.as_deref(), Some("2"));
        assert_eq!(step.gpu_mem.as_deref(), Some("8192M"));
        assert!(
            step_from_slurm_sample_row(SlurmSampleRow {
                sampled_at: "2026-04-10T10:00:00Z".into(),
                step_id: None,
                ntasks: None,
                ave_cpu: None,
                ave_rss: None,
                max_rss: None,
                alloc_tres: None,
                tres_usage_in_ave: None,
            })
            .expect_err("missing step id")
            .to_string()
            .contains("missing required field 'step_id'")
        );

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let sstat_fail = tmpdir.path().join("fake-sstat.sh");
        fs::write(&sstat_fail, "#!/bin/sh\nexit 1\n").expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&sstat_fail).expect("metadata").permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&sstat_fail, perms).expect("chmod");
        }
        let sstat_err = probe_step_stats("123", sstat_fail.to_string_lossy().as_ref())
            .expect_err("sstat failure");
        assert!(sstat_err.to_string().contains("sstat failed for job 123"));
    }
}
