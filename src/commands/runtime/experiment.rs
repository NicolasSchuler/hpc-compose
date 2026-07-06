//! `hpc-compose experiment show` — read-only "one JSON object per run"
//! aggregator over already-persisted tracked state — plus `experiment tag` /
//! `experiment note`, which annotate the tracked record. The sibling
//! `experiment bundle` command reuses the same aggregate builder and writes its
//! bundle-specific files in `experiment_bundle`.
//!
//! `show` is static-safe: it contacts a scheduler only as much as `status`
//! (squeue plus a terminal-only sacct probe via [`build_status_snapshot`]) and
//! `score` (the same plus the terminal-only sstat efficiency probe via
//! [`build_efficiency_score_report`]) already do. It never submits, cancels,
//! exports, writes a file, or opens a connection. SSH/ControlMaster guidance and
//! per-service tunnel hints are PRINTED strings only.
//!
//! `tag` and `note` contact no scheduler either; they rewrite only the tracked
//! record file (and its latest-pointer duplicate when that pointer already
//! names the job) via [`update_submission_record`].

use hpc_compose::job::{
    ArtifactManifest, EfficiencyScoreReport, JobNote, JobProvenance, StatusSnapshot,
    append_job_note, apply_tag_changes, artifact_manifest_path_for_record,
    update_submission_record,
};

use super::*;

/// One JSON object aggregating the read-only state of a single tracked run.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentShowOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    name: String,
    state: String,
    services: Vec<ExperimentService>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<JobProvenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<ArtifactManifest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    efficiency: Option<EfficiencyScoreReport>,
    /// User-assigned labels on the tracked record (see `experiment tag`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    /// Append-only timestamped observations (see `experiment note`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    notes: Vec<JobNote>,
    next_commands: Vec<String>,
}

/// `experiment tag` result (`--format json`): the record's full tag set after
/// the change.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentTagOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    tags: Vec<String>,
}

/// `experiment note` result (`--format json`): the record's full note list
/// after the append.
#[derive(Debug, Serialize, schemars::JsonSchema)]
pub(crate) struct ExperimentNoteOutput {
    pub(crate) schema_version: u32,
    job_id: String,
    notes: Vec<JobNote>,
}

/// Per-service slice of the aggregate: tracked placement plus a printable tunnel
/// hint when the service exposes a TCP/HTTP readiness port.
#[derive(Debug, Serialize, PartialEq, Eq, schemars::JsonSchema)]
struct ExperimentService {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    nodelist: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tunnel_hint: Option<String>,
}

pub(crate) fn experiment_show(
    context: ResolvedContext,
    job_id: Option<String>,
    format: Option<OutputFormat>,
    pue: f64,
    gpu_tdp_w: f64,
    cpu_watts_per_core: f64,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let output =
        collect_experiment_show_output(&context, &record, pue, gpu_tdp_w, cpu_watts_per_core)?;

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment show output")?
            );
        }
        OutputFormat::Text => print_experiment_show_output(&output),
    }
    Ok(())
}

pub(crate) fn collect_experiment_show_output(
    context: &ResolvedContext,
    record: &SubmissionRecord,
    pue: f64,
    gpu_tdp_w: f64,
    cpu_watts_per_core: f64,
) -> Result<ExperimentShowOutput> {
    let runtime_plan =
        load::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &record.compose_file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )
        .with_context(|| {
            format!(
                "failed to load runtime plan for tracked job {} from {}",
                record.job_id,
                record.compose_file.display()
            )
        })?;

    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    // Same scheduler contact as `status`: squeue + terminal-only sacct.
    let snapshot = build_status_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )
    .context("failed to inspect tracked scheduler state")?;

    // Best-effort efficiency: identical probe to `score`, but a non-terminal job
    // (or a local run) degrades to `None` rather than failing the aggregate.
    let efficiency = build_efficiency_score_report(
        &runtime_plan,
        record,
        &EfficiencyScoreOptions {
            scheduler: scheduler_options,
            sstat_bin: context.binaries.sstat.value.clone(),
            pue,
            gpu_tdp_w,
            cpu_watts_per_core,
        },
    )
    .ok();

    // Pure, side-effect-free manifest read (never export_artifacts).
    let results = read_artifact_manifest(record);

    Ok(build_experiment_show_output(
        record,
        &runtime_plan,
        &snapshot,
        results,
        efficiency,
        context.login_host.as_deref(),
    ))
}

/// Pure read of the persisted artifact manifest for a record. Returns `None`
/// when the manifest is absent or unreadable; never copies, exports, or writes.
fn read_artifact_manifest(record: &SubmissionRecord) -> Option<ArtifactManifest> {
    let manifest_path = artifact_manifest_path_for_record(record);
    let contents = fs::read_to_string(&manifest_path).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Pure assembly of the aggregate from already-fetched reports. Keeps the
/// field-mapping and tunnel-hint logic unit-testable without a scheduler.
fn build_experiment_show_output(
    record: &SubmissionRecord,
    plan: &RuntimePlan,
    snapshot: &StatusSnapshot,
    results: Option<ArtifactManifest>,
    efficiency: Option<EfficiencyScoreReport>,
    login_host: Option<&str>,
) -> ExperimentShowOutput {
    let job_id = record.job_id.as_str();
    // Readiness-derived endpoints (TCP/HTTP only), keyed by service name. The
    // port + placeholder handling mirrors `reach`/`build_submit_endpoints`.
    let endpoints = output::build_submit_endpoints(plan);

    let services = plan
        .ordered_services
        .iter()
        .map(|service| {
            let row = snapshot
                .services
                .iter()
                .find(|row| row.service_name == service.name);
            let nodelist = row.and_then(|row| row.nodelist.clone());
            let status = row.and_then(|row| row.status.clone());
            let tunnel_hint = endpoints
                .iter()
                .find(|endpoint| endpoint.service == service.name)
                .map(|endpoint| {
                    let compute = nodelist
                        .as_deref()
                        .and_then(|nodes| nodes.split(',').next())
                        .unwrap_or("<compute-node>");
                    let login = login_host.unwrap_or("<login-node>");
                    ssh_forward_command(endpoint.port, endpoint.port, compute, login)
                });
            ExperimentService {
                name: service.name.clone(),
                nodelist,
                status,
                tunnel_hint,
            }
        })
        .collect();

    ExperimentShowOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: job_id.to_string(),
        name: plan.name.clone(),
        state: snapshot.scheduler.state.clone(),
        services,
        provenance: record.provenance.clone(),
        results,
        efficiency,
        tags: record.tags.clone(),
        notes: record.notes.clone(),
        next_commands: experiment_next_commands(job_id, output::artifact_export_configured(plan)),
    }
}

/// `experiment tag`: add and/or remove labels on one tracked record. The
/// record file (and, when it names this job, the latest-pointer duplicate) is
/// the only thing rewritten; no scheduler is contacted.
pub(crate) fn experiment_tag(
    context: ResolvedContext,
    tags: Vec<String>,
    remove: Vec<String>,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    if tags.is_empty() && remove.is_empty() {
        bail!("pass at least one tag to add, or --remove <TAG> to remove one");
    }
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let updated = update_submission_record(&record.compose_file, &record.job_id, |record| {
        apply_tag_changes(&mut record.tags, &tags, &remove)
    })?;
    let output = ExperimentTagOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: updated.job_id.clone(),
        tags: updated.tags.clone(),
    };
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment tag output")?
            );
        }
        OutputFormat::Text => {
            let tags = if output.tags.is_empty() {
                "(none)".to_string()
            } else {
                output.tags.join(", ")
            };
            println!("tags for job {}: {tags}", output.job_id);
        }
    }
    Ok(())
}

/// `experiment note`: append one timestamped observation to one tracked
/// record. Same write scope as `experiment tag`; no scheduler is contacted.
pub(crate) fn experiment_note(
    context: ResolvedContext,
    text: String,
    job_id: Option<String>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;
    let updated = update_submission_record(&record.compose_file, &record.job_id, |record| {
        append_job_note(record, &text)
    })?;
    let output = ExperimentNoteOutput {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        job_id: updated.job_id.clone(),
        notes: updated.notes.clone(),
    };
    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output)
                    .context("failed to serialize experiment note output")?
            );
        }
        OutputFormat::Text => {
            println!(
                "note added to job {} ({} note{})",
                output.job_id,
                output.notes.len(),
                if output.notes.len() == 1 { "" } else { "s" }
            );
        }
    }
    Ok(())
}

/// Suggested follow-up reads. References only shipped commands and carries the
/// ControlMaster/ControlPath/ControlPersist multiplexing note so a 2FA/OTP user
/// authenticates once. Never opens a connection.
fn experiment_next_commands(job_id: &str, export_dir_configured: bool) -> Vec<String> {
    let mut commands = vec![
        format!("hpc-compose status --job-id {job_id}"),
        format!("hpc-compose score --job-id {job_id}"),
    ];
    // `artifacts` exports the collected payload into the configured export_dir
    // (the cluster results dir downstream jobs read); `pull` only prints a laptop
    // rsync. Suggest the export step only when an export_dir is configured.
    if export_dir_configured {
        commands.push(format!("hpc-compose artifacts --job-id {job_id}"));
    }
    commands.push(format!(
        "hpc-compose pull --job-id {job_id} --into ./results"
    ));
    commands.push("hpc-compose down".to_string());
    commands.push(format!(
        "ssh {opts} <login-node>  # {note}",
        opts = control_master_opts_str(),
        note = OTP_MULTIPLEX_NOTE,
    ));
    commands
}

fn print_experiment_show_output(output: &ExperimentShowOutput) {
    println!("{}", term::styled_section_header("Experiment"));
    println!("  run:   {} (job {})", output.name, output.job_id);
    println!("  state: {}", output.state);
    if !output.tags.is_empty() {
        println!("  tags:  {}", output.tags.join(", "));
    }
    if !output.services.is_empty() {
        println!();
        println!("Services:");
        for service in &output.services {
            let node = service.nodelist.as_deref().unwrap_or("-");
            let status = service.status.as_deref().unwrap_or("-");
            println!("  {} [{status}] {node}", service.name);
            if let Some(hint) = &service.tunnel_hint {
                println!("    tunnel: {hint}");
            }
        }
    }
    if let Some(provenance) = &output.provenance {
        println!();
        println!("Provenance:");
        println!("  tool: {}", provenance.tool_version);
        if let Some(git) = &provenance.git {
            let dirty = if git.dirty { " (dirty)" } else { "" };
            println!("  git:  {}{dirty}", git.sha);
        }
    }
    if let Some(efficiency) = &output.efficiency {
        println!();
        println!(
            "Efficiency: {}/100 ({})",
            efficiency.score, efficiency.grade
        );
    }
    if let Some(results) = &output.results {
        let files = results.copied_relative_paths.len()
            + results
                .bundles
                .values()
                .map(|bundle| bundle.copied_relative_paths.len())
                .sum::<usize>();
        println!();
        println!("Artifacts: {files} collected file(s)");
    }
    if !output.notes.is_empty() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        println!();
        println!("Notes:");
        for note in &output.notes {
            println!(
                "  [{}] {}",
                output::format_age_seconds(now.saturating_sub(note.created_at)),
                note.text
            );
        }
    }
    println!();
    println!("Next:");
    for command in &output.next_commands {
        println!("  {command}");
    }
    println!();
    println!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use hpc_compose::job::{ArtifactManifest, GitProvenance, JobProvenance};
    use hpc_compose::spec::ComposeSpec;

    use super::*;

    fn plan_with_services(yaml: &str) -> RuntimePlan {
        // Pure plan derivation only: build the normalized plan and map it into a
        // RuntimePlan without importing or preparing any images.
        let dir = tempfile::tempdir().expect("tmpdir");
        let compose = dir.path().join("compose.yaml");
        fs::write(&compose, yaml).expect("write compose");
        let spec = ComposeSpec::load(&compose).expect("spec");
        let plan = hpc_compose::planner::build_plan(&compose, spec).expect("plan");
        hpc_compose::prepare::build_runtime_plan(&plan)
    }

    fn record_for(job_id: &str) -> SubmissionRecord {
        // Minimal tracked record via serde (all optional/additive fields take
        // their defaults) so the test does not enumerate every record field.
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "backend": "slurm",
            "kind": "main",
            "job_id": job_id,
            "submitted_at": 1,
            "compose_file": "/tmp/compose.yaml",
            "submit_dir": "/tmp",
            "script_path": "/tmp/x.sbatch",
            "cache_dir": "/tmp/cache",
            "batch_log": "/tmp/batch.log",
            "service_logs": {},
        }))
        .expect("submission record")
    }

    fn snapshot_for(plan: &RuntimePlan, state: &str) -> StatusSnapshot {
        // A minimal synthetic status snapshot: round-trips JSON so the test does
        // not depend on private field construction. Mirrors what
        // build_status_snapshot would produce for a degraded probe.
        let services = plan
            .ordered_services
            .iter()
            .map(|service| {
                serde_json::json!({
                    "service_name": service.name,
                    "path": "/tmp/log",
                    "present": false,
                    "updated_at": null,
                    "updated_age_seconds": null,
                    "nodelist": "gpu042,gpu043",
                    "status": "running",
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "record": {
                "schema_version": 1,
                "backend": "slurm",
                "kind": "main",
                "job_id": "12345",
                "submitted_at": 1,
                "compose_file": "/tmp/compose.yaml",
                "submit_dir": "/tmp",
                "script_path": "/tmp/x.sbatch",
                "cache_dir": "/tmp/cache",
                "batch_log": "/tmp/batch.log",
                "service_logs": {},
            },
            "scheduler": {
                "state": state,
                "source": "squeue",
                "terminal": false,
                "failed": false,
                "detail": null,
            },
            "log_dir": "/tmp",
            "batch_log": {
                "path": "/tmp/batch.log",
                "present": false,
                "updated_at": null,
                "updated_age_seconds": null,
            },
            "services": services,
            "attempt": null,
            "is_resume": null,
            "resume_dir": null,
        });
        serde_json::from_value(value).expect("status snapshot")
    }

    const TCP_COMPOSE: &str = r#"name: exp-test
x-slurm:
  time: "00:10:00"
services:
  api:
    image: docker://python:3.12
    command: ["true"]
    readiness:
      type: tcp
      port: 8000
  worker:
    image: docker://python:3.12
    command: ["true"]
"#;

    #[test]
    fn assembles_aggregate_with_field_mapping() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "RUNNING");
        let mut image_refs = BTreeMap::new();
        image_refs.insert("api".to_string(), "docker://python:3.12".to_string());
        let mut record = record_for("12345");
        record.provenance = Some(JobProvenance {
            tool_version: "9.9.9".to_string(),
            git: Some(GitProvenance {
                sha: "abc123".to_string(),
                dirty: false,
                branch: Some("main".to_string()),
            }),
            image_refs,
            source_content_hash: None,
        });
        record.tags = vec!["baseline".to_string(), "lr-bug".to_string()];
        record.notes = vec![hpc_compose::job::JobNote {
            text: "diverged after epoch 3".to_string(),
            created_at: 42,
        }];

        let output =
            build_experiment_show_output(&record, &plan, &snapshot, None, None, Some("login01"));

        assert_eq!(output.job_id, "12345");
        assert_eq!(output.name, "exp-test");
        assert_eq!(output.state, "RUNNING");
        assert_eq!(output.services.len(), 2);
        let api = output
            .services
            .iter()
            .find(|service| service.name == "api")
            .expect("api service");
        assert_eq!(api.nodelist.as_deref(), Some("gpu042,gpu043"));
        assert_eq!(api.status.as_deref(), Some("running"));
        // TCP readiness -> a tunnel hint with the first node and login host.
        let hint = api.tunnel_hint.as_deref().expect("tunnel hint");
        assert!(hint.contains("-L 8000:gpu042:8000 login01"), "hint: {hint}");
        assert!(hint.contains("ControlMaster=auto"), "multiplex: {hint}");
        assert!(output.provenance.is_some());
        // Tags and notes on the record surface in the aggregate.
        assert_eq!(output.tags, vec!["baseline", "lr-bug"]);
        assert_eq!(output.notes.len(), 1);
        assert_eq!(output.notes[0].text, "diverged after epoch 3");
        assert_eq!(output.notes[0].created_at, 42);
    }

    #[test]
    fn tunnel_hint_only_for_tcp_or_http_services_and_placeholders_when_unknown() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "RUNNING");
        // No login host and the worker has no readiness port.
        let output =
            build_experiment_show_output(&record_for("12345"), &plan, &snapshot, None, None, None);

        let worker = output
            .services
            .iter()
            .find(|service| service.name == "worker")
            .expect("worker service");
        assert!(worker.tunnel_hint.is_none(), "no readiness port -> no hint");

        let api = output
            .services
            .iter()
            .find(|service| service.name == "api")
            .expect("api service");
        let hint = api.tunnel_hint.as_deref().expect("tunnel hint");
        // Compute node is present in the snapshot; login host degrades.
        assert!(
            hint.contains("-L 8000:gpu042:8000 <login-node>"),
            "hint: {hint}"
        );
    }

    #[test]
    fn degrades_to_none_for_legacy_and_missing_data() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "PENDING");
        let output =
            build_experiment_show_output(&record_for("12345"), &plan, &snapshot, None, None, None);
        assert!(
            output.provenance.is_none(),
            "legacy record -> no provenance"
        );
        assert!(output.results.is_none(), "no manifest -> no results");
        assert!(output.efficiency.is_none(), "non-terminal -> no efficiency");
        assert_eq!(output.state, "PENDING");
    }

    #[test]
    fn results_round_trip_into_aggregate() {
        let plan = plan_with_services(TCP_COMPOSE);
        let snapshot = snapshot_for(&plan, "COMPLETED");
        let manifest: ArtifactManifest = serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "job_id": "12345",
            "collect_policy": "always",
            "collected_at": "2026-01-01T00:00:00Z",
            "job_outcome": "completed",
            "copied_relative_paths": ["a.txt", "b.txt"],
        }))
        .expect("manifest");
        let output = build_experiment_show_output(
            &record_for("12345"),
            &plan,
            &snapshot,
            Some(manifest),
            None,
            None,
        );
        let results = output.results.expect("results");
        assert_eq!(results.copied_relative_paths.len(), 2);
    }

    #[test]
    fn next_commands_reference_only_shipped_commands_with_multiplex_hint() {
        let commands = experiment_next_commands("12345", true);
        // Every entry names a shipped command path (or is the ssh multiplex hint).
        for command in &commands {
            assert!(
                command.starts_with("hpc-compose status")
                    || command.starts_with("hpc-compose score")
                    || command.starts_with("hpc-compose artifacts")
                    || command.starts_with("hpc-compose pull")
                    || command.starts_with("hpc-compose down")
                    || command.starts_with("ssh "),
                "unexpected next_command: {command}"
            );
        }
        // The export step is surfaced only when an export_dir is configured.
        assert!(
            commands
                .iter()
                .any(|command| command == "hpc-compose artifacts --job-id 12345"),
            "artifacts export hint must appear when export_dir is configured: {commands:?}"
        );
        assert!(
            experiment_next_commands("12345", false)
                .iter()
                .all(|command| !command.starts_with("hpc-compose artifacts")),
            "artifacts export hint must be omitted without an export_dir"
        );
        // The ControlMaster/ControlPath/ControlPersist multiplexing line is present.
        let ssh = commands
            .iter()
            .find(|command| command.starts_with("ssh "))
            .expect("ssh multiplex hint");
        assert!(ssh.contains("ControlMaster=auto"), "control master: {ssh}");
        assert!(ssh.contains("ControlPath="), "control path: {ssh}");
        assert!(ssh.contains("ControlPersist="), "control persist: {ssh}");
    }
}
