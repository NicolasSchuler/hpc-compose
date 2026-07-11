//! Experiment bundle writer for tracked runs.
//!
//! The writer is deliberately local and evidence-preserving: it reads already
//! persisted tracking, provenance, checkpoint, and artifact files, writes a
//! self-contained bundle directory, and never contacts or mutates scheduler
//! state.

use std::collections::BTreeSet;

use super::artifacts::{
    copy_path_recursive_within, resolve_selected_bundles, validate_manifest_relative_path,
    validate_payload_source,
};
use super::*;

const EXPERIMENT_BUNDLE_SCHEMA_VERSION: u32 = 1;

/// Options controlling `hpc-compose experiment bundle` materialization.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct ExperimentBundleOptions {
    pub into_dir: PathBuf,
    pub tarball: bool,
    pub include_artifacts: bool,
    pub selected_bundles: Vec<String>,
}

impl Default for ExperimentBundleOptions {
    fn default() -> Self {
        Self {
            into_dir: PathBuf::from("."),
            tarball: false,
            include_artifacts: false,
            selected_bundles: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BundleEvidenceAvailability {
    config: bool,
    script: bool,
    provenance: bool,
    run_evidence: bool,
}

/// Authoritative manifest written at the root of an experiment bundle.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ExperimentBundleManifest {
    pub schema_version: u32,
    pub job_id: String,
    pub created_at_unix: u64,
    pub bundle_root: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tarball_path: Option<PathBuf>,
    pub artifact_payload_included: bool,
    #[serde(default)]
    pub selected_bundles: Vec<String>,
    pub files: Vec<ExperimentBundleFileEntry>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// One file materialized inside an experiment bundle.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ExperimentBundleFileEntry {
    pub relative_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Writes a deterministic experiment bundle directory for one tracked record.
///
/// The caller supplies the already-collected `experiment show` object so command
/// orchestration can preserve the existing aggregation semantics while this
/// module stays focused on filesystem layout and metadata integrity.
pub fn write_experiment_bundle<T: Serialize>(
    record: &SubmissionRecord,
    experiment_show: &T,
    checkpoint_history: &CheckpointHistory,
    options: &ExperimentBundleOptions,
) -> Result<ExperimentBundleManifest> {
    let into_dir = absolute_path(&options.into_dir)?;
    fs::create_dir_all(&into_dir).context(format!("failed to create {}", into_dir.display()))?;

    let dir_name = bundle_dir_name(&record.job_id)?;
    let bundle_root = into_dir.join(&dir_name);
    if bundle_root.exists() {
        bail!(
            "experiment bundle directory already exists: {}; choose a different --into directory or remove it first",
            bundle_root.display()
        );
    }
    let tarball_path = options
        .tarball
        .then(|| into_dir.join(format!("{dir_name}.tar.gz")));
    if let Some(path) = &tarball_path
        && path.exists()
    {
        bail!(
            "experiment bundle tarball already exists: {}; choose a different --into directory or remove it first",
            path.display()
        );
    }

    let staging_dir = StagingBundleDir::create(&into_dir, &dir_name)?;
    let staging_root = staging_dir.path().to_path_buf();

    let mut warnings = Vec::new();
    write_json_pretty(
        &staging_root.join("run").join("experiment-show.json"),
        experiment_show,
    )?;
    write_json_pretty(
        &staging_root.join("run").join("submission-record.json"),
        record,
    )?;
    let config_written = write_effective_config(record, &staging_root, &mut warnings)?;
    let script_written = write_submitted_script(record, &staging_root, &mut warnings)?;
    write_json_pretty(
        &staging_root.join("run").join("checkpoint-history.json"),
        checkpoint_history,
    )?;
    for degraded in &checkpoint_history.degraded {
        warnings.push(format!("checkpoint history degraded: {degraded}"));
    }
    let provenance_written = write_provenance(record, &staging_root, &mut warnings)?;
    let run_evidence_written = write_run_evidence(record, &staging_root, &mut warnings)?;
    let evidence_availability = BundleEvidenceAvailability {
        config: config_written,
        script: script_written,
        provenance: provenance_written,
        run_evidence: run_evidence_written,
    };
    let selected_bundles = write_artifact_section(record, &staging_root, options, &mut warnings)?;

    write_text(
        &staging_root.join("README.md"),
        &bundle_readme(
            record,
            options.include_artifacts,
            &selected_bundles,
            &evidence_availability,
            &warnings,
        ),
    )?;
    write_text(
        &staging_root.join("methods.md"),
        &bundle_methods(
            record,
            options.include_artifacts,
            &selected_bundles,
            &evidence_availability,
            &warnings,
        ),
    )?;

    let mut files = vec![ExperimentBundleFileEntry {
        relative_path: "manifest.json".to_string(),
        size_bytes: None,
        sha256: None,
    }];
    files.extend(collect_bundle_file_entries(&staging_root)?);
    let artifact_payload_included = files
        .iter()
        .any(|entry| entry.relative_path.starts_with("artifacts/payload/"));

    let manifest = ExperimentBundleManifest {
        schema_version: EXPERIMENT_BUNDLE_SCHEMA_VERSION,
        job_id: record.job_id.clone(),
        created_at_unix: crate::time_util::unix_timestamp_now(),
        bundle_root: bundle_root.clone(),
        tarball_path: tarball_path.clone(),
        artifact_payload_included,
        selected_bundles,
        files,
        warnings,
    };
    write_json_pretty(&staging_root.join("manifest.json"), &manifest)?;

    staging_dir.publish(&bundle_root)?;

    if let Some(path) = &tarball_path {
        write_bundle_tarball(path, &bundle_root, &dir_name)?;
    }

    Ok(manifest)
}

fn bundle_dir_name(job_id: &str) -> Result<String> {
    let path = Path::new(job_id);
    let mut components = path.components();
    let Some(std::path::Component::Normal(_)) = components.next() else {
        bail!("tracked job id '{job_id}' cannot be used as a bundle directory name");
    };
    if components.next().is_some() || job_id.contains('\0') {
        bail!("tracked job id '{job_id}' cannot be used as a bundle directory name");
    }
    Ok(format!("hpc-compose-bundle-{job_id}"))
}

struct StagingBundleDir {
    path: PathBuf,
    active: bool,
}

impl StagingBundleDir {
    fn create(into_dir: &Path, dir_name: &str) -> Result<Self> {
        for attempt in 0..100_u32 {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            let path = into_dir.join(format!(
                ".{dir_name}.{}.{}.{}.tmp",
                std::process::id(),
                nanos,
                attempt
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path, active: true }),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(err).context(format!(
                        "failed to create temporary bundle directory under {}",
                        into_dir.display()
                    ));
                }
            }
        }
        bail!(
            "failed to allocate a unique temporary bundle directory under {}",
            into_dir.display()
        )
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, bundle_root: &Path) -> Result<()> {
        fs::rename(&self.path, bundle_root).with_context(|| {
            format!(
                "failed to publish experiment bundle {}",
                bundle_root.display()
            )
        })?;
        self.active = false;
        Ok(())
    }
}

impl Drop for StagingBundleDir {
    fn drop(&mut self) {
        if self.active {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn write_effective_config(
    record: &SubmissionRecord,
    bundle_root: &Path,
    warnings: &mut Vec<String>,
) -> Result<bool> {
    let Some(snapshot) = record.config_snapshot_yaml.as_deref() else {
        warnings.push(format!(
            "tracked job {} has no submit-time effective config snapshot; run/effective-config.yaml was omitted",
            record.job_id
        ));
        return Ok(false);
    };
    write_text(
        &bundle_root.join("run").join("effective-config.yaml"),
        snapshot,
    )?;
    Ok(true)
}

fn write_submitted_script(
    record: &SubmissionRecord,
    bundle_root: &Path,
    warnings: &mut Vec<String>,
) -> Result<bool> {
    match fs::read(&record.script_path) {
        Ok(bytes) => {
            write_bytes(&bundle_root.join("run").join("submitted.sbatch"), &bytes)?;
            Ok(true)
        }
        Err(err) => {
            warnings.push(format!(
                "submitted script was not readable at {}: {err}; run/submitted.sbatch was omitted",
                record.script_path.display()
            ));
            Ok(false)
        }
    }
}

fn write_provenance(
    record: &SubmissionRecord,
    bundle_root: &Path,
    warnings: &mut Vec<String>,
) -> Result<bool> {
    let Some(provenance) = record.provenance.as_ref() else {
        warnings.push(format!(
            "tracked job {} has no submit-time provenance; provenance/provenance.json was omitted",
            record.job_id
        ));
        return Ok(false);
    };
    write_json_pretty(
        &bundle_root.join("provenance").join("provenance.json"),
        provenance,
    )?;
    Ok(true)
}

fn write_run_evidence(
    record: &SubmissionRecord,
    staging_root: &Path,
    warnings: &mut Vec<String>,
) -> Result<bool> {
    let record_identity = super::record::submission_record_identity_sha256(record)?;
    let files = match super::evidence::export_run_evidence_files(
        &record.compose_file,
        &record.job_id,
        &record_identity,
    ) {
        Ok(Some(files)) => files,
        Ok(None) => {
            warnings.push(
                "no run evidence is available for this tracked record; run/evidence was omitted"
                    .to_string(),
            );
            return Ok(false);
        }
        Err(error) => {
            warnings.push(format!(
                "run evidence for job {} was invalid or unreadable and was omitted: {error:#}",
                record.job_id
            ));
            return Ok(false);
        }
    };

    let evidence_root = staging_root.join("run").join("evidence");
    write_bytes(&evidence_root.join("manifest.json"), &files.manifest)?;
    write_bytes(&evidence_root.join("inputs.lock.json"), &files.inputs_lock)?;
    write_bytes(&evidence_root.join("events.jsonl"), &files.events)?;
    write_bytes(&evidence_root.join("view.json"), &files.view)?;
    Ok(true)
}

fn write_artifact_section(
    record: &SubmissionRecord,
    bundle_root: &Path,
    options: &ExperimentBundleOptions,
    warnings: &mut Vec<String>,
) -> Result<Vec<String>> {
    let manifest_path = artifact_manifest_path_for_record(record);
    let raw_manifest = match fs::read(&manifest_path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            warnings.push(format!(
                "artifact manifest was not found at {}; artifacts/manifest.json was omitted",
                manifest_path.display()
            ));
            if options.include_artifacts {
                warnings.push(
                    "artifact payload was requested, but no artifact manifest was available"
                        .to_string(),
                );
            }
            return Ok(Vec::new());
        }
        Err(err) => {
            warnings.push(format!(
                "artifact manifest was not readable at {}: {err}; artifacts/manifest.json was omitted",
                manifest_path.display()
            ));
            if options.include_artifacts {
                warnings.push(
                    "artifact payload was requested, but the artifact manifest was unreadable"
                        .to_string(),
                );
            }
            return Ok(Vec::new());
        }
    };
    write_bytes(
        &bundle_root.join("artifacts").join("manifest.json"),
        &raw_manifest,
    )?;

    let manifest: ArtifactManifest = match serde_json::from_slice(&raw_manifest) {
        Ok(manifest) => manifest,
        Err(err) => {
            warnings.push(format!(
                "artifact manifest at {} could not be parsed: {err}; artifact payload was not copied",
                manifest_path.display()
            ));
            return Ok(Vec::new());
        }
    };
    if manifest.schema_version > ARTIFACT_MANIFEST_SCHEMA_VERSION {
        warnings.push(format!(
            "artifact manifest schema version {} is newer than this hpc-compose build supports; artifact payload was not copied",
            manifest.schema_version
        ));
        return Ok(Vec::new());
    }
    if manifest.job_id != record.job_id {
        warnings.push(format!(
            "artifact manifest job id {} does not match tracked job {}; artifact payload was not copied",
            manifest.job_id, record.job_id
        ));
        return Ok(Vec::new());
    }
    for warning in &manifest.warnings {
        warnings.push(format!("artifact manifest warning: {warning}"));
    }

    if !options.include_artifacts {
        if !options.selected_bundles.is_empty() {
            warnings.push(
                "--bundle filters were supplied without --include-artifacts; artifact payload filters were ignored"
                    .to_string(),
            );
        }
        return Ok(Vec::new());
    }

    let bundles = manifest.normalized_bundles();
    let selected_bundles = resolve_selected_bundles(&bundles, &options.selected_bundles)?;
    let payload_dir = artifact_payload_dir_for_record(record);
    if !payload_dir.is_dir() {
        warnings.push(format!(
            "artifact payload directory was not found at {}; artifact payload was not copied",
            payload_dir.display()
        ));
        return Ok(selected_bundles);
    }

    let payload_dest = bundle_root.join("artifacts").join("payload");
    fs::create_dir_all(&payload_dest)
        .context(format!("failed to create {}", payload_dest.display()))?;
    let mut copied = BTreeSet::new();
    for bundle_name in &selected_bundles {
        let bundle = bundles
            .get(bundle_name)
            .with_context(|| format!("artifact bundle '{bundle_name}' is not available"))?;
        for warning in &bundle.warnings {
            warnings.push(format!(
                "artifact bundle '{bundle_name}' warning: {warning}"
            ));
        }
        for relative in &bundle.copied_relative_paths {
            let relative = validate_manifest_relative_path(relative)?;
            if !copied.insert(relative.clone()) {
                continue;
            }
            let source = payload_dir.join(&relative);
            if let Err(err) = fs::symlink_metadata(&source) {
                warnings.push(format!(
                    "artifact payload path {} was not readable: {err}; it was omitted",
                    source.display()
                ));
                continue;
            }
            validate_payload_source(&payload_dir, &source)?;
            copy_path_recursive_within(&source, &payload_dest.join(&relative), &payload_dest)?;
        }
    }

    Ok(selected_bundles)
}

fn bundle_readme(
    record: &SubmissionRecord,
    include_artifacts: bool,
    selected_bundles: &[String],
    evidence_availability: &BundleEvidenceAvailability,
    warnings: &[String],
) -> String {
    let artifact_payload = if include_artifacts {
        if selected_bundles.is_empty() {
            "requested; no payload bundle was copied".to_string()
        } else {
            format!(
                "requested for bundle(s): {}; see manifest entries and warnings for copied files",
                selected_bundles.join(", ")
            )
        }
    } else {
        "not included; only artifact metadata is present".to_string()
    };
    let run_evidence = if evidence_availability.run_evidence {
        "included as a validated snapshot under `run/evidence/`"
    } else {
        "not included; see bundle warnings"
    };
    let warnings_block = if warnings.is_empty() {
        "No bundle warnings were recorded.\n".to_string()
    } else {
        warnings
            .iter()
            .map(|warning| format!("- {warning}\n"))
            .collect::<String>()
    };

    format!(
        "# hpc-compose experiment bundle: job {job_id}\n\n\
This directory contains a local evidence bundle for one tracked hpc-compose run.\n\
It does not copy the current compose file as submit-time source. Prefer the\n\
submit-time effective config snapshot, submitted batch script, and persisted\n\
provenance/source hash when they are present.\n\n\
## Contents\n\n\
- `manifest.json`: authoritative file list, hashes, options, and warnings.\n\
- `run/experiment-show.json`: bundle-time `experiment show` aggregate.\n\
- `run/submission-record.json`: tracked submission metadata.\n\
- `run/effective-config.yaml`: submit-time effective config, when recorded.\n\
- `run/submitted.sbatch`: submitted batch script, when readable.\n\
- `run/checkpoint-history.json`: local attempt/requeue history.\n\
- `run/evidence/`: validated manifest, input lock, events, and rebuilt view, when available.\n\
- `provenance/provenance.json`: submit-time provenance, when recorded.\n\
- `artifacts/manifest.json`: tracked artifact manifest, when present.\n\
- `artifacts/payload/`: copied artifact payload, when requested.\n\n\
Artifact payload: {artifact_payload}.\n\n\
Run evidence: {run_evidence}.\n\n\
## Warnings\n\n\
{warnings_block}",
        job_id = record.job_id
    )
}

fn bundle_methods(
    record: &SubmissionRecord,
    include_artifacts: bool,
    selected_bundles: &[String],
    evidence_availability: &BundleEvidenceAvailability,
    warnings: &[String],
) -> String {
    let config_source = if evidence_availability.config {
        "`run/effective-config.yaml` is the submit-time effective config snapshot."
    } else {
        "No submit-time effective config snapshot was available."
    };
    let script_source = if evidence_availability.script {
        "`run/submitted.sbatch` was copied from the persisted script path."
    } else {
        "The persisted submitted script path was unavailable or unreadable."
    };
    let provenance_source = if evidence_availability.provenance {
        "`provenance/provenance.json` contains the submit-time tool/git/image provenance."
    } else {
        "No submit-time provenance was recorded in the tracked record."
    };
    let run_evidence_source = if evidence_availability.run_evidence {
        "`run/evidence/` is a validated snapshot of the immutable manifest and input lock, the append-only event stream, and a view rebuilt from those events."
    } else {
        "No validated additive run-evidence snapshot was included; see bundle warnings."
    };
    let artifact_source = if include_artifacts {
        if selected_bundles.is_empty() {
            "Artifact payload copy was requested, but no payload bundle was copied.".to_string()
        } else {
            format!(
                "Artifact payload copy was requested for bundle(s): {}; see `manifest.json` for the files that were actually materialized.",
                selected_bundles.join(", ")
            )
        }
    } else {
        "Artifact payload was not copied; artifact metadata is included when present.".to_string()
    };
    let warnings_block = if warnings.is_empty() {
        "None.\n".to_string()
    } else {
        warnings
            .iter()
            .map(|warning| format!("- {warning}\n"))
            .collect::<String>()
    };

    format!(
        "# Methods and Provenance\n\n\
Job id: `{job_id}`\n\n\
Submitted at Unix time: `{submitted_at}`\n\n\
Tracked compose path at submission time: `{compose_file}`\n\n\
This bundle was produced from persisted hpc-compose tracking metadata. The\n\
current compose file was not copied as submit-time source.\n\n\
{config_source}\n\n\
{script_source}\n\n\
{provenance_source}\n\n\
{run_evidence_source}\n\n\
{artifact_source}\n\n\
Checkpoint history was reconstructed from local tracked state only; it does not\n\
query Slurm or cluster storage.\n\n\
## Warnings\n\n\
{warnings_block}",
        job_id = record.job_id,
        submitted_at = record.submitted_at,
        compose_file = record.compose_file.display(),
    )
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize bundle JSON")?;
    write_bytes(path, &bytes)
}

fn write_text(path: &Path, text: &str) -> Result<()> {
    write_bytes(path, text.as_bytes())
}

fn write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    crate::secure_io::write_atomic(path, bytes, true)
        .context(format!("failed to write {}", path.display()))
}

fn collect_bundle_file_entries(bundle_root: &Path) -> Result<Vec<ExperimentBundleFileEntry>> {
    let mut entries = Vec::new();
    collect_bundle_file_entries_inner(bundle_root, bundle_root, &mut entries)?;
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(entries)
}

fn collect_bundle_file_entries_inner(
    bundle_root: &Path,
    path: &Path,
    entries: &mut Vec<ExperimentBundleFileEntry>,
) -> Result<()> {
    let mut dir_entries = fs::read_dir(path)
        .context(format!("failed to read {}", path.display()))?
        .collect::<io::Result<Vec<_>>>()
        .context(format!("failed to read {}", path.display()))?;
    dir_entries.sort_by_key(|entry| entry.file_name());
    for entry in dir_entries {
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path).context(format!(
            "failed to read metadata for {}",
            entry_path.display()
        ))?;
        if metadata.is_dir() {
            collect_bundle_file_entries_inner(bundle_root, &entry_path, entries)?;
            continue;
        }
        let relative_path = entry_path
            .strip_prefix(bundle_root)
            .unwrap_or(&entry_path)
            .to_string_lossy()
            .to_string();
        if relative_path == "manifest.json" {
            continue;
        }
        let (size_bytes, sha256) = if metadata.is_file() {
            (Some(metadata.len()), Some(hash_bundle_file(&entry_path)?))
        } else {
            (None, None)
        };
        entries.push(ExperimentBundleFileEntry {
            relative_path,
            size_bytes,
            sha256,
        });
    }
    Ok(())
}

fn hash_bundle_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).context(format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .context(format!("failed to read {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn write_bundle_tarball(tarball_path: &Path, bundle_root: &Path, dir_name: &str) -> Result<()> {
    if let Some(parent) = tarball_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let file = File::create(tarball_path)
        .context(format!("failed to create {}", tarball_path.display()))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);
    let tar_root = Path::new(dir_name);
    builder.append_dir(tar_root, bundle_root).context(format!(
        "failed to append {} to tarball",
        bundle_root.display()
    ))?;
    append_dir_contents_to_tar(&mut builder, bundle_root, tar_root)?;
    builder
        .finish()
        .context(format!("failed to finalize {}", tarball_path.display()))?;
    let encoder = builder
        .into_inner()
        .context(format!("failed to finalize {}", tarball_path.display()))?;
    encoder
        .finish()
        .context(format!("failed to finalize {}", tarball_path.display()))?;
    Ok(())
}

fn append_dir_contents_to_tar<W: Write>(
    builder: &mut Builder<W>,
    source_dir: &Path,
    archive_dir: &Path,
) -> Result<()> {
    let mut entries = fs::read_dir(source_dir)
        .context(format!("failed to read {}", source_dir.display()))?
        .collect::<io::Result<Vec<_>>>()
        .context(format!("failed to read {}", source_dir.display()))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let source = entry.path();
        let archive_path = archive_dir.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source)
            .context(format!("failed to read metadata for {}", source.display()))?;
        if metadata.is_dir() {
            builder
                .append_dir(&archive_path, &source)
                .context(format!("failed to append {} to tarball", source.display()))?;
            append_dir_contents_to_tar(builder, &source, &archive_path)?;
        } else {
            builder
                .append_path_with_name(&source, &archive_path)
                .context(format!("failed to append {} to tarball", source.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use flate2::read::GzDecoder;
    use tar::Archive;

    use super::*;

    fn record_for(tmpdir: &Path, job_id: &str) -> SubmissionRecord {
        let compose = tmpdir.join("compose.yaml");
        let script = tmpdir.join("submitted.sbatch");
        fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
        fs::write(&script, "#!/bin/sh\nsrun true\n").expect("script");
        SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: job_id.to_string(),
            submitted_at: 42,
            compose_file: compose,
            submit_dir: tmpdir.to_path_buf(),
            script_path: script,
            cache_dir: tmpdir.join("cache"),
            runtime_root: None,
            batch_log: tmpdir.join("batch.log"),
            batch_log_managed: true,
            service_logs: BTreeMap::new(),
            artifact_export_dir: None,
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            slurm_array: None,
            sweep: None,
            config_snapshot_yaml: Some("name: snap\nservices: {}\n".to_string()),
            cached_artifacts: Vec::new(),
            provenance: Some(JobProvenance {
                tool_version: "9.9.9".to_string(),
                git: None,
                image_refs: BTreeMap::new(),
                source_content_hash: Some("source-sha".to_string()),
            }),
            tags: Vec::new(),
            notes: Vec::new(),
        }
    }

    fn checkpoint_history(record: &SubmissionRecord) -> CheckpointHistory {
        CheckpointHistory {
            job_id: record.job_id.clone(),
            compose_file: record.compose_file.clone(),
            submitted_at: record.submitted_at,
            resume_configured: false,
            attempts: 1,
            requeues: 0,
            current_attempt: None,
            is_resume: None,
            resume_dir: None,
            entries: Vec::new(),
            degraded: Vec::new(),
        }
    }

    fn show_json(job_id: &str) -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "job_id": job_id,
            "name": "snap",
            "state": "COMPLETED",
            "services": [],
            "next_commands": []
        })
    }

    fn artifact_manifest(job_id: &str) -> ArtifactManifest {
        ArtifactManifest {
            schema_version: ARTIFACT_MANIFEST_SCHEMA_VERSION,
            job_id: job_id.to_string(),
            collect_policy: "always".to_string(),
            collected_at: "2026-07-06T00:00:00Z".to_string(),
            job_outcome: "completed".to_string(),
            attempt: None,
            is_resume: None,
            resume_dir: None,
            declared_source_patterns: Vec::new(),
            matched_source_paths: Vec::new(),
            copied_relative_paths: Vec::new(),
            warnings: Vec::new(),
            bundles: BTreeMap::new(),
        }
    }

    fn write_artifacts(record: &SubmissionRecord, manifest: &ArtifactManifest) {
        let manifest_path = artifact_manifest_path_for_record(record);
        let payload_dir = artifact_payload_dir_for_record(record);
        fs::create_dir_all(&payload_dir).expect("payload dir");
        write_json(&manifest_path, manifest).expect("artifact manifest");
    }

    fn manifest_file<'a>(
        manifest: &'a ExperimentBundleManifest,
        path: &str,
    ) -> &'a ExperimentBundleFileEntry {
        manifest
            .files
            .iter()
            .find(|entry| entry.relative_path == path)
            .unwrap_or_else(|| panic!("missing manifest entry for {path}: {manifest:?}"))
    }

    #[test]
    fn writes_complete_bundle_metadata() {
        let tmp = tempfile::tempdir().expect("tmp");
        let record = record_for(tmp.path(), "12345");
        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");

        let root = tmp.path().join("out").join("hpc-compose-bundle-12345");
        assert_eq!(manifest.bundle_root, root);
        assert!(root.join("manifest.json").exists());
        assert!(root.join("README.md").exists());
        assert!(root.join("methods.md").exists());
        assert!(root.join("run/experiment-show.json").exists());
        assert!(root.join("run/submission-record.json").exists());
        assert!(root.join("run/effective-config.yaml").exists());
        assert!(root.join("run/submitted.sbatch").exists());
        assert!(root.join("run/checkpoint-history.json").exists());
        assert!(root.join("provenance/provenance.json").exists());
        assert_eq!(manifest_file(&manifest, "manifest.json").sha256, None);
        let show = manifest_file(&manifest, "run/experiment-show.json");
        assert!(show.size_bytes.unwrap_or(0) > 0);
        assert_eq!(show.sha256.as_deref().map(str::len), Some(64));
    }

    #[test]
    fn exports_validated_run_evidence_without_breaking_the_input_lock_digest() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut record = record_for(tmp.path(), "12345");
        record.batch_log_managed = false;
        write_submission_record(&record).expect("track submission");
        let record = update_submission_record(&record.compose_file, &record.job_id, |record| {
            apply_tag_changes(&mut record.tags, &["baseline".to_string()], &[])?;
            append_job_note(record, "stable loss")
        })
        .expect("annotate submission");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");

        let evidence_root = manifest.bundle_root.join("run/evidence");
        for relative in [
            "manifest.json",
            "inputs.lock.json",
            "events.jsonl",
            "view.json",
        ] {
            assert!(
                evidence_root.join(relative).is_file(),
                "bundle must include validated {relative}"
            );
            manifest_file(&manifest, &format!("run/evidence/{relative}"));
        }

        let run_manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(evidence_root.join("manifest.json")).expect("run manifest"),
        )
        .expect("parse run manifest");
        let inputs_lock = fs::read(evidence_root.join("inputs.lock.json")).expect("inputs lock");
        assert_eq!(
            run_manifest["inputs_lock_sha256"].as_str(),
            Some(hex::encode(Sha256::digest(&inputs_lock)).as_str()),
            "the bundle must preserve the exact immutable input-lock bytes"
        );

        let view: serde_json::Value =
            serde_json::from_slice(&fs::read(evidence_root.join("view.json")).expect("run view"))
                .expect("parse run view");
        assert_eq!(view["tags"], serde_json::json!(["baseline"]));
        assert_eq!(view["notes"][0]["text"], "stable loss");
    }

    #[test]
    fn stale_record_cannot_export_evidence_from_a_reused_scheduler_job_id() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut stale = record_for(tmp.path(), "12345");
        stale.batch_log_managed = false;
        write_submission_record(&stale).expect("old tracked submission");
        remove_submission_record(&stale).expect("remove old tracked submission");

        let mut replacement = stale.clone();
        replacement.submitted_at = stale.submitted_at + 1;
        replacement.config_snapshot_yaml = Some("name: replacement\nservices: {}\n".to_string());
        write_submission_record(&replacement).expect("reused scheduler id");

        let manifest = write_experiment_bundle(
            &stale,
            &show_json(&stale.job_id),
            &checkpoint_history(&stale),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("legacy bundle remains available");

        assert!(
            !manifest.bundle_root.join("run/evidence").exists(),
            "evidence belonging to the replacement run must not be packaged"
        );
        assert!(
            manifest.warnings.iter().any(|warning| {
                warning.contains("run evidence")
                    && warning.contains("submission record identity")
                    && warning.contains("omitted")
            }),
            "warnings: {:?}",
            manifest.warnings
        );
    }

    #[test]
    fn warns_for_legacy_missing_provenance_and_config() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut record = record_for(tmp.path(), "12345");
        record.config_snapshot_yaml = None;
        record.provenance = None;
        record.script_path = tmp.path().join("missing.sbatch");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");

        let warnings = manifest.warnings.join("\n");
        assert!(warnings.contains("no submit-time effective config snapshot"));
        assert!(warnings.contains("submitted script was not readable"));
        assert!(warnings.contains("no submit-time provenance"));
        assert!(warnings.contains("no run evidence"));
        let root = tmp.path().join("out").join("hpc-compose-bundle-12345");
        assert!(!root.join("run/effective-config.yaml").exists());
        assert!(!root.join("run/submitted.sbatch").exists());
        assert!(!root.join("provenance/provenance.json").exists());
    }

    #[test]
    fn corrupt_run_evidence_is_omitted_with_a_warning() {
        let tmp = tempfile::tempdir().expect("tmp");
        let mut record = record_for(tmp.path(), "12345");
        record.batch_log_managed = false;
        write_submission_record(&record).expect("track submission");
        let evidence_root =
            crate::tracked_paths::run_evidence_dir_for(&record.compose_file, &record.job_id);
        fs::write(evidence_root.join("inputs.lock.json"), b"{}\n").expect("corrupt input lock");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle should preserve legacy behavior");

        assert!(
            manifest
                .warnings
                .iter()
                .any(|warning| warning.contains("run evidence") && warning.contains("omitted")),
            "warnings: {:?}",
            manifest.warnings
        );
        assert!(
            !manifest.bundle_root.join("run/evidence").exists(),
            "invalid evidence must fail closed instead of being copied"
        );
    }

    #[test]
    fn artifact_payload_is_excluded_by_default() {
        let tmp = tempfile::tempdir().expect("tmp");
        let record = record_for(tmp.path(), "12345");
        let mut artifacts = artifact_manifest(&record.job_id);
        artifacts.copied_relative_paths = vec!["metrics/out.json".to_string()];
        write_artifacts(&record, &artifacts);
        fs::create_dir_all(artifact_payload_dir_for_record(&record).join("metrics"))
            .expect("metrics dir");
        fs::write(
            artifact_payload_dir_for_record(&record).join("metrics/out.json"),
            "{}\n",
        )
        .expect("payload");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");
        let root = tmp.path().join("out").join("hpc-compose-bundle-12345");
        assert!(root.join("artifacts/manifest.json").exists());
        assert!(!root.join("artifacts/payload/metrics/out.json").exists());
        assert!(!manifest.artifact_payload_included);
        assert!(manifest.selected_bundles.is_empty());
    }

    #[test]
    fn selected_bundle_payload_is_copied() {
        let tmp = tempfile::tempdir().expect("tmp");
        let record = record_for(tmp.path(), "12345");
        let mut artifacts = artifact_manifest(&record.job_id);
        artifacts.bundles = BTreeMap::from([
            (
                "metrics".to_string(),
                ArtifactBundleManifest {
                    copied_relative_paths: vec!["metrics/out.json".to_string()],
                    ..ArtifactBundleManifest::default()
                },
            ),
            (
                "logs".to_string(),
                ArtifactBundleManifest {
                    copied_relative_paths: vec!["logs/app.log".to_string()],
                    ..ArtifactBundleManifest::default()
                },
            ),
        ]);
        write_artifacts(&record, &artifacts);
        let payload = artifact_payload_dir_for_record(&record);
        fs::create_dir_all(payload.join("metrics")).expect("metrics dir");
        fs::create_dir_all(payload.join("logs")).expect("logs dir");
        fs::write(payload.join("metrics/out.json"), "{}\n").expect("metric payload");
        fs::write(payload.join("logs/app.log"), "hello\n").expect("log payload");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                include_artifacts: true,
                selected_bundles: vec!["metrics".to_string()],
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");
        let root = tmp.path().join("out").join("hpc-compose-bundle-12345");
        assert!(root.join("artifacts/payload/metrics/out.json").exists());
        assert!(!root.join("artifacts/payload/logs/app.log").exists());
        assert!(manifest.artifact_payload_included);
        assert_eq!(manifest.selected_bundles, vec!["metrics".to_string()]);
        assert!(
            manifest_file(&manifest, "artifacts/payload/metrics/out.json")
                .sha256
                .is_some()
        );
    }

    #[test]
    fn newer_artifact_manifest_is_preserved_but_payload_is_not_interpreted() {
        let tmp = tempfile::tempdir().expect("tmp");
        let record = record_for(tmp.path(), "12345");
        let mut artifacts = artifact_manifest(&record.job_id);
        artifacts.schema_version = ARTIFACT_MANIFEST_SCHEMA_VERSION + 1;
        artifacts.copied_relative_paths = vec!["metrics/out.json".to_string()];
        write_artifacts(&record, &artifacts);
        let payload = artifact_payload_dir_for_record(&record);
        fs::create_dir_all(payload.join("metrics")).expect("metrics dir");
        fs::write(payload.join("metrics/out.json"), "{}\n").expect("payload");

        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                include_artifacts: true,
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");
        let root = tmp.path().join("out").join("hpc-compose-bundle-12345");
        assert!(root.join("artifacts/manifest.json").exists());
        assert!(!root.join("artifacts/payload/metrics/out.json").exists());
        assert!(!manifest.artifact_payload_included);
        assert!(manifest.selected_bundles.is_empty());
        assert!(
            manifest
                .warnings
                .iter()
                .any(|warning| warning.contains("schema version"))
        );
    }

    #[test]
    fn tarball_is_readable() {
        let tmp = tempfile::tempdir().expect("tmp");
        let record = record_for(tmp.path(), "12345");
        let manifest = write_experiment_bundle(
            &record,
            &show_json(&record.job_id),
            &checkpoint_history(&record),
            &ExperimentBundleOptions {
                into_dir: tmp.path().join("out"),
                tarball: true,
                ..ExperimentBundleOptions::default()
            },
        )
        .expect("bundle");

        let tarball = manifest.tarball_path.expect("tarball path");
        assert!(tarball.exists());
        let file = File::open(&tarball).expect("open tarball");
        let decoder = GzDecoder::new(file);
        let mut archive = Archive::new(decoder);
        let mut names = archive
            .entries()
            .expect("entries")
            .map(|entry| {
                entry
                    .expect("entry")
                    .path()
                    .expect("entry path")
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        names.sort();
        assert!(
            names
                .iter()
                .any(|name| name == "hpc-compose-bundle-12345/manifest.json"),
            "names: {names:?}"
        );
        assert!(
            names
                .iter()
                .any(|name| name == "hpc-compose-bundle-12345/run/experiment-show.json"),
            "names: {names:?}"
        );
    }
}
