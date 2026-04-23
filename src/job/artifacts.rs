use super::scheduler::unix_timestamp_now;
use super::*;

/// Manifest produced when teardown exports tracked artifacts.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactManifest {
    #[serde(default = "default_artifact_manifest_schema_version")]
    pub schema_version: u32,
    pub job_id: String,
    pub collect_policy: String,
    pub collected_at: String,
    pub job_outcome: String,
    #[serde(default)]
    pub attempt: Option<u32>,
    #[serde(default)]
    pub is_resume: Option<bool>,
    #[serde(default)]
    pub resume_dir: Option<PathBuf>,
    #[serde(default)]
    pub declared_source_patterns: Vec<String>,
    #[serde(default)]
    pub matched_source_paths: Vec<String>,
    #[serde(default)]
    pub copied_relative_paths: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub bundles: BTreeMap<String, ArtifactBundleManifest>,
}

/// Bundle-specific entries tracked in an artifact manifest.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactBundleManifest {
    #[serde(default)]
    pub declared_source_patterns: Vec<String>,
    #[serde(default)]
    pub matched_source_paths: Vec<String>,
    #[serde(default)]
    pub copied_relative_paths: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Result of copying tracked artifacts into the configured export directory.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactExportReport {
    pub record: SubmissionRecord,
    pub manifest_path: PathBuf,
    pub payload_dir: PathBuf,
    pub export_dir: PathBuf,
    pub manifest: ArtifactManifest,
    pub selected_bundles: Vec<String>,
    pub bundles: Vec<BundleExportReport>,
    pub exported_paths: Vec<PathBuf>,
    pub tarball_paths: Vec<PathBuf>,
    pub warnings: Vec<String>,
}

/// Export result for one artifact bundle.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct BundleExportReport {
    pub name: String,
    pub export_dir: PathBuf,
    pub provenance_path: PathBuf,
    pub tarball_path: Option<PathBuf>,
    pub exported_paths: Vec<PathBuf>,
    pub files: Vec<ArtifactEntryMetadata>,
    pub warnings: Vec<String>,
}

/// One exported artifact entry captured in provenance output.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactEntryMetadata {
    pub relative_path: String,
    pub entry_type: String,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub link_target: Option<String>,
}

/// Per-bundle provenance file written during artifact export.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize)]
pub struct ArtifactBundleProvenance {
    pub schema_version: u32,
    pub job_id: String,
    pub attempt: Option<u32>,
    pub is_resume: Option<bool>,
    pub resume_dir: Option<PathBuf>,
    pub bundle: String,
    pub compose_file: PathBuf,
    pub script_path: PathBuf,
    pub collect_policy: String,
    pub job_outcome: String,
    pub collected_at: String,
    pub exported_at_unix: u64,
    pub export_dir: PathBuf,
    pub tarball_path: Option<PathBuf>,
    pub selected_bundles: Vec<String>,
    pub declared_source_patterns: Vec<String>,
    pub matched_source_paths: Vec<String>,
    pub copied_relative_paths: Vec<String>,
    pub warnings: Vec<String>,
    pub files: Vec<ArtifactEntryMetadata>,
}

/// Options controlling tracked artifact export.
#[allow(missing_docs)]
#[derive(Debug, Clone, Default)]
pub struct ArtifactExportOptions {
    pub selected_bundles: Vec<String>,
    pub tarball: bool,
}

pub(super) fn default_artifact_manifest_schema_version() -> u32 {
    1
}

impl ArtifactManifest {
    pub(super) fn normalized_bundles(&self) -> BTreeMap<String, ArtifactBundleManifest> {
        if !self.bundles.is_empty() {
            return self.bundles.clone();
        }

        if self.declared_source_patterns.is_empty()
            && self.matched_source_paths.is_empty()
            && self.copied_relative_paths.is_empty()
            && self.warnings.is_empty()
        {
            return BTreeMap::new();
        }

        BTreeMap::from([(
            "default".to_string(),
            ArtifactBundleManifest {
                declared_source_patterns: self.declared_source_patterns.clone(),
                matched_source_paths: self.matched_source_paths.clone(),
                copied_relative_paths: self.copied_relative_paths.clone(),
                warnings: self.warnings.clone(),
            },
        )])
    }
}

/// Returns the tracked artifacts directory for a submission record.
pub fn artifacts_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::latest_artifacts_dir(&tracked_paths::runtime_job_root(
        &record.submit_dir,
        &record.job_id,
    ))
}

/// Returns the tracked artifact manifest path for a submission record.
pub fn artifact_manifest_path_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::artifact_manifest_path(&artifacts_dir_for_record(record))
}

/// Returns the tracked artifact payload directory for a submission record.
pub fn artifact_payload_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::artifact_payload_dir(&artifacts_dir_for_record(record))
}

/// Copies tracked artifacts for a completed job into its configured export directory.
pub fn export_artifacts(
    spec_path: &Path,
    job_id: Option<&str>,
    options: &ArtifactExportOptions,
) -> Result<ArtifactExportReport> {
    let record = load_submission_record(spec_path, job_id)?;
    let export_dir_template = record.artifact_export_dir.as_deref().context(format!(
        "tracked submission metadata for job {} does not include x-slurm.artifacts.export_dir; resubmit with artifact tracking enabled",
        record.job_id
    ))?;

    let manifest_path = artifact_manifest_path_for_record(&record);
    if !manifest_path.exists() {
        bail!(
            "tracked artifact manifest does not exist for job {} at {}; submit the job and wait for teardown collection to finish first",
            record.job_id,
            manifest_path.display()
        );
    }
    let manifest: ArtifactManifest = read_json(&manifest_path)?;
    if manifest.schema_version > ARTIFACT_MANIFEST_SCHEMA_VERSION {
        bail!(
            "artifact manifest schema version {} is newer than this hpc-compose build supports",
            manifest.schema_version
        );
    }
    if manifest.job_id != record.job_id {
        bail!(
            "artifact manifest job id {} does not match tracked job {}",
            manifest.job_id,
            record.job_id
        );
    }

    let payload_dir = artifact_payload_dir_for_record(&record);
    let export_dir = resolve_export_dir(&record.compose_file, export_dir_template, &record.job_id);
    fs::create_dir_all(&export_dir)
        .context(format!("failed to create {}", export_dir.display()))?;

    let mut warnings = manifest.warnings.clone();
    let mut exported_paths = Vec::new();
    let bundles = manifest.normalized_bundles();
    let selected_bundles = resolve_selected_bundles(&bundles, &options.selected_bundles)?;
    let exported_at_unix = unix_timestamp_now();
    let mut bundle_reports = Vec::new();
    let mut tarball_paths = Vec::new();

    for bundle_name in &selected_bundles {
        let bundle_manifest = bundles
            .get(bundle_name)
            .cloned()
            .context(format!("artifact bundle '{bundle_name}' is not available"))?;
        let bundle_export_dir = bundle_export_dir(&export_dir, bundle_name);
        fs::create_dir_all(&bundle_export_dir)
            .context(format!("failed to create {}", bundle_export_dir.display()))?;

        let mut bundle_warnings = bundle_manifest.warnings.clone();
        let mut bundle_exported_paths = Vec::new();
        for relative_path in &bundle_manifest.copied_relative_paths {
            let source = payload_dir.join(relative_path);
            if !source.exists() {
                let warning = format!(
                    "collected payload path '{}' is missing under {}",
                    relative_path,
                    payload_dir.display()
                );
                warnings.push(warning.clone());
                bundle_warnings.push(warning);
                continue;
            }
            let destination = bundle_export_dir.join(relative_path);
            copy_path_recursive(&source, &destination).context(format!(
                "failed to export artifact '{}' to {}",
                source.display(),
                destination.display()
            ))?;
            exported_paths.push(destination.clone());
            bundle_exported_paths.push(destination);
        }

        let files =
            collect_bundle_metadata(&bundle_export_dir, &bundle_manifest.copied_relative_paths)?;
        let provenance_path = export_dir
            .join("_hpc-compose")
            .join("bundles")
            .join(format!("{bundle_name}.json"));
        let tarball_path = if options.tarball {
            let tarball = export_dir.join(format!("{bundle_name}.tar.gz"));
            write_bundle_tarball(
                &tarball,
                &bundle_export_dir,
                &bundle_manifest.copied_relative_paths,
            )?;
            tarball_paths.push(tarball.clone());
            Some(tarball)
        } else {
            None
        };
        let provenance = ArtifactBundleProvenance {
            schema_version: ARTIFACT_PROVENANCE_SCHEMA_VERSION,
            job_id: record.job_id.clone(),
            attempt: manifest.attempt,
            is_resume: manifest.is_resume,
            resume_dir: manifest.resume_dir.clone(),
            bundle: bundle_name.clone(),
            compose_file: record.compose_file.clone(),
            script_path: record.script_path.clone(),
            collect_policy: manifest.collect_policy.clone(),
            job_outcome: manifest.job_outcome.clone(),
            collected_at: manifest.collected_at.clone(),
            exported_at_unix,
            export_dir: bundle_export_dir.clone(),
            tarball_path: tarball_path.clone(),
            selected_bundles: selected_bundles.clone(),
            declared_source_patterns: bundle_manifest.declared_source_patterns.clone(),
            matched_source_paths: bundle_manifest.matched_source_paths.clone(),
            copied_relative_paths: bundle_manifest.copied_relative_paths.clone(),
            warnings: bundle_warnings.clone(),
            files: files.clone(),
        };
        write_json(&provenance_path, &provenance)?;
        bundle_reports.push(BundleExportReport {
            name: bundle_name.clone(),
            export_dir: bundle_export_dir,
            provenance_path,
            tarball_path,
            exported_paths: bundle_exported_paths,
            files,
            warnings: bundle_warnings,
        });
    }

    Ok(ArtifactExportReport {
        record,
        manifest_path,
        payload_dir,
        export_dir,
        manifest,
        selected_bundles,
        bundles: bundle_reports,
        exported_paths,
        tarball_paths,
        warnings,
    })
}

fn resolve_selected_bundles(
    bundles: &BTreeMap<String, ArtifactBundleManifest>,
    requested: &[String],
) -> Result<Vec<String>> {
    if requested.is_empty() {
        return Ok(bundles.keys().cloned().collect());
    }

    let mut selected = Vec::new();
    for bundle in requested {
        if !bundles.contains_key(bundle) {
            bail!("artifact bundle '{bundle}' is not available");
        }
        if !selected.contains(bundle) {
            selected.push(bundle.clone());
        }
    }
    Ok(selected)
}

fn bundle_export_dir(export_dir: &Path, bundle_name: &str) -> PathBuf {
    if bundle_name == "default" {
        export_dir.to_path_buf()
    } else {
        export_dir.join("bundles").join(bundle_name)
    }
}

pub(crate) fn resolve_export_dir(compose_file: &Path, template: &str, job_id: &str) -> PathBuf {
    let rendered = template.replace("${SLURM_JOB_ID}", job_id);
    let candidate = PathBuf::from(rendered);
    if candidate.is_absolute() {
        candidate
    } else {
        let parent = match compose_file.parent() {
            Some(parent) => parent,
            None => Path::new("."),
        };
        parent.join(candidate)
    }
}

fn collect_bundle_metadata(
    bundle_root: &Path,
    copied_relative_paths: &[String],
) -> Result<Vec<ArtifactEntryMetadata>> {
    let mut files = Vec::new();
    for relative_path in copied_relative_paths {
        let relative = PathBuf::from(relative_path);
        let path = bundle_root.join(&relative);
        if !path.exists() && fs::symlink_metadata(&path).is_err() {
            continue;
        }
        collect_path_metadata(bundle_root, &path, &mut files)?;
    }
    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(files)
}

fn collect_path_metadata(
    bundle_root: &Path,
    path: &Path,
    files: &mut Vec<ArtifactEntryMetadata>,
) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .context(format!("failed to read metadata for {}", path.display()))?;
    let relative_path = path
        .strip_prefix(bundle_root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string();

    if metadata.file_type().is_symlink() {
        let target =
            fs::read_link(path).context(format!("failed to read link {}", path.display()))?;
        files.push(ArtifactEntryMetadata {
            relative_path,
            entry_type: "symlink".to_string(),
            size_bytes: None,
            sha256: None,
            link_target: Some(target.to_string_lossy().to_string()),
        });
        return Ok(());
    }

    if metadata.is_dir() {
        files.push(ArtifactEntryMetadata {
            relative_path,
            entry_type: "directory".to_string(),
            size_bytes: None,
            sha256: None,
            link_target: None,
        });
        let mut entries = fs::read_dir(path)
            .context(format!("failed to read {}", path.display()))?
            .collect::<io::Result<Vec<_>>>()
            .context(format!("failed to read {}", path.display()))?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            collect_path_metadata(bundle_root, &entry.path(), files)?;
        }
        return Ok(());
    }

    files.push(ArtifactEntryMetadata {
        relative_path,
        entry_type: "file".to_string(),
        size_bytes: Some(metadata.len()),
        sha256: Some(hash_file(path)?),
        link_target: None,
    });
    Ok(())
}

fn hash_file(path: &Path) -> Result<String> {
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

fn write_bundle_tarball(
    tarball_path: &Path,
    bundle_root: &Path,
    copied_relative_paths: &[String],
) -> Result<()> {
    if let Some(parent) = tarball_path.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    let file = File::create(tarball_path)
        .context(format!("failed to create {}", tarball_path.display()))?;
    let encoder = GzEncoder::new(file, Compression::default());
    let mut builder = Builder::new(encoder);
    for relative_path in copied_relative_paths {
        let relative = PathBuf::from(relative_path);
        let source = bundle_root.join(&relative);
        if !source.exists() && fs::symlink_metadata(&source).is_err() {
            continue;
        }
        append_path_to_tar(&mut builder, &source, &relative)?;
    }
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

fn append_path_to_tar<W: Write>(
    builder: &mut Builder<W>,
    source: &Path,
    relative: &Path,
) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .context(format!("failed to read metadata for {}", source.display()))?;
    if metadata.is_dir() {
        if !relative.as_os_str().is_empty() {
            builder
                .append_dir(relative, source)
                .context(format!("failed to append {} to tarball", source.display()))?;
        }
        let mut entries = fs::read_dir(source)
            .context(format!("failed to read {}", source.display()))?
            .collect::<io::Result<Vec<_>>>()
            .context(format!("failed to read {}", source.display()))?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            append_path_to_tar(builder, &entry.path(), &relative.join(entry.file_name()))?;
        }
        return Ok(());
    }

    builder
        .append_path_with_name(source, relative)
        .context(format!("failed to append {} to tarball", source.display()))?;
    Ok(())
}

pub(crate) fn copy_path_recursive(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .context(format!("failed to read metadata for {}", source.display()))?;
    if metadata.file_type().is_symlink() {
        return copy_symlink(source, destination);
    }

    if metadata.is_dir() {
        fs::create_dir_all(destination)
            .context(format!("failed to create {}", destination.display()))?;
        for entry in fs::read_dir(source).context(format!("failed to read {}", source.display()))? {
            let entry = entry?;
            copy_path_recursive(&entry.path(), &destination.join(entry.file_name()))?;
        }
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    fs::copy(source, destination).context(format!(
        "failed to copy {} to {}",
        source.display(),
        destination.display()
    ))?;
    Ok(())
}

pub(crate) fn remove_existing_destination(path: &Path) -> Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.file_type().is_symlink() || metadata.is_file() {
        fs::remove_file(path).context(format!("failed to remove {}", path.display()))?;
    } else if metadata.is_dir() {
        fs::remove_dir_all(path).context(format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn copy_symlink(source: &Path, destination: &Path) -> Result<()> {
    let target =
        fs::read_link(source).context(format!("failed to read link {}", source.display()))?;
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent).context(format!("failed to create {}", parent.display()))?;
    }
    remove_existing_destination(destination)?;
    std::os::unix::fs::symlink(&target, destination).context(format!(
        "failed to recreate symlink {} -> {}",
        destination.display(),
        target.display()
    ))?;
    Ok(())
}

#[cfg(not(unix))]
fn copy_symlink(source: &Path, _destination: &Path) -> Result<()> {
    bail!(
        "exporting symlinks is not supported on this platform: {}",
        source.display()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_manifest(job_id: &str) -> ArtifactManifest {
        ArtifactManifest {
            schema_version: ARTIFACT_MANIFEST_SCHEMA_VERSION,
            job_id: job_id.to_string(),
            collect_policy: "always".into(),
            collected_at: "2026-04-05T10:00:00Z".into(),
            job_outcome: "success".into(),
            attempt: Some(2),
            is_resume: Some(true),
            resume_dir: Some(PathBuf::from("/shared/resume")),
            declared_source_patterns: Vec::new(),
            matched_source_paths: Vec::new(),
            copied_relative_paths: Vec::new(),
            warnings: Vec::new(),
            bundles: BTreeMap::new(),
        }
    }

    fn artifact_record(tmpdir: &Path, job_id: &str) -> SubmissionRecord {
        let compose = tmpdir.join("compose.yaml");
        fs::write(&compose, "services:\n  app:\n    image: redis:7\n").expect("compose");
        let record = SubmissionRecord {
            schema_version: SUBMISSION_SCHEMA_VERSION,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            job_id: job_id.to_string(),
            submitted_at: 1,
            compose_file: compose,
            submit_dir: tmpdir.to_path_buf(),
            script_path: tmpdir.join("job.sbatch"),
            cache_dir: tmpdir.join("cache"),
            batch_log: tmpdir.join(format!("slurm-{job_id}.out")),
            service_logs: BTreeMap::new(),
            artifact_export_dir: Some("./results/${SLURM_JOB_ID}".into()),
            resume_dir: None,
            service_name: None,
            command_override: None,
            requested_walltime: None,
            config_snapshot_yaml: None,
            cached_artifacts: Vec::new(),
        };
        write_submission_record(&record).expect("write record");
        record
    }

    fn write_artifact_manifest(record: &SubmissionRecord, manifest: &ArtifactManifest) {
        let path = artifact_manifest_path_for_record(record);
        fs::create_dir_all(path.parent().expect("manifest parent")).expect("manifest dir");
        fs::write(
            path,
            serde_json::to_vec_pretty(manifest).expect("manifest json"),
        )
        .expect("write manifest");
    }

    #[test]
    fn normalized_bundles_preserves_empty_and_legacy_top_level_fields() {
        let empty = artifact_manifest("12345");
        assert!(empty.normalized_bundles().is_empty());

        let mut legacy = artifact_manifest("12345");
        legacy.declared_source_patterns = vec!["/hpc-compose/job/metrics/**".into()];
        legacy.matched_source_paths = vec!["/hpc-compose/job/metrics/meta.json".into()];
        legacy.copied_relative_paths = vec!["metrics/meta.json".into()];
        legacy.warnings = vec!["legacy warning".into()];

        let bundles = legacy.normalized_bundles();
        let default = bundles.get("default").expect("default bundle");
        assert_eq!(
            default.declared_source_patterns,
            vec!["/hpc-compose/job/metrics/**".to_string()]
        );
        assert_eq!(
            default.matched_source_paths,
            vec!["/hpc-compose/job/metrics/meta.json".to_string()]
        );
        assert_eq!(
            default.copied_relative_paths,
            vec!["metrics/meta.json".to_string()]
        );
        assert_eq!(default.warnings, vec!["legacy warning".to_string()]);
    }

    #[test]
    fn export_artifacts_rejects_newer_manifest_schema() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = artifact_record(tmpdir.path(), "12345");
        let mut manifest = artifact_manifest("12345");
        manifest.schema_version = ARTIFACT_MANIFEST_SCHEMA_VERSION + 1;
        write_artifact_manifest(&record, &manifest);

        let err = export_artifacts(
            &record.compose_file,
            Some("12345"),
            &ArtifactExportOptions::default(),
        )
        .expect_err("newer manifest schema");
        assert!(
            err.to_string()
                .contains("newer than this hpc-compose build supports")
        );
    }

    #[test]
    fn export_artifacts_dedupes_selected_bundle_and_reports_missing_payloads() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let record = artifact_record(tmpdir.path(), "12345");
        let payload_dir = artifact_payload_dir_for_record(&record);
        fs::create_dir_all(payload_dir.join("logs")).expect("logs dir");
        fs::create_dir_all(payload_dir.join("metrics")).expect("metrics dir");
        fs::write(payload_dir.join("logs/app.log"), "ready\n").expect("log");
        fs::write(payload_dir.join("metrics/meta.json"), "{}\n").expect("metrics");

        let mut log_paths = vec!["logs/app.log".to_string(), "logs/missing.log".to_string()];
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("app.log", payload_dir.join("logs/latest"))
                .expect("symlink");
            log_paths.push("logs/latest".to_string());
        }

        let mut manifest = artifact_manifest("12345");
        manifest.bundles = BTreeMap::from([
            (
                "default".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/metrics/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/metrics/meta.json".into()],
                    copied_relative_paths: vec!["metrics/meta.json".into()],
                    warnings: Vec::new(),
                },
            ),
            (
                "logs".into(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/hpc-compose/job/logs/**".into()],
                    matched_source_paths: vec!["/hpc-compose/job/logs/app.log".into()],
                    copied_relative_paths: log_paths,
                    warnings: vec!["preexisting bundle warning".into()],
                },
            ),
        ]);
        write_artifact_manifest(&record, &manifest);

        let report = export_artifacts(
            &record.compose_file,
            Some("12345"),
            &ArtifactExportOptions {
                selected_bundles: vec!["logs".into(), "logs".into()],
                tarball: true,
            },
        )
        .expect("export");

        assert_eq!(report.selected_bundles, vec!["logs".to_string()]);
        assert_eq!(report.bundles.len(), 1);
        assert!(report.export_dir.join("bundles/logs/logs/app.log").exists());
        assert!(!report.export_dir.join("metrics/meta.json").exists());
        assert!(report.tarball_paths[0].exists());
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains("collected payload path"))
        );
        assert!(
            report.bundles[0]
                .warnings
                .iter()
                .any(|warning| warning.contains("preexisting bundle warning"))
        );

        let provenance: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(&report.bundles[0].provenance_path).expect("provenance"),
        )
        .expect("provenance json");
        assert_eq!(provenance["bundle"], serde_json::json!("logs"));
        assert_eq!(provenance["attempt"], serde_json::json!(2));
        assert_eq!(provenance["is_resume"], serde_json::json!(true));

        #[cfg(unix)]
        assert!(report.bundles[0].files.iter().any(|entry| {
            entry.relative_path == "logs/latest"
                && entry.entry_type == "symlink"
                && entry.link_target.as_deref() == Some("app.log")
        }));
    }

    #[test]
    fn resolve_selected_bundles_deduplicates_requested_names() {
        let bundles = BTreeMap::from([(
            "logs".to_string(),
            ArtifactBundleManifest {
                declared_source_patterns: vec!["/hpc-compose/job/logs/**".into()],
                matched_source_paths: vec!["/hpc-compose/job/logs/app.log".into()],
                copied_relative_paths: vec!["logs/app.log".into()],
                warnings: Vec::new(),
            },
        )]);
        let selected = resolve_selected_bundles(
            &bundles,
            &["logs".to_string(), "logs".to_string(), "logs".to_string()],
        )
        .expect("select deduped");
        assert_eq!(selected, vec!["logs"]);
    }

    #[test]
    fn resolve_export_dir_handles_absolute_and_relative() {
        let compose = Path::new("/project/compose.yaml");
        assert_eq!(
            resolve_export_dir(compose, "/shared/results/${SLURM_JOB_ID}", "42"),
            PathBuf::from("/shared/results/42")
        );
        assert_eq!(
            resolve_export_dir(compose, "./results", "42"),
            PathBuf::from("/project/results")
        );
    }

    #[test]
    fn resolve_export_dir_replaces_job_id_placeholder() {
        let compose = Path::new("/project/compose.yaml");
        let result = resolve_export_dir(compose, "/out/${SLURM_JOB_ID}/artifacts", "123");
        assert_eq!(result, PathBuf::from("/out/123/artifacts"));
    }

    #[test]
    fn bundle_export_dir_defaults_to_root_for_default_bundle() {
        assert_eq!(
            bundle_export_dir(Path::new("/export"), "default"),
            PathBuf::from("/export")
        );
        assert_eq!(
            bundle_export_dir(Path::new("/export"), "checkpoints"),
            PathBuf::from("/export/bundles/checkpoints")
        );
    }

    #[test]
    fn resolve_selected_bundles_returns_all_when_empty() {
        let bundles = BTreeMap::from([
            (
                "default".to_string(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/a".into()],
                    matched_source_paths: vec!["/a".into()],
                    copied_relative_paths: vec!["a".into()],
                    warnings: Vec::new(),
                },
            ),
            (
                "logs".to_string(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/b".into()],
                    matched_source_paths: vec!["/b".into()],
                    copied_relative_paths: vec!["b".into()],
                    warnings: Vec::new(),
                },
            ),
        ]);
        let selected = resolve_selected_bundles(&bundles, &[]).expect("select all");
        assert_eq!(selected, vec!["default", "logs"]);
    }

    #[test]
    fn resolve_selected_bundles_filters_to_requested() {
        let bundles = BTreeMap::from([
            (
                "default".to_string(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/a".into()],
                    matched_source_paths: vec!["/a".into()],
                    copied_relative_paths: vec!["a".into()],
                    warnings: Vec::new(),
                },
            ),
            (
                "logs".to_string(),
                ArtifactBundleManifest {
                    declared_source_patterns: vec!["/b".into()],
                    matched_source_paths: vec!["/b".into()],
                    copied_relative_paths: vec!["b".into()],
                    warnings: Vec::new(),
                },
            ),
        ]);
        let selected = resolve_selected_bundles(&bundles, &["logs".to_string()]).expect("select");
        assert_eq!(selected, vec!["logs"]);
    }

    #[test]
    fn resolve_selected_bundles_rejects_unknown_bundle() {
        let bundles = BTreeMap::new();
        let err = resolve_selected_bundles(&bundles, &["missing".to_string()])
            .expect_err("unknown bundle");
        assert!(err.to_string().contains("not available"));
    }

    #[test]
    fn copy_path_recursive_copies_files_and_directories() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let src_dir = tmpdir.path().join("src");
        fs::create_dir_all(src_dir.join("sub")).expect("sub");
        fs::write(src_dir.join("a.txt"), "hello").expect("a");
        fs::write(src_dir.join("sub/b.txt"), "world").expect("b");

        let dest = tmpdir.path().join("dest");
        copy_path_recursive(&src_dir, &dest).expect("copy");
        assert!(dest.join("a.txt").exists());
        assert!(dest.join("sub/b.txt").exists());
        assert_eq!(
            fs::read_to_string(dest.join("a.txt")).expect("read"),
            "hello"
        );
    }

    #[test]
    fn remove_existing_destination_handles_missing_path() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let missing = tmpdir.path().join("does-not-exist");
        remove_existing_destination(&missing).expect("no error for missing");
    }

    #[test]
    fn remove_existing_destination_removes_file() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("file.txt");
        fs::write(&file, "x").expect("write");
        remove_existing_destination(&file).expect("remove");
        assert!(!file.exists());
    }

    #[test]
    fn remove_existing_destination_removes_directory() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let dir = tmpdir.path().join("subdir");
        fs::create_dir_all(&dir).expect("dir");
        fs::write(dir.join("inner.txt"), "x").expect("write");
        remove_existing_destination(&dir).expect("remove");
        assert!(!dir.exists());
    }

    #[test]
    fn hash_file_produces_sha256_hex() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("data.bin");
        fs::write(&file, "hello world").expect("write");
        let hash = hash_file(&file).expect("hash");
        assert_eq!(hash.len(), 64);
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn collect_bundle_metadata_lists_files_and_directories() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::write(tmpdir.path().join("a.txt"), "aaa").expect("a");
        fs::create_dir_all(tmpdir.path().join("sub")).expect("sub");
        fs::write(tmpdir.path().join("sub/b.txt"), "bbb").expect("b");

        let files = collect_bundle_metadata(tmpdir.path(), &["a.txt".into(), "sub".into()])
            .expect("collect");
        let paths: Vec<&str> = files.iter().map(|f| f.relative_path.as_str()).collect();
        assert!(paths.contains(&"a.txt"));
        assert!(paths.contains(&"sub"));
        assert!(paths.contains(&"sub/b.txt"));
        let file_entry = files
            .iter()
            .find(|f| f.relative_path == "a.txt")
            .expect("a.txt");
        assert_eq!(file_entry.entry_type, "file");
        assert!(file_entry.size_bytes.is_some());
        assert!(file_entry.sha256.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn collect_bundle_metadata_records_symlinks_and_skips_missing_paths() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join("logs")).expect("logs dir");
        fs::write(tmpdir.path().join("logs/app.log"), "ready\n").expect("log");
        std::os::unix::fs::symlink("app.log", tmpdir.path().join("logs/latest")).expect("symlink");

        let files = collect_bundle_metadata(
            tmpdir.path(),
            &[
                "logs/latest".into(),
                "logs/missing.log".into(),
                "logs/app.log".into(),
            ],
        )
        .expect("metadata");

        assert_eq!(files.len(), 2);
        let symlink = files
            .iter()
            .find(|entry| entry.relative_path == "logs/latest")
            .expect("symlink entry");
        assert_eq!(symlink.entry_type, "symlink");
        assert_eq!(symlink.link_target.as_deref(), Some("app.log"));
        assert!(symlink.sha256.is_none());

        let file = files
            .iter()
            .find(|entry| entry.relative_path == "logs/app.log")
            .expect("file entry");
        assert_eq!(file.entry_type, "file");
        assert_eq!(file.size_bytes, Some(6));
        assert!(file.sha256.is_some());
    }
}
