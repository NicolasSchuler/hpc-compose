use super::*;

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
