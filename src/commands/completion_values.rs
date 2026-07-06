use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use hpc_compose::cli::CompletionValueKind;
use hpc_compose::cluster::{discover_cluster_profile_path, load_cluster_profile};
use hpc_compose::context::{BinaryOverrides, ResolvedContext};
use hpc_compose::job::{
    ArtifactManifest, SubmissionRecord, artifact_manifest_path_for_record,
    find_submission_record_in_repo, load_submission_record_optional, scan_job_inventory,
    scan_sweep_manifests,
};

use super::{GlobalCommandOptions, resolve_command_context};

pub(super) fn complete_values(
    options: &GlobalCommandOptions,
    kind: CompletionValueKind,
    file: Option<PathBuf>,
    job_id: Option<String>,
    prefix: String,
) -> Result<()> {
    let candidates = collect_completion_values(options, kind, file, job_id.as_deref());
    write_candidates(&mut io::stdout(), &candidates, &prefix)
}

fn collect_completion_values(
    options: &GlobalCommandOptions,
    kind: CompletionValueKind,
    file: Option<PathBuf>,
    job_id: Option<&str>,
) -> BTreeSet<String> {
    let context = resolve_context_quiet(options, file.clone());
    match kind {
        CompletionValueKind::Partition => partition_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::Qos => qos_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::Resources => resource_profile_candidates(context.as_ref()),
        CompletionValueKind::Service => service_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::JobId => job_id_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::Tag => tag_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::SweepId => sweep_id_candidates(context.as_ref(), file.as_ref()),
        CompletionValueKind::Bundle => bundle_candidates(context.as_ref(), file.as_ref(), job_id),
    }
}

fn resolve_context_quiet(
    options: &GlobalCommandOptions,
    file: Option<PathBuf>,
) -> Option<ResolvedContext> {
    resolve_command_context(options, file, BinaryOverrides::default(), None).ok()
}

fn partition_candidates(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();
    if let Some(context) = context {
        for profile in context.resource_profiles.values() {
            insert_optional(&mut candidates, profile.partition.as_deref());
        }
    }
    if let Some(profile) = load_cluster_profile_quiet(context, file) {
        for partition in profile.partitions {
            insert_candidate(&mut candidates, partition.name);
        }
    }
    candidates
}

fn qos_candidates(context: Option<&ResolvedContext>, file: Option<&PathBuf>) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();
    if let Some(context) = context {
        for profile in context.resource_profiles.values() {
            insert_optional(&mut candidates, profile.qos.as_deref());
        }
    }
    if let Some(profile) = load_cluster_profile_quiet(context, file) {
        for qos in profile.qos {
            insert_candidate(&mut candidates, qos);
        }
        for partition in profile.partitions {
            insert_optional(&mut candidates, partition.default_qos.as_deref());
            for qos in partition.qos {
                insert_candidate(&mut candidates, qos);
            }
        }
    }
    candidates
}

fn resource_profile_candidates(context: Option<&ResolvedContext>) -> BTreeSet<String> {
    context
        .map(|context| {
            context
                .resource_profiles
                .keys()
                .filter(|name| valid_candidate(name))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

fn service_candidates(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> BTreeSet<String> {
    let Some(compose_file) = compose_path_for_completion(context, file) else {
        return BTreeSet::new();
    };
    service_names_from_yaml(&compose_file).unwrap_or_default()
}

fn job_id_candidates(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();
    for entry in filtered_inventory(context, file) {
        insert_candidate(&mut candidates, entry.job_id);
    }
    candidates
}

fn tag_candidates(context: Option<&ResolvedContext>, file: Option<&PathBuf>) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();
    for entry in filtered_inventory(context, file) {
        for tag in entry.tags {
            insert_candidate(&mut candidates, tag);
        }
    }
    candidates
}

fn sweep_id_candidates(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> BTreeSet<String> {
    let mut candidates = BTreeSet::new();
    if let Some(compose_file) = compose_path_for_completion(context, file) {
        if let Ok(manifests) = scan_sweep_manifests(&compose_file) {
            for manifest in manifests {
                insert_candidate(&mut candidates, manifest.sweep_id);
            }
        }
    }
    for record in filtered_submission_records(context, file) {
        if let Some(sweep) = record.sweep {
            insert_candidate(&mut candidates, sweep.sweep_id);
        }
    }
    candidates
}

fn bundle_candidates(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
    job_id: Option<&str>,
) -> BTreeSet<String> {
    let Some(record) = resolve_record_quiet(context, file, job_id) else {
        return BTreeSet::new();
    };
    let manifest_path = artifact_manifest_path_for_record(&record);
    let Ok(raw) = fs::read(&manifest_path) else {
        return BTreeSet::new();
    };
    let Ok(manifest) = serde_json::from_slice::<ArtifactManifest>(&raw) else {
        return BTreeSet::new();
    };
    let mut candidates = BTreeSet::new();
    if manifest.bundles.is_empty() {
        if !manifest.declared_source_patterns.is_empty()
            || !manifest.matched_source_paths.is_empty()
            || !manifest.copied_relative_paths.is_empty()
            || !manifest.warnings.is_empty()
        {
            insert_candidate(&mut candidates, "default");
        }
    } else {
        for bundle in manifest.bundles.keys() {
            insert_candidate(&mut candidates, bundle);
        }
    }
    candidates
}

fn load_cluster_profile_quiet(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> Option<hpc_compose::cluster::ClusterProfile> {
    let start = context
        .and_then(|context| context.compose_file.value.parent().map(Path::to_path_buf))
        .or_else(|| {
            file.and_then(|file| absolute_path_quiet(file))
                .and_then(|path| path.parent().map(Path::to_path_buf))
        })
        .or_else(|| env::current_dir().ok())?;
    let path = discover_cluster_profile_path(&start)?;
    load_cluster_profile(&path).ok()
}

fn compose_path_for_completion(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> Option<PathBuf> {
    if let Some(context) = context {
        return Some(context.compose_file.value.clone());
    }
    if let Some(path) = file.and_then(|file| absolute_path_quiet(file)) {
        return Some(path);
    }
    let candidate = env::current_dir().ok()?.join("compose.yaml");
    candidate.is_file().then_some(candidate)
}

fn service_names_from_yaml(path: &Path) -> Result<BTreeSet<String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read compose file {}", path.display()))?;
    let value: serde_norway::Value = serde_norway::from_str(&raw)
        .with_context(|| format!("failed to parse compose file {}", path.display()))?;
    let Some(services) = value
        .get("services")
        .and_then(serde_norway::Value::as_mapping)
    else {
        return Ok(BTreeSet::new());
    };
    let mut names = BTreeSet::new();
    for key in services.keys() {
        if let Some(name) = key.as_str() {
            insert_candidate(&mut names, name);
        }
    }
    Ok(names)
}

fn filtered_inventory(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> Vec<hpc_compose::job::JobInventoryEntry> {
    let cwd = env::current_dir().ok();
    let scan_start = context
        .map(|context| context.cwd.clone())
        .or_else(|| cwd.clone())
        .unwrap_or_else(|| PathBuf::from("."));
    let Ok(scan) = scan_job_inventory(&scan_start, false) else {
        return Vec::new();
    };
    let compose_file = compose_path_for_completion(context, file);
    scan.jobs
        .into_iter()
        .filter(|entry| {
            compose_file
                .as_ref()
                .map(|compose_file| entry.compose_file == *compose_file)
                .unwrap_or(true)
        })
        .collect()
}

fn filtered_submission_records(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
) -> Vec<SubmissionRecord> {
    filtered_inventory(context, file)
        .into_iter()
        .filter_map(|entry| {
            let raw = fs::read(&entry.record_path).ok()?;
            serde_json::from_slice::<SubmissionRecord>(&raw).ok()
        })
        .collect()
}

fn resolve_record_quiet(
    context: Option<&ResolvedContext>,
    file: Option<&PathBuf>,
    job_id: Option<&str>,
) -> Option<SubmissionRecord> {
    if let Some(compose_file) = compose_path_for_completion(context, file) {
        if let Some(record) = load_submission_record_optional(&compose_file, job_id) {
            return Some(record);
        }
    }
    let scan_start = context
        .map(|context| context.cwd.clone())
        .or_else(|| env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    if let Some(job_id) = job_id {
        return find_submission_record_in_repo(&scan_start, job_id).ok();
    }
    filtered_submission_records(context, file)
        .into_iter()
        .next()
}

fn absolute_path_quiet(path: &Path) -> Option<PathBuf> {
    let cwd = env::current_dir().ok()?;
    Some(crate::path_util::absolute_path(path, &cwd))
}

fn write_candidates(
    writer: &mut impl Write,
    candidates: &BTreeSet<String>,
    prefix: &str,
) -> Result<()> {
    for candidate in candidates
        .iter()
        .filter(|candidate| prefix.is_empty() || candidate.starts_with(prefix))
    {
        writeln!(writer, "{candidate}").context("failed to write completion candidate")?;
    }
    Ok(())
}

fn insert_optional(candidates: &mut BTreeSet<String>, value: Option<&str>) {
    if let Some(value) = value {
        insert_candidate(candidates, value);
    }
}

fn insert_candidate(candidates: &mut BTreeSet<String>, value: impl AsRef<str>) {
    let value = value.as_ref();
    if valid_candidate(value) {
        candidates.insert(value.to_string());
    }
}

fn valid_candidate(value: &str) -> bool {
    !value.is_empty() && !value.contains('\n') && !value.contains('\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_candidates_sorts_deduplicates_and_prefix_filters() {
        let candidates =
            BTreeSet::from(["gpu".to_string(), "gpu-long".to_string(), "cpu".to_string()]);
        let mut output = Vec::new();

        write_candidates(&mut output, &candidates, "gpu").expect("write candidates");

        assert_eq!(String::from_utf8(output).expect("utf8"), "gpu\ngpu-long\n");
    }

    #[test]
    fn service_candidates_parse_unresolved_compose_yaml() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(
            &compose,
            "\
services:
  trainer:
    image: ${MISSING_IMAGE}
  worker:
    image: alpine
",
        )
        .expect("write compose");
        let options = GlobalCommandOptions::default();

        let candidates =
            collect_completion_values(&options, CompletionValueKind::Service, Some(compose), None);

        assert_eq!(
            candidates,
            BTreeSet::from(["trainer".to_string(), "worker".to_string()])
        );
    }
}
