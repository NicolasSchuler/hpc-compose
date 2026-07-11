use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::ensure;

use crate::context::repo_root_or_cwd;

use super::scheduler::unix_timestamp_now;
use super::*;

/// One tracked job discovered from recorded submission metadata.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct JobInventoryEntry {
    pub compose_file: PathBuf,
    pub compose_metadata_root: PathBuf,
    pub job_id: String,
    pub kind: SubmissionKind,
    pub is_latest: bool,
    pub submitted_at: u64,
    pub age_seconds: u64,
    pub submit_dir: PathBuf,
    pub record_path: PathBuf,
    pub runtime_job_root: PathBuf,
    pub runtime_job_root_present: bool,
    pub legacy_runtime_job_root: PathBuf,
    pub legacy_runtime_job_root_present: bool,
    #[serde(default)]
    pub runtime_cache_dir: PathBuf,
    #[serde(default)]
    pub runtime_cache_dir_present: bool,
    #[serde(default)]
    pub batch_log: PathBuf,
    #[serde(default)]
    pub batch_log_managed: bool,
    #[serde(default)]
    pub disk_usage_bytes: Option<u64>,
    /// User-assigned tags copied from the record (see `experiment tag`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Number of notes attached to the record (see `experiment note`).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub note_count: usize,
}

fn is_zero(value: &usize) -> bool {
    *value == 0
}

/// Repo-tree scan result returned by `jobs list`.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct JobInventoryScan {
    pub scan_root: PathBuf,
    pub jobs: Vec<JobInventoryEntry>,
}

/// Planned or executed tracked-job cleanup report.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct CleanupReport {
    pub compose_file: PathBuf,
    pub mode: String,
    pub dry_run: bool,
    pub removed_job_ids: Vec<String>,
    pub kept_job_ids: Vec<String>,
    pub latest_pointer_job_id_before: Option<String>,
    pub latest_job_id_before: Option<String>,
    pub latest_job_id_after: Option<String>,
    pub total_bytes_reclaimed: Option<u64>,
    pub jobs: Vec<CleanupJobReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deep: Option<DeepCleanupDetails>,
}

/// Cleanup planning details for one tracked job.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
pub struct CleanupJobReport {
    #[serde(flatten)]
    pub inventory: JobInventoryEntry,
    pub selected: bool,
    pub bytes_reclaimed: Option<u64>,
    #[serde(skip)]
    pub removable_paths: Vec<PathBuf>,
}

/// Cleanup selection strategy.
#[allow(missing_docs)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupMode {
    Age { age_days: u64 },
    AllExceptLatest,
}

/// Returns the `.hpc-compose` metadata directory for a compose file.
pub fn metadata_root_for(spec_path: &Path) -> PathBuf {
    tracked_paths::metadata_root_for(spec_path)
}

/// Returns the tracked job-record directory for a compose file.
pub fn jobs_dir_for(spec_path: &Path) -> PathBuf {
    tracked_paths::jobs_dir_for(spec_path)
}

/// Returns the path to the "latest tracked job" record file.
pub fn latest_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_record_path_for(spec_path)
}

/// Returns the path to the "latest tracked run job" record file.
pub fn latest_run_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_run_record_path_for(spec_path)
}

/// Returns the path to the "latest tracked canary job" record file.
pub fn latest_canary_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_canary_record_path_for(spec_path)
}

/// Returns the path to the "latest tracked notebook job" record file.
pub fn latest_notebook_record_path_for(spec_path: &Path) -> PathBuf {
    tracked_paths::latest_notebook_record_path_for(spec_path)
}

/// Builds and persists a new submission record for a submitted job.
pub fn persist_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    let record = build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )?;
    write_submission_record(&record)?;
    Ok(record)
}

/// Builds the submission record structure for a job without writing it.
pub fn build_submission_record(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        &SubmissionRecordBuildOptions::default(),
    )
}

/// Builds the submission record structure for a job without writing it, with
/// extra tracked metadata.
pub fn build_submission_record_with_options(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    options: &SubmissionRecordBuildOptions,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        SubmissionBackend::Slurm,
        options,
    )
}

/// Builds the submission record structure for a job without writing it.
pub fn build_submission_record_with_backend(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    backend: SubmissionBackend,
) -> Result<SubmissionRecord> {
    build_submission_record_with_backend_and_options(
        spec_path,
        submit_dir,
        script_path,
        plan,
        job_id,
        backend,
        &SubmissionRecordBuildOptions::default(),
    )
}

/// Builds the submission record structure for a job without writing it, with
/// an explicit backend and extra tracked metadata.
pub fn build_submission_record_with_backend_and_options(
    spec_path: &Path,
    submit_dir: &Path,
    script_path: &Path,
    plan: &RuntimePlan,
    job_id: &str,
    backend: SubmissionBackend,
    options: &SubmissionRecordBuildOptions,
) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let submit_dir = absolute_path(submit_dir)?;
    let script_path = absolute_path(script_path)?;
    // Persist only an explicit `x-slurm.runtime_root` override (resolved
    // absolute); `None` means the default `<submit_dir>/.hpc-compose` layout,
    // which `runtime_job_root_for_record` reconstructs from `submit_dir`.
    let runtime_root = plan
        .slurm
        .runtime_root
        .as_deref()
        .map(|raw| crate::path_util::absolute_path(Path::new(raw), &submit_dir));
    let job_root = match &runtime_root {
        Some(root) => root.join(job_id),
        None => tracked_paths::runtime_job_root(&submit_dir, job_id),
    };
    let log_dir = tracked_paths::latest_logs_dir(&job_root);
    let service_logs = plan
        .ordered_services
        .iter()
        .map(|service| {
            (
                service.name.clone(),
                log_dir.join(log_file_name_for_service(&service.name)),
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(SubmissionRecord {
        schema_version: SUBMISSION_SCHEMA_VERSION,
        backend,
        kind: options.kind,
        job_id: job_id.to_string(),
        submitted_at: unix_timestamp_now(),
        compose_file,
        submit_dir: submit_dir.clone(),
        script_path,
        cache_dir: plan.cache_dir.clone(),
        runtime_root,
        batch_log: batch_log_path_for_backend(plan, &submit_dir, job_id, backend),
        batch_log_managed: plan.slurm.output.is_none(),
        service_logs,
        artifact_export_dir: plan
            .slurm
            .artifacts
            .as_ref()
            .and_then(|artifacts| artifacts.export_dir.clone()),
        resume_dir: plan.slurm.resume_dir().map(PathBuf::from),
        service_name: options.service_name.clone(),
        command_override: options.command_override.clone(),
        requested_walltime: options.requested_walltime.clone(),
        slurm_array: options
            .slurm_array
            .clone()
            .or_else(|| plan.slurm.array.clone()),
        sweep: options.sweep.clone(),
        config_snapshot_yaml: options.config_snapshot_yaml.clone(),
        cached_artifacts: options.cached_artifacts.clone(),
        provenance: options.provenance.clone(),
        tags: Vec::new(),
        notes: Vec::new(),
    })
}

/// Writes a submission record to the jobs directory and latest pointer.
pub fn write_submission_record(record: &SubmissionRecord) -> Result<()> {
    let metadata_root = metadata_root_for(&record.compose_file);
    let record_path = checked_record_path_for_job_id(&record.compose_file, &record.job_id)?;
    validate_submission_record_location(record, &record_path, &metadata_root, true)?;
    ensure_managed_record_directories(&record.compose_file)?;
    let _record_lock = lock_submission_record(&record_path)?;

    let record_exists = match fs::symlink_metadata(&record_path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "canonical submission record {} is not a regular file",
                record_path.display()
            );
            let existing = validate_submission_record_for_metadata_root(
                read_json(&record_path)?,
                &record_path,
                &metadata_root,
                true,
            )?;
            ensure!(
                submission_records_equal(&existing, record)?,
                "scheduler job id {} is already tracked by a different canonical submission record; refusing to overwrite it",
                record.job_id
            );
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect {}", record_path.display()));
        }
    };
    if !record_exists {
        reject_orphaned_run_evidence(record)?;
    }

    {
        let _latest_lock = latest_pointer_path_for_kind(&record.compose_file, record.kind)
            .as_deref()
            .map(lock_latest_pointer)
            .transpose()?;
        write_json(&record_path, record)?;
        if let Some(latest_path) = latest_pointer_path_for_kind(&record.compose_file, record.kind) {
            write_json(&latest_path, record)?;
        }
    }
    if let Err(error) = initialize_record_run_evidence(record) {
        crate::diagnostics::warn_with_code(
            "run_evidence_degraded",
            format!(
                "tracked job {} was persisted, but its additive run evidence could not be initialized: {error:#}",
                record.job_id
            ),
        );
    }
    Ok(())
}

/// Maximum number of tags one tracked record can carry.
pub const MAX_TAGS_PER_RECORD: usize = 32;
/// Maximum length of one tag, in characters.
pub const MAX_TAG_LEN: usize = 64;
/// Maximum length of one note text, in characters (after trimming).
pub const MAX_NOTE_LEN: usize = 4096;

const SUBMISSION_RECORD_LOCK_TIMEOUT: Duration = Duration::from_secs(30);
const RUN_EVIDENCE_FILE_HASH_LIMIT: u64 = 16 * 1024 * 1024;

fn initialize_record_run_evidence(
    record: &SubmissionRecord,
) -> Result<super::evidence::RunEvidenceState> {
    use super::evidence::{
        ContentDigest, Evidence, InputDigestStrategy, InputEvidence, InputKind, RunEvidenceSeed,
    };

    let config = match record.config_snapshot_yaml.as_deref() {
        Some(snapshot) => Evidence::available(ContentDigest::from_bytes(snapshot.as_bytes())),
        None => Evidence::missing("submission record has no effective config snapshot")?,
    };
    let script = super::evidence::attest_bounded_file(
        "submitted_script",
        InputKind::File,
        &record.script_path,
        RUN_EVIDENCE_FILE_HASH_LIMIT,
    )?
    .content;
    let provenance = match record.provenance.clone() {
        Some(provenance) => Evidence::available(provenance),
        None => Evidence::missing("submission record has no submit-time provenance")?,
    };

    let mut inputs = Vec::new();
    if let Some(provenance) = record.provenance.as_ref() {
        if let Some(source_hash) = provenance.source_content_hash.as_ref() {
            inputs.push(InputEvidence {
                name: "source_snapshot".to_string(),
                kind: InputKind::SourceSnapshot,
                source: record.submit_dir.display().to_string(),
                identity: Evidence::available(format!("sha256:{source_hash}")),
                digest_strategy: InputDigestStrategy::SourceCas,
                materialized_path: None,
                content: Evidence::unsupported(
                    "source CAS identity is preserved without recursively re-hashing the snapshot",
                )?,
            });
        }
        for (service, image_ref) in &provenance.image_refs {
            inputs.push(InputEvidence {
                name: format!("image_{service}"),
                kind: InputKind::ContainerImage,
                source: image_ref.clone(),
                identity: Evidence::available(image_ref.clone()),
                digest_strategy: InputDigestStrategy::ImageReference,
                materialized_path: None,
                content: Evidence::unsupported(
                    "container reference is preserved without hashing provider-managed bytes",
                )?,
            });
        }
    }
    for (index, path) in record.cached_artifacts.iter().enumerate() {
        inputs.push(super::evidence::attest_bounded_file(
            format!("cached_artifact_{index:04}"),
            InputKind::Other,
            path,
            RUN_EVIDENCE_FILE_HASH_LIMIT,
        )?);
    }

    super::evidence::initialize_run_evidence(
        &record.compose_file,
        &RunEvidenceSeed {
            scheduler_job_id: record.job_id.clone(),
            submission_record_sha256: submission_record_identity_sha256(record)?,
            backend: record.backend,
            kind: record.kind,
            submitted_at: record.submitted_at,
            config,
            script,
            provenance,
            inputs,
        },
    )
    .with_context(|| {
        format!(
            "failed to initialize run evidence for job {}",
            record.job_id
        )
    })
}

fn sync_record_run_evidence(record: &SubmissionRecord) -> Result<()> {
    if super::evidence::load_run_evidence(&record.compose_file, &record.job_id)?.is_none() {
        initialize_record_run_evidence(record)?;
    }
    super::evidence::sync_record_annotations(
        &record.compose_file,
        &record.job_id,
        unix_timestamp_now(),
        &record.tags,
        &record.notes,
    )
    .with_context(|| {
        format!(
            "failed to synchronize run evidence for job {}",
            record.job_id
        )
    })?;
    Ok(())
}

/// Returns the per-kind latest pointer file for a compose file, or `None` for
/// kinds without a pointer (sweep trials).
fn latest_pointer_path_for_kind(compose_file: &Path, kind: SubmissionKind) -> Option<PathBuf> {
    match kind {
        SubmissionKind::Main => Some(latest_record_path_for(compose_file)),
        SubmissionKind::Run => Some(latest_run_record_path_for(compose_file)),
        SubmissionKind::Canary => Some(latest_canary_record_path_for(compose_file)),
        SubmissionKind::Notebook => Some(latest_notebook_record_path_for(compose_file)),
        SubmissionKind::SweepTrial => None,
    }
}

/// Applies a post-submit mutation to one tracked submission record and
/// persists it.
///
/// Unlike [`write_submission_record`], this never *repoints* the per-kind
/// `latest*.json` pointer: the pointer file is a full duplicate of the record,
/// so it is rewritten (synced) only when it already names `job_id`. Mutating a
/// non-latest job leaves the pointer file untouched; sweep trials have no
/// pointer and skip the sync entirely. The mutation closure may fail, in which
/// case nothing is written.
pub fn update_submission_record(
    spec_path: &Path,
    job_id: &str,
    mutate: impl FnOnce(&mut SubmissionRecord) -> Result<()>,
) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let record_path = checked_record_path_for_job_id(&compose_file, job_id)?;
    if !managed_record_directories_exist(&compose_file)? {
        bail!(
            "no tracked submission metadata exists for job '{}' under {}",
            job_id,
            metadata_root_for(&compose_file).display()
        );
    }
    if !path_is_regular_file(&record_path)? {
        bail!(
            "no tracked submission metadata exists for job '{}' under {}",
            job_id,
            metadata_root_for(&compose_file).display()
        );
    }
    let _record_lock = lock_submission_record(&record_path)?;
    if !path_is_regular_file(&record_path)? {
        bail!(
            "no tracked submission metadata exists for job '{}' under {}",
            job_id,
            metadata_root_for(&compose_file).display()
        );
    }
    let metadata_root = metadata_root_for(&compose_file);
    let mut record = validate_submission_record_for_metadata_root(
        read_json(&record_path)?,
        &record_path,
        &metadata_root,
        true,
    )?;
    let immutable_identity_before = submission_record_identity_sha256(&record)?;
    mutate(&mut record)?;
    validate_submission_record_location(&record, &record_path, &metadata_root, true)?;
    ensure!(
        submission_record_identity_sha256(&record)? == immutable_identity_before,
        "post-submit mutation changed immutable submission-record identity; only tags and notes may be updated"
    );
    write_json(&record_path, &record)?;
    // Sync (never repoint) the per-kind latest pointer duplicate: rewrite it
    // only when it currently names this job, so mutating an old job can never
    // repoint "latest" at it, while readers of the pointer file (the no-id
    // default paths) never observe a stale copy.
    if let Some(latest_path) = latest_pointer_path_for_kind(&compose_file, record.kind) {
        let _latest_lock = lock_latest_pointer(&latest_path)?;
        if read_latest_pointer_job_id(&metadata_root_for(&compose_file), record.kind)?.as_deref()
            == Some(job_id)
        {
            write_json(&latest_path, &record)?;
        }
    }
    if let Err(error) = sync_record_run_evidence(&record) {
        crate::diagnostics::warn_with_code(
            "run_evidence_degraded",
            format!(
                "tracked job {} was updated, but its additive run evidence could not be synchronized: {error:#}",
                record.job_id
            ),
        );
    }
    Ok(record)
}

/// Validates one tag label: non-empty, at most [`MAX_TAG_LEN`] characters, and
/// only `[A-Za-z0-9._-]` characters.
pub fn validate_tag(tag: &str) -> Result<()> {
    if tag.is_empty() {
        bail!("tag must not be empty");
    }
    if tag.chars().count() > MAX_TAG_LEN {
        bail!("tag '{tag}' is longer than the maximum of {MAX_TAG_LEN} characters");
    }
    if !tag
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        bail!(
            "tag '{tag}' contains unsupported characters; use only letters, digits, '.', '_', and '-'"
        );
    }
    Ok(())
}

/// Applies sorted-set tag semantics to a record's tag list: `add` then `remove`,
/// deduplicated and sorted. Adding a tag that is already present and removing a
/// tag that is absent are idempotent no-ops. Fails without mutating semantics
/// callers care about when a tag is invalid or the result would exceed
/// [`MAX_TAGS_PER_RECORD`] tags.
pub fn apply_tag_changes(
    existing: &mut Vec<String>,
    add: &[String],
    remove: &[String],
) -> Result<()> {
    for tag in add.iter().chain(remove.iter()) {
        validate_tag(tag)?;
    }
    let mut set: BTreeSet<String> = existing.iter().cloned().collect();
    for tag in add {
        set.insert(tag.clone());
    }
    for tag in remove {
        set.remove(tag.as_str());
    }
    if set.len() > MAX_TAGS_PER_RECORD {
        bail!(
            "a tracked record can carry at most {MAX_TAGS_PER_RECORD} tags ({} after this change); remove tags with 'experiment tag --remove <TAG>' first",
            set.len()
        );
    }
    *existing = set.into_iter().collect();
    Ok(())
}

/// Validates and normalizes one note text: trimmed, non-empty, at most
/// [`MAX_NOTE_LEN`] characters. Returns the trimmed text.
pub fn validate_note_text(text: &str) -> Result<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        bail!("note text must not be empty");
    }
    if trimmed.chars().count() > MAX_NOTE_LEN {
        bail!("note text is longer than the maximum of {MAX_NOTE_LEN} characters");
    }
    Ok(trimmed.to_string())
}

/// Appends one timestamped note to a record's append-only note list.
pub fn append_job_note(record: &mut SubmissionRecord, text: &str) -> Result<()> {
    let text = validate_note_text(text)?;
    record.notes.push(JobNote {
        text,
        created_at: unix_timestamp_now(),
    });
    Ok(())
}

/// Removes one tracked submission record and repairs the latest pointer.
///
/// Also reaps host-side per-job state that the batch teardown trap cannot
/// (cancelled/crashed jobs never run it): the per-job enroot runtime cache and
/// this job's owned rendezvous records. Both are job-namespaced / owner-guarded.
pub fn remove_submission_record(record: &SubmissionRecord) -> Result<()> {
    let record_path = checked_record_path_for_job_id(&record.compose_file, &record.job_id)?;
    let metadata_root = metadata_root_for(&record.compose_file);
    validate_submission_record_location(record, &record_path, &metadata_root, true)?;
    validate_managed_record_directories(&record.compose_file)?;
    let _record_lock = lock_submission_record(&record_path)?;
    ensure!(
        path_is_regular_file(&record_path)?,
        "canonical submission record {} is missing or is not a regular file",
        record_path.display()
    );
    let canonical = validate_submission_record_for_metadata_root(
        read_json(&record_path)?,
        &record_path,
        &metadata_root,
        true,
    )?;
    ensure!(
        submission_records_equal(&canonical, record)?,
        "the supplied submission record does not match the canonical record {}; refusing destructive removal",
        record_path.display()
    );
    validate_owned_removal_parents(&canonical, &record_path)?;
    let _evidence_lock =
        super::evidence::lock_run_evidence_for_removal(&canonical.compose_file, &canonical.job_id)?;
    validate_owned_removal_parents(&canonical, &record_path)?;
    remove_path_if_present(&runtime_job_root_for_record(&canonical))?;
    remove_path_if_present(&tracked_paths::enroot_runtime_job_dir(
        &canonical.cache_dir,
        &canonical.job_id,
    ))?;
    remove_path_if_present(&tracked_paths::run_evidence_dir_for(
        &canonical.compose_file,
        &canonical.job_id,
    ))?;
    if canonical.batch_log_managed {
        remove_path_if_present(&canonical.batch_log)?;
    }
    // Best-effort: never block teardown on a stale/unreadable rendezvous file.
    let _ = crate::rendezvous::reap_job_records(&canonical.cache_dir, &canonical.job_id);
    // The canonical record is the retry authority for every owned path above.
    // Commit its deletion, and the matching pointer repair, only after those
    // removals succeed.
    if let Some(latest_path) = latest_pointer_path_for_kind(&canonical.compose_file, canonical.kind)
    {
        let _latest_lock = lock_latest_pointer(&latest_path)?;
        remove_path_if_present(&record_path)?;
        repair_latest_record_for_kind_locked(
            &canonical.compose_file,
            canonical.kind,
            &latest_path,
        )?;
    } else {
        remove_path_if_present(&record_path)?;
    }
    Ok(())
}

/// Loads every tracked job record for the given compose file.
pub fn scan_job_records(spec_path: &Path) -> Result<Vec<SubmissionRecord>> {
    let compose_file = absolute_path(spec_path)?;
    Ok(
        scan_job_records_with_paths(&metadata_root_for(&compose_file))?
            .into_iter()
            .map(|(_, record)| record)
            .collect(),
    )
}

/// Removes tracked job metadata older than the given age in days.
pub fn clean_by_age(spec_path: &Path, age_days: u64) -> Result<CleanupReport> {
    let report = build_cleanup_report(spec_path, CleanupMode::Age { age_days }, false, false)?;
    run_cleanup_report(&report)?;
    Ok(report)
}

/// Removes all tracked job metadata except the latest record.
pub fn clean_all_except_latest(spec_path: &Path) -> Result<CleanupReport> {
    let report = build_cleanup_report(spec_path, CleanupMode::AllExceptLatest, false, false)?;
    run_cleanup_report(&report)?;
    Ok(report)
}

/// Scans the repo tree for tracked job records.
pub fn scan_job_inventory(scan_start: &Path, include_disk_usage: bool) -> Result<JobInventoryScan> {
    let scan_root = repo_root_or_cwd(scan_start);
    scan_job_inventory_from_root(&scan_root, include_disk_usage)
}

pub(crate) fn scan_job_inventory_from_root(
    scan_root: &Path,
    include_disk_usage: bool,
) -> Result<JobInventoryScan> {
    let now = unix_timestamp_now();
    let mut jobs = Vec::new();
    scan_inventory_recursive(scan_root, include_disk_usage, now, &mut jobs)?;
    jobs.sort_by(|left, right| {
        right
            .submitted_at
            .cmp(&left.submitted_at)
            .then_with(|| left.compose_file.cmp(&right.compose_file))
            .then_with(|| left.job_id.cmp(&right.job_id))
    });
    Ok(JobInventoryScan {
        scan_root: scan_root.to_path_buf(),
        jobs,
    })
}

/// Resolves one tracked record by job id by scanning from the nearest repo
/// root or current directory.
pub fn find_submission_record_in_repo(scan_start: &Path, job_id: &str) -> Result<SubmissionRecord> {
    let inventory = scan_job_inventory(scan_start, false)?;
    let matches = inventory
        .jobs
        .into_iter()
        .filter(|entry| entry.job_id == job_id)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => bail!(
            "no tracked submission metadata exists for job '{}' under {}",
            job_id,
            inventory.scan_root.display()
        ),
        [entry] => validate_submission_record_for_metadata_root(
            read_json(&entry.record_path)?,
            &entry.record_path,
            &entry.compose_metadata_root,
            true,
        ),
        _ => bail!(
            "multiple tracked submissions with job id '{}' were found under {}; pass -f/--file to disambiguate",
            job_id,
            inventory.scan_root.display()
        ),
    }
}

/// Builds the tracked-job cleanup report for a compose context.
pub fn build_cleanup_report(
    spec_path: &Path,
    mode: CleanupMode,
    include_disk_usage: bool,
    dry_run: bool,
) -> Result<CleanupReport> {
    let compose_file = absolute_path(spec_path)?;
    let metadata_root = metadata_root_for(&compose_file);
    let now = unix_timestamp_now();
    let latest_pointer_job_id_before =
        read_latest_pointer_job_id(&metadata_root, SubmissionKind::Main)?;
    let inventory =
        build_inventory_entries_for_metadata_root(&metadata_root, include_disk_usage, now)?;
    let latest_job_id_before = inventory
        .iter()
        .find(|entry| entry.kind == SubmissionKind::Main && entry.is_latest)
        .map(|entry| entry.job_id.clone());
    let cutoff = match mode {
        CleanupMode::Age { age_days } => Some(now.saturating_sub(age_days * 86_400)),
        CleanupMode::AllExceptLatest => None,
    };

    let jobs = inventory
        .into_iter()
        .map(|entry| {
            let selected = match mode {
                CleanupMode::Age { .. } => entry.submitted_at < cutoff.unwrap_or(0),
                CleanupMode::AllExceptLatest => !entry.is_latest,
            };
            let removable_paths = removable_paths_for_job(&entry);
            let bytes_reclaimed = if selected && include_disk_usage {
                entry.disk_usage_bytes
            } else {
                None
            };
            CleanupJobReport {
                inventory: entry,
                selected,
                bytes_reclaimed,
                removable_paths,
            }
        })
        .collect::<Vec<_>>();

    let removed_job_ids = jobs
        .iter()
        .filter(|job| job.selected)
        .map(|job| job.inventory.job_id.clone())
        .collect::<Vec<_>>();
    let kept_job_ids = jobs
        .iter()
        .filter(|job| !job.selected)
        .map(|job| job.inventory.job_id.clone())
        .collect::<Vec<_>>();
    let latest_job_id_after = jobs
        .iter()
        .filter(|job| !job.selected && job.inventory.kind == SubmissionKind::Main)
        .max_by(|left, right| compare_records(&left.inventory, &right.inventory))
        .map(|job| job.inventory.job_id.clone());
    let total_bytes_reclaimed = if include_disk_usage {
        Some(
            jobs.iter()
                .filter_map(|job| job.bytes_reclaimed)
                .fold(0_u64, u64::saturating_add),
        )
    } else {
        None
    };

    Ok(CleanupReport {
        compose_file,
        mode: cleanup_mode_label(mode).to_string(),
        dry_run,
        removed_job_ids,
        kept_job_ids,
        latest_pointer_job_id_before,
        latest_job_id_before,
        latest_job_id_after,
        total_bytes_reclaimed,
        jobs,
        deep: None,
    })
}

/// Executes the tracked-job cleanup report generated by [`build_cleanup_report`].
pub fn run_cleanup_report(report: &CleanupReport) -> Result<()> {
    if report.jobs.iter().any(|job| job.selected) {
        validate_managed_record_directories(&report.compose_file)?;
    }
    // Validate the entire destructive plan before removing its first path. The
    // skipped serde field is intentionally not trusted here: library callers
    // can still construct or mutate a report in memory.
    let mut plans = Vec::new();
    for job in report.jobs.iter().filter(|job| job.selected) {
        let claimed_paths = validate_cleanup_job(report, job)?;
        let record_path = job.inventory.record_path.clone();
        let record_lock = lock_submission_record(&record_path)?;
        let metadata = fs::symlink_metadata(&record_path).with_context(|| {
            format!("failed to inspect cleanup record {}", record_path.display())
        })?;
        if !metadata.file_type().is_file() {
            bail!(
                "unsafe cleanup plan for job {:?}: record {} is not a regular file",
                job.inventory.job_id,
                record_path.display()
            );
        }
        let metadata_root = metadata_root_for(&report.compose_file);
        let record = validate_submission_record_for_metadata_root(
            read_json(&record_path)?,
            &record_path,
            &metadata_root,
            true,
        )?;
        validate_owned_removal_parents(&record, &record_path)?;
        let evidence_lock =
            super::evidence::lock_run_evidence_for_removal(&record.compose_file, &record.job_id)?;
        let persisted_paths = removable_paths_from_paths(
            &record_path,
            &runtime_job_root_for_record(&record),
            &metadata_root.join(&record.job_id),
            &tracked_paths::enroot_runtime_job_dir(&record.cache_dir, &record.job_id),
            &tracked_paths::run_evidence_dir_for(&record.compose_file, &record.job_id),
            record
                .batch_log_managed
                .then_some(record.batch_log.as_path()),
        );
        if claimed_paths != persisted_paths {
            bail!(
                "unsafe cleanup plan for job {:?}: inventory paths do not match the persisted submission record",
                job.inventory.job_id
            );
        }
        plans.push((record_lock, evidence_lock, record, persisted_paths));
    }
    // Phase one removes only owned payload paths. Keep every canonical record,
    // record lock, and evidence lock alive until all payload removals succeed,
    // so any failure remains retryable and pointer repair follows the global
    // record -> evidence -> latest lock order.
    for (_record_lock, _evidence_lock, record, paths) in &plans {
        let record_path = checked_record_path_for_job_id(&record.compose_file, &record.job_id)?;
        validate_owned_removal_parents(record, &record_path)?;
        let normalized_record_path = normalized(&record_path);
        for path in paths {
            if normalized(path) == normalized_record_path {
                continue;
            }
            remove_path_if_present(path)?;
        }
    }
    // Phase two commits canonical-record deletion only after every selected
    // job's owned paths are gone. The guards in `plans` intentionally remain
    // live through latest-pointer repair.
    for (_record_lock, _evidence_lock, record, _paths) in &plans {
        let record_path = checked_record_path_for_job_id(&record.compose_file, &record.job_id)?;
        remove_path_if_present(&record_path)?;
    }
    repair_latest_records(&report.compose_file)
}

/// Loads one tracked submission record, defaulting to the latest job.
pub fn load_submission_record(spec_path: &Path, job_id: Option<&str>) -> Result<SubmissionRecord> {
    let compose_file = absolute_path(spec_path)?;
    let _ = managed_record_directories_exist(&compose_file)?;
    let path = match job_id {
        Some(job_id) => checked_record_path_for_job_id(&compose_file, job_id)?,
        None => latest_record_path_for(&compose_file),
    };
    if !path.exists() {
        if let Some(job_id) = job_id {
            bail!(
                "no tracked submission metadata exists for job '{}' under {}; run 'hpc-compose up' for {} first",
                job_id,
                metadata_root_for(&compose_file).display(),
                compose_file.display()
            );
        }
        bail!(
            "no tracked submission metadata exists for {}; run 'hpc-compose up' first",
            compose_file.display()
        );
    }
    validate_submission_record_for_metadata_root(
        read_json(&path)?,
        &path,
        &metadata_root_for(&compose_file),
        job_id.is_some(),
    )
}

/// Like [`load_submission_record`], but a legitimately absent record is a silent
/// `None` while a present-but-broken record (corrupt JSON or failed validation) is
/// a *degraded* `None`: one `WARN` line to stderr naming the path, then `None`.
///
/// Read-only callers (e.g. `stats`) use this so a truncated record no longer makes
/// a tracked job silently vanish, without turning an ordinary "no such job yet" into
/// a hard error.
pub fn load_submission_record_optional(
    spec_path: &Path,
    job_id: Option<&str>,
) -> Option<SubmissionRecord> {
    let compose_file = absolute_path(spec_path).ok()?;
    if let Err(error) = managed_record_directories_exist(&compose_file) {
        crate::diagnostics::warn_with_code(
            "corrupt_submission_record",
            format!("{}: {error:#}", metadata_root_for(&compose_file).display()),
        );
        return None;
    }
    let path = match job_id {
        Some(job_id) => checked_record_path_for_job_id(&compose_file, job_id).ok()?,
        None => latest_record_path_for(&compose_file),
    };
    if !path.exists() {
        return None;
    }
    let record = read_json_optional::<SubmissionRecord>(&path)?;
    match validate_submission_record_for_metadata_root(
        record,
        &path,
        &metadata_root_for(&compose_file),
        job_id.is_some(),
    ) {
        Ok(record) => Some(record),
        Err(err) => {
            crate::diagnostics::warn_with_code(
                "corrupt_submission_record",
                format!("{}: {err:#}", path.display()),
            );
            None
        }
    }
}

#[cfg(test)]
fn validate_submission_record(record: SubmissionRecord, path: &Path) -> Result<SubmissionRecord> {
    validate_submission_record_fields(&record, path)?;
    Ok(record)
}

fn validate_submission_record_fields(record: &SubmissionRecord, path: &Path) -> Result<()> {
    // Guard teardown/cleanup against a corrupt or hand-tampered record: an empty
    // job id would collapse the per-job runtime/enroot paths to their shared
    // parents (e.g. `<cache_dir>/runtime/`), so refuse to use such a record.
    if record.job_id.trim().is_empty() {
        bail!(
            "submission record {} has an empty job id; refusing to use it for tracking/cleanup",
            path.display()
        );
    }
    validate_job_id_component(&record.job_id, path)?;
    if record.schema_version > SUBMISSION_SCHEMA_VERSION {
        bail!(
            "submission record {} uses schema version {} but this version of hpc-compose only supports up to {}; please upgrade hpc-compose",
            path.display(),
            record.schema_version,
            SUBMISSION_SCHEMA_VERSION
        );
    }
    Ok(())
}

fn validate_job_id_component(job_id: &str, path: &Path) -> Result<()> {
    let mut components = Path::new(job_id).components();
    let is_one_normal_component = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(component)), None)
            if component == std::ffi::OsStr::new(job_id)
    );
    if job_id.trim() != job_id || !is_one_normal_component {
        bail!(
            "submission record {} has job id {:?}, which is not a safe path component; refusing to use it for tracking/cleanup",
            path.display(),
            job_id
        );
    }
    Ok(())
}

fn checked_record_path_for_job_id(compose_file: &Path, job_id: &str) -> Result<PathBuf> {
    let jobs_dir = jobs_dir_for(compose_file);
    validate_job_id_component(job_id, &jobs_dir)?;
    Ok(jobs_dir.join(format!("{job_id}.json")))
}

fn managed_directory_exists(path: &Path, label: &str) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to inspect {label} {}", path.display()));
        }
    };
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "managed {label} {} must be a real directory, not a symlink or another file type",
        path.display()
    );
    Ok(true)
}

fn create_managed_directory(path: &Path, label: &str) -> Result<()> {
    if managed_directory_exists(path, label)? {
        return Ok(());
    }
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to create managed {label} {}", path.display()));
        }
    }
    ensure!(
        managed_directory_exists(path, label)?,
        "managed {label} {} disappeared after creation",
        path.display()
    );
    Ok(())
}

fn ensure_managed_record_directories(compose_file: &Path) -> Result<()> {
    let metadata_root = metadata_root_for(compose_file);
    create_managed_directory(&metadata_root, "metadata directory")?;
    create_managed_directory(
        &metadata_root.join(tracked_paths::JOBS_DIR_NAME),
        "jobs directory",
    )
}

fn validate_managed_record_directories(compose_file: &Path) -> Result<()> {
    ensure!(
        managed_record_directories_exist(compose_file)?,
        "managed record directories for {} do not exist",
        compose_file.display()
    );
    Ok(())
}

fn managed_record_directories_exist(compose_file: &Path) -> Result<bool> {
    let metadata_root = metadata_root_for(compose_file);
    if !managed_directory_exists(&metadata_root, "metadata directory")? {
        return Ok(false);
    }
    let jobs_dir = metadata_root.join(tracked_paths::JOBS_DIR_NAME);
    managed_directory_exists(&jobs_dir, "jobs directory")
}

fn path_is_regular_file(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
                "managed record {} is not a regular file",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("failed to inspect managed record {}", path.display())),
    }
}

fn submission_records_equal(left: &SubmissionRecord, right: &SubmissionRecord) -> Result<bool> {
    let left =
        serde_json::to_value(left).context("failed to compare canonical submission record")?;
    let right =
        serde_json::to_value(right).context("failed to compare candidate submission record")?;
    Ok(left == right)
}

#[derive(Serialize)]
struct SubmissionRecordIdentityV1<'a> {
    identity_schema_version: u32,
    record_schema_version: u32,
    backend: SubmissionBackend,
    kind: SubmissionKind,
    job_id: &'a str,
    submitted_at: u64,
    compose_file: &'a Path,
    submit_dir: &'a Path,
    script_path: &'a Path,
    cache_dir: &'a Path,
    runtime_root: Option<&'a Path>,
    batch_log: &'a Path,
    batch_log_managed: bool,
    service_logs: &'a BTreeMap<String, PathBuf>,
    artifact_export_dir: Option<&'a str>,
    resume_dir: Option<&'a Path>,
    service_name: Option<&'a str>,
    command_override: Option<&'a [String]>,
    requested_walltime: Option<&'a RequestedWalltime>,
    slurm_array: Option<&'a str>,
    sweep: Option<&'a SweepTrialMetadata>,
    config_snapshot_yaml: Option<&'a str>,
    cached_artifacts: &'a [PathBuf],
    provenance: Option<&'a JobProvenance>,
}

pub(super) fn submission_record_identity_sha256(record: &SubmissionRecord) -> Result<String> {
    // This is an intentionally frozen v1 projection. Future additive record
    // fields cannot silently change an existing protocol identity; incorporating
    // one requires an explicit projection/schema decision. Tags and notes are
    // excluded because they are represented by post-submit evidence events.
    let identity = SubmissionRecordIdentityV1 {
        identity_schema_version: 1,
        record_schema_version: record.schema_version,
        backend: record.backend,
        kind: record.kind,
        job_id: &record.job_id,
        submitted_at: record.submitted_at,
        compose_file: &record.compose_file,
        submit_dir: &record.submit_dir,
        script_path: &record.script_path,
        cache_dir: &record.cache_dir,
        runtime_root: record.runtime_root.as_deref(),
        batch_log: &record.batch_log,
        batch_log_managed: record.batch_log_managed,
        service_logs: &record.service_logs,
        artifact_export_dir: record.artifact_export_dir.as_deref(),
        resume_dir: record.resume_dir.as_deref(),
        service_name: record.service_name.as_deref(),
        command_override: record.command_override.as_deref(),
        requested_walltime: record.requested_walltime.as_ref(),
        slurm_array: record.slurm_array.as_deref(),
        sweep: record.sweep.as_ref(),
        config_snapshot_yaml: record.config_snapshot_yaml.as_deref(),
        cached_artifacts: &record.cached_artifacts,
        provenance: record.provenance.as_ref(),
    };
    let bytes = serde_json::to_vec(&identity)
        .context("failed to serialize immutable submission-record identity")?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn reject_orphaned_run_evidence(record: &SubmissionRecord) -> Result<()> {
    let evidence_root = tracked_paths::run_evidence_dir_for(&record.compose_file, &record.job_id);
    match fs::symlink_metadata(&evidence_root) {
        Ok(_) => bail!(
            "run evidence {} exists for scheduler job id {} but no canonical submission record exists; refusing to pair a new record with orphaned evidence",
            evidence_root.display(),
            record.job_id
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to inspect run evidence {}", evidence_root.display())),
    }
}

fn submission_record_lock_path(record_path: &Path) -> PathBuf {
    record_path.with_extension("json.lock")
}

fn lock_submission_record(record_path: &Path) -> Result<crate::secure_io::StrictFlockGuard> {
    let lock_path = submission_record_lock_path(record_path);
    crate::secure_io::acquire_flock_strict(
        &lock_path,
        crate::secure_io::LockKind::Exclusive,
        SUBMISSION_RECORD_LOCK_TIMEOUT,
    )
    .with_context(|| format!("failed to lock submission record {}", record_path.display()))
}

fn latest_pointer_lock_path(latest_path: &Path) -> PathBuf {
    latest_path.with_extension("json.lock")
}

fn lock_latest_pointer(latest_path: &Path) -> Result<crate::secure_io::StrictFlockGuard> {
    let lock_path = latest_pointer_lock_path(latest_path);
    if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
        ensure!(
            metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
            "latest-pointer lock {} is not a regular file",
            lock_path.display()
        );
    }
    crate::secure_io::acquire_flock_strict(
        &lock_path,
        crate::secure_io::LockKind::Exclusive,
        SUBMISSION_RECORD_LOCK_TIMEOUT,
    )
    .with_context(|| format!("failed to lock latest pointer {}", latest_path.display()))
}

fn normalized(path: &Path) -> PathBuf {
    crate::path_util::normalize_path(path.to_path_buf())
}

fn paths_equivalent(left: &Path, right: &Path) -> bool {
    normalized(left) == normalized(right)
        || match (fs::canonicalize(left), fs::canonicalize(right)) {
            (Ok(left), Ok(right)) => left == right,
            _ => false,
        }
}

fn validate_direct_job_child(
    label: &str,
    candidate: &Path,
    parent: &Path,
    job_id: &str,
    record_path: &Path,
) -> Result<()> {
    let parent = normalized(parent);
    let candidate = normalized(candidate);
    if !parent.is_absolute()
        || !candidate.is_absolute()
        || candidate.parent() != Some(parent.as_path())
        || candidate.file_name() != Some(std::ffi::OsStr::new(job_id))
    {
        bail!(
            "submission record {} has unsafe {label} {}; expected a direct per-job child named {:?} under {}",
            record_path.display(),
            candidate.display(),
            job_id,
            parent.display()
        );
    }
    Ok(())
}

fn runtime_root_parent_for_record(record: &SubmissionRecord) -> PathBuf {
    record
        .runtime_root
        .clone()
        .unwrap_or_else(|| tracked_paths::runtime_root_for(&record.submit_dir))
}

fn expected_managed_batch_log(record: &SubmissionRecord) -> PathBuf {
    let filename = tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN.replace("%j", &record.job_id);
    runtime_root_parent_for_record(record)
        .join(tracked_paths::LOGS_DIR_NAME)
        .join(filename)
}

fn validate_submission_record_location(
    record: &SubmissionRecord,
    record_path: &Path,
    metadata_root: &Path,
    require_job_filename: bool,
) -> Result<()> {
    validate_submission_record_fields(record, record_path)?;
    let metadata_root = normalized(metadata_root);
    let record_metadata_root = normalized(&metadata_root_for(&record.compose_file));
    if !paths_equivalent(&record_metadata_root, &metadata_root) {
        bail!(
            "submission record {} belongs to metadata root {}, not {}",
            record_path.display(),
            record_metadata_root.display(),
            metadata_root.display()
        );
    }
    if require_job_filename {
        let expected_record_path = metadata_root
            .join(tracked_paths::JOBS_DIR_NAME)
            .join(format!("{}.json", record.job_id));
        if normalized(record_path) != expected_record_path {
            bail!(
                "submission record {} names job {:?}, but its tracked filename must be {}",
                record_path.display(),
                record.job_id,
                expected_record_path.display()
            );
        }
    }

    let runtime_parent = runtime_root_parent_for_record(record);
    validate_direct_job_child(
        "runtime root",
        &runtime_job_root_for_record(record),
        &runtime_parent,
        &record.job_id,
        record_path,
    )?;
    validate_direct_job_child(
        "legacy runtime root",
        &metadata_root.join(&record.job_id),
        &metadata_root,
        &record.job_id,
        record_path,
    )?;
    let runtime_cache_parent = record
        .cache_dir
        .join(tracked_paths::ENROOT_RUNTIME_DIR_NAME);
    validate_direct_job_child(
        "runtime cache directory",
        &tracked_paths::enroot_runtime_job_dir(&record.cache_dir, &record.job_id),
        &runtime_cache_parent,
        &record.job_id,
        record_path,
    )?;
    if record.batch_log_managed {
        let expected = normalized(&expected_managed_batch_log(record));
        if normalized(&record.batch_log) != expected {
            bail!(
                "submission record {} marks batch log {} as managed, but the owned path is {}",
                record_path.display(),
                record.batch_log.display(),
                expected.display()
            );
        }
    }
    Ok(())
}

fn validate_submission_record_for_metadata_root(
    record: SubmissionRecord,
    record_path: &Path,
    metadata_root: &Path,
    require_job_filename: bool,
) -> Result<SubmissionRecord> {
    validate_submission_record_location(&record, record_path, metadata_root, require_job_filename)?;
    Ok(record)
}

fn validate_managed_removal_parent(label: &str, parent: &Path) -> Result<()> {
    match fs::symlink_metadata(parent) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
                "managed {label} parent {} must be a real directory; refusing destructive removal through a symlink or another file type",
                parent.display()
            );
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to inspect managed {label} parent {}",
                parent.display()
            )
        }),
    }
}

fn validate_owned_removal_parents(record: &SubmissionRecord, record_path: &Path) -> Result<()> {
    let record_parent = record_path
        .parent()
        .context("canonical submission record has no parent")?;
    validate_managed_removal_parent("record", record_parent)?;

    let runtime_parent = runtime_root_parent_for_record(record);
    validate_managed_removal_parent("runtime", &runtime_parent)?;

    let metadata_root = metadata_root_for(&record.compose_file);
    validate_managed_removal_parent("legacy runtime", &metadata_root)?;

    let runtime_cache_parent = record
        .cache_dir
        .join(tracked_paths::ENROOT_RUNTIME_DIR_NAME);
    validate_managed_removal_parent("runtime cache", &runtime_cache_parent)?;

    let evidence_parent = metadata_root.join(tracked_paths::RUN_EVIDENCE_DIR_NAME);
    validate_managed_removal_parent("run evidence", &evidence_parent)?;

    if record.batch_log_managed {
        let batch_parent = record
            .batch_log
            .parent()
            .context("managed batch log has no parent")?;
        validate_managed_removal_parent("batch log", batch_parent)?;
    }
    Ok(())
}

/// Returns the tracked log directory for a submission record.
pub fn log_dir_for_record(record: &SubmissionRecord) -> PathBuf {
    record
        .service_logs
        .values()
        .next()
        .and_then(|path| path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| tracked_paths::latest_logs_dir(&runtime_job_root_for_record(record)))
}

/// Returns the tracked runtime root for a submission record.
///
/// Records that carry an explicit `runtime_root` override (schema v3+) address
/// `<runtime_root>/<job_id>`; older records and default-layout records fall back
/// to `<submit_dir>/.hpc-compose/<job_id>`.
pub fn runtime_job_root_for_record(record: &SubmissionRecord) -> PathBuf {
    match &record.runtime_root {
        Some(runtime_root) => runtime_root.join(&record.job_id),
        None => tracked_paths::runtime_job_root(&record.submit_dir, &record.job_id),
    }
}

/// Returns the tracked runtime state path for a submission record.
pub fn state_path_for_record(record: &SubmissionRecord) -> PathBuf {
    tracked_paths::latest_state_path(&runtime_job_root_for_record(record))
}

fn scan_inventory_recursive(
    root: &Path,
    include_disk_usage: bool,
    now: u64,
    jobs: &mut Vec<JobInventoryEntry>,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }

    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context(format!("failed to read {}", root.display())),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).context(format!("failed to read entry under {}", root.display()));
            }
        };
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err).context(format!("failed to stat {}", path.display())),
        };
        if !file_type.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == tracked_paths::METADATA_DIR_NAME {
            jobs.extend(build_inventory_entries_for_metadata_root(
                &path,
                include_disk_usage,
                now,
            )?);
            continue;
        }
        // Prune directories that never hold tracked job metadata so the scan stays
        // bounded on large working trees. Do not skip every hidden directory:
        // gitignored work dirs such as `.tmp/...` can contain compose-local
        // `.hpc-compose` metadata from real smoke runs.
        if matches!(
            name.as_ref(),
            ".git"
                | ".hg"
                | ".svn"
                | ".mypy_cache"
                | ".pytest_cache"
                | ".ruff_cache"
                | ".tox"
                | ".venv"
                | "target"
                | "node_modules"
                | "__pycache__"
                | "venv"
        ) {
            continue;
        }
        scan_inventory_recursive(&path, include_disk_usage, now, jobs)?;
    }

    Ok(())
}

fn build_inventory_entries_for_metadata_root(
    metadata_root: &Path,
    include_disk_usage: bool,
    now: u64,
) -> Result<Vec<JobInventoryEntry>> {
    let records = scan_job_records_with_paths(metadata_root)?;
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let latest_main_job_id = resolved_latest_job_id(metadata_root, &records, SubmissionKind::Main);
    let latest_run_job_id = resolved_latest_job_id(metadata_root, &records, SubmissionKind::Run);
    let latest_canary_job_id =
        resolved_latest_job_id(metadata_root, &records, SubmissionKind::Canary);
    let latest_notebook_job_id =
        resolved_latest_job_id(metadata_root, &records, SubmissionKind::Notebook);
    let mut inventory = Vec::with_capacity(records.len());
    for (record_path, record) in records {
        let runtime_job_root = runtime_job_root_for_record(&record);
        let legacy_runtime_job_root = metadata_root.join(&record.job_id);
        let runtime_cache_dir =
            tracked_paths::enroot_runtime_job_dir(&record.cache_dir, &record.job_id);
        let batch_log = record.batch_log.clone();
        let removable_paths = removable_paths_from_paths(
            &record_path,
            &runtime_job_root,
            &legacy_runtime_job_root,
            &runtime_cache_dir,
            &tracked_paths::run_evidence_dir_for(&record.compose_file, &record.job_id),
            record.batch_log_managed.then_some(batch_log.as_path()),
        );
        let disk_usage_bytes = if include_disk_usage {
            Some(size_of_paths(&removable_paths)?)
        } else {
            None
        };
        inventory.push(JobInventoryEntry {
            compose_file: record.compose_file.clone(),
            compose_metadata_root: metadata_root.to_path_buf(),
            job_id: record.job_id.clone(),
            kind: record.kind,
            is_latest: match record.kind {
                SubmissionKind::Main => {
                    latest_main_job_id.as_deref() == Some(record.job_id.as_str())
                }
                SubmissionKind::Run => latest_run_job_id.as_deref() == Some(record.job_id.as_str()),
                SubmissionKind::Canary => {
                    latest_canary_job_id.as_deref() == Some(record.job_id.as_str())
                }
                SubmissionKind::Notebook => {
                    latest_notebook_job_id.as_deref() == Some(record.job_id.as_str())
                }
                SubmissionKind::SweepTrial => false,
            },
            submitted_at: record.submitted_at,
            age_seconds: now.saturating_sub(record.submitted_at),
            submit_dir: record.submit_dir.clone(),
            record_path,
            runtime_job_root_present: runtime_job_root.exists(),
            runtime_job_root,
            legacy_runtime_job_root_present: legacy_runtime_job_root.exists(),
            legacy_runtime_job_root,
            runtime_cache_dir_present: runtime_cache_dir.exists(),
            runtime_cache_dir,
            batch_log,
            batch_log_managed: record.batch_log_managed,
            disk_usage_bytes,
            tags: record.tags.clone(),
            note_count: record.notes.len(),
        });
    }

    Ok(inventory)
}

fn scan_job_records_with_paths(metadata_root: &Path) -> Result<Vec<(PathBuf, SubmissionRecord)>> {
    if !managed_directory_exists(metadata_root, "metadata directory")? {
        return Ok(Vec::new());
    }
    let jobs_dir = metadata_root.join(tracked_paths::JOBS_DIR_NAME);
    if !managed_directory_exists(&jobs_dir, "jobs directory")? {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    for entry in
        fs::read_dir(&jobs_dir).context(format!("failed to read {}", jobs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if !entry.file_type()?.is_file() {
            crate::diagnostics::warn_with_code(
                "corrupt_job_record",
                format!(
                    "ignoring job record {} because it is not a regular file",
                    path.display()
                ),
            );
            continue;
        }
        match read_json::<SubmissionRecord>(&path).and_then(|record| {
            validate_submission_record_for_metadata_root(record, &path, metadata_root, true)
        }) {
            Ok(record) => records.push((path, record)),
            Err(err) => crate::diagnostics::warn_with_code(
                "corrupt_job_record",
                format!("ignoring corrupt job record {} ({err})", path.display()),
            ),
        }
    }
    Ok(records)
}

fn read_latest_pointer_job_id(
    metadata_root: &Path,
    kind: SubmissionKind,
) -> Result<Option<String>> {
    let latest_path = match kind {
        SubmissionKind::Main => metadata_root.join(tracked_paths::LATEST_RECORD_FILE_NAME),
        SubmissionKind::Run => metadata_root.join(tracked_paths::RUN_LATEST_RECORD_FILE_NAME),
        SubmissionKind::Canary => metadata_root.join(tracked_paths::CANARY_LATEST_RECORD_FILE_NAME),
        SubmissionKind::Notebook => {
            metadata_root.join(tracked_paths::NOTEBOOK_LATEST_RECORD_FILE_NAME)
        }
        SubmissionKind::SweepTrial => return Ok(None),
    };
    if !path_is_regular_file(&latest_path)? {
        return Ok(None);
    }
    let record = validate_submission_record_for_metadata_root(
        read_json(&latest_path)?,
        &latest_path,
        metadata_root,
        false,
    )?;
    Ok(Some(record.job_id))
}

fn resolved_latest_job_id(
    metadata_root: &Path,
    records: &[(PathBuf, SubmissionRecord)],
    kind: SubmissionKind,
) -> Option<String> {
    let filtered = records
        .iter()
        .filter(|(_, record)| record.kind == kind)
        .collect::<Vec<_>>();
    let newest = filtered
        .iter()
        .max_by(|(_, left), (_, right)| compare_submission_records(left, right))?;
    let latest_path = match kind {
        SubmissionKind::Main => metadata_root.join(tracked_paths::LATEST_RECORD_FILE_NAME),
        SubmissionKind::Run => metadata_root.join(tracked_paths::RUN_LATEST_RECORD_FILE_NAME),
        SubmissionKind::Canary => metadata_root.join(tracked_paths::CANARY_LATEST_RECORD_FILE_NAME),
        SubmissionKind::Notebook => {
            metadata_root.join(tracked_paths::NOTEBOOK_LATEST_RECORD_FILE_NAME)
        }
        SubmissionKind::SweepTrial => return None,
    };
    if latest_path.exists()
        && let Ok(latest) = read_json::<SubmissionRecord>(&latest_path)
        && let Some((_, pointed_record)) = filtered
            .iter()
            .find(|(_, record)| record.job_id == latest.job_id)
        && compare_submission_records(pointed_record, &newest.1) != Ordering::Less
    {
        return Some(latest.job_id);
    }

    Some(newest.1.job_id.clone())
}

fn cleanup_mode_label(mode: CleanupMode) -> &'static str {
    match mode {
        CleanupMode::Age { .. } => "age",
        CleanupMode::AllExceptLatest => "all_except_latest",
    }
}

fn compare_records(left: &JobInventoryEntry, right: &JobInventoryEntry) -> Ordering {
    left.submitted_at
        .cmp(&right.submitted_at)
        .then_with(|| left.job_id.cmp(&right.job_id))
}

fn compare_submission_records(left: &SubmissionRecord, right: &SubmissionRecord) -> Ordering {
    left.submitted_at
        .cmp(&right.submitted_at)
        .then_with(|| left.job_id.cmp(&right.job_id))
}

fn removable_paths_for_job(job: &JobInventoryEntry) -> Vec<PathBuf> {
    removable_paths_from_paths(
        &job.record_path,
        &job.runtime_job_root,
        &job.legacy_runtime_job_root,
        &job.runtime_cache_dir,
        &tracked_paths::run_evidence_dir_for(&job.compose_file, &job.job_id),
        job.batch_log_managed.then_some(job.batch_log.as_path()),
    )
}

fn validate_cleanup_job(report: &CleanupReport, job: &CleanupJobReport) -> Result<Vec<PathBuf>> {
    let inventory = &job.inventory;
    let fail = |message: String| {
        anyhow::anyhow!(
            "unsafe cleanup plan for job {:?}: {message}",
            inventory.job_id
        )
    };
    validate_job_id_component(&inventory.job_id, &inventory.record_path)
        .map_err(|err| fail(err.to_string()))?;

    let compose_file = normalized(&report.compose_file);
    if normalized(&inventory.compose_file) != compose_file {
        return Err(fail(format!(
            "record compose file {} does not match report compose file {}",
            inventory.compose_file.display(),
            report.compose_file.display()
        )));
    }
    let metadata_root = normalized(&metadata_root_for(&compose_file));
    if normalized(&inventory.compose_metadata_root) != metadata_root {
        return Err(fail(format!(
            "metadata root {} does not match {}",
            inventory.compose_metadata_root.display(),
            metadata_root.display()
        )));
    }
    let expected_record_path = metadata_root
        .join(tracked_paths::JOBS_DIR_NAME)
        .join(format!("{}.json", inventory.job_id));
    if normalized(&inventory.record_path) != expected_record_path {
        return Err(fail(format!(
            "record path {} does not match {}",
            inventory.record_path.display(),
            expected_record_path.display()
        )));
    }

    let runtime_parent = inventory.runtime_job_root.parent().ok_or_else(|| {
        fail(format!(
            "runtime root {} has no parent",
            inventory.runtime_job_root.display()
        ))
    })?;
    validate_direct_job_child(
        "runtime root",
        &inventory.runtime_job_root,
        runtime_parent,
        &inventory.job_id,
        &inventory.record_path,
    )
    .map_err(|err| fail(err.to_string()))?;
    let expected_legacy_root = metadata_root.join(&inventory.job_id);
    if normalized(&inventory.legacy_runtime_job_root) != expected_legacy_root {
        return Err(fail(format!(
            "legacy runtime root {} does not match {}",
            inventory.legacy_runtime_job_root.display(),
            expected_legacy_root.display()
        )));
    }
    let runtime_cache_parent = inventory.runtime_cache_dir.parent().ok_or_else(|| {
        fail(format!(
            "runtime cache directory {} has no parent",
            inventory.runtime_cache_dir.display()
        ))
    })?;
    if runtime_cache_parent.file_name()
        != Some(std::ffi::OsStr::new(tracked_paths::ENROOT_RUNTIME_DIR_NAME))
    {
        return Err(fail(format!(
            "runtime cache directory {} is not under a managed '{}' directory",
            inventory.runtime_cache_dir.display(),
            tracked_paths::ENROOT_RUNTIME_DIR_NAME
        )));
    }
    validate_direct_job_child(
        "runtime cache directory",
        &inventory.runtime_cache_dir,
        runtime_cache_parent,
        &inventory.job_id,
        &inventory.record_path,
    )
    .map_err(|err| fail(err.to_string()))?;
    if inventory.batch_log_managed {
        let filename =
            tracked_paths::DEFAULT_BATCH_LOG_FILE_PATTERN.replace("%j", &inventory.job_id);
        let expected_batch_log = normalized(
            &runtime_parent
                .join(tracked_paths::LOGS_DIR_NAME)
                .join(filename),
        );
        if normalized(&inventory.batch_log) != expected_batch_log {
            return Err(fail(format!(
                "managed batch log {} does not match {}",
                inventory.batch_log.display(),
                expected_batch_log.display()
            )));
        }
    }

    let expected_paths = removable_paths_for_job(inventory);
    if job.removable_paths != expected_paths {
        return Err(fail(
            "removable paths differ from the paths derived from validated inventory".into(),
        ));
    }
    Ok(expected_paths)
}

fn removable_paths_from_paths(
    record_path: &Path,
    runtime_job_root: &Path,
    legacy_runtime_job_root: &Path,
    runtime_cache_dir: &Path,
    run_evidence_dir: &Path,
    managed_batch_log: Option<&Path>,
) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for path in [
        runtime_job_root,
        legacy_runtime_job_root,
        runtime_cache_dir,
        run_evidence_dir,
    ]
    .into_iter()
    .chain(managed_batch_log)
    .chain(std::iter::once(record_path))
    {
        // Tolerate an empty path (e.g. the serde default for an inventory entry
        // persisted before this field existed) so we never `rm` a bare/relative path.
        if path.as_os_str().is_empty() {
            continue;
        }
        let normalized = crate::path_util::normalize_path(path.to_path_buf());
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    out
}

fn size_of_paths(paths: &[PathBuf]) -> Result<u64> {
    paths.iter().try_fold(0_u64, |total, path| {
        Ok(total.saturating_add(size_of_path(path)?))
    })
}

fn size_of_path(path: &Path) -> Result<u64> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err).context(format!("failed to stat {}", path.display())),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() || file_type.is_file() {
        return Ok(metadata.len());
    }
    if !file_type.is_dir() {
        return Ok(0);
    }

    let mut total = metadata.len();
    for entry in fs::read_dir(path).context(format!("failed to read {}", path.display()))? {
        let entry = entry?;
        total = total.saturating_add(size_of_path(&entry.path())?);
    }
    Ok(total)
}

fn remove_path_if_present(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context(format!("failed to stat {}", path.display())),
    };

    let file_type = metadata.file_type();
    if file_type.is_dir() && !file_type.is_symlink() {
        let mut attempts = 0;
        loop {
            match fs::remove_dir_all(path) {
                Ok(()) => break,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
                Err(err)
                    if err.kind() == std::io::ErrorKind::DirectoryNotEmpty && attempts < 20 =>
                {
                    attempts += 1;
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => {
                    return Err(err).context(format!("failed to remove {}", path.display()));
                }
            }
        }
    } else {
        fs::remove_file(path).context(format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

fn repair_latest_records(compose_file: &Path) -> Result<()> {
    if !managed_record_directories_exist(compose_file)? {
        return Ok(());
    }
    repair_latest_record_for_kind(compose_file, SubmissionKind::Main)?;
    repair_latest_record_for_kind(compose_file, SubmissionKind::Run)?;
    repair_latest_record_for_kind(compose_file, SubmissionKind::Canary)?;
    repair_latest_record_for_kind(compose_file, SubmissionKind::Notebook)
}

fn repair_latest_record_for_kind(compose_file: &Path, kind: SubmissionKind) -> Result<()> {
    let Some(latest_path) = latest_pointer_path_for_kind(compose_file, kind) else {
        return Ok(());
    };
    let _latest_lock = lock_latest_pointer(&latest_path)?;
    repair_latest_record_for_kind_locked(compose_file, kind, &latest_path)
}

fn repair_latest_record_for_kind_locked(
    compose_file: &Path,
    kind: SubmissionKind,
    latest_path: &Path,
) -> Result<()> {
    // The scan belongs inside the stable pointer transaction. Otherwise a
    // concurrent submission can publish a newer record after the scan and be
    // overwritten by a repair based on stale input.
    let records = scan_job_records(compose_file)?;
    if let Some(latest) = records
        .iter()
        .filter(|record| record.kind == kind)
        .max_by(|left, right| compare_submission_records(left, right))
    {
        write_json(latest_path, latest)
    } else if latest_path.exists() {
        fs::remove_file(latest_path).context(format!("failed to remove {}", latest_path.display()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn removable_paths_deduplicates_identical_paths() {
        let record = PathBuf::from("/tmp/job/42.json");
        let runtime = PathBuf::from("/tmp/job/42");
        let legacy = PathBuf::from("/tmp/job/42");
        let cache = PathBuf::from("/tmp/job/42");
        let paths =
            removable_paths_from_paths(&record, &runtime, &legacy, &cache, &cache, Some(&cache));
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn removable_paths_keeps_distinct_paths() {
        let record = PathBuf::from("/tmp/a/42.json");
        let runtime = PathBuf::from("/tmp/b/42");
        let legacy = PathBuf::from("/tmp/c/42");
        let cache = PathBuf::from("/tmp/d/42");
        let evidence = PathBuf::from("/tmp/e/42");
        let batch = PathBuf::from("/tmp/f/42.out");
        let paths =
            removable_paths_from_paths(&record, &runtime, &legacy, &cache, &evidence, Some(&batch));
        assert_eq!(paths.len(), 6);
    }

    #[test]
    fn removable_paths_skips_empty_runtime_cache_dir() {
        let record = PathBuf::from("/tmp/a/42.json");
        let runtime = PathBuf::from("/tmp/b/42");
        let legacy = PathBuf::from("/tmp/c/42");
        let cache = PathBuf::new();
        let paths = removable_paths_from_paths(&record, &runtime, &legacy, &cache, &cache, None);
        assert_eq!(paths.len(), 3);
    }

    #[test]
    fn cleanup_mode_label_matches_variants() {
        assert_eq!(cleanup_mode_label(CleanupMode::Age { age_days: 7 }), "age");
        assert_eq!(
            cleanup_mode_label(CleanupMode::AllExceptLatest),
            "all_except_latest"
        );
    }

    #[test]
    fn size_of_path_returns_zero_for_missing() {
        let missing = PathBuf::from("/definitely/does/not/exist/xyz");
        assert_eq!(size_of_path(&missing).expect("size"), 0);
    }

    #[test]
    fn size_of_path_measures_file_and_directory() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("data.bin");
        fs::write(&file, [0u8; 100]).expect("write");
        assert!(size_of_path(&file).expect("file size") >= 100);

        let sub = tmpdir.path().join("sub");
        fs::create_dir_all(&sub).expect("dir");
        fs::write(sub.join("inner.txt"), "hi").expect("inner");
        let dir_size = size_of_path(tmpdir.path()).expect("dir size");
        assert!(dir_size > 0);
    }

    #[test]
    fn remove_path_if_present_is_ok_for_missing() {
        let missing = PathBuf::from("/definitely/does/not/exist/xyz");
        remove_path_if_present(&missing).expect("no error for missing");
    }

    #[test]
    fn remove_path_if_present_removes_file_and_dir() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let file = tmpdir.path().join("f.txt");
        fs::write(&file, "x").expect("write");
        remove_path_if_present(&file).expect("remove file");
        assert!(!file.exists());

        let dir = tmpdir.path().join("d");
        fs::create_dir_all(&dir).expect("dir");
        fs::write(dir.join("inner.txt"), "x").expect("write");
        remove_path_if_present(&dir).expect("remove dir");
        assert!(!dir.exists());
    }

    #[test]
    fn read_json_optional_returns_none_for_missing() {
        let missing = PathBuf::from("/definitely/does/not/exist/state.json");
        let value: Option<SubmissionRecord> = read_json_optional(&missing);
        assert!(value.is_none());
    }

    #[test]
    fn read_json_optional_returns_none_for_corrupt() {
        // A present-but-truncated JSON must degrade to None without panicking (the
        // caller keeps its fall-through), while still being distinguishable from the
        // legitimately-absent case handled above.
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("state.json");
        fs::write(&path, b"{ \"job_id\": \"42\", ").expect("write corrupt");
        let value: Option<SubmissionRecord> = read_json_optional(&path);
        assert!(value.is_none());
    }

    #[test]
    fn load_submission_record_optional_missing_is_none() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let spec = tmpdir.path().join("compose.yaml");
        // No job records written at all: absent, so a silent None.
        assert!(load_submission_record_optional(&spec, Some("42")).is_none());
    }

    #[test]
    fn load_submission_record_optional_corrupt_is_none() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let spec = tmpdir.path().join("compose.yaml");
        let jobs_dir = jobs_dir_for(&spec);
        fs::create_dir_all(&jobs_dir).expect("jobs dir");
        fs::write(jobs_dir.join("42.json"), b"{ truncated").expect("write corrupt record");
        // Present-but-broken: degrades to None (warns) rather than vanishing silently
        // or hard-failing.
        assert!(load_submission_record_optional(&spec, Some("42")).is_none());
    }

    #[test]
    fn scan_inventory_recursive_skips_git_and_target() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        fs::create_dir_all(tmpdir.path().join(".git/objects")).expect("git");
        fs::create_dir_all(tmpdir.path().join("target/debug")).expect("target");
        fs::create_dir_all(
            tmpdir
                .path()
                .join(format!("{}/jobs", tracked_paths::METADATA_DIR_NAME)),
        )
        .expect("meta");
        let mut jobs = Vec::new();
        scan_inventory_recursive(tmpdir.path(), false, 0, &mut jobs).expect("scan");
        assert!(jobs.is_empty());
    }

    fn record_json(job_id: &str, schema_version: u32) -> serde_json::Value {
        serde_json::json!({
            "schema_version": schema_version,
            "backend": "slurm",
            "kind": "main",
            "job_id": job_id,
            "submitted_at": 0,
            "compose_file": "/tmp/p/compose.yaml",
            "submit_dir": "/tmp/p",
            "script_path": "/tmp/p/run.sbatch",
            "cache_dir": "/tmp/cache",
            "batch_log": "/tmp/p/logs/x.out",
            "service_logs": {}
        })
    }

    #[test]
    fn validate_submission_record_rejects_empty_job_id() {
        let record: SubmissionRecord =
            serde_json::from_value(record_json("   ", SUBMISSION_SCHEMA_VERSION))
                .expect("deserialize record");
        let err = validate_submission_record(record, Path::new("/tmp/p/x.json")).unwrap_err();
        assert!(
            err.to_string().contains("has an empty job id"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_submission_record_rejects_non_component_job_ids() {
        for job_id in ["../outside", "nested/job", ".", ".."] {
            let record: SubmissionRecord =
                serde_json::from_value(record_json(job_id, SUBMISSION_SCHEMA_VERSION))
                    .expect("deserialize record");
            let err = validate_submission_record(record, Path::new("/tmp/p/x.json"))
                .expect_err("unsafe job id must be rejected");
            assert!(
                err.to_string().contains("safe path component"),
                "job id {job_id:?}: {err}"
            );
        }
    }

    #[test]
    fn validate_submission_record_rejects_future_schema_version() {
        let record: SubmissionRecord =
            serde_json::from_value(record_json("12345", SUBMISSION_SCHEMA_VERSION + 1))
                .expect("deserialize record");
        let err = validate_submission_record(record, Path::new("/tmp/p/x.json")).unwrap_err();
        assert!(
            err.to_string().contains("uses schema version"),
            "got: {err}"
        );
        assert!(
            err.to_string().contains("please upgrade hpc-compose"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_submission_record_accepts_current_schema_and_nonempty_job_id() {
        let record: SubmissionRecord =
            serde_json::from_value(record_json("12345", SUBMISSION_SCHEMA_VERSION))
                .expect("deserialize record");
        let ok = validate_submission_record(record, Path::new("/tmp/p/x.json"))
            .expect("valid record accepted");
        assert_eq!(ok.job_id, "12345");
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn validate_tag_accepts_allowed_charset_and_rejects_the_rest() {
        validate_tag("baseline").expect("plain tag");
        validate_tag("lr-bug_v1.2").expect("dots dashes underscores digits");
        assert!(validate_tag("").unwrap_err().to_string().contains("empty"));
        assert!(
            validate_tag("has space")
                .unwrap_err()
                .to_string()
                .contains("unsupported characters")
        );
        assert!(
            validate_tag("emoji🙂")
                .unwrap_err()
                .to_string()
                .contains("unsupported characters")
        );
        let long = "a".repeat(MAX_TAG_LEN + 1);
        assert!(
            validate_tag(&long)
                .unwrap_err()
                .to_string()
                .contains("longer than")
        );
        validate_tag(&"a".repeat(MAX_TAG_LEN)).expect("max length is allowed");
    }

    #[test]
    fn apply_tag_changes_is_a_sorted_idempotent_set() {
        let mut tags = strings(&["zeta", "baseline"]);
        apply_tag_changes(&mut tags, &strings(&["alpha", "baseline"]), &[]).expect("add");
        assert_eq!(tags, strings(&["alpha", "baseline", "zeta"]));

        // Re-adding an existing tag and removing an absent one are no-ops.
        apply_tag_changes(&mut tags, &strings(&["alpha"]), &strings(&["missing"]))
            .expect("idempotent");
        assert_eq!(tags, strings(&["alpha", "baseline", "zeta"]));

        apply_tag_changes(&mut tags, &[], &strings(&["zeta"])).expect("remove");
        assert_eq!(tags, strings(&["alpha", "baseline"]));
    }

    #[test]
    fn apply_tag_changes_rejects_invalid_tags_and_overflow() {
        let mut tags = Vec::new();
        let err = apply_tag_changes(&mut tags, &strings(&["bad tag!"]), &[]).unwrap_err();
        assert!(err.to_string().contains("unsupported characters"));
        assert!(tags.is_empty(), "failed change must not mutate");

        let mut full = (0..MAX_TAGS_PER_RECORD)
            .map(|index| format!("tag{index:03}"))
            .collect::<Vec<_>>();
        let err = apply_tag_changes(&mut full, &strings(&["one-more"]), &[]).unwrap_err();
        assert!(err.to_string().contains("at most"), "got: {err}");
        assert_eq!(full.len(), MAX_TAGS_PER_RECORD);
    }

    #[test]
    fn validate_note_text_trims_and_bounds() {
        assert_eq!(
            validate_note_text("  diverged after epoch 3\n").expect("note"),
            "diverged after epoch 3"
        );
        assert!(
            validate_note_text("   \n\t")
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        let long = "n".repeat(MAX_NOTE_LEN + 1);
        assert!(
            validate_note_text(&long)
                .unwrap_err()
                .to_string()
                .contains("longer than")
        );
    }

    #[test]
    fn append_job_note_appends_in_order_with_timestamps() {
        let mut record: SubmissionRecord =
            serde_json::from_value(record_json("12345", SUBMISSION_SCHEMA_VERSION))
                .expect("record");
        append_job_note(&mut record, "first").expect("first note");
        append_job_note(&mut record, "second").expect("second note");
        assert_eq!(record.notes.len(), 2);
        assert_eq!(record.notes[0].text, "first");
        assert_eq!(record.notes[1].text, "second");
        assert!(record.notes[0].created_at > 0);
        assert!(record.notes[1].created_at >= record.notes[0].created_at);
    }

    fn tracked_record(
        compose: &Path,
        job_id: &str,
        submitted_at: u64,
        kind: &str,
    ) -> SubmissionRecord {
        let submit_dir = compose.parent().expect("compose parent");
        serde_json::from_value(serde_json::json!({
            "schema_version": SUBMISSION_SCHEMA_VERSION,
            "backend": "slurm",
            "kind": kind,
            "job_id": job_id,
            "submitted_at": submitted_at,
            "compose_file": compose,
            "submit_dir": submit_dir,
            "script_path": submit_dir.join("run.sbatch"),
            "cache_dir": submit_dir.join("cache"),
            "batch_log": submit_dir.join("logs/x.out"),
            "service_logs": {}
        }))
        .expect("tracked record")
    }

    #[test]
    fn writing_a_record_initializes_idempotent_run_evidence() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let record = tracked_record(&compose, "11111", 10, "main");

        write_submission_record(&record).expect("first write");
        let first = crate::job::evidence::load_run_evidence(&compose, "11111")
            .expect("load evidence")
            .expect("new records must have evidence");
        let paths = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths");
        let manifest_bytes = fs::read(&paths.manifest).expect("manifest bytes");

        write_submission_record(&record).expect("idempotent rewrite");
        let second = crate::job::evidence::load_run_evidence(&compose, "11111")
            .expect("reload evidence")
            .expect("evidence remains present");
        assert_eq!(second.manifest.run_id, first.manifest.run_id);
        assert_eq!(second.events.len(), 1, "submitted event must not duplicate");
        assert_eq!(
            fs::read(&paths.manifest).expect("manifest bytes"),
            manifest_bytes
        );
    }

    #[test]
    fn scheduler_job_id_reuse_never_replaces_a_different_canonical_record() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let original = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&original).expect("original record");

        let record_path = jobs_dir_for(&compose).join("11111.json");
        let record_before = fs::read(&record_path).expect("record before");
        let evidence_paths = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths");
        let manifest_before = fs::read(&evidence_paths.manifest).expect("manifest before");
        let events_before = fs::read(&evidence_paths.events).expect("events before");

        let mut reused = original.clone();
        reused.submitted_at = 20;
        reused.config_snapshot_yaml = Some("services:\n  changed: {}\n".to_string());
        let error = write_submission_record(&reused)
            .expect_err("a reused scheduler id must not overwrite its canonical record");
        assert!(
            error.to_string().contains("already tracked"),
            "unexpected error: {error:#}"
        );
        assert_eq!(fs::read(&record_path).expect("record after"), record_before);
        assert_eq!(
            fs::read(&evidence_paths.manifest).expect("manifest after"),
            manifest_before
        );
        assert_eq!(
            fs::read(&evidence_paths.events).expect("events after"),
            events_before
        );

        let update_error = update_submission_record(&compose, "11111", |record| {
            append_job_note(record, "belongs to the original run")
        });
        assert!(update_error.is_ok(), "original record remains usable");
        let canonical = load_submission_record(&compose, Some("11111")).expect("canonical record");
        assert_eq!(canonical.submitted_at, 10);
    }

    #[test]
    fn new_record_rejects_orphaned_evidence_for_the_same_scheduler_id() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let orphan = tracked_record(&compose, "11111", 10, "main");
        initialize_record_run_evidence(&orphan).expect("orphan evidence fixture");

        let error = write_submission_record(&orphan)
            .expect_err("orphaned immutable evidence must not be adopted by a new record");
        assert!(
            error.to_string().contains("run evidence")
                && error.to_string().contains("canonical submission record"),
            "unexpected error: {error:#}"
        );
        assert!(!jobs_dir_for(&compose).join("11111.json").exists());
    }

    #[test]
    fn record_mutations_project_typed_annotation_events() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("record");
        fs::write(
            tmpdir.path().join("run.sbatch"),
            "changed after submission\n",
        )
        .expect("changed script");

        update_submission_record(&compose, "11111", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["baseline"]), &[])?;
            append_job_note(record, "stable loss")
        })
        .expect("mutate record");

        let evidence = crate::job::evidence::load_run_evidence(&compose, "11111")
            .expect("load evidence")
            .expect("evidence");
        assert_eq!(evidence.view.tags, strings(&["baseline"]));
        assert_eq!(evidence.view.notes.len(), 1);
        assert_eq!(evidence.view.notes[0].text, "stable loss");
        assert_eq!(evidence.events.len(), 3, "submitted + tags + note");
    }

    #[test]
    fn cleanup_removes_the_per_job_run_evidence_directory() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        write_submission_record(&tracked_record(&compose, "11111", 0, "main")).expect("record");
        let evidence_root = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths")
            .root;
        assert!(evidence_root.is_dir(), "write must initialize evidence");

        let report = build_cleanup_report(&compose, CleanupMode::Age { age_days: 0 }, false, false)
            .expect("cleanup report");
        run_cleanup_report(&report).expect("cleanup");
        assert!(!evidence_root.exists(), "cleanup must remove run evidence");
    }

    #[test]
    fn update_submission_record_on_non_latest_leaves_pointer_byte_identical() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");

        let latest_path = latest_record_path_for(&compose);
        let pointer_before = fs::read(&latest_path).expect("latest before");

        let updated = update_submission_record(&compose, "11111", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["baseline"]), &[])
        })
        .expect("update older");
        assert_eq!(updated.tags, strings(&["baseline"]));

        // The mutated per-job record carries the tag ...
        let record: SubmissionRecord =
            read_json(&jobs_dir_for(&compose).join("11111.json")).expect("older record");
        assert_eq!(record.tags, strings(&["baseline"]));
        // ... while the latest pointer still names the newer job, byte for byte.
        let pointer_after = fs::read(&latest_path).expect("latest after");
        assert_eq!(pointer_before, pointer_after);
    }

    #[test]
    fn update_submission_record_on_latest_syncs_the_pointer_duplicate() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");

        update_submission_record(&compose, "22222", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["baseline"]), &[])
        })
        .expect("update latest");

        let record: SubmissionRecord =
            read_json(&jobs_dir_for(&compose).join("22222.json")).expect("latest record");
        assert_eq!(record.tags, strings(&["baseline"]));
        let pointer: SubmissionRecord =
            read_json(&latest_record_path_for(&compose)).expect("latest pointer");
        assert_eq!(pointer.job_id, "22222");
        assert_eq!(pointer.tags, strings(&["baseline"]));
    }

    #[test]
    fn latest_pointer_transactions_use_the_stable_per_kind_lock() {
        use std::sync::mpsc;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        let first = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&first).expect("first record");

        let latest_path = latest_record_path_for(&compose);
        let latest_lock_path = latest_path.with_extension("json.lock");
        let latest_guard = crate::secure_io::acquire_flock_strict(
            &latest_lock_path,
            crate::secure_io::LockKind::Exclusive,
            Duration::from_secs(1),
        )
        .expect("hold latest lock");

        let (update_entered_tx, update_entered_rx) = mpsc::channel();
        let (update_done_tx, update_done_rx) = mpsc::channel();
        let update_compose = compose.clone();
        let update = std::thread::spawn(move || {
            let result = update_submission_record(&update_compose, "11111", |record| {
                update_entered_tx.send(()).expect("update entered");
                apply_tag_changes(&mut record.tags, &strings(&["updated"]), &[])
            });
            update_done_tx.send(result).expect("update result");
        });
        update_entered_rx.recv().expect("update mutation entered");

        let (submit_done_tx, submit_done_rx) = mpsc::channel();
        let second = tracked_record(&compose, "22222", 20, "main");
        let submit = std::thread::spawn(move || {
            submit_done_tx
                .send(write_submission_record(&second))
                .expect("submit result");
        });

        assert!(
            update_done_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err(),
            "record update must wait for the stable latest-pointer lock"
        );
        assert!(
            submit_done_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err(),
            "record submission must wait for the stable latest-pointer lock"
        );

        drop(latest_guard);
        let update_result = update_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("update completed");
        update_result.expect("update result");
        let submit_result = submit_done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("submit completed");
        submit_result.expect("submit result");
        update.join().expect("update thread");
        submit.join().expect("submit thread");

        let latest = load_submission_record(&compose, None).expect("latest pointer");
        assert_eq!(latest.job_id, "22222");
    }

    #[test]
    fn latest_pointer_repair_and_direct_removal_share_the_stable_lock() {
        use std::sync::mpsc;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        let newer = tracked_record(&compose, "22222", 20, "main");
        write_submission_record(&newer).expect("newer");
        let latest_path = latest_record_path_for(&compose);
        let latest_lock_path = latest_pointer_lock_path(&latest_path);

        let repair_guard = crate::secure_io::acquire_flock_strict(
            &latest_lock_path,
            crate::secure_io::LockKind::Exclusive,
            Duration::from_secs(1),
        )
        .expect("hold repair lock");
        let (repair_tx, repair_rx) = mpsc::channel();
        let repair_compose = compose.clone();
        let repair = std::thread::spawn(move || {
            repair_tx
                .send(repair_latest_records(&repair_compose))
                .expect("repair result");
        });
        assert!(
            repair_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "latest-pointer repair must wait for the stable lock"
        );
        drop(repair_guard);
        repair_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("repair completed")
            .expect("repair result");
        repair.join().expect("repair thread");

        let removal_guard = crate::secure_io::acquire_flock_strict(
            &latest_lock_path,
            crate::secure_io::LockKind::Exclusive,
            Duration::from_secs(1),
        )
        .expect("hold removal lock");
        let (remove_tx, remove_rx) = mpsc::channel();
        let remove = std::thread::spawn(move || {
            remove_tx
                .send(remove_submission_record(&newer))
                .expect("remove result");
        });
        assert!(
            remove_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "direct removal must wait for the stable latest-pointer lock"
        );
        assert!(
            jobs_dir_for(&compose).join("22222.json").exists(),
            "the canonical record must remain until pointer repair can commit"
        );
        drop(removal_guard);
        remove_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("remove completed")
            .expect("remove result");
        remove.join().expect("remove thread");
        let latest = load_submission_record(&compose, None).expect("repaired latest");
        assert_eq!(latest.job_id, "11111");
    }

    #[test]
    fn update_submission_record_tags_sweep_trials_without_touching_pointers() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        write_submission_record(&tracked_record(&compose, "33333", 30, "sweep_trial"))
            .expect("sweep trial");

        update_submission_record(&compose, "33333", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["sweep-best"]), &[])
        })
        .expect("update sweep trial");

        let record: SubmissionRecord =
            read_json(&jobs_dir_for(&compose).join("33333.json")).expect("trial record");
        assert_eq!(record.tags, strings(&["sweep-best"]));
        assert!(
            !latest_record_path_for(&compose).exists(),
            "sweep trials have no latest pointer to create"
        );
    }

    #[test]
    fn update_submission_record_missing_job_errors_and_failed_mutation_writes_nothing() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        let err = update_submission_record(&compose, "99999", |_| Ok(())).unwrap_err();
        assert!(
            err.to_string()
                .contains("no tracked submission metadata exists"),
            "got: {err}"
        );

        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("record");
        let record_path = jobs_dir_for(&compose).join("11111.json");
        let bytes_before = fs::read(&record_path).expect("record before");
        let err = update_submission_record(&compose, "11111", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["bad tag!"]), &[])
        })
        .unwrap_err();
        assert!(err.to_string().contains("unsupported characters"));
        assert_eq!(
            bytes_before,
            fs::read(&record_path).expect("record after"),
            "a failed mutation must not rewrite the record"
        );
    }

    #[test]
    fn update_rejects_immutable_identity_changes_without_touching_record_or_evidence() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let mut original = tracked_record(&compose, "11111", 10, "main");
        original.config_snapshot_yaml = Some("services: {}\n".to_string());
        write_submission_record(&original).expect("record");
        let record_path = jobs_dir_for(&compose).join("11111.json");
        let evidence_paths = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths");
        let record_before = fs::read(&record_path).expect("record before");
        let manifest_before = fs::read(&evidence_paths.manifest).expect("manifest before");
        let events_before = fs::read(&evidence_paths.events).expect("events before");

        let error = update_submission_record(&compose, "11111", |record| {
            record.submitted_at += 1;
            record.config_snapshot_yaml = Some("services:\n  changed: {}\n".to_string());
            Ok(())
        })
        .expect_err("post-submit immutable identity changes must be rejected");
        assert!(error.to_string().contains("immutable"), "got: {error:#}");
        assert_eq!(fs::read(&record_path).expect("record after"), record_before);
        assert_eq!(
            fs::read(&evidence_paths.manifest).expect("manifest after"),
            manifest_before
        );
        assert_eq!(
            fs::read(&evidence_paths.events).expect("events after"),
            events_before
        );
    }

    #[test]
    fn cleanup_skips_unsafe_records_without_touching_outside_sentinels() {
        struct Case {
            name: &'static str,
            file_id: &'static str,
            record_id: &'static str,
            configure: fn(&mut SubmissionRecord, &Path) -> PathBuf,
        }

        let cases = [
            Case {
                name: "empty id",
                file_id: "empty",
                record_id: "",
                configure: |record, root| {
                    record.submit_dir = root.to_path_buf();
                    metadata_root_for(&record.compose_file).join("sentinel-empty")
                },
            },
            Case {
                name: "traversal id",
                file_id: "escape",
                record_id: "../outside",
                configure: |record, root| {
                    record.submit_dir = root.to_path_buf();
                    root.join("outside/sentinel-traversal")
                },
            },
            Case {
                name: "mismatched file id",
                file_id: "alias",
                record_id: "victim",
                configure: |record, root| {
                    record.submit_dir = root.to_path_buf();
                    metadata_root_for(&record.compose_file).join("victim/sentinel-mismatch")
                },
            },
            Case {
                name: "unowned managed batch log",
                file_id: "owned",
                record_id: "owned",
                configure: |record, root| {
                    record.batch_log_managed = true;
                    record.batch_log = root.join("sentinel-batch.log");
                    record.batch_log.clone()
                },
            },
        ];

        for case in cases {
            let tmpdir = tempfile::tempdir().expect("tmpdir");
            let compose = tmpdir.path().join("compose.yaml");
            fs::write(&compose, "").expect("compose");
            let mut record = tracked_record(&compose, case.record_id, 0, "main");
            let sentinel = (case.configure)(&mut record, tmpdir.path());
            if let Some(parent) = sentinel.parent() {
                fs::create_dir_all(parent).expect("sentinel parent");
            }
            fs::write(&sentinel, case.name).expect("sentinel");

            let jobs_dir = jobs_dir_for(&compose);
            fs::create_dir_all(&jobs_dir).expect("jobs dir");
            let record_path = jobs_dir.join(format!("{}.json", case.file_id));
            write_json(&record_path, &record).expect("unsafe record fixture");

            let report =
                build_cleanup_report(&compose, CleanupMode::Age { age_days: 0 }, false, false)
                    .expect("cleanup planning should degrade past an unsafe record");
            run_cleanup_report(&report).expect("unsafe records must not break cleanup");
            assert!(
                sentinel.exists(),
                "{} must not remove {}",
                case.name,
                sentinel.display()
            );
            assert!(
                report.jobs.is_empty(),
                "{} should not enter a destructive cleanup plan",
                case.name
            );
            assert!(
                record_path.exists(),
                "the skipped record should remain available for diagnosis"
            );
        }
    }

    #[test]
    fn cleanup_rejects_a_tampered_plan_before_removing_anything() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "").expect("compose");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");

        let outside = tmpdir.path().join("outside/sentinel");
        fs::create_dir_all(outside.parent().expect("outside parent")).expect("outside parent");
        fs::write(&outside, "keep").expect("outside sentinel");

        let mut report = build_cleanup_report(&compose, CleanupMode::AllExceptLatest, false, false)
            .expect("cleanup report");
        let selected = report
            .jobs
            .iter_mut()
            .find(|job| job.selected)
            .expect("selected old job");
        selected.removable_paths.push(outside.clone());

        let err = run_cleanup_report(&report).expect_err("tampered cleanup plan must be rejected");
        assert!(err.to_string().contains("cleanup plan"), "got: {err}");
        assert!(outside.exists(), "outside sentinel must survive");
        assert!(
            jobs_dir_for(&compose).join("11111.json").exists(),
            "validation must happen before the first removal"
        );
    }

    #[test]
    fn cleanup_rejects_tampered_inventory_even_when_removal_paths_match_it() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "").expect("compose");
        let mut older = tracked_record(&compose, "11111", 10, "main");
        older.runtime_root = Some(tmpdir.path().join("owned-runtime"));
        write_submission_record(&older).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");

        let outside_root = tmpdir.path().join("unowned/11111");
        fs::create_dir_all(&outside_root).expect("outside root");
        let sentinel = outside_root.join("sentinel");
        fs::write(&sentinel, "keep").expect("outside sentinel");

        let mut report = build_cleanup_report(&compose, CleanupMode::AllExceptLatest, false, false)
            .expect("cleanup report");
        let selected = report
            .jobs
            .iter_mut()
            .find(|job| job.selected)
            .expect("selected old job");
        let original_runtime_root = selected.inventory.runtime_job_root.clone();
        selected.inventory.runtime_job_root = outside_root.clone();
        selected.removable_paths = selected
            .removable_paths
            .iter()
            .map(|path| {
                if *path == original_runtime_root {
                    outside_root.clone()
                } else {
                    path.clone()
                }
            })
            .collect();

        let err = run_cleanup_report(&report)
            .expect_err("cleanup inventory must be checked against the persisted record");
        assert!(err.to_string().contains("cleanup plan"), "got: {err}");
        assert!(
            sentinel.exists(),
            "unowned job-shaped directory must survive"
        );
        assert!(
            jobs_dir_for(&compose).join("11111.json").exists(),
            "all selected records must be validated before any removal"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_keeps_the_canonical_record_when_owned_path_removal_fails() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");
        let report = build_cleanup_report(&compose, CleanupMode::AllExceptLatest, false, false)
            .expect("cleanup report");
        let evidence_base = metadata_root_for(&compose).join(tracked_paths::RUN_EVIDENCE_DIR_NAME);
        fs::set_permissions(&evidence_base, fs::Permissions::from_mode(0o500))
            .expect("make evidence base read-only");

        let error = run_cleanup_report(&report).expect_err("owned path removal must fail");
        let record_survived = jobs_dir_for(&compose).join("11111.json").exists();
        fs::set_permissions(&evidence_base, fs::Permissions::from_mode(0o700))
            .expect("restore evidence base");
        assert!(error.to_string().contains("remove"), "got: {error:#}");
        assert!(
            record_survived,
            "cleanup must retain the canonical record as its retry authority"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_rejects_a_symlinked_evidence_parent_before_external_deletion() {
        use std::os::unix::fs::symlink;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");
        let report = build_cleanup_report(&compose, CleanupMode::AllExceptLatest, false, false)
            .expect("cleanup report");

        let evidence_parent =
            metadata_root_for(&compose).join(tracked_paths::RUN_EVIDENCE_DIR_NAME);
        let original_evidence = metadata_root_for(&compose).join("evidence-original");
        fs::rename(&evidence_parent, &original_evidence).expect("move real evidence");
        let outside_parent = tmpdir.path().join("outside-evidence");
        let outside_job = outside_parent.join("11111");
        fs::create_dir_all(&outside_job).expect("outside job");
        let sentinel = outside_job.join("keep");
        fs::write(&sentinel, "keep").expect("outside sentinel");
        symlink(&outside_parent, &evidence_parent).expect("evidence parent symlink");

        let error = run_cleanup_report(&report)
            .expect_err("a managed evidence-parent symlink must fail closed");
        assert!(error.to_string().contains("evidence"), "got: {error:#}");
        assert!(
            sentinel.exists(),
            "cleanup must not follow the parent symlink"
        );
        assert!(
            jobs_dir_for(&compose).join("11111.json").exists(),
            "canonical record must remain retryable"
        );
    }

    #[test]
    fn cleanup_waits_for_the_persistent_evidence_lock_before_any_deletion() {
        use std::sync::mpsc;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let older = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&older).expect("older");
        write_submission_record(&tracked_record(&compose, "22222", 20, "main")).expect("newer");
        let runtime_root = runtime_job_root_for_record(&older);
        fs::create_dir_all(&runtime_root).expect("runtime root");
        let sentinel = runtime_root.join("keep");
        fs::write(&sentinel, "keep").expect("runtime sentinel");
        let report = build_cleanup_report(&compose, CleanupMode::AllExceptLatest, false, false)
            .expect("cleanup report");
        let evidence_paths = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths");
        let evidence_guard = crate::secure_io::acquire_flock_strict(
            &evidence_paths.lock,
            crate::secure_io::LockKind::Exclusive,
            Duration::from_secs(1),
        )
        .expect("hold evidence lock");

        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let cleanup = std::thread::spawn(move || {
            started_tx.send(()).expect("cleanup started");
            done_tx
                .send(run_cleanup_report(&report))
                .expect("cleanup result");
        });
        started_rx.recv().expect("cleanup started");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "cleanup must wait for the evidence lock"
        );
        assert!(
            sentinel.exists(),
            "no owned path may be deleted before the evidence lock is acquired"
        );
        drop(evidence_guard);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cleanup completed")
            .expect("cleanup result");
        cleanup.join().expect("cleanup thread");
    }

    #[test]
    fn write_and_direct_remove_reject_unsafe_job_identity() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        let mut record = tracked_record(&compose, "../outside", 10, "main");
        record.submit_dir = tmpdir.path().to_path_buf();

        let err = write_submission_record(&record).expect_err("unsafe id must not be written");
        assert!(
            err.to_string().contains("safe path component"),
            "got: {err}"
        );
        assert!(
            !metadata_root_for(&compose).join("outside.json").exists(),
            "an unsafe id must not escape the jobs directory"
        );

        let sentinel = tmpdir.path().join("outside/sentinel");
        fs::create_dir_all(sentinel.parent().expect("sentinel parent")).expect("sentinel parent");
        fs::write(&sentinel, "keep").expect("sentinel");
        let err = remove_submission_record(&record)
            .expect_err("unsafe id must be rejected at the destructive boundary");
        assert!(
            err.to_string().contains("safe path component"),
            "got: {err}"
        );
        assert!(sentinel.exists(), "outside sentinel must survive");
    }

    #[test]
    fn direct_remove_rejects_a_stale_or_fabricated_record_before_deleting_anything() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let original = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&original).expect("record");
        let canonical = update_submission_record(&compose, "11111", |record| {
            apply_tag_changes(&mut record.tags, &strings(&["canonical"]), &[])
        })
        .expect("canonical update");

        let runtime_root = runtime_job_root_for_record(&canonical);
        fs::create_dir_all(&runtime_root).expect("runtime root");
        let runtime_sentinel = runtime_root.join("keep");
        fs::write(&runtime_sentinel, "keep").expect("runtime sentinel");
        let record_path = jobs_dir_for(&compose).join("11111.json");
        let evidence_root = crate::tracked_paths::run_evidence_dir_for(&compose, "11111");

        let error = remove_submission_record(&original)
            .expect_err("a stale caller snapshot must not authorize deletion");
        assert!(error.to_string().contains("canonical"), "got: {error:#}");
        assert!(record_path.exists());
        assert!(evidence_root.exists());
        assert!(runtime_sentinel.exists());

        let mut fabricated = canonical.clone();
        fabricated.cache_dir = tmpdir.path().join("fabricated-cache");
        let fabricated_runtime =
            crate::tracked_paths::enroot_runtime_job_dir(&fabricated.cache_dir, "11111");
        fs::create_dir_all(&fabricated_runtime).expect("fabricated runtime");
        let fabricated_sentinel = fabricated_runtime.join("keep");
        fs::write(&fabricated_sentinel, "keep").expect("fabricated sentinel");
        let error = remove_submission_record(&fabricated)
            .expect_err("a fabricated caller snapshot must not authorize deletion");
        assert!(error.to_string().contains("canonical"), "got: {error:#}");
        assert!(record_path.exists());
        assert!(evidence_root.exists());
        assert!(runtime_sentinel.exists());
        assert!(fabricated_sentinel.exists());
    }

    #[cfg(unix)]
    #[test]
    fn direct_remove_commits_the_record_only_after_owned_paths_are_removed() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let record = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&record).expect("record");
        let record_path = jobs_dir_for(&compose).join("11111.json");
        let evidence_base = metadata_root_for(&compose).join(tracked_paths::RUN_EVIDENCE_DIR_NAME);
        fs::set_permissions(&evidence_base, fs::Permissions::from_mode(0o500))
            .expect("make evidence base read-only");

        let error = remove_submission_record(&record).expect_err("owned path removal must fail");
        let record_survived = record_path.exists();
        let latest_survived = latest_record_path_for(&compose).exists();
        fs::set_permissions(&evidence_base, fs::Permissions::from_mode(0o700))
            .expect("restore evidence base");
        assert!(error.to_string().contains("remove"), "got: {error:#}");
        assert!(record_survived, "canonical record must remain retryable");
        assert!(latest_survived, "latest pointer must remain retryable");
    }

    #[cfg(unix)]
    #[test]
    fn direct_remove_rejects_a_symlinked_cache_runtime_parent() {
        use std::os::unix::fs::symlink;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let mut record = tracked_record(&compose, "11111", 10, "main");
        record.cache_dir = tmpdir.path().join("cache");
        write_submission_record(&record).expect("record");

        fs::create_dir(&record.cache_dir).expect("cache root");
        let outside_runtime = tmpdir.path().join("outside-runtime");
        let outside_job = outside_runtime.join("11111");
        fs::create_dir_all(&outside_job).expect("outside job");
        let sentinel = outside_job.join("keep");
        fs::write(&sentinel, "keep").expect("outside sentinel");
        symlink(
            &outside_runtime,
            record
                .cache_dir
                .join(tracked_paths::ENROOT_RUNTIME_DIR_NAME),
        )
        .expect("runtime parent symlink");

        let error = remove_submission_record(&record)
            .expect_err("a managed runtime-parent symlink must fail closed");
        assert!(error.to_string().contains("runtime"), "got: {error:#}");
        assert!(
            sentinel.exists(),
            "removal must not follow the parent symlink"
        );
        assert!(jobs_dir_for(&compose).join("11111.json").exists());
    }

    #[test]
    fn direct_remove_waits_for_evidence_lock_before_owned_path_deletion() {
        use std::sync::mpsc;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let record = tracked_record(&compose, "11111", 10, "main");
        write_submission_record(&record).expect("record");
        let runtime_root = runtime_job_root_for_record(&record);
        fs::create_dir_all(&runtime_root).expect("runtime root");
        let sentinel = runtime_root.join("keep");
        fs::write(&sentinel, "keep").expect("runtime sentinel");
        let evidence_paths = crate::job::evidence::RunEvidencePaths::for_job(&compose, "11111")
            .expect("evidence paths");
        let evidence_guard = crate::secure_io::acquire_flock_strict(
            &evidence_paths.lock,
            crate::secure_io::LockKind::Exclusive,
            Duration::from_secs(1),
        )
        .expect("hold evidence lock");

        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let remove = std::thread::spawn(move || {
            started_tx.send(()).expect("remove started");
            done_tx
                .send(remove_submission_record(&record))
                .expect("remove result");
        });
        started_rx.recv().expect("remove started");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "direct removal must wait for the evidence lock"
        );
        assert!(
            sentinel.exists(),
            "no owned path may be deleted before the evidence lock is acquired"
        );
        drop(evidence_guard);
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("remove completed")
            .expect("remove result");
        remove.join().expect("remove thread");
    }

    #[cfg(unix)]
    #[test]
    fn managed_metadata_and_jobs_directory_symlinks_fail_closed() {
        use std::os::unix::fs::symlink;

        let metadata_case = tempfile::tempdir().expect("metadata case");
        let compose = metadata_case.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let outside_metadata = metadata_case.path().join("outside-metadata");
        fs::create_dir(&outside_metadata).expect("outside metadata");
        symlink(&outside_metadata, metadata_root_for(&compose)).expect("metadata symlink");
        let error = write_submission_record(&tracked_record(&compose, "11111", 10, "main"))
            .expect_err("metadata symlink must be rejected");
        assert!(
            error.to_string().contains("real directory"),
            "got: {error:#}"
        );
        assert!(!outside_metadata.join("jobs/11111.json").exists());

        let jobs_case = tempfile::tempdir().expect("jobs case");
        let compose = jobs_case.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");
        let metadata_root = metadata_root_for(&compose);
        fs::create_dir(&metadata_root).expect("metadata root");
        let outside_jobs = jobs_case.path().join("outside-jobs");
        fs::create_dir(&outside_jobs).expect("outside jobs");
        symlink(&outside_jobs, jobs_dir_for(&compose)).expect("jobs symlink");
        let error = write_submission_record(&tracked_record(&compose, "22222", 20, "main"))
            .expect_err("jobs symlink must be rejected");
        assert!(
            error.to_string().contains("real directory"),
            "got: {error:#}"
        );
        assert!(!outside_jobs.join("22222.json").exists());
    }

    #[test]
    fn concurrent_record_updates_are_serialized_without_lost_changes() {
        use std::sync::mpsc;

        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let compose = tmpdir.path().join("compose.yaml");
        write_submission_record(&tracked_record(&compose, "11111", 10, "main")).expect("record");

        let (first_entered_tx, first_entered_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let first_compose = compose.clone();
        let first = std::thread::spawn(move || {
            update_submission_record(&first_compose, "11111", |record| {
                first_entered_tx.send(()).expect("signal first entered");
                release_first_rx.recv().expect("release first");
                apply_tag_changes(&mut record.tags, &strings(&["first"]), &[])
            })
            .expect("first update");
        });
        first_entered_rx.recv().expect("first mutation entered");

        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second_compose = compose.clone();
        let second = std::thread::spawn(move || {
            update_submission_record(&second_compose, "11111", |record| {
                apply_tag_changes(&mut record.tags, &strings(&["second"]), &[])
            })
            .expect("second update");
            second_done_tx.send(()).expect("signal second done");
        });

        let second_completed_while_first_was_open = second_done_rx
            .recv_timeout(Duration::from_millis(250))
            .is_ok();
        release_first_tx.send(()).expect("release first update");
        first.join().expect("first thread");
        second.join().expect("second thread");

        assert!(
            !second_completed_while_first_was_open,
            "the second read-mutate-write must wait for the first transaction"
        );
        let record = load_submission_record(&compose, Some("11111")).expect("updated record");
        assert_eq!(record.tags, strings(&["first", "second"]));
        let latest = load_submission_record(&compose, None).expect("latest pointer");
        assert_eq!(latest.tags, record.tags);
    }
}
