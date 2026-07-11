//! Additive, local-first run evidence stored alongside legacy submission records.

use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize, de};
use sha2::{Digest, Sha256};

use super::model::{JobNote, SubmissionBackend, SubmissionKind};
use super::provenance::JobProvenance;

/// Current schema version shared by the additive run-evidence files.
const RUN_EVIDENCE_SCHEMA_VERSION: u32 = 1;

const RUN_EVIDENCE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ANNOTATION_BYTES: usize = 16 * 1024;
static RUN_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Status of one independently observable piece of run evidence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum EvidenceStatus {
    /// The evidence was collected and its value is present.
    Available,
    /// The source was expected but absent.
    Missing,
    /// The source cannot safely or meaningfully be inspected.
    Unsupported,
    /// Collection was attempted but produced incomplete evidence.
    Degraded,
}

/// A value with an explicit evidence state and reason contract.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct Evidence<T> {
    status: EvidenceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// SHA-256 evidence for bounded content.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct ContentDigest {
    sha256: String,
    size_bytes: u64,
}

/// Persistent category of a run input.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub(super) enum InputKind {
    /// A bounded regular file.
    File,
    /// A content-addressed source snapshot.
    SourceSnapshot,
    /// A container image reference or artifact.
    ContainerImage,
    /// A potentially large dataset.
    Dataset,
    /// An external reference that is not locally hashable.
    ExternalReference,
    /// Another explicitly described input kind.
    Other,
}

/// Strategy used, or deliberately not used, for byte-level input attestation.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub(super) enum InputDigestStrategy {
    /// Hash the complete bytes of a bounded regular file with SHA-256.
    FullSha256,
    /// Reuse a source content-addressed-storage identity.
    SourceCas,
    /// Preserve an immutable or user-supplied container image reference.
    ImageReference,
    /// Byte hashing was explicitly not performed.
    NotHashed,
}

/// One input recorded in an immutable input lock.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct InputEvidence {
    pub(super) name: String,
    pub(super) kind: InputKind,
    pub(super) source: String,
    pub(super) identity: Evidence<String>,
    pub(super) digest_strategy: InputDigestStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) materialized_path: Option<PathBuf>,
    pub(super) content: Evidence<ContentDigest>,
}

/// Immutable, schema-versioned input evidence for a run.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct InputsLock {
    schema_version: u32,
    run_id: String,
    inputs: Vec<InputEvidence>,
}

/// Immutable submit-time identity and evidence for a run.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct RunManifest {
    schema_version: u32,
    pub(super) run_id: String,
    scheduler_job_id: String,
    submission_record_sha256: String,
    backend: SubmissionBackend,
    kind: SubmissionKind,
    submitted_at: u64,
    config: Evidence<ContentDigest>,
    script: Evidence<ContentDigest>,
    provenance: Evidence<JobProvenance>,
    inputs_lock_sha256: String,
}

/// Payload of one append-only run event.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RunEventPayload {
    Submitted { scheduler_job_id: String },
    Annotation { text: String },
    TagsUpdated { tags: Vec<String> },
    NoteAdded { note: JobNote },
}

/// One append-only, monotonically sequenced run event.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct RunEvent {
    schema_version: u32,
    run_id: String,
    sequence: u64,
    at_unix: u64,
    #[serde(flatten)]
    payload: RunEventPayload,
}

/// One annotation materialized into the current run view.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
struct RunAnnotation {
    sequence: u64,
    at_unix: u64,
    text: String,
}

/// Rebuildable, atomically published projection of a run's event log.
#[allow(missing_docs)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub(super) struct RunView {
    schema_version: u32,
    run_id: String,
    scheduler_job_id: String,
    submitted_at: u64,
    event_count: u64,
    last_sequence: u64,
    last_event_at: u64,
    pub(super) tags: Vec<String>,
    pub(super) notes: Vec<JobNote>,
    annotations: Vec<RunAnnotation>,
}

/// Caller-supplied submit-time evidence used for idempotent initialization.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunEvidenceSeed {
    pub(super) scheduler_job_id: String,
    pub(super) submission_record_sha256: String,
    pub(super) backend: SubmissionBackend,
    pub(super) kind: SubmissionKind,
    pub(super) submitted_at: u64,
    pub(super) config: Evidence<ContentDigest>,
    pub(super) script: Evidence<ContentDigest>,
    pub(super) provenance: Evidence<JobProvenance>,
    pub(super) inputs: Vec<InputEvidence>,
}

/// Stable paths for one validated per-job evidence directory.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunEvidencePaths {
    pub(super) root: PathBuf,
    pub(super) manifest: PathBuf,
    pub(super) inputs_lock: PathBuf,
    pub(super) events: PathBuf,
    pub(super) view: PathBuf,
    pub(super) lock: PathBuf,
}

/// Complete currently materialized run evidence.
#[allow(missing_docs)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RunEvidenceState {
    pub(super) manifest: RunManifest,
    inputs_lock: InputsLock,
    pub(super) events: Vec<RunEvent>,
    pub(super) view: RunView,
}

/// Validated bytes exposed to read-only consumers such as experiment bundles.
///
/// Immutable files retain their exact persisted bytes so manifest-linked
/// digests remain valid. The view is rebuilt from the validated event stream
/// instead of trusting the materialized cache on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ValidatedRunEvidenceFiles {
    pub(super) manifest: Vec<u8>,
    pub(super) inputs_lock: Vec<u8>,
    pub(super) events: Vec<u8>,
    pub(super) view: Vec<u8>,
}

impl<T> Evidence<T> {
    /// Constructs available evidence.
    #[must_use]
    pub(super) fn available(value: T) -> Self {
        Self {
            status: EvidenceStatus::Available,
            value: Some(value),
            reason: None,
        }
    }

    /// Constructs missing evidence with a non-empty reason.
    pub(super) fn missing(reason: impl Into<String>) -> Result<Self> {
        Self::from_parts(EvidenceStatus::Missing, None, Some(reason.into()))
    }

    /// Constructs unsupported evidence with a non-empty reason.
    pub(super) fn unsupported(reason: impl Into<String>) -> Result<Self> {
        Self::from_parts(EvidenceStatus::Unsupported, None, Some(reason.into()))
    }

    /// Constructs degraded evidence with a non-empty reason and optional value.
    fn degraded(value: Option<T>, reason: impl Into<String>) -> Result<Self> {
        Self::from_parts(EvidenceStatus::Degraded, value, Some(reason.into()))
    }

    /// Returns the explicit evidence status.
    #[must_use]
    fn status(&self) -> EvidenceStatus {
        self.status
    }

    /// Returns the collected value, when present.
    #[must_use]
    fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    /// Returns the reason for non-available evidence.
    #[must_use]
    #[cfg(test)]
    fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    fn from_parts(
        status: EvidenceStatus,
        value: Option<T>,
        reason: Option<String>,
    ) -> Result<Self> {
        match status {
            EvidenceStatus::Available => {
                ensure!(value.is_some(), "available evidence requires a value");
                ensure!(
                    reason.is_none(),
                    "available evidence cannot include a reason"
                );
            }
            EvidenceStatus::Missing | EvidenceStatus::Unsupported => {
                ensure!(value.is_none(), "non-value evidence cannot include a value");
                validate_reason(reason.as_deref())?;
            }
            EvidenceStatus::Degraded => validate_reason(reason.as_deref())?,
        }
        Ok(Self {
            status,
            value,
            reason,
        })
    }
}

impl ContentDigest {
    /// Hashes an in-memory bounded value.
    #[must_use]
    pub(super) fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self {
            sha256: hex::encode(hasher.finalize()),
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        }
    }
}

impl RunEvidencePaths {
    /// Resolves the safe evidence layout for one scheduler job id.
    pub(super) fn for_job(compose_file: &Path, scheduler_job_id: &str) -> Result<Self> {
        validate_job_component(scheduler_job_id)?;
        let root = crate::tracked_paths::run_evidence_dir_for(compose_file, scheduler_job_id);
        Ok(Self {
            manifest: crate::tracked_paths::run_manifest_path(&root),
            inputs_lock: crate::tracked_paths::inputs_lock_path(&root),
            events: crate::tracked_paths::run_events_path(&root),
            view: crate::tracked_paths::run_view_path(&root),
            lock: crate::tracked_paths::run_events_lock_path(&root),
            root,
        })
    }
}

/// Initializes immutable evidence, one submitted event, and the atomic view.
pub(super) fn initialize_run_evidence(
    compose_file: &Path,
    seed: &RunEvidenceSeed,
) -> Result<RunEvidenceState> {
    ensure!(
        !seed.scheduler_job_id.trim().is_empty(),
        "run evidence requires a scheduler job id"
    );
    let paths = RunEvidencePaths::for_job(compose_file, &seed.scheduler_job_id)?;
    ensure_evidence_directory(&paths)?;
    let _guard = lock_evidence_for_initialization(&paths)?;

    let existing_manifest = read_json_optional::<RunManifest>(&paths.manifest)?;
    if let Some((manifest, _)) = &existing_manifest {
        validate_manifest(manifest, &seed.scheduler_job_id)?;
    }
    let existing_inputs = read_json_optional::<InputsLock>(&paths.inputs_lock)?;
    if let Some((inputs, _)) = &existing_inputs {
        validate_inputs_lock(inputs)?;
    }

    let run_id = existing_manifest
        .as_ref()
        .map(|(manifest, _)| manifest.run_id.clone())
        .or_else(|| {
            existing_inputs
                .as_ref()
                .map(|(inputs, _)| inputs.run_id.clone())
        })
        .unwrap_or_else(|| generate_run_id(compose_file, &seed.scheduler_job_id));
    validate_run_id(&run_id)?;
    if let Some((inputs, _)) = &existing_inputs {
        ensure!(
            inputs.run_id == run_id,
            "input lock run id does not match the immutable manifest"
        );
    }

    let mut inputs = seed.inputs.clone();
    canonicalize_inputs(&mut inputs)?;
    let candidate_inputs = InputsLock {
        schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
        run_id: run_id.clone(),
        inputs,
    };
    let candidate_input_bytes = serialize_pretty(&candidate_inputs, "input lock")?;
    let input_bytes = match &existing_inputs {
        Some((persisted, bytes)) => {
            ensure!(
                persisted == &candidate_inputs,
                "run evidence input lock already exists with different submit-time evidence"
            );
            bytes.clone()
        }
        None => candidate_input_bytes,
    };
    let inputs_lock_sha256 = ContentDigest::from_bytes(&input_bytes).sha256;

    let candidate_manifest = RunManifest {
        schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
        run_id: run_id.clone(),
        scheduler_job_id: seed.scheduler_job_id.clone(),
        submission_record_sha256: seed.submission_record_sha256.clone(),
        backend: seed.backend,
        kind: seed.kind,
        submitted_at: seed.submitted_at,
        config: seed.config.clone(),
        script: seed.script.clone(),
        provenance: seed.provenance.clone(),
        inputs_lock_sha256,
    };
    validate_manifest(&candidate_manifest, &seed.scheduler_job_id)?;
    if let Some((persisted, _)) = &existing_manifest {
        ensure!(
            persisted == &candidate_manifest,
            "run manifest already exists with different submit-time evidence"
        );
    }

    if existing_inputs.is_none() {
        write_immutable(&paths.inputs_lock, &input_bytes, "input lock")?;
    }
    if existing_manifest.is_none() {
        let bytes = serialize_pretty(&candidate_manifest, "run manifest")?;
        write_immutable(&paths.manifest, &bytes, "run manifest")?;
    }

    let mut events = read_events(&paths.events, &candidate_manifest)?;
    if events.is_empty() {
        let submitted = RunEvent {
            schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
            run_id,
            sequence: 1,
            at_unix: seed.submitted_at,
            payload: RunEventPayload::Submitted {
                scheduler_job_id: seed.scheduler_job_id.clone(),
            },
        };
        append_event_lines(&paths.events, std::slice::from_ref(&submitted))?;
        events.push(submitted);
    }
    let view = project_view(&candidate_manifest, &events)?;
    write_view(&paths.view, &view)?;
    Ok(RunEvidenceState {
        manifest: candidate_manifest,
        inputs_lock: candidate_inputs,
        events,
        view,
    })
}

/// Loads evidence when present, returning `None` for legacy records.
pub(super) fn load_run_evidence(
    compose_file: &Path,
    scheduler_job_id: &str,
) -> Result<Option<RunEvidenceState>> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    if !safe_evidence_directory_exists(&paths)? {
        return Ok(None);
    }
    validate_existing_evidence_lock(&paths)?;
    let (manifest, _) = read_json_required::<RunManifest>(&paths.manifest)
        .context("run evidence directory exists but its manifest is missing")?;
    validate_manifest(&manifest, scheduler_job_id)?;
    let (inputs_lock, input_bytes) = read_json_required::<InputsLock>(&paths.inputs_lock)?;
    validate_inputs_lock(&inputs_lock)?;
    ensure!(
        inputs_lock.run_id == manifest.run_id,
        "input lock run id does not match run manifest"
    );
    ensure!(
        ContentDigest::from_bytes(&input_bytes).sha256 == manifest.inputs_lock_sha256,
        "input lock digest does not match run manifest"
    );
    let events = read_events(&paths.events, &manifest)?;
    let view = project_view(&manifest, &events)?;
    Ok(Some(RunEvidenceState {
        manifest,
        inputs_lock,
        events,
        view,
    }))
}

/// Returns a consistent, validated snapshot for a read-only evidence consumer.
///
/// `None` means the tracked record predates the additive evidence protocol.
/// A partially present or invalid protocol directory is an error and must not
/// be copied as credible evidence.
pub(super) fn export_run_evidence_files(
    compose_file: &Path,
    scheduler_job_id: &str,
    expected_submission_record_sha256: &str,
) -> Result<Option<ValidatedRunEvidenceFiles>> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    if !safe_evidence_directory_exists(&paths)? {
        return Ok(None);
    }
    ensure!(
        read_bytes_optional(&paths.manifest)?.is_some(),
        "run evidence directory exists but its manifest is missing"
    );

    let _guard = lock_existing_evidence(&paths)?;
    let state = load_run_evidence(compose_file, scheduler_job_id)?
        .context("run evidence disappeared during export")?;
    ensure!(
        state.manifest.submission_record_sha256 == expected_submission_record_sha256,
        "run evidence submission record identity does not match the requested tracked record"
    );

    let manifest =
        read_bytes_optional(&paths.manifest)?.context("run manifest disappeared during export")?;
    let inputs_lock =
        read_bytes_optional(&paths.inputs_lock)?.context("input lock disappeared during export")?;
    let events =
        read_bytes_optional(&paths.events)?.context("run event log disappeared during export")?;
    let view = serialize_pretty(&state.view, "run view")?;

    Ok(Some(ValidatedRunEvidenceFiles {
        manifest,
        inputs_lock,
        events,
        view,
    }))
}

/// Rebuilds and atomically republishes the materialized view from its event log.
#[cfg(test)]
fn rebuild_run_view(compose_file: &Path, scheduler_job_id: &str) -> Result<Option<RunView>> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    if !safe_evidence_directory_exists(&paths)? {
        return Ok(None);
    }
    let _guard = lock_existing_evidence(&paths)?;
    let Some((manifest, _)) = read_json_optional::<RunManifest>(&paths.manifest)? else {
        return Ok(None);
    };
    validate_manifest(&manifest, scheduler_job_id)?;
    let events = read_events(&paths.events, &manifest)?;
    let view = project_view(&manifest, &events)?;
    write_view(&paths.view, &view)?;
    Ok(Some(view))
}

/// Appends an annotation under a strict lock and atomically advances the view.
#[cfg(test)]
fn append_annotation_event(
    compose_file: &Path,
    scheduler_job_id: &str,
    at_unix: u64,
    text: impl Into<String>,
) -> Result<RunEvent> {
    let text = text.into();
    validate_annotation_text(&text)?;
    append_payload_event(
        compose_file,
        scheduler_job_id,
        at_unix,
        RunEventPayload::Annotation { text },
    )
}

/// Idempotently projects the tags and append-only notes of a submission record.
pub(super) fn sync_record_annotations(
    compose_file: &Path,
    scheduler_job_id: &str,
    at_unix: u64,
    tags: &[String],
    notes: &[JobNote],
) -> Result<()> {
    sync_record_annotations_inner(compose_file, scheduler_job_id, at_unix, tags, notes).map(drop)
}

fn sync_record_annotations_inner(
    compose_file: &Path,
    scheduler_job_id: &str,
    at_unix: u64,
    tags: &[String],
    notes: &[JobNote],
) -> Result<Vec<RunEvent>> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    ensure!(
        safe_evidence_directory_exists(&paths)?,
        "run evidence does not exist for scheduler job {scheduler_job_id}"
    );
    let _guard = lock_existing_evidence(&paths)?;
    let (manifest, _) = read_json_required::<RunManifest>(&paths.manifest)?;
    validate_manifest(&manifest, scheduler_job_id)?;
    let mut events = read_events(&paths.events, &manifest)?;
    let view = project_view(&manifest, &events)?;

    let desired_tags = canonical_tags(tags.to_vec())?;
    for note in notes {
        validate_note(note)?;
    }
    ensure!(
        notes.starts_with(&view.notes),
        "submission notes are not an append-only extension of run evidence"
    );

    let mut appended = Vec::new();
    let mut next_sequence = view
        .last_sequence
        .checked_add(1)
        .context("run event sequence overflow")?;
    if view.tags != desired_tags {
        appended.push(RunEvent {
            schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
            run_id: manifest.run_id.clone(),
            sequence: next_sequence,
            at_unix,
            payload: RunEventPayload::TagsUpdated { tags: desired_tags },
        });
        next_sequence = next_sequence
            .checked_add(1)
            .context("run event sequence overflow")?;
    }
    for note in &notes[view.notes.len()..] {
        appended.push(RunEvent {
            schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
            run_id: manifest.run_id.clone(),
            sequence: next_sequence,
            at_unix: note.created_at,
            payload: RunEventPayload::NoteAdded { note: note.clone() },
        });
        next_sequence = next_sequence
            .checked_add(1)
            .context("run event sequence overflow")?;
    }
    if !appended.is_empty() {
        append_event_lines(&paths.events, &appended)?;
        events.extend(appended.iter().cloned());
    }
    write_view(&paths.view, &project_view(&manifest, &events)?)?;
    Ok(appended)
}

/// Attests only a bounded regular file; directories and symlinks are not walked.
pub(super) fn attest_bounded_file(
    name: impl Into<String>,
    kind: InputKind,
    path: &Path,
    max_bytes: u64,
) -> Result<InputEvidence> {
    let name = name.into();
    ensure!(
        !name.trim().is_empty(),
        "input evidence name cannot be empty"
    );
    ensure!(
        name.trim() == name,
        "input evidence name cannot have outer whitespace"
    );
    ensure!(
        !path.as_os_str().is_empty(),
        "input evidence path cannot be empty"
    );
    let source = path.display().to_string();
    let identity = Evidence::available(source.clone());

    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(InputEvidence {
                name,
                kind,
                source,
                identity,
                digest_strategy: InputDigestStrategy::NotHashed,
                materialized_path: None,
                content: Evidence::missing("input path does not exist")?,
            });
        }
        Err(error) => {
            return Ok(InputEvidence {
                name,
                kind,
                source,
                identity,
                digest_strategy: InputDigestStrategy::NotHashed,
                materialized_path: None,
                content: Evidence::degraded(
                    None,
                    format!("failed to inspect input path: {error}"),
                )?,
            });
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(InputEvidence {
            name,
            kind,
            source,
            identity,
            digest_strategy: InputDigestStrategy::NotHashed,
            materialized_path: Some(path.to_path_buf()),
            content: Evidence::unsupported("symlink byte hashing is not supported")?,
        });
    }
    if metadata.is_dir() {
        return Ok(InputEvidence {
            name,
            kind,
            source,
            identity,
            digest_strategy: InputDigestStrategy::NotHashed,
            materialized_path: Some(path.to_path_buf()),
            content: Evidence::unsupported(
                "directory byte hashing is not supported; directories are never walked recursively",
            )?,
        });
    }
    if !metadata.is_file() {
        return Ok(InputEvidence {
            name,
            kind,
            source,
            identity,
            digest_strategy: InputDigestStrategy::NotHashed,
            materialized_path: Some(path.to_path_buf()),
            content: Evidence::unsupported("only bounded regular files can be byte-attested")?,
        });
    }
    if metadata.len() > max_bytes {
        return Ok(InputEvidence {
            name,
            kind,
            source,
            identity,
            digest_strategy: InputDigestStrategy::NotHashed,
            materialized_path: Some(path.to_path_buf()),
            content: Evidence::degraded(
                None,
                format!(
                    "regular file is {} bytes, above the {max_bytes}-byte hashing bound",
                    metadata.len()
                ),
            )?,
        });
    }

    let content = hash_open_bounded_file(path, max_bytes)?;
    let digest_strategy = if content.status() == EvidenceStatus::Available {
        InputDigestStrategy::FullSha256
    } else {
        InputDigestStrategy::NotHashed
    };
    Ok(InputEvidence {
        name,
        kind,
        source,
        identity,
        digest_strategy,
        materialized_path: Some(path.to_path_buf()),
        content,
    })
}

impl<'de, T> Deserialize<'de> for Evidence<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawEvidence<T> {
            status: EvidenceStatus,
            value: Option<T>,
            reason: Option<String>,
        }

        let raw = RawEvidence::deserialize(deserializer)?;
        Evidence::from_parts(raw.status, raw.value, raw.reason).map_err(de::Error::custom)
    }
}

fn validate_reason(reason: Option<&str>) -> Result<()> {
    let reason = reason.context("non-available evidence requires a reason")?;
    ensure!(!reason.trim().is_empty(), "evidence reason cannot be empty");
    ensure!(
        reason.trim() == reason,
        "evidence reason cannot have outer whitespace"
    );
    Ok(())
}

fn validate_job_component(job_id: &str) -> Result<()> {
    ensure!(
        !job_id.trim().is_empty(),
        "scheduler job id cannot be empty"
    );
    ensure!(
        job_id.trim() == job_id,
        "scheduler job id cannot have outer whitespace"
    );
    ensure!(
        !job_id.contains('\0'),
        "scheduler job id cannot contain NUL"
    );
    let mut components = Path::new(job_id).components();
    let safe = matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(component)), None) if component == std::ffi::OsStr::new(job_id)
    );
    ensure!(safe, "scheduler job id must be one safe path component");
    Ok(())
}

fn validate_run_id(run_id: &str) -> Result<()> {
    let Some(suffix) = run_id.strip_prefix("run_") else {
        bail!("run id must use the opaque run_ prefix");
    };
    ensure!(
        suffix.len() == 64
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "run id must contain a 256-bit lowercase hexadecimal identity"
    );
    Ok(())
}

fn generate_run_id(compose_file: &Path, scheduler_job_id: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = RUN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut hasher = Sha256::new();
    hasher.update(nanos.to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(counter.to_le_bytes());
    hasher.update(compose_file.as_os_str().to_string_lossy().as_bytes());
    hasher.update(scheduler_job_id.as_bytes());
    if let Some(host) = std::env::var_os("HOSTNAME") {
        hasher.update(host.to_string_lossy().as_bytes());
    }
    format!("run_{}", hex::encode(hasher.finalize()))
}

fn ensure_evidence_directory(paths: &RunEvidencePaths) -> Result<()> {
    let base = paths
        .root
        .parent()
        .context("run evidence path has no parent")?;
    fs::create_dir_all(base)
        .with_context(|| format!("failed to create run evidence root {}", base.display()))?;
    ensure_private_directory(base)?;
    match fs::create_dir(&paths.root) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to create run evidence directory {}",
                    paths.root.display()
                )
            });
        }
    }
    ensure_private_directory(&paths.root)
}

fn safe_evidence_directory_exists(paths: &RunEvidencePaths) -> Result<bool> {
    let base = paths
        .root
        .parent()
        .context("run evidence path has no parent")?;
    match fs::symlink_metadata(base) {
        Ok(_) => validate_private_directory(base)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", base.display()));
        }
    }
    match fs::symlink_metadata(&paths.root) {
        Ok(_) => {
            validate_private_directory(&paths.root)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => {
            Err(error).with_context(|| format!("failed to inspect {}", paths.root.display()))
        }
    }
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "run evidence path {} is not a real directory",
        path.display()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "run evidence directory {} must have owner-only permissions",
            path.display()
        );
    }
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "run evidence path {} is not a real directory",
        path.display()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("failed to restrict {}", path.display()))?;
    }
    Ok(())
}

fn lock_evidence_for_initialization(
    paths: &RunEvidencePaths,
) -> Result<crate::secure_io::StrictFlockGuard> {
    match fs::symlink_metadata(&paths.lock) {
        Ok(metadata) => ensure!(
            !metadata.file_type().is_symlink() && metadata.file_type().is_file(),
            "run evidence lock {} is not a regular file",
            paths.lock.display()
        ),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let protocol_files_exist = [
                &paths.manifest,
                &paths.inputs_lock,
                &paths.events,
                &paths.view,
            ]
            .iter()
            .any(|path| fs::symlink_metadata(path).is_ok());
            ensure!(
                !protocol_files_exist,
                "run evidence lock {} is missing from an existing protocol directory; refusing to recreate its coordination inode",
                paths.lock.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect run evidence lock {}",
                    paths.lock.display()
                )
            });
        }
    }
    crate::secure_io::acquire_flock_strict(
        &paths.lock,
        crate::secure_io::LockKind::Exclusive,
        RUN_EVIDENCE_LOCK_TIMEOUT,
    )
    .with_context(|| format!("failed to lock run evidence {}", paths.root.display()))
}

fn lock_existing_evidence(paths: &RunEvidencePaths) -> Result<crate::secure_io::StrictFlockGuard> {
    validate_existing_evidence_lock(paths)?;
    crate::secure_io::acquire_flock_strict(
        &paths.lock,
        crate::secure_io::LockKind::Exclusive,
        RUN_EVIDENCE_LOCK_TIMEOUT,
    )
    .with_context(|| format!("failed to lock run evidence {}", paths.root.display()))
}

/// Acquires the persistent per-run lock before whole-run evidence deletion.
///
/// Absence is legitimate for pre-protocol records. A present directory must
/// retain its original lock inode; cleanup must not race annotation writers or
/// read-only snapshot exporters by unlinking the evidence tree unlocked.
pub(super) fn lock_run_evidence_for_removal(
    compose_file: &Path,
    scheduler_job_id: &str,
) -> Result<Option<crate::secure_io::StrictFlockGuard>> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    if !safe_evidence_directory_exists(&paths)? {
        return Ok(None);
    }
    lock_existing_evidence(&paths).map(Some)
}

fn validate_existing_evidence_lock(paths: &RunEvidencePaths) -> Result<()> {
    let metadata = fs::symlink_metadata(&paths.lock).with_context(|| {
        format!(
            "run evidence lock {} is missing; refusing to recreate its coordination inode",
            paths.lock.display()
        )
    })?;
    ensure!(
        !metadata.file_type().is_symlink() && metadata.file_type().is_file(),
        "run evidence lock {} is not a regular file",
        paths.lock.display()
    );
    Ok(())
}

fn serialize_pretty<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>> {
    let mut bytes =
        serde_json::to_vec_pretty(value).with_context(|| format!("failed to serialize {label}"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_immutable(path: &Path, bytes: &[u8], label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => bail!("immutable {label} {} already exists", path.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to inspect immutable {label} {}", path.display())
            });
        }
    }
    // The strict evidence lock serializes cooperative writers. Atomic rename
    // publishes either the complete immutable document or nothing, so a crash
    // cannot strand a partially initialized manifest/input lock.
    crate::secure_io::write_atomic(path, bytes, true)
        .with_context(|| format!("failed to atomically create {label} {}", path.display()))
}

fn write_view(path: &Path, view: &RunView) -> Result<()> {
    let bytes = serialize_pretty(view, "run view")?;
    crate::secure_io::write_atomic(path, bytes, true)
        .with_context(|| format!("failed to atomically publish run view {}", path.display()))
}

fn read_bytes_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "run evidence file {} is not a regular file",
        path.display()
    );
    fs::read(path)
        .with_context(|| format!("failed to read {}", path.display()))
        .map(Some)
}

fn read_json_optional<T>(path: &Path) -> Result<Option<(T, Vec<u8>)>>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(bytes) = read_bytes_optional(path)? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(Some((value, bytes)))
}

fn read_json_required<T>(path: &Path) -> Result<(T, Vec<u8>)>
where
    T: for<'de> Deserialize<'de>,
{
    read_json_optional(path)?.with_context(|| format!("missing run evidence {}", path.display()))
}

fn validate_schema(schema_version: u32, label: &str) -> Result<()> {
    ensure!(
        schema_version == RUN_EVIDENCE_SCHEMA_VERSION,
        "{label} uses unsupported schema version {schema_version} (expected {RUN_EVIDENCE_SCHEMA_VERSION})"
    );
    Ok(())
}

fn validate_manifest(manifest: &RunManifest, scheduler_job_id: &str) -> Result<()> {
    validate_schema(manifest.schema_version, "run manifest")?;
    validate_run_id(&manifest.run_id)?;
    ensure!(
        manifest.scheduler_job_id == scheduler_job_id,
        "run manifest scheduler job id does not match its evidence directory"
    );
    validate_job_component(&manifest.scheduler_job_id)?;
    ensure!(
        is_lower_sha256(&manifest.submission_record_sha256),
        "run manifest submission-record identity is not lowercase SHA-256"
    );
    validate_content_evidence(&manifest.config)?;
    validate_content_evidence(&manifest.script)?;
    ensure!(
        is_lower_sha256(&manifest.inputs_lock_sha256),
        "run manifest input-lock digest is not lowercase SHA-256"
    );
    Ok(())
}

fn validate_inputs_lock(inputs_lock: &InputsLock) -> Result<()> {
    validate_schema(inputs_lock.schema_version, "input lock")?;
    validate_run_id(&inputs_lock.run_id)?;
    let mut previous_name: Option<&str> = None;
    for input in &inputs_lock.inputs {
        validate_input(input)?;
        if let Some(previous) = previous_name {
            ensure!(
                previous < input.name.as_str(),
                "input lock entries must be uniquely sorted by name"
            );
        }
        previous_name = Some(&input.name);
    }
    Ok(())
}

fn validate_input(input: &InputEvidence) -> Result<()> {
    ensure!(
        !input.name.trim().is_empty(),
        "input evidence name cannot be empty"
    );
    ensure!(
        input.name.trim() == input.name,
        "input evidence name has outer whitespace"
    );
    ensure!(
        !input.source.trim().is_empty(),
        "input evidence source cannot be empty"
    );
    if let Some(identity) = input.identity.value() {
        ensure!(
            !identity.trim().is_empty(),
            "available input identity cannot be empty"
        );
    }
    validate_content_evidence(&input.content)?;
    if input.digest_strategy == InputDigestStrategy::NotHashed {
        ensure!(
            input.content.status() != EvidenceStatus::Available,
            "not_hashed input evidence cannot include an available byte digest"
        );
    }
    Ok(())
}

fn validate_content_evidence(evidence: &Evidence<ContentDigest>) -> Result<()> {
    if let Some(digest) = evidence.value() {
        ensure!(
            is_lower_sha256(&digest.sha256),
            "content digest is not lowercase SHA-256"
        );
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn canonicalize_inputs(inputs: &mut [InputEvidence]) -> Result<()> {
    inputs.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.source.cmp(&right.source))
    });
    for input in inputs.iter() {
        validate_input(input)?;
    }
    ensure!(
        inputs.windows(2).all(|pair| pair[0].name != pair[1].name),
        "input evidence names must be unique"
    );
    Ok(())
}

fn read_events(path: &Path, manifest: &RunManifest) -> Result<Vec<RunEvent>> {
    let Some(bytes) = read_bytes_optional(path)? else {
        return Ok(Vec::new());
    };
    ensure!(
        bytes.is_empty() || bytes.ends_with(b"\n"),
        "run event log {} has a partial trailing line without a newline",
        path.display()
    );
    let raw = std::str::from_utf8(&bytes)
        .with_context(|| format!("run event log {} is not UTF-8", path.display()))?;
    let mut events = Vec::new();
    for (index, line) in raw.lines().enumerate() {
        ensure!(
            !line.trim().is_empty(),
            "run event log {} contains an empty line at {}",
            path.display(),
            index + 1
        );
        let event: RunEvent = serde_json::from_str(line).with_context(|| {
            format!(
                "failed to parse run event {} line {}",
                path.display(),
                index + 1
            )
        })?;
        events.push(event);
    }
    validate_events(manifest, &events)?;
    Ok(events)
}

fn validate_events(manifest: &RunManifest, events: &[RunEvent]) -> Result<()> {
    for (index, event) in events.iter().enumerate() {
        validate_schema(event.schema_version, "run event")?;
        ensure!(
            event.run_id == manifest.run_id,
            "run event has a mismatched run id"
        );
        let expected = u64::try_from(index)
            .unwrap_or(u64::MAX)
            .checked_add(1)
            .context("run event sequence overflow")?;
        ensure!(
            event.sequence == expected,
            "run event sequence {} is not the expected monotonic sequence {expected}",
            event.sequence
        );
        match &event.payload {
            RunEventPayload::Submitted { scheduler_job_id } => {
                ensure!(
                    index == 0,
                    "submitted must be the first and only submitted event"
                );
                ensure!(
                    scheduler_job_id == &manifest.scheduler_job_id,
                    "submitted event scheduler job id does not match manifest"
                );
                ensure!(
                    event.at_unix == manifest.submitted_at,
                    "submitted event timestamp does not match manifest"
                );
            }
            RunEventPayload::Annotation { text } => validate_annotation_text(text)?,
            RunEventPayload::TagsUpdated { tags } => {
                ensure!(
                    canonical_tags(tags.clone())? == *tags,
                    "tag snapshot is not canonical"
                );
            }
            RunEventPayload::NoteAdded { note } => validate_note(note)?,
        }
    }
    if !events.is_empty() {
        ensure!(
            matches!(events[0].payload, RunEventPayload::Submitted { .. }),
            "run event log must begin with submitted"
        );
    }
    Ok(())
}

fn project_view(manifest: &RunManifest, events: &[RunEvent]) -> Result<RunView> {
    validate_events(manifest, events)?;
    let submitted = events
        .first()
        .context("run event log has no submitted event")?;
    let mut tags = Vec::new();
    let mut notes = Vec::new();
    let mut annotations = Vec::new();
    for event in events {
        match &event.payload {
            RunEventPayload::Submitted { .. } => {}
            RunEventPayload::Annotation { text } => annotations.push(RunAnnotation {
                sequence: event.sequence,
                at_unix: event.at_unix,
                text: text.clone(),
            }),
            RunEventPayload::TagsUpdated { tags: snapshot } => tags.clone_from(snapshot),
            RunEventPayload::NoteAdded { note } => notes.push(note.clone()),
        }
    }
    let last = events
        .last()
        .context("run event log has no submitted event")?;
    Ok(RunView {
        schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
        run_id: manifest.run_id.clone(),
        scheduler_job_id: manifest.scheduler_job_id.clone(),
        submitted_at: submitted.at_unix,
        event_count: u64::try_from(events.len()).unwrap_or(u64::MAX),
        last_sequence: last.sequence,
        last_event_at: last.at_unix,
        tags,
        notes,
        annotations,
    })
}

#[cfg(test)]
fn append_payload_event(
    compose_file: &Path,
    scheduler_job_id: &str,
    at_unix: u64,
    payload: RunEventPayload,
) -> Result<RunEvent> {
    let paths = RunEvidencePaths::for_job(compose_file, scheduler_job_id)?;
    ensure!(
        safe_evidence_directory_exists(&paths)?,
        "run evidence does not exist for scheduler job {scheduler_job_id}"
    );
    let _guard = lock_existing_evidence(&paths)?;
    let (manifest, _) = read_json_required::<RunManifest>(&paths.manifest)?;
    validate_manifest(&manifest, scheduler_job_id)?;
    let mut events = read_events(&paths.events, &manifest)?;
    let sequence = events
        .last()
        .context("run event log has no submitted event")?
        .sequence
        .checked_add(1)
        .context("run event sequence overflow")?;
    let event = RunEvent {
        schema_version: RUN_EVIDENCE_SCHEMA_VERSION,
        run_id: manifest.run_id.clone(),
        sequence,
        at_unix,
        payload,
    };
    validate_event_payload(&event.payload)?;
    append_event_lines(&paths.events, std::slice::from_ref(&event))?;
    events.push(event.clone());
    write_view(&paths.view, &project_view(&manifest, &events)?)?;
    Ok(event)
}

#[cfg(test)]
fn validate_event_payload(payload: &RunEventPayload) -> Result<()> {
    match payload {
        RunEventPayload::Submitted { scheduler_job_id } => validate_job_component(scheduler_job_id),
        RunEventPayload::Annotation { text } => validate_annotation_text(text),
        RunEventPayload::TagsUpdated { tags } => {
            ensure!(
                canonical_tags(tags.clone())? == *tags,
                "tag snapshot is not canonical"
            );
            Ok(())
        }
        RunEventPayload::NoteAdded { note } => validate_note(note),
    }
}

fn append_event_lines(path: &Path, events: &[RunEvent]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let mut bytes = match read_bytes_optional(path)? {
        Some(bytes) => {
            ensure!(
                bytes.is_empty() || bytes.ends_with(b"\n"),
                "run event log {} has a partial trailing line",
                path.display()
            );
            bytes
        }
        None => Vec::new(),
    };
    for event in events {
        serde_json::to_writer(&mut bytes, event).context("failed to serialize run event")?;
        bytes.push(b'\n');
    }
    // Logical history is append-only, while the byte stream is atomically
    // replaced under the strict sidecar lock. A crash therefore leaves either
    // the previous complete stream or the previous stream plus all new lines.
    crate::secure_io::write_atomic(path, bytes, true)
        .with_context(|| format!("failed to atomically append {}", path.display()))
}

fn validate_annotation_text(text: &str) -> Result<()> {
    ensure!(!text.trim().is_empty(), "run annotation cannot be empty");
    ensure!(
        text.trim() == text,
        "run annotation cannot have outer whitespace"
    );
    ensure!(
        text.len() <= MAX_ANNOTATION_BYTES,
        "run annotation exceeds {MAX_ANNOTATION_BYTES} bytes"
    );
    Ok(())
}

fn validate_note(note: &JobNote) -> Result<()> {
    let normalized = super::record::validate_note_text(&note.text)?;
    ensure!(
        normalized == note.text,
        "run note must already use the record layer's normalized text"
    );
    ensure!(note.created_at != 0, "run note timestamp cannot be zero");
    Ok(())
}

fn canonical_tags(mut tags: Vec<String>) -> Result<Vec<String>> {
    for tag in &tags {
        super::record::validate_tag(tag)?;
    }
    tags.sort();
    tags.dedup();
    ensure!(
        tags.len() <= super::record::MAX_TAGS_PER_RECORD,
        "run tag snapshot exceeds the record limit of {} tags",
        super::record::MAX_TAGS_PER_RECORD
    );
    Ok(tags)
}

fn hash_open_bounded_file(path: &Path, max_bytes: u64) -> Result<Evidence<ContentDigest>> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) => {
            return Evidence::degraded(None, format!("failed to open bounded input: {error}"));
        }
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(error) => {
            return Evidence::degraded(None, format!("failed to inspect open input: {error}"));
        }
    };
    if !metadata.is_file() {
        return Evidence::degraded(None, "input changed and is no longer a regular file");
    }
    if metadata.len() > max_bytes {
        return Evidence::degraded(
            None,
            format!(
                "input changed to {} bytes, above the {max_bytes}-byte hashing bound",
                metadata.len()
            ),
        );
    }

    let mut hasher = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let remaining = max_bytes.saturating_sub(size_bytes);
        let limit = usize::try_from(remaining.saturating_add(1))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = match file.read(&mut buffer[..limit]) {
            Ok(read) => read,
            Err(error) => {
                return Evidence::degraded(None, format!("failed to hash bounded input: {error}"));
            }
        };
        if read == 0 {
            break;
        }
        let read_u64 = u64::try_from(read).unwrap_or(u64::MAX);
        let Some(next_size) = size_bytes.checked_add(read_u64) else {
            return Evidence::degraded(None, "bounded input size overflowed while hashing");
        };
        if next_size > max_bytes {
            return Evidence::degraded(None, "input grew beyond the hashing bound while reading");
        }
        hasher.update(&buffer[..read]);
        size_bytes = next_size;
    }
    Ok(Evidence::available(ContentDigest {
        sha256: hex::encode(hasher.finalize()),
        size_bytes,
    }))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write as _;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn seed(job_id: &str) -> RunEvidenceSeed {
        RunEvidenceSeed {
            scheduler_job_id: job_id.to_string(),
            submission_record_sha256: ContentDigest::from_bytes(b"submission-record").sha256,
            backend: SubmissionBackend::Slurm,
            kind: SubmissionKind::Main,
            submitted_at: 1_700_000_000,
            config: Evidence::available(ContentDigest::from_bytes(b"services: {}\n")),
            script: Evidence::available(ContentDigest::from_bytes(b"#!/bin/sh\ntrue\n")),
            provenance: Evidence::missing("legacy submission has no provenance")
                .expect("missing evidence"),
            inputs: Vec::new(),
        }
    }

    #[test]
    fn initialization_creates_immutable_manifest_lock_event_and_atomic_view() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        fs::write(&compose, "services: {}\n").expect("compose");

        let state = initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");

        assert!(state.manifest.run_id.starts_with("run_"));
        assert_ne!(state.manifest.run_id, state.manifest.scheduler_job_id);
        assert!(
            paths
                .root
                .starts_with(tmp.path().join(".hpc-compose/evidence"))
        );
        assert!(paths.manifest.is_file());
        assert!(paths.inputs_lock.is_file());
        assert!(paths.events.is_file());
        assert!(paths.view.is_file());
        assert_eq!(state.events.len(), 1);
        assert!(matches!(
            state.events[0].payload,
            RunEventPayload::Submitted { .. }
        ));
        assert_eq!(state.events[0].sequence, 1);
        assert_eq!(state.view.last_sequence, 1);
        assert_eq!(state.view.event_count, 1);
        assert_eq!(
            ContentDigest::from_bytes(&fs::read(&paths.inputs_lock).expect("lock bytes")).sha256,
            state.manifest.inputs_lock_sha256
        );
    }

    #[test]
    fn repeated_initialization_preserves_identity_and_does_not_duplicate_submitted() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        let seed = seed("12345");

        let first = initialize_run_evidence(&compose, &seed).expect("first");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let manifest_bytes = fs::read(&paths.manifest).expect("manifest");
        let second = initialize_run_evidence(&compose, &seed).expect("second");

        assert_eq!(second.manifest.run_id, first.manifest.run_id);
        assert_eq!(fs::read(&paths.manifest).expect("manifest"), manifest_bytes);
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.view.event_count, 1);
    }

    #[test]
    fn annotation_events_are_monotonic_and_atomically_advance_view() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");

        let first = append_annotation_event(&compose, "12345", 1_700_000_010, "baseline")
            .expect("first annotation");
        let second = append_annotation_event(&compose, "12345", 1_700_000_020, "retry")
            .expect("second annotation");
        let loaded = load_run_evidence(&compose, "12345")
            .expect("load")
            .expect("present");

        assert_eq!((first.sequence, second.sequence), (2, 3));
        assert_eq!(loaded.view.last_sequence, 3);
        assert_eq!(loaded.view.event_count, 3);
        assert_eq!(
            loaded
                .view
                .annotations
                .iter()
                .map(|annotation| annotation.text.as_str())
                .collect::<Vec<_>>(),
            ["baseline", "retry"]
        );
        assert!(
            RunEvidencePaths::for_job(&compose, "12345")
                .expect("paths")
                .lock
                .is_file()
        );
    }

    #[test]
    fn legacy_absence_loads_as_none() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        assert!(
            load_run_evidence(&compose, "12345")
                .expect("legacy absence")
                .is_none()
        );
    }

    #[test]
    fn a_partial_evidence_directory_is_not_misclassified_as_legacy_absence() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        fs::remove_file(&paths.manifest).expect("remove manifest");

        let load_error = load_run_evidence(&compose, "12345")
            .expect_err("partial evidence must fail closed instead of looking legacy");
        assert!(
            load_error.to_string().contains("manifest"),
            "got: {load_error:#}"
        );
        let export_error =
            export_run_evidence_files(&compose, "12345", &seed("12345").submission_record_sha256)
                .expect_err("read-only export must distinguish partial evidence from absence");
        assert!(
            export_error.to_string().contains("manifest"),
            "got: {export_error:#}"
        );
    }

    #[test]
    fn a_missing_persistent_event_lock_is_corruption_not_a_lock_to_recreate() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        fs::remove_file(&paths.lock).expect("remove lock");

        let load_error = load_run_evidence(&compose, "12345")
            .expect_err("typed readers must reject a missing coordination inode");
        assert!(
            load_error.to_string().contains("lock"),
            "got: {load_error:#}"
        );
        let error = sync_record_annotations(&compose, "12345", 1_700_000_010, &[], &[])
            .expect_err("an existing protocol directory must never recreate its lock inode");
        assert!(error.to_string().contains("lock"), "got: {error:#}");
        assert!(
            !paths.lock.exists(),
            "a failed mutation must not silently replace the persistent lock inode"
        );
    }

    #[cfg(unix)]
    #[test]
    fn read_only_export_validates_directory_privacy_without_chmod_repair() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let base = paths.root.parent().expect("evidence base");

        fs::set_permissions(base, fs::Permissions::from_mode(0o500)).expect("base read-only");
        fs::set_permissions(&paths.root, fs::Permissions::from_mode(0o500))
            .expect("run root read-only");
        let exported =
            export_run_evidence_files(&compose, "12345", &seed("12345").submission_record_sha256);
        let base_mode = fs::metadata(base)
            .expect("base metadata")
            .permissions()
            .mode()
            & 0o777;
        let root_mode = fs::metadata(&paths.root)
            .expect("root metadata")
            .permissions()
            .mode()
            & 0o777;
        fs::set_permissions(base, fs::Permissions::from_mode(0o700)).expect("restore base");
        fs::set_permissions(&paths.root, fs::Permissions::from_mode(0o700)).expect("restore root");

        exported.expect("owner-only read-only directories remain valid");
        assert_eq!(
            base_mode, 0o500,
            "read-only export must not chmod its source"
        );
        assert_eq!(
            root_mode, 0o500,
            "read-only export must not chmod its source"
        );

        fs::set_permissions(&paths.root, fs::Permissions::from_mode(0o750))
            .expect("make root too broad");
        let error =
            export_run_evidence_files(&compose, "12345", &seed("12345").submission_record_sha256)
                .expect_err("group-accessible evidence must fail closed without repair");
        let broad_mode = fs::metadata(&paths.root)
            .expect("broad metadata")
            .permissions()
            .mode()
            & 0o777;
        fs::set_permissions(&paths.root, fs::Permissions::from_mode(0o700)).expect("restore root");
        assert!(error.to_string().contains("owner-only"), "got: {error:#}");
        assert_eq!(broad_mode, 0o750, "validation must not repair permissions");
    }

    #[test]
    fn bounded_input_attestation_distinguishes_states_without_walking_directories() {
        let tmp = tempfile::tempdir().expect("tmp");
        let small = tmp.path().join("small.bin");
        let large = tmp.path().join("large.bin");
        let missing = tmp.path().join("missing.bin");
        let dataset = tmp.path().join("dataset");
        fs::write(&small, b"abc").expect("small");
        fs::write(&large, b"0123456789").expect("large");
        fs::create_dir(&dataset).expect("dataset");
        fs::write(dataset.join("huge-shard.bin"), vec![7_u8; 64 * 1024]).expect("shard");

        let available =
            attest_bounded_file("small", InputKind::File, &small, 3).expect("available");
        let missing =
            attest_bounded_file("missing", InputKind::File, &missing, 3).expect("missing");
        let degraded = attest_bounded_file("large", InputKind::File, &large, 3).expect("degraded");
        let unsupported = attest_bounded_file("dataset", InputKind::Dataset, &dataset, 1_000_000)
            .expect("unsupported");

        assert_eq!(available.content.status(), EvidenceStatus::Available);
        assert_eq!(available.content.value().expect("digest").size_bytes, 3);
        assert_eq!(missing.content.status(), EvidenceStatus::Missing);
        assert!(missing.content.value().is_none());
        assert_eq!(degraded.content.status(), EvidenceStatus::Degraded);
        assert!(degraded.content.value().is_none());
        assert_eq!(unsupported.content.status(), EvidenceStatus::Unsupported);
        assert!(unsupported.content.value().is_none());
        assert!(
            unsupported
                .content
                .reason()
                .expect("reason")
                .contains("directory")
        );
        assert_eq!(unsupported.identity.status(), EvidenceStatus::Available);
        assert_eq!(unsupported.digest_strategy, InputDigestStrategy::NotHashed);
        assert!(unsupported.materialized_path.is_some());
    }

    #[test]
    fn typed_record_annotation_projection_is_idempotent_and_rebuildable() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let tags = vec!["baseline".to_string(), "published".to_string()];
        let notes = vec![
            JobNote {
                text: "first observation".to_string(),
                created_at: 1_700_000_030,
            },
            JobNote {
                text: "second observation".to_string(),
                created_at: 1_700_000_040,
            },
        ];

        let appended =
            sync_record_annotations_inner(&compose, "12345", 1_700_000_050, &tags, &notes)
                .expect("first sync");
        let repeated =
            sync_record_annotations_inner(&compose, "12345", 1_700_000_060, &tags, &notes)
                .expect("idempotent sync");

        assert_eq!(appended.len(), 3);
        assert!(repeated.is_empty());
        let loaded = load_run_evidence(&compose, "12345")
            .expect("load")
            .expect("present");
        assert_eq!(loaded.view.tags, tags);
        assert_eq!(loaded.view.notes, notes);
        assert_eq!(loaded.view.event_count, 4);

        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        fs::write(&paths.view, b"{ stale projection }").expect("corrupt view");
        let rebuilt = rebuild_run_view(&compose, "12345")
            .expect("rebuild view")
            .expect("present view");
        assert_eq!(rebuilt, loaded.view);
        let repaired = sync_record_annotations_inner(
            &compose,
            "12345",
            1_700_000_070,
            &loaded.view.tags,
            &loaded.view.notes,
        )
        .expect("rebuild stale view");
        assert!(repaired.is_empty());
        let rebuilt: RunView = serde_json::from_slice(&fs::read(&paths.view).expect("view bytes"))
            .expect("rebuilt view");
        assert_eq!(rebuilt, loaded.view);
    }

    #[test]
    fn evidence_deserialization_rejects_invalid_value_reason_combinations() {
        let raw = r#"{"status":"available","reason":"but no value"}"#;
        assert!(serde_json::from_str::<Evidence<String>>(raw).is_err());
        let raw = r#"{"status":"missing","value":"unexpected","reason":"gone"}"#;
        assert!(serde_json::from_str::<Evidence<String>>(raw).is_err());
        let raw = r#"{"status":"degraded","reason":"   "}"#;
        assert!(serde_json::from_str::<Evidence<String>>(raw).is_err());
    }

    #[test]
    fn concurrent_appenders_keep_every_monotonic_sequence() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for index in 0..8_u64 {
            let compose = compose.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                append_annotation_event(
                    &compose,
                    "12345",
                    1_700_001_000 + index,
                    format!("annotation-{index}"),
                )
                .expect("concurrent append")
            }));
        }
        let mut returned_sequences = handles
            .into_iter()
            .map(|handle| handle.join().expect("thread").sequence)
            .collect::<Vec<_>>();
        returned_sequences.sort_unstable();

        let loaded = load_run_evidence(&compose, "12345")
            .expect("load")
            .expect("present");
        assert_eq!(returned_sequences, (2..=9).collect::<Vec<_>>());
        assert_eq!(
            loaded
                .events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=9).collect::<Vec<_>>()
        );
        assert_eq!(loaded.view.event_count, 9);
    }

    #[test]
    fn partial_trailing_event_fails_closed_without_discarding_history() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let valid_prefix = fs::read(&paths.events).expect("valid event prefix");
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&paths.events)
            .expect("open events");
        file.write_all(b"{\"schema_version\":1")
            .expect("partial event");
        drop(file);

        let error = load_run_evidence(&compose, "12345").expect_err("partial log must fail");
        assert!(
            error.to_string().contains("failed to parse run event")
                || error.to_string().contains("partial trailing line")
        );
        assert!(
            fs::read(&paths.events)
                .expect("unchanged corrupt stream")
                .starts_with(&valid_prefix)
        );
        assert!(append_annotation_event(&compose, "12345", 1_700_000_100, "not-appended").is_err());
    }

    #[test]
    fn complete_event_json_without_its_record_terminator_fails_closed() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let mut bytes = fs::read(&paths.events).expect("events");
        assert_eq!(bytes.pop(), Some(b'\n'));
        fs::write(&paths.events, &bytes).expect("unterminated event record");

        let error = load_run_evidence(&compose, "12345")
            .expect_err("every committed JSONL event must end with a newline");
        assert!(
            error.to_string().contains("trailing") || error.to_string().contains("newline"),
            "got: {error:#}"
        );
        assert_eq!(
            fs::read(&paths.events).expect("preserved event bytes"),
            bytes,
            "validation must preserve the damaged stream for diagnosis"
        );
    }

    #[test]
    fn future_manifest_schema_is_rejected() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        initialize_run_evidence(&compose, &seed("12345")).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&paths.manifest).expect("manifest"))
                .expect("manifest json");
        manifest["schema_version"] = serde_json::json!(RUN_EVIDENCE_SCHEMA_VERSION + 1);
        fs::write(
            &paths.manifest,
            serde_json::to_vec_pretty(&manifest).expect("serialize"),
        )
        .expect("future manifest");

        let error = load_run_evidence(&compose, "12345").expect_err("future schema");
        assert!(error.to_string().contains("unsupported schema version"));
    }

    #[test]
    fn immutable_initialization_conflict_preserves_original_bytes_and_events() {
        let tmp = tempfile::tempdir().expect("tmp");
        let compose = tmp.path().join("compose.yaml");
        let original = seed("12345");
        initialize_run_evidence(&compose, &original).expect("initialize");
        let paths = RunEvidencePaths::for_job(&compose, "12345").expect("paths");
        let manifest_before = fs::read(&paths.manifest).expect("manifest");
        let inputs_before = fs::read(&paths.inputs_lock).expect("inputs");
        let events_before = fs::read(&paths.events).expect("events");

        let mut conflicting = original;
        conflicting.config = Evidence::available(ContentDigest::from_bytes(b"different"));
        let error = initialize_run_evidence(&compose, &conflicting).expect_err("conflict");
        assert!(error.to_string().contains("different submit-time evidence"));
        assert_eq!(
            fs::read(&paths.manifest).expect("manifest"),
            manifest_before
        );
        assert_eq!(fs::read(&paths.inputs_lock).expect("inputs"), inputs_before);
        assert_eq!(fs::read(&paths.events).expect("events"), events_before);
    }
}
