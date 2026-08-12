//! Read-only cache state derived from the artifacts visible at observation time.

use std::path::Path;

use crate::planner::ImageSource;
use crate::runtime_plan::RuntimeService;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArtifactPresence {
    Present,
    Missing,
}

impl ArtifactPresence {
    pub(crate) fn is_present(self) -> bool {
        matches!(self, Self::Present)
    }

    pub(crate) fn cache_state_label(self) -> &'static str {
        match self {
            Self::Present => "cache hit",
            Self::Missing => "cache miss",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReuseExpectation {
    RebuildOnPrepare,
    CacheHit,
    CacheMiss,
    LocalImagePresent,
    LocalImageMissing,
    HostRuntime,
}

impl ReuseExpectation {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::RebuildOnPrepare => "rebuild on prepare",
            Self::CacheHit => "cache hit",
            Self::CacheMiss => "cache miss",
            Self::LocalImagePresent => "local image present",
            Self::LocalImageMissing => "local image missing",
            Self::HostRuntime => "host runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildReason {
    PrepareMountsPresent,
    RuntimeArtifactMissing,
}

impl RebuildReason {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PrepareMountsPresent => "prepare.mounts are present",
            Self::RuntimeArtifactMissing => "runtime cache artifact is missing",
        }
    }
}

pub(crate) fn artifact_presence(path: &Path) -> ArtifactPresence {
    if path.exists() {
        ArtifactPresence::Present
    } else {
        ArtifactPresence::Missing
    }
}

pub(crate) fn reuse_expectation(service: &RuntimeService) -> ReuseExpectation {
    if let Some(prepare) = &service.prepare {
        if prepare.force_rebuild {
            ReuseExpectation::RebuildOnPrepare
        } else {
            match artifact_presence(&service.runtime_image) {
                ArtifactPresence::Present => ReuseExpectation::CacheHit,
                ArtifactPresence::Missing => ReuseExpectation::CacheMiss,
            }
        }
    } else {
        match &service.source {
            ImageSource::LocalSqsh(path) | ImageSource::LocalSif(path) => {
                match artifact_presence(path) {
                    ArtifactPresence::Present => ReuseExpectation::LocalImagePresent,
                    ArtifactPresence::Missing => ReuseExpectation::LocalImageMissing,
                }
            }
            ImageSource::Remote(_) => match artifact_presence(&service.runtime_image) {
                ArtifactPresence::Present => ReuseExpectation::CacheHit,
                ArtifactPresence::Missing => ReuseExpectation::CacheMiss,
            },
            ImageSource::Host => ReuseExpectation::HostRuntime,
        }
    }
}

pub(crate) fn rebuild_reason(service: &RuntimeService) -> Option<RebuildReason> {
    let prepare = service.prepare.as_ref()?;
    if prepare.force_rebuild {
        Some(RebuildReason::PrepareMountsPresent)
    } else if artifact_presence(&service.runtime_image) == ArtifactPresence::Missing {
        Some(RebuildReason::RuntimeArtifactMissing)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;
    use crate::planner::{ExecutionSpec, PreparedImageSpec, ServicePlacement};
    use crate::spec::{ServiceFailurePolicy, ServiceSlurmConfig};

    fn runtime_service(
        source: ImageSource,
        runtime_image: PathBuf,
        prepare: Option<PreparedImageSpec>,
    ) -> RuntimeService {
        RuntimeService {
            name: "service".into(),
            runtime_image,
            execution: ExecutionSpec::Shell("true".into()),
            environment: Vec::new(),
            volumes: Vec::new(),
            working_dir: None,
            depends_on: Vec::new(),
            readiness: None,
            assertions: None,
            failure_policy: ServiceFailurePolicy::default(),
            placement: ServicePlacement::default(),
            slurm: ServiceSlurmConfig::default(),
            prepare,
            source,
        }
    }

    fn prepare(force_rebuild: bool) -> PreparedImageSpec {
        PreparedImageSpec {
            commands: vec!["true".into()],
            mounts: if force_rebuild {
                vec!["/host:/mnt".into()]
            } else {
                Vec::new()
            },
            env: Vec::new(),
            root: true,
            force_rebuild,
        }
    }

    #[test]
    fn artifact_presence_uses_path_exists_and_exact_labels() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let path = tmpdir.path().join("artifact.sqsh");

        let missing = artifact_presence(&path);
        assert_eq!(missing, ArtifactPresence::Missing);
        assert!(!missing.is_present());
        assert_eq!(missing.cache_state_label(), "cache miss");

        fs::write(&path, "artifact").expect("artifact");
        let present = artifact_presence(&path);
        assert_eq!(present, ArtifactPresence::Present);
        assert!(present.is_present());
        assert_eq!(present.cache_state_label(), "cache hit");
    }

    #[test]
    fn reuse_expectation_preserves_prepare_source_and_runtime_precedence() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let present = tmpdir.path().join("present.sqsh");
        let missing = tmpdir.path().join("missing.sqsh");
        fs::write(&present, "artifact").expect("artifact");

        let forced = runtime_service(
            ImageSource::Remote("docker://example/image:tag".into()),
            missing.clone(),
            Some(prepare(true)),
        );
        assert_eq!(
            reuse_expectation(&forced),
            ReuseExpectation::RebuildOnPrepare
        );

        let prepared = runtime_service(
            ImageSource::LocalSif(missing.clone()),
            present.clone(),
            Some(prepare(false)),
        );
        assert_eq!(reuse_expectation(&prepared), ReuseExpectation::CacheHit);

        let local_present = runtime_service(
            ImageSource::LocalSqsh(present.clone()),
            missing.clone(),
            None,
        );
        assert_eq!(
            reuse_expectation(&local_present),
            ReuseExpectation::LocalImagePresent
        );

        let local_missing = runtime_service(
            ImageSource::LocalSif(missing.clone()),
            present.clone(),
            None,
        );
        assert_eq!(
            reuse_expectation(&local_missing),
            ReuseExpectation::LocalImageMissing
        );

        let remote = runtime_service(
            ImageSource::Remote("docker://example/image:tag".into()),
            present.clone(),
            None,
        );
        assert_eq!(reuse_expectation(&remote), ReuseExpectation::CacheHit);

        let host = runtime_service(ImageSource::Host, present, None);
        assert_eq!(reuse_expectation(&host), ReuseExpectation::HostRuntime);
    }

    #[test]
    fn rebuild_reason_observes_only_nonforced_prepared_runtime_artifacts() {
        let tmpdir = tempfile::tempdir().expect("tmpdir");
        let present = tmpdir.path().join("present.sqsh");
        let missing = tmpdir.path().join("missing.sqsh");
        fs::write(&present, "artifact").expect("artifact");

        let unprepared = runtime_service(ImageSource::Host, missing.clone(), None);
        assert_eq!(rebuild_reason(&unprepared), None);

        let forced = runtime_service(
            ImageSource::Remote("docker://example/image:tag".into()),
            missing.clone(),
            Some(prepare(true)),
        );
        assert_eq!(
            rebuild_reason(&forced),
            Some(RebuildReason::PrepareMountsPresent)
        );

        let missing_runtime = runtime_service(
            ImageSource::Remote("docker://example/image:tag".into()),
            missing,
            Some(prepare(false)),
        );
        assert_eq!(
            rebuild_reason(&missing_runtime),
            Some(RebuildReason::RuntimeArtifactMissing)
        );

        let cached = runtime_service(
            ImageSource::Remote("docker://example/image:tag".into()),
            present,
            Some(prepare(false)),
        );
        assert_eq!(rebuild_reason(&cached), None);
    }
}
