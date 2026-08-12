//! Fallible cache-inspect query assembly.

use std::path::Path;

use anyhow::Result;
use hpc_compose::cache::{load_manifest_if_exists, manifest_path_for};
use hpc_compose::planner::ImageSource;
use hpc_compose::runtime_plan::{RuntimePlan, base_image_path_for_backend};

use crate::cache::observation::{artifact_presence, reuse_expectation};
use crate::domain::registry_host_for_remote;
use crate::output::cache::{
    CacheArtifactInspect, CacheInspectReport, CacheInspectService, source_image_display,
};

pub(super) fn build(plan: &RuntimePlan, filter: Option<&str>) -> Result<CacheInspectReport> {
    let mut services = Vec::new();
    for service in &plan.ordered_services {
        if let Some(filter_name) = filter
            && service.name != filter_name
        {
            continue;
        }

        let base_artifact = if let ImageSource::Remote(_) = &service.source {
            let base_path =
                base_image_path_for_backend(&plan.cache_dir, service, plan.runtime.backend);
            Some(CacheArtifactInspect {
                path: base_path.clone(),
                artifact_present: artifact_presence(&base_path).is_present(),
                manifest_path: manifest_path_for(&base_path),
                manifest: load_manifest_if_exists(&base_path)?,
            })
        } else {
            None
        };

        services.push(CacheInspectService {
            service_name: service.name.clone(),
            source_image: source_image_display(&service.source),
            base_registry: match &service.source {
                ImageSource::Remote(remote) => Some(registry_host_for_remote(remote)),
                ImageSource::LocalSqsh(_) | ImageSource::LocalSif(_) | ImageSource::Host => None,
            },
            base_artifact,
            runtime_artifact: build_artifact_inspect(&service.runtime_image)?,
            current_reuse_expectation: reuse_expectation(service).label().to_string(),
            note: service.prepare.as_ref().and_then(|prepare| {
                if prepare.force_rebuild {
                    Some(
                        "this service rebuilds on prepare because prepare.mounts are present"
                            .into(),
                    )
                } else {
                    None
                }
            }),
        });
    }

    Ok(CacheInspectReport {
        schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
        cache_dir: plan.cache_dir.clone(),
        services,
    })
}

fn build_artifact_inspect(path: &Path) -> Result<CacheArtifactInspect> {
    let has_artifact_path = !path.as_os_str().is_empty();
    Ok(CacheArtifactInspect {
        path: path.to_path_buf(),
        artifact_present: has_artifact_path && artifact_presence(path).is_present(),
        manifest_path: manifest_path_for(path),
        manifest: if has_artifact_path {
            load_manifest_if_exists(path)?
        } else {
            None
        },
    })
}
