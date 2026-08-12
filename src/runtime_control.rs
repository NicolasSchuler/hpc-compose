//! File-based control messages consumed by the local runtime supervisor.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Publishes a dev restart request beneath `control_dir`.
///
/// Service names are sorted so callers with different collection types emit
/// the same newline-delimited protocol body.
pub(crate) fn write_restart_request<I, S>(control_dir: &Path, services: I) -> Result<PathBuf>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let request_dir = control_dir.join("restart");
    fs::create_dir_all(&request_dir)
        .with_context(|| format!("failed to create {}", request_dir.display()))?;
    let millis = crate::time_util::unix_timestamp_millis();
    let path = request_dir.join(format!("restart-{}-{millis}.request", std::process::id()));
    let mut services = services
        .into_iter()
        .map(|service| service.as_ref().to_owned())
        .collect::<Vec<_>>();
    services.sort();
    let body = services.join("\n");
    crate::secure_io::write_atomic(&path, format!("{body}\n").as_bytes(), false)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}
