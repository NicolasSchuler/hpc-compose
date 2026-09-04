//! `hpc-compose pull` — resolve a tracked job's artifact payload and print the
//! `rsync` command to copy it to a laptop.
//!
//! Read-only and connection-free: it loads the tracked record + artifact
//! manifest, summarizes the collected files, and prints an `rsync` line (with
//! SSH connection multiplexing so an OTP login node prompts once). It never
//! copies anything, opens a connection, or spawns a process.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::ResolvedContext;
use hpc_compose::job::{
    ArtifactManifest, artifact_manifest_path_for_record, artifact_payload_dir_for_record,
    load_submission_record,
};

use super::ssh_hint::{OTP_MULTIPLEX_NOTE, control_master_opts_str};
pub(crate) use crate::output::runtime::PullOutput;
use crate::{output, term};

/// Build the cluster-side path label and the rsync command `pull` suggests.
/// Factored out so the ControlMaster opts (one OTP per session) and the
/// login_host-filled-vs-`<login-node>`-placeholder behavior are unit-testable:
/// a divergent ControlPath spelling here would silently cost a second OTP prompt.
fn pull_rsync_command(
    login_host: Option<&str>,
    payload_dir: &Path,
    into_display: &str,
) -> (String, String) {
    let login = login_host.unwrap_or("<login-node>");
    let cluster_path = format!("{login}:{}", payload_dir.display());
    // Quote once for the local shell. Modern rsync protects these literal
    // filename bytes when passing them onward to the remote shell.
    let source = crate::shell_quote::quote_if_needed_for_display(&format!("{cluster_path}/"));
    let destination = crate::shell_quote::quote_if_needed_for_display(&format!("{into_display}/"));
    let suggested_command = format!(
        "rsync -avz -e 'ssh {opts}' -- {source} {destination}",
        opts = control_master_opts_str(),
    );
    (cluster_path, suggested_command)
}

pub(crate) fn pull(
    context: ResolvedContext,
    job_id: Option<String>,
    into: Option<PathBuf>,
    format: Option<OutputFormat>,
) -> Result<()> {
    let record = load_submission_record(&context.compose_file.value, job_id.as_deref())?;
    // Same preconditions as `artifacts`/export_artifacts, but read-only.
    record.artifact_export_dir.as_deref().with_context(|| {
        format!(
            "tracked submission metadata for job {} does not include x-slurm.artifacts.export_dir; resubmit with artifact tracking enabled",
            record.job_id
        )
    })?;
    let manifest_path = artifact_manifest_path_for_record(&record);
    if !manifest_path.exists() {
        bail!(
            "tracked artifact manifest does not exist for job {} at {}; run the job and wait for teardown collection to finish first",
            record.job_id,
            manifest_path.display()
        );
    }
    let manifest: ArtifactManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).with_context(|| {
            format!(
                "failed to read artifact manifest {}",
                manifest_path.display()
            )
        })?)
        .with_context(|| {
            format!(
                "failed to parse artifact manifest {}",
                manifest_path.display()
            )
        })?;

    let payload_dir = artifact_payload_dir_for_record(&record);

    // Every collected relative path: the implicit default plus each bundle.
    let mut relative_paths: std::collections::BTreeSet<String> =
        manifest.copied_relative_paths.iter().cloned().collect();
    for bundle in manifest.bundles.values() {
        relative_paths.extend(bundle.copied_relative_paths.iter().cloned());
    }
    let files = relative_paths.len();
    // Best-effort byte total from the locally-present payload files: bounded
    // metadata reads over the known file list, never a tree walk, copy, or
    // network call. Files absent locally are skipped.
    let bytes = relative_paths
        .iter()
        .filter_map(|rel| std::fs::metadata(payload_dir.join(rel)).ok())
        .map(|meta| meta.len())
        .sum();
    let bundles: Vec<String> = manifest.bundles.keys().cloned().collect();

    let into = into.unwrap_or_else(|| PathBuf::from("."));
    let into_display = into.display().to_string();
    // Descriptive only. Never fall back to current_hostname: that is the laptop
    // host, not the cluster login host — a `<login-node>` placeholder is safer.
    let login_host = context.login_host.clone();
    let (cluster_path, suggested_command) =
        pull_rsync_command(login_host.as_deref(), &payload_dir, &into_display);

    match output::resolve_output_format(format) {
        OutputFormat::Json => {
            let out = PullOutput {
                schema_version: crate::output::OUTPUT_SCHEMA_VERSION,
                job_id: record.job_id.clone(),
                bundles,
                login_host,
                cluster_path,
                into: into_display,
                files,
                bytes,
                suggested_command,
                ssh_multiplex_hint: OTP_MULTIPLEX_NOTE.to_string(),
            };
            println!(
                "{}",
                crate::output::to_pretty_json(&out).context("failed to serialize pull output")?
            );
        }
        OutputFormat::Text => {
            println!("{}", term::styled_section_header("Pull artifacts"));
            println!("  job:     {}", record.job_id);
            println!("  files:   {files} ({bytes} bytes)");
            if !bundles.is_empty() {
                println!("  bundles: {}", bundles.join(", "));
            }
            println!();
            println!("Copy the artifacts to your laptop:");
            println!("  {suggested_command}");
            println!();
            println!(
                "{}",
                term::styled_dim(&format!(
                    "This copies the payload to your machine. To populate the configured \
                     export_dir on the cluster (what downstream jobs read), run \
                     `hpc-compose artifacts --job-id {}`.",
                    record.job_id
                ))
            );
            println!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));
            if login_host.is_none() {
                println!(
                    "{}",
                    term::styled_dim("Set login_host in settings to fill in the cluster host.")
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pull_output_preserves_exact_pretty_json_bytes() {
        let output = PullOutput {
            schema_version: 1,
            job_id: "42".to_string(),
            bundles: Vec::new(),
            login_host: None,
            cluster_path: "<login-node>:/cache/payload".to_string(),
            into: "results".to_string(),
            files: 0,
            bytes: 0,
            suggested_command: "rsync fixture".to_string(),
            ssh_multiplex_hint: String::new(),
        };

        let actual = crate::output::to_pretty_json(&output).expect("serialize pull fixture");
        let expected = r#"{
  "schema_version": 1,
  "job_id": "42",
  "bundles": [],
  "cluster_path": "<login-node>:/cache/payload",
  "into": "results",
  "files": 0,
  "bytes": 0,
  "suggested_command": "rsync fixture",
  "ssh_multiplex_hint": ""
}"#;
        assert_eq!(actual, expected);
        assert!(!actual.ends_with('\n'));
    }

    #[test]
    fn pull_rsync_command_carries_controlmaster_opts_and_otp_note_surface() {
        let (cluster_path, command) =
            pull_rsync_command(Some("login01"), Path::new("/scratch/job/42/artifacts"), ".");
        assert_eq!(cluster_path, "login01:/scratch/job/42/artifacts");
        // The exact ControlMaster triplet — a divergent ControlPath spelling here
        // would silently cost a second OTP prompt (the design's flagged risk).
        assert!(command.contains("ControlMaster=auto"));
        assert!(command.contains("ControlPath=~/.ssh/cm-%r@%h:%p"));
        assert!(command.contains("ControlPersist=10m"));
        assert!(command.starts_with("rsync -avz -e 'ssh "));
        assert!(command.ends_with("login01:/scratch/job/42/artifacts/ ./"));
    }

    #[test]
    fn pull_rsync_command_uses_placeholder_without_login_host() {
        let (cluster_path, command) =
            pull_rsync_command(None, Path::new("/scratch/job/42/artifacts"), "out");
        assert_eq!(cluster_path, "<login-node>:/scratch/job/42/artifacts");
        assert!(command.contains("'<login-node>:/scratch/job/42/artifacts/' out/"));
    }

    #[test]
    fn pull_rsync_command_preserves_shell_argument_boundaries() {
        let payload = Path::new("/scratch/study's results/42/artifacts");
        for into in ["/tmp/laptop's results", "-results"] {
            let (cluster_path, command) = pull_rsync_command(Some("user@login01"), payload, into);
            // Define a shell function that captures argv: no rsync or SSH process
            // can run, even if the command's quoting regresses.
            let captured = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(format!("rsync() {{ printf '%s\\000' \"$@\"; }}; {command}"))
                .output()
                .expect("run shell argument capture");
            assert!(
                captured.status.success(),
                "{}",
                String::from_utf8_lossy(&captured.stderr)
            );
            let args = captured.stdout.split(|byte| *byte == 0).collect::<Vec<_>>();
            assert_eq!(args.len(), 7, "one source and one destination: {args:?}");
            assert_eq!(args[0], b"-avz");
            assert_eq!(args[1], b"-e");
            assert_eq!(args[3], b"--");
            assert_eq!(args[4], format!("{cluster_path}/").as_bytes());
            assert_eq!(args[5], format!("{into}/").as_bytes());
        }
    }
}
