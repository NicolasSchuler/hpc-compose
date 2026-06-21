//! Presets and helpers for the `hpc-compose notebook` command.
//!
//! `notebook` launches a tracked interactive server (JupyterLab or VS Code
//! `code tunnel`) as a one-service compose job. This module holds the
//! preset table and the pure helpers (command construction, URL extraction,
//! tunnel-hint rendering); the submit/readiness/URL-print flow lives in
//! [`crate::commands::runtime`].

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use hpc_compose::spec::{
    CommandSpec, DependsOnSpec, EnvironmentSpec, ReadinessSpec, ServiceEnrootConfig,
    ServiceRuntimeConfig, ServiceSlurmConfig, ServiceSpec, SoftwareEnvConfig,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Which interactive server preset to launch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NotebookKind {
    /// JupyterLab notebook server.
    #[default]
    Jupyter,
    /// VS Code remote tunnel (`code tunnel`).
    VsCode,
}

impl NotebookKind {
    /// Returns the stable preset label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jupyter => "jupyter",
            Self::VsCode => "vscode",
        }
    }
}

/// Resolved notebook options produced from CLI flags.
#[derive(Debug, Clone)]
pub struct NotebookArgs {
    pub kind: NotebookKind,
    /// Override image; when `None`, the preset default is used (vscode has no
    /// default and requires an explicit image).
    pub image: Option<String>,
    pub port: u16,
    /// Jupyter auth token; when `None` a random token is generated.
    pub token: Option<String>,
    pub working_dir: Option<String>,
    /// `host:container` mounts in addition to the preset's workspace.
    pub volumes: Vec<String>,
    /// VS Code tunnel name.
    pub tunnel_name: String,
    /// Extra argv appended to the server command (trailing args after `--`).
    pub extra_args: Vec<String>,
}

/// One notebook preset.
#[derive(Debug, Clone)]
pub struct NotebookPreset {
    pub kind: NotebookKind,
    pub default_image: Option<&'static str>,
    /// Regex matched against the service log to confirm the server is up.
    pub readiness_log_pattern: &'static str,
    /// Regex used to scrape the connection URL from the service log. `None`
    /// means the URL is constructed from known pieces (Jupyter: port+token).
    pub scrape_url_regex: Option<&'static str>,
    /// Whether the user needs an SSH tunnel to reach the server.
    pub needs_tunnel: bool,
}

/// Returns the preset for a given kind.
#[must_use]
pub fn preset_for(kind: NotebookKind) -> NotebookPreset {
    match kind {
        NotebookKind::Jupyter => NotebookPreset {
            kind,
            default_image: Some("jupyter/scipy-notebook:latest"),
            // Jupyter prints `... is running at:` then a URL ending in
            // `/lab?token=`. Matching the tokenized URL is robust across
            // Jupyter Server versions.
            readiness_log_pattern: r"/lab\?token=",
            // URL is constructed from the known port + token, not scraped,
            // so the printed link always points at localhost (what the user
            // reaches after tunneling).
            scrape_url_regex: None,
            needs_tunnel: true,
        },
        NotebookKind::VsCode => NotebookPreset {
            kind,
            default_image: None,
            readiness_log_pattern: r"vscode\.dev/tunnel/",
            scrape_url_regex: Some(r"https://vscode\.dev/tunnel/\S+"),
            needs_tunnel: false,
        },
    }
}

/// Resolves the effective image, bailing when a required one is missing.
pub fn resolve_image(args: &NotebookArgs, preset: &NotebookPreset) -> Result<String> {
    if let Some(image) = args.image.clone() {
        if image.trim().is_empty() {
            bail!("--image requires a non-empty image");
        }
        return Ok(image);
    }
    match preset.default_image {
        Some(image) => Ok(image.to_string()),
        None => bail!(
            "notebook --kind {} requires --image (no default image is shipped); supply an image containing the `code` CLI",
            preset.kind.as_str()
        ),
    }
}

/// Builds the server command for the preset.
#[must_use]
pub fn build_server_command(args: &NotebookArgs, token: &str) -> Vec<String> {
    let mut command = match args.kind {
        NotebookKind::Jupyter => vec![
            "jupyter".to_string(),
            "lab".to_string(),
            "--no-browser".to_string(),
            "--ip=0.0.0.0".to_string(),
            "--port".to_string(),
            args.port.to_string(),
            "--ServerApp.token".to_string(),
            token.to_string(),
            "--ServerApp.allow_remote_access".to_string(),
            "True".to_string(),
            "--ServerApp.allow_origin".to_string(),
            "'*'".to_string(),
        ],
        NotebookKind::VsCode => vec![
            "code".to_string(),
            "tunnel".to_string(),
            "--accept-server-license-terms".to_string(),
            "--name".to_string(),
            args.tunnel_name.clone(),
        ],
    };
    command.extend(args.extra_args.iter().cloned());
    command
}

/// Returns the readiness probe to attach to the synthesized service.
#[must_use]
pub fn readiness_spec(preset: &NotebookPreset) -> ReadinessSpec {
    ReadinessSpec::Log {
        pattern: preset.readiness_log_pattern.to_string(),
        timeout_seconds: None,
    }
}

/// Builds the synthesized notebook service spec.
#[must_use]
pub fn build_notebook_service_spec(
    args: &NotebookArgs,
    image: &str,
    command: Vec<String>,
    readiness: ReadinessSpec,
) -> ServiceSpec {
    let mut env: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    if args.kind == NotebookKind::Jupyter {
        env.insert("JUPYTER_PORT".to_string(), args.port.to_string());
    }
    ServiceSpec {
        image: Some(image.to_string()),
        command: Some(CommandSpec::Vec(command)),
        entrypoint: None,
        script: None,
        environment: EnvironmentSpec::Map(env),
        volumes: args.volumes.clone(),
        working_dir: args.working_dir.clone(),
        depends_on: DependsOnSpec::None,
        readiness: Some(readiness),
        healthcheck: None,
        assertions: None,
        software_env: SoftwareEnvConfig::default(),
        slurm: ServiceSlurmConfig::default(),
        runtime: ServiceRuntimeConfig::default(),
        enroot: ServiceEnrootConfig::default(),
    }
}

/// The connection information printed to the user once the server is ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotebookConnection {
    /// The URL the user should open (already rewritten to localhost for
    /// Jupyter, or the scraped vscode.dev link).
    pub url: String,
    /// Optional SSH tunnel hint for Jupyter-on-Slurm.
    pub tunnel_hint: Option<String>,
}

/// Computes the connection to print.
///
/// Jupyter: the URL is constructed from the known port + token so it always
/// points at `127.0.0.1` (what the user reaches after tunneling). On Slurm a
/// tunnel hint is added. VS Code: the URL is scraped from the log and no
/// tunnel is needed.
pub fn build_connection(
    args: &NotebookArgs,
    preset: &NotebookPreset,
    token: &str,
    log_text: &str,
    compute_node: Option<&str>,
    login_node: Option<&str>,
    local: bool,
) -> Result<NotebookConnection> {
    match args.kind {
        NotebookKind::Jupyter => {
            let url = format!(
                "http://127.0.0.1:{port}/lab?token={token}",
                port = args.port
            );
            let tunnel_hint = if local || !preset.needs_tunnel {
                None
            } else {
                Some(jupyter_tunnel_hint(args.port, compute_node, login_node))
            };
            Ok(NotebookConnection { url, tunnel_hint })
        }
        NotebookKind::VsCode => {
            let regex = preset
                .scrape_url_regex
                .context("vscode preset must define a scrape regex")?;
            let re = regex::Regex::new(regex)
                .with_context(|| format!("invalid notebook url regex `{regex}`"))?;
            let url = re
                .find(log_text)
                .with_context(|| {
                    "notebook became ready but the connection URL was not found in the service log"
                })?
                .as_str()
                .trim_end_matches(')')
                .to_string();
            Ok(NotebookConnection {
                url,
                tunnel_hint: None,
            })
        }
    }
}

/// Renders the SSH tunnel hint for a Jupyter server on a remote compute node.
#[must_use]
pub fn jupyter_tunnel_hint(
    port: u16,
    compute_node: Option<&str>,
    login_node: Option<&str>,
) -> String {
    let compute = compute_node.unwrap_or("<compute-node>");
    let login = login_node.unwrap_or("<login-node>");
    format!(
        "On your laptop, forward the port:\n  \
         ssh -L {port}:{compute}:{port} {login}\n\
         then open the URL above in your browser."
    )
}

/// Machine-readable form of [`NotebookConnection`] for `--format json`.
///
/// Mirrors the human-readable output. `compute_node` and `login_host` are the
/// resolved hosts used to render the tunnel hint; they are descriptive only —
/// nothing here opens a connection.
#[derive(Debug, Clone, Serialize)]
pub struct NotebookConnectionOutput {
    /// The URL to open (localhost for Jupyter, scraped link for VS Code).
    pub url: String,
    /// SSH tunnel hint, when one is needed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tunnel_hint: Option<String>,
    /// Resolved compute node the server runs on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_node: Option<String>,
    /// Resolved SSH login/jump host.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login_host: Option<String>,
    /// Tracked Slurm (or local) job id.
    pub job_id: String,
    /// Suggested follow-up commands an agent can run next.
    pub next_commands: Vec<String>,
}

/// Builds the machine-readable connection output. Pure; opens no connection.
#[must_use]
pub fn build_connection_output(
    connection: &NotebookConnection,
    compute_node: Option<&str>,
    login_host: Option<&str>,
    job_id: &str,
    file: &std::path::Path,
) -> NotebookConnectionOutput {
    let file = file.display();
    NotebookConnectionOutput {
        url: connection.url.clone(),
        tunnel_hint: connection.tunnel_hint.clone(),
        compute_node: compute_node.map(str::to_string),
        login_host: login_host.map(str::to_string),
        job_id: job_id.to_string(),
        next_commands: vec![
            format!("hpc-compose status -f {file}"),
            format!("hpc-compose cancel -f {file}"),
        ],
    }
}

/// Generates an opaque random-looking hex token. Not cryptographically strong;
/// its job is only to keep an interactive notebook URL unguessable.
#[must_use]
pub fn generate_token() -> String {
    let mut digest = Sha256::new();
    digest.update(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    digest.update(std::process::id().to_le_bytes());
    digest.update(b"hpc-compose-notebook");
    let bytes = digest.finalize();
    // 48 hex chars is plenty for an unguessable interactive token.
    hex::encode(&bytes[..24])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jupyter_command_includes_port_and_token() {
        let args = NotebookArgs {
            kind: NotebookKind::Jupyter,
            image: None,
            port: 8888,
            token: None,
            working_dir: None,
            volumes: vec![],
            tunnel_name: "demo".into(),
            extra_args: vec!["--NotebookApp.password='".to_string()],
        };
        let command = build_server_command(&args, "tok123");
        assert_eq!(command[..2], ["jupyter", "lab"][..]);
        let port_idx = command.iter().position(|c| c == "8888").expect("port");
        assert_eq!(command[port_idx - 1], "--port");
        let token_idx = command.iter().position(|c| c == "tok123").expect("token");
        assert_eq!(command[token_idx - 1], "--ServerApp.token");
        // Extra trailing args are appended verbatim.
        assert!(command.iter().any(|c| c.starts_with("--NotebookApp")));
    }

    #[test]
    fn vscode_command_uses_tunnel_subcommand() {
        let args = NotebookArgs {
            kind: NotebookKind::VsCode,
            image: Some("ghcr.io/example/code:1".into()),
            port: 0,
            token: None,
            working_dir: None,
            volumes: vec![],
            tunnel_name: "my-tunnel".into(),
            extra_args: vec![],
        };
        let command = build_server_command(&args, "");
        assert_eq!(
            command[..3],
            ["code", "tunnel", "--accept-server-license-terms"][..]
        );
        let name_idx = command.iter().position(|c| c == "my-tunnel").expect("name");
        assert_eq!(command[name_idx - 1], "--name");
    }

    #[test]
    fn resolve_image_requires_explicit_image_for_vscode() {
        let args = NotebookArgs {
            kind: NotebookKind::VsCode,
            image: None,
            port: 0,
            token: None,
            working_dir: None,
            volumes: vec![],
            tunnel_name: "t".into(),
            extra_args: vec![],
        };
        let preset = preset_for(NotebookKind::VsCode);
        let err = resolve_image(&args, &preset).expect_err("should require image");
        assert!(err.to_string().contains("requires --image"));
    }

    #[test]
    fn resolve_image_uses_preset_default_for_jupyter() {
        let args = NotebookArgs {
            kind: NotebookKind::Jupyter,
            image: None,
            port: 8888,
            token: None,
            working_dir: None,
            volumes: vec![],
            tunnel_name: "t".into(),
            extra_args: vec![],
        };
        let preset = preset_for(NotebookKind::Jupyter);
        let image = resolve_image(&args, &preset).expect("default image");
        assert!(image.starts_with("jupyter/scipy-notebook"));
    }

    #[test]
    fn jupyter_connection_constructs_localhost_url_with_tunnel_on_slurm() {
        let args = NotebookArgs {
            kind: NotebookKind::Jupyter,
            image: None,
            port: 8888,
            token: Some("abc".into()),
            working_dir: None,
            volumes: vec![],
            tunnel_name: "t".into(),
            extra_args: vec![],
        };
        let preset = preset_for(NotebookKind::Jupyter);
        let conn = build_connection(
            &args,
            &preset,
            "abc",
            "ignored",
            Some("gpu-node-07"),
            Some("login.hpc.example"),
            false,
        )
        .expect("conn");
        assert_eq!(conn.url, "http://127.0.0.1:8888/lab?token=abc");
        let hint = conn.tunnel_hint.expect("tunnel hint on slurm");
        assert!(hint.contains("ssh -L 8888:gpu-node-07:8888 login.hpc.example"));
    }

    #[test]
    fn jupyter_connection_local_has_no_tunnel_hint() {
        let args = NotebookArgs {
            kind: NotebookKind::Jupyter,
            image: None,
            port: 8888,
            token: Some("t".into()),
            working_dir: None,
            volumes: vec![],
            tunnel_name: "t".into(),
            extra_args: vec![],
        };
        let preset = preset_for(NotebookKind::Jupyter);
        let conn = build_connection(&args, &preset, "t", "", None, None, true).expect("conn");
        assert!(conn.tunnel_hint.is_none());
    }

    #[test]
    fn vscode_connection_scrapes_url_and_needs_no_tunnel() {
        let args = NotebookKind::build_vscode_args();
        let preset = preset_for(NotebookKind::VsCode);
        let log = "info: To access this tunnel, open this link in your browser: https://vscode.dev/tunnel/hpc-node/my-tunnel\n";
        let conn = build_connection(&args, &preset, "", log, None, None, false).expect("conn");
        assert_eq!(conn.url, "https://vscode.dev/tunnel/hpc-node/my-tunnel");
        assert!(conn.tunnel_hint.is_none());
    }

    #[test]
    fn vscode_connection_errors_when_url_missing() {
        let args = NotebookKind::build_vscode_args();
        let preset = preset_for(NotebookKind::VsCode);
        let err = build_connection(&args, &preset, "", "no url here", None, None, false)
            .expect_err("missing url");
        assert!(err.to_string().contains("connection URL was not found"));
    }

    impl NotebookKind {
        fn build_vscode_args() -> NotebookArgs {
            NotebookArgs {
                kind: NotebookKind::VsCode,
                image: Some("img".into()),
                port: 0,
                token: None,
                working_dir: None,
                volumes: vec![],
                tunnel_name: "my-tunnel".into(),
                extra_args: vec![],
            }
        }
    }

    #[test]
    fn build_connection_output_carries_fields_and_next_commands() {
        let conn = NotebookConnection {
            url: "http://127.0.0.1:8888/lab?token=abc".to_string(),
            tunnel_hint: Some("ssh -L 8888:gpu07:8888 login01".to_string()),
        };
        let out = build_connection_output(
            &conn,
            Some("gpu07"),
            Some("login01"),
            "4815162",
            std::path::Path::new("nb.yaml"),
        );
        assert_eq!(out.url, conn.url);
        assert_eq!(out.compute_node.as_deref(), Some("gpu07"));
        assert_eq!(out.login_host.as_deref(), Some("login01"));
        assert_eq!(out.job_id, "4815162");
        assert!(
            out.next_commands
                .iter()
                .any(|c| c.contains("status -f nb.yaml"))
        );
        assert!(
            out.next_commands
                .iter()
                .any(|c| c.contains("cancel -f nb.yaml"))
        );
        // Round-trips through serde and includes the resolved login host.
        let json = serde_json::to_string(&out).expect("json");
        assert!(json.contains("\"login_host\":\"login01\""));
    }

    #[test]
    fn generate_token_is_hex_and_changes() {
        let a = generate_token();
        let b = generate_token();
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(a.len(), 48);
        // Practically always different; guard against a constant fallback.
        assert_ne!(a, b);
    }

    #[test]
    fn readiness_spec_carries_preset_pattern() {
        let preset = preset_for(NotebookKind::Jupyter);
        let readiness = readiness_spec(&preset);
        match readiness {
            ReadinessSpec::Log { pattern, .. } => assert_eq!(pattern, preset.readiness_log_pattern),
            _ => panic!("expected log readiness"),
        }
    }

    #[test]
    fn build_service_spec_wires_image_command_and_readiness() {
        let args = NotebookArgs {
            kind: NotebookKind::Jupyter,
            image: None,
            port: 8888,
            token: Some("t".into()),
            working_dir: Some("/workspace".into()),
            volumes: vec!["./project:/workspace".into()],
            tunnel_name: "t".into(),
            extra_args: vec![],
        };
        let preset = preset_for(NotebookKind::Jupyter);
        let image = resolve_image(&args, &preset).unwrap();
        let command = build_server_command(&args, "t");
        let readiness = readiness_spec(&preset);
        let spec = build_notebook_service_spec(&args, &image, command.clone(), readiness);
        assert_eq!(spec.image.as_deref(), Some("jupyter/scipy-notebook:latest"));
        assert_eq!(spec.command, Some(CommandSpec::Vec(command)));
        assert_eq!(spec.working_dir.as_deref(), Some("/workspace"));
        assert_eq!(spec.volumes, vec!["./project:/workspace".to_string()]);
        assert!(spec.readiness.is_some());
        assert_eq!(
            spec.environment,
            EnvironmentSpec::Map(
                [("JUPYTER_PORT".to_string(), "8888".to_string())]
                    .into_iter()
                    .collect()
            )
        );
    }
}
