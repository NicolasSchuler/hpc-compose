//! `hpc-compose reach` — resolve the SSH port-forward to reach a tracked
//! service from a laptop.
//!
//! Read-only: the compute node comes from tracked status and the port from the
//! service's TCP/HTTP readiness. By default it prints the `ssh -L` command (with
//! connection multiplexing so an OTP login node only prompts once); `--open`
//! runs that forward in the foreground (Ctrl-C to stop) and never daemonizes.

use super::*;

/// Machine-readable output for `reach --format json`.
#[derive(Debug, Serialize)]
struct ReachOutput {
    service: String,
    job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    compute_node: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    login_host: Option<String>,
    local_port: u16,
    remote_port: u16,
    url: String,
    ssh_command: String,
}

pub(crate) fn reach(
    context: ResolvedContext,
    service: String,
    job_id: Option<String>,
    port: Option<u16>,
    open: bool,
    format: Option<OutputFormat>,
) -> Result<()> {
    let json = matches!(
        output::resolve_output_format(format, false),
        OutputFormat::Json
    );
    if open && json {
        bail!("reach --open cannot be combined with --format json (it runs an interactive ssh)");
    }

    let record = resolve_tracked_record(&context, job_id.as_deref())?
        .with_context(|| tracked_job_hint(job_id.as_deref()))?;

    let plan =
        output::load_runtime_plan_with_interpolation_vars_cache_default_and_resource_profiles(
            &record.compose_file,
            &context.interpolation_vars,
            Some(&context.cache_dir.value),
            &context.resource_profiles,
        )?;
    if !plan.ordered_services.iter().any(|s| s.name == service) {
        let available: Vec<&str> = plan
            .ordered_services
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        let mut message = format!(
            "service '{service}' is not defined in {}",
            record.compose_file.display()
        );
        if let Some(suggestion) = crate::suggest::nearest_default(&service, &available) {
            message.push_str(&format!("; did you mean '{suggestion}'?"));
        }
        if !available.is_empty() {
            message.push_str(&format!(" (available: {})", available.join(", ")));
        }
        bail!(message);
    }

    // Remote port + URL: an explicit --port wins; otherwise reuse the
    // readiness-derived endpoint (TCP/HTTP only). Sleep/Log services need --port.
    let endpoint = output::build_submit_endpoints(&plan)
        .into_iter()
        .find(|endpoint| endpoint.service == service);
    let (remote_port, url) = match (port, endpoint) {
        (Some(port), _) => (port, format!("http://127.0.0.1:{port}")),
        (None, Some(endpoint)) => {
            let url = endpoint
                .url
                .clone()
                .unwrap_or_else(|| format!("http://127.0.0.1:{}", endpoint.port));
            (endpoint.port, url)
        }
        (None, None) => bail!(
            "service '{service}' has no TCP or HTTP readiness port to forward; pass --port <PORT>"
        ),
    };
    let local_port = remote_port;

    // Compute node from tracked status; login host from settings or the host.
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };
    let snapshot = build_status_snapshot(
        &record.compose_file,
        Some(&record.job_id),
        &scheduler_options,
    )?;
    let compute_node = snapshot
        .services
        .iter()
        .find(|row| row.service_name == service)
        .and_then(|row| row.nodelist.clone())
        .and_then(|nodes| nodes.split(',').next().map(str::to_string));
    let login_host = context.login_host.clone().or_else(current_hostname);

    if open && login_host.is_none() {
        bail!(
            "reach --open needs a login host; set `login_host` in settings or run from the login node"
        );
    }

    let compute = compute_node.as_deref().unwrap_or("<compute-node>");
    let login = login_host.as_deref().unwrap_or("<login-node>");
    let ssh_command = ssh_forward_command(local_port, remote_port, compute, login);

    if json {
        let out = ReachOutput {
            service,
            job_id: record.job_id.clone(),
            compute_node,
            login_host,
            local_port,
            remote_port,
            url,
            ssh_command,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&out).context("failed to serialize reach output")?
        );
        return Ok(());
    }

    if open {
        return run_reach_forward(local_port, remote_port, compute, login);
    }

    println!("{}", term::styled_section_header("Reach service"));
    println!("  service: {service} (job {})", record.job_id);
    println!("  url:     {url}");
    println!();
    println!("Forward the port from your laptop:");
    println!("  {ssh_command}");
    println!();
    println!("{}", term::styled_dim(OTP_MULTIPLEX_NOTE));
    Ok(())
}

/// Runs the port-forward in the foreground (Ctrl-C to stop). Never daemonized.
fn run_reach_forward(local_port: u16, remote_port: u16, compute: &str, login: &str) -> Result<()> {
    println!(
        "forwarding 127.0.0.1:{local_port} -> {compute}:{remote_port} via {login} (Ctrl-C to stop)"
    );
    let forward = format!("{local_port}:{compute}:{remote_port}");
    let mut args: Vec<&str> = vec!["-N"];
    args.extend(CONTROL_MASTER_SSH_OPTS);
    args.extend(["-L", forward.as_str(), login]);
    let status = std::process::Command::new("ssh")
        .args(&args)
        .status()
        .context("failed to execute 'ssh'")?;
    if !status.success() {
        if let Some(code) = status.code() {
            return Err(crate::exit::ExitCodeError(code).into());
        }
        bail!("ssh exited abnormally");
    }
    Ok(())
}
