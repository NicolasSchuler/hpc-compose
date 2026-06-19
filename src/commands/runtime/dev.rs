use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DevPathState {
    modified_nanos: Option<u128>,
    len: u64,
    is_dir: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DevWatchTarget {
    pub(crate) root: PathBuf,
    pub(crate) services: BTreeSet<String>,
    pub(crate) snapshot: BTreeMap<PathBuf, DevPathState>,
}

pub(crate) type DevWatchSnapshot = BTreeMap<PathBuf, DevPathState>;

fn normalize_dev_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn absolute_dev_path(cwd: &Path, path: &Path) -> PathBuf {
    normalize_dev_path(if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    })
}

pub(crate) fn canonical_dev_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn mount_host_path(mount: &str) -> Option<PathBuf> {
    let (host, rest) = mount.split_once(':')?;
    if host.is_empty() || rest.is_empty() {
        return None;
    }
    Some(PathBuf::from(host))
}

pub(crate) fn infer_dev_watch_targets(
    plan: &RuntimePlan,
    cwd: &Path,
    explicit_paths: &[PathBuf],
) -> Result<Vec<DevWatchTarget>> {
    let mut roots: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let cache_dir = canonical_dev_path(&plan.cache_dir);
    for service in &plan.ordered_services {
        for mount in &service.volumes {
            let Some(host) = mount_host_path(mount) else {
                continue;
            };
            let host = absolute_dev_path(cwd, &host);
            if !host.is_dir() {
                continue;
            }
            let host = canonical_dev_path(&host);
            if host.starts_with(&cache_dir) {
                continue;
            }
            roots.entry(host).or_default().insert(service.name.clone());
        }
    }
    let all_services = plan
        .ordered_services
        .iter()
        .map(|service| service.name.clone())
        .collect::<BTreeSet<_>>();
    for raw_path in explicit_paths {
        let path = absolute_dev_path(cwd, raw_path);
        if !path.is_dir() {
            bail!(
                "dev --watch-paths must point to an existing directory: {}",
                path.display()
            );
        }
        let path = canonical_dev_path(&path);
        roots.entry(path).or_default().extend(all_services.clone());
    }
    if roots.is_empty() {
        bail!(
            "dev could not infer any watchable source directories from service volumes; add --watch-paths PATH"
        );
    }
    roots
        .into_iter()
        .map(|(root, services)| {
            Ok(DevWatchTarget {
                snapshot: collect_dev_snapshot(&root)?,
                root,
                services,
            })
        })
        .collect()
}

fn path_modified_nanos(path: &Path) -> Option<u128> {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
}

pub(crate) fn collect_dev_snapshot(root: &Path) -> Result<DevWatchSnapshot> {
    let mut snapshot = BTreeMap::new();
    collect_dev_snapshot_inner(root, &mut snapshot)?;
    Ok(snapshot)
}

fn collect_dev_snapshot_inner(root: &Path, snapshot: &mut DevWatchSnapshot) -> Result<()> {
    let metadata = match fs::metadata(root) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(()),
    };
    snapshot.insert(
        root.to_path_buf(),
        DevPathState {
            modified_nanos: path_modified_nanos(root),
            len: metadata.len(),
            is_dir: metadata.is_dir(),
        },
    );
    if !metadata.is_dir() {
        return Ok(());
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if file_type.is_symlink() {
            continue;
        }
        collect_dev_snapshot_inner(&path, snapshot)?;
    }
    Ok(())
}

fn write_dev_restart_request(control_dir: &Path, services: &BTreeSet<String>) -> Result<PathBuf> {
    let request_dir = control_dir.join("restart");
    fs::create_dir_all(&request_dir)
        .with_context(|| format!("failed to create {}", request_dir.display()))?;
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let path = request_dir.join(format!("restart-{}-{millis}.request", std::process::id()));
    let body = services.iter().cloned().collect::<Vec<_>>().join("\n");
    fs::write(&path, format!("{body}\n"))
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

/// Detects changes across all dev watch targets once, returning the services to
/// restart and updating each target's snapshot in place.
pub(crate) fn detect_dev_changes(targets: &mut [DevWatchTarget]) -> BTreeSet<String> {
    let mut affected = BTreeSet::new();
    for target in targets {
        if let Ok(current) = collect_dev_snapshot(&target.root)
            && current != target.snapshot
        {
            affected.extend(target.services.iter().cloned());
            target.snapshot = current;
        }
    }
    affected
}

/// Spawns a background thread that watches the dev source directories and writes
/// restart requests on change, mirroring the text-mode dev loop. It runs until
/// [`DEV_SHUTDOWN_REQUESTED`] is set so the foreground watch UI stays in control.
fn spawn_dev_file_watch(
    mut targets: Vec<DevWatchTarget>,
    control_dir: PathBuf,
    debounce_ms: u64,
) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        while !DEV_SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            let mut affected = detect_dev_changes(&mut targets);
            if !affected.is_empty() {
                thread::sleep(Duration::from_millis(debounce_ms));
                affected.extend(detect_dev_changes(&mut targets));
                let _ = write_dev_restart_request(&control_dir, &affected);
            }
            thread::sleep(Duration::from_millis(250));
        }
    })
}

#[cfg(unix)]
extern "C" fn handle_dev_signal(_: libc::c_int) {
    DEV_SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_dev_signal_handlers() {
    DEV_SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGINT, handle_dev_signal as *const () as usize);
        libc::signal(libc::SIGTERM, handle_dev_signal as *const () as usize);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn dev(
    context: ResolvedContext,
    watch_paths: Vec<PathBuf>,
    debounce_ms: u64,
    keep_running: bool,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
    tui: bool,
) -> Result<()> {
    let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
    let prepared = prepare_local_launch(
        &context,
        script_out,
        flags,
        OutputFormat::Text,
        quiet,
        true,
        |plan| infer_dev_watch_targets(plan, &context.cwd, &watch_paths).map(|_| ()),
    )?;
    let mut targets = infer_dev_watch_targets(&prepared.runtime_plan, &context.cwd, &watch_paths)?;
    let outcome = start_prepared_local_launch(&prepared)?;
    if !quiet {
        print_local_launch_outcome(&prepared, &outcome)?;
        println!("watching source directories:");
        for target in &targets {
            println!(
                "  {} -> {}",
                target.root.display(),
                target
                    .services
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    let control_dir = runtime_job_root_for_record(&outcome.record).join("dev-control");
    install_dev_signal_handlers();
    let scheduler_options = SchedulerOptions {
        squeue_bin: context.binaries.squeue.value.clone(),
        sacct_bin: context.binaries.sacct.value.clone(),
    };

    if tui {
        // Drive file-watch reloads from a background thread while the live watch
        // UI runs in the foreground. The in-job supervisor consumes the restart
        // requests both threads write (auto-reloads here, the `r` key in the UI).
        let prefs = watch_ui::WatchPrefs::resolve(&context.watch);
        let watcher = spawn_dev_file_watch(targets, control_dir, debounce_ms);
        let ui_result = watch_ui::run_watch_ui(
            &outcome.record,
            &scheduler_options,
            None,
            200,
            HoldOnExit::Failure,
            prefs,
        );
        DEV_SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        let _ = watcher.join();
        ui_result?;
        if !keep_running && let Some(pid) = read_local_supervisor_pid(&outcome.record)? {
            kill_pid(pid).with_context(|| {
                format!("failed to stop local dev job {}", outcome.record.job_id)
            })?;
            if !quiet {
                println!("stopped local dev job: {}", outcome.record.job_id);
            }
        }
        return Ok(());
    }

    loop {
        if DEV_SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            if !keep_running && let Some(pid) = read_local_supervisor_pid(&outcome.record)? {
                kill_pid(pid).with_context(|| {
                    format!("failed to stop local dev job {}", outcome.record.job_id)
                })?;
                if !quiet {
                    println!("stopped local dev job: {}", outcome.record.job_id);
                }
            }
            return Ok(());
        }
        let snapshot = build_status_snapshot(
            &outcome.record.compose_file,
            Some(&outcome.record.job_id),
            &scheduler_options,
        )?;
        if snapshot.scheduler.terminal {
            if snapshot.scheduler.failed {
                bail!(
                    "local dev job {} reached terminal state {}",
                    outcome.record.job_id,
                    snapshot.scheduler.state
                );
            }
            if !quiet {
                println!(
                    "local dev job {} completed; leaving dev mode",
                    outcome.record.job_id
                );
            }
            return Ok(());
        }

        let mut affected = BTreeSet::new();
        for target in &mut targets {
            let current = collect_dev_snapshot(&target.root)?;
            if current != target.snapshot {
                affected.extend(target.services.iter().cloned());
                target.snapshot = current;
            }
        }
        if !affected.is_empty() {
            thread::sleep(Duration::from_millis(debounce_ms));
            for target in &mut targets {
                let current = collect_dev_snapshot(&target.root)?;
                if current != target.snapshot {
                    affected.extend(target.services.iter().cloned());
                }
                target.snapshot = current;
            }
            write_dev_restart_request(&control_dir, &affected)?;
            if !quiet {
                println!(
                    "dev reload requested: {}",
                    affected.iter().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn shell_quote_for_tmux_command(value: &Path) -> String {
    let raw = value.to_string_lossy();
    format!("'{}'", raw.replace('\'', "'\\''"))
}

fn ensure_tmux_available(tmux_bin: &str) -> Result<()> {
    let output = Command::new(tmux_bin)
        .arg("-V")
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if !output.status.success() {
        bail!(
            "tmux binary '{}' is not usable: {}",
            tmux_bin,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

fn tmux_session_exists(tmux_bin: &str, session: &str) -> bool {
    Command::new(tmux_bin)
        .args(["has-session", "-t", session])
        .status()
        .is_ok_and(|status| status.success())
}

fn run_tmux(tmux_bin: &str, args: &[&str]) -> Result<()> {
    let output = Command::new(tmux_bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if output.status.success() {
        return Ok(());
    }
    bail!(
        "tmux command failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn run_tmux_capture(tmux_bin: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(tmux_bin)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute tmux binary '{tmux_bin}'"))?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).trim().to_string());
    }
    bail!(
        "tmux command failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

pub(crate) fn tmux_tail_command(path: &Path, lines: usize) -> String {
    format!("tail -n {lines} -F {}", shell_quote_for_tmux_command(path))
}

fn open_tmux_dashboard(
    record: &SubmissionRecord,
    tmux_bin: &str,
    session: Option<String>,
    no_attach: bool,
    lines: usize,
) -> Result<String> {
    if record.backend != SubmissionBackend::Local {
        bail!(
            "tmux only supports tracked local jobs; job {} uses {:?}",
            record.job_id,
            record.backend
        );
    }
    ensure_tmux_available(tmux_bin)?;
    if record.service_logs.is_empty() {
        bail!(
            "tracked job {} does not contain any service logs",
            record.job_id
        );
    }
    let session_name = session.unwrap_or_else(|| format!("hpc-compose-{}", record.job_id));
    if !tmux_session_exists(tmux_bin, &session_name) {
        let mut services = record.service_logs.iter();
        let (first_service, first_log) = services.next().expect("checked non-empty");
        let first_cmd = tmux_tail_command(first_log, lines);
        run_tmux(
            tmux_bin,
            &[
                "new-session",
                "-d",
                "-s",
                &session_name,
                "-n",
                "logs",
                &first_cmd,
            ],
        )?;
        run_tmux(
            tmux_bin,
            &[
                "select-pane",
                "-t",
                &format!("{session_name}:0.0"),
                "-T",
                first_service,
            ],
        )?;
        for (service, log_path) in services {
            let command = tmux_tail_command(log_path, lines);
            let pane_id = run_tmux_capture(
                tmux_bin,
                &[
                    "split-window",
                    "-t",
                    &format!("{session_name}:0"),
                    "-d",
                    "-P",
                    "-F",
                    "#{pane_id}",
                    &command,
                ],
            )?;
            let target = if pane_id.is_empty() {
                format!("{session_name}:0")
            } else {
                pane_id
            };
            run_tmux(tmux_bin, &["select-pane", "-t", &target, "-T", service])?;
        }
        run_tmux(
            tmux_bin,
            &["select-layout", "-t", &format!("{session_name}:0"), "tiled"],
        )?;
    }
    if !no_attach {
        run_tmux(tmux_bin, &["attach-session", "-t", &session_name])?;
    }
    Ok(session_name)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn tmux(
    context: ResolvedContext,
    job_id: Option<String>,
    session: Option<String>,
    tmux_bin: String,
    no_attach: bool,
    lines: usize,
    script_out: Option<PathBuf>,
    flags: PrepareFlags,
    quiet: bool,
) -> Result<()> {
    let record = if let Some(job_id) = job_id {
        resolve_tracked_record(&context, Some(&job_id))?
            .with_context(|| format!("tracked job '{job_id}' was not found"))?
    } else {
        let _up_lock = acquire_up_invocation_lock(&context.compose_file.value)?;
        let prepared = prepare_local_launch(
            &context,
            script_out,
            flags,
            OutputFormat::Text,
            quiet,
            false,
            |_| Ok(()),
        )?;
        let outcome = start_prepared_local_launch(&prepared)?;
        if !quiet {
            print_local_launch_outcome(&prepared, &outcome)?;
        }
        outcome.record
    };
    let session_name = open_tmux_dashboard(&record, &tmux_bin, session, no_attach, lines)?;
    if no_attach && !quiet {
        println!("tmux session: {session_name}");
    }
    Ok(())
}
