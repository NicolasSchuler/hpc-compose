use std::env;
use std::path::PathBuf;
use std::process::Command;

use anyhow::Result;
use hpc_compose::cli::OutputFormat;
use hpc_compose::context::ResolvedBinaries;
use hpc_compose::preflight::{Item, Level, Report};

use crate::output;

pub(crate) fn doctor(format: Option<OutputFormat>, binaries: &ResolvedBinaries) -> Result<()> {
    let output_format = output::resolve_output_format(format, false);
    let report = run_doctor(binaries);
    match output_format {
        OutputFormat::Text => print_doctor_report(&report),
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.grouped())
                    .map_err(|e| anyhow::anyhow!("failed to serialize doctor report: {e}"))?
            );
        }
    }
    Ok(())
}

fn run_doctor(binaries: &ResolvedBinaries) -> Report {
    let mut items = Vec::new();

    check_slurm(&mut items, binaries);
    check_enroot(&mut items, binaries.enroot.value.as_str());
    check_pyxis(&mut items, binaries.srun.value.as_str());
    check_gpu(&mut items);
    check_cache_dir(&mut items);
    check_completions(&mut items);

    Report { items }
}

fn check_slurm(items: &mut Vec<Item>, binaries: &ResolvedBinaries) {
    let sbatch_out = run_capture(binaries.sbatch.value.as_str(), &["--version"]);
    match sbatch_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("sbatch: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "sbatch not found".into(),
            remediation: Some("Install Slurm workload manager".into()),
        }),
    }

    let srun_out = run_capture(binaries.srun.value.as_str(), &["--version"]);
    match srun_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("srun: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "srun not found".into(),
            remediation: Some("Install Slurm workload manager".into()),
        }),
    }

    let squeue_out = run_capture(binaries.squeue.value.as_str(), &["--version"]);
    match squeue_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("squeue: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "squeue not found".into(),
            remediation: Some("squeue is needed for live status and watch".into()),
        }),
    }

    let sacct_out = run_capture(binaries.sacct.value.as_str(), &["--version"]);
    match sacct_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("sacct: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "sacct not found".into(),
            remediation: Some("sacct is needed for post-job status and stats".into()),
        }),
    }

    let scancel_out = run_capture(binaries.scancel.value.as_str(), &["--version"]);
    match scancel_out {
        Some(version) => items.push(Item {
            level: Level::Ok,
            message: format!("scancel: {version}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Warn,
            message: "scancel not found".into(),
            remediation: Some("scancel is needed for the cancel/down commands".into()),
        }),
    }
}

fn check_enroot(items: &mut Vec<Item>, enroot_bin: &str) {
    let version = run_capture(enroot_bin, &["version"]);
    match version {
        Some(v) => items.push(Item {
            level: Level::Ok,
            message: format!("enroot: {v}"),
            remediation: None,
        }),
        None => items.push(Item {
            level: Level::Error,
            message: "enroot not found".into(),
            remediation: Some("Install Enroot and ensure 'enroot' is on PATH".into()),
        }),
    }
}

fn check_pyxis(items: &mut Vec<Item>, srun_bin: &str) {
    match Command::new(srun_bin).arg("--help").output() {
        Ok(output) => {
            let text = String::from_utf8_lossy(&output.stdout).to_string()
                + &String::from_utf8_lossy(&output.stderr);
            if text.contains("--container-image") {
                items.push(Item {
                    level: Level::Ok,
                    message: "Pyxis: available".into(),
                    remediation: None,
                });
            } else {
                items.push(Item {
                    level: Level::Error,
                    message:
                        "Pyxis not available (srun --help does not advertise --container-image)"
                            .into(),
                    remediation: Some("Install or enable the Pyxis Slurm plugin".into()),
                });
            }
        }
        Err(_) => items.push(Item {
            level: Level::Error,
            message: "Pyxis not available (failed to run srun --help)".into(),
            remediation: Some("Install or enable the Pyxis Slurm plugin".into()),
        }),
    }
}

fn check_gpu(items: &mut Vec<Item>) {
    let output = Command::new("nvidia-smi").arg("-L").output();
    match output {
        Ok(out) if out.status.success() => {
            let count = String::from_utf8_lossy(&out.stdout)
                .lines()
                .filter(|l| l.contains("GPU"))
                .count();
            items.push(Item {
                level: Level::Ok,
                message: format!("GPU: {count} device(s) detected"),
                remediation: None,
            });
        }
        _ => items.push(Item {
            level: Level::Warn,
            message: "nvidia-smi not available".into(),
            remediation: Some("GPU metrics collection requires nvidia-smi".into()),
        }),
    }
}

fn check_cache_dir(items: &mut Vec<Item>) {
    let cache_dir = env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .ok()
        .or_else(|| {
            env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".cache"))
        })
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("hpc-compose");

    if let Err(e) = std::fs::create_dir_all(&cache_dir) {
        items.push(Item {
            level: Level::Error,
            message: format!("cache dir {}: cannot create ({e})", cache_dir.display()),
            remediation: Some("Ensure the cache directory path is writable".into()),
        });
        return;
    }

    let probe = cache_dir.join(".doctor-probe");
    match std::fs::write(&probe, b"test") {
        Ok(()) => {
            let _ = std::fs::remove_file(&probe);
            items.push(Item {
                level: Level::Ok,
                message: format!("cache dir: {} (writable)", cache_dir.display()),
                remediation: None,
            });
        }
        Err(e) => items.push(Item {
            level: Level::Error,
            message: format!("cache dir {}: not writable ({e})", cache_dir.display()),
            remediation: Some("Ensure the cache directory is writable".into()),
        }),
    }
}

fn check_completions(items: &mut Vec<Item>) {
    let home = env::var("HOME").ok().map(PathBuf::from);
    let Some(home) = home else {
        return;
    };

    let shell_rcs = [(home.join(".bashrc"), "bash"), (home.join(".zshrc"), "zsh")];

    let mut found = false;
    for (rc_path, shell) in &shell_rcs {
        if let Ok(contents) = std::fs::read_to_string(rc_path) {
            if contents.contains("hpc-compose") {
                items.push(Item {
                    level: Level::Ok,
                    message: format!("shell completions: found in {shell} config"),
                    remediation: None,
                });
                found = true;
                break;
            }
        }
    }

    if !found {
        items.push(Item {
            level: Level::Warn,
            message: "shell completions: not found".into(),
            remediation: Some("Run 'hpc-compose completions bash >> ~/.bashrc' or 'hpc-compose completions zsh >> ~/.zshrc'".into()),
        });
    }
}

fn run_capture(bin: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(bin).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        None
    } else {
        Some(stdout)
    }
}

fn print_doctor_report(report: &Report) {
    let grouped = report.grouped();
    for item in &grouped.passed_checks {
        println!("  {} {}", crate::term::symbol_ok(), item.message);
    }
    for item in &grouped.actionable_warnings {
        println!("  {} {}", crate::term::styled_warning("WARN"), item.message);
        if let Some(ref remediation) = item.remediation {
            println!("    remediation: {remediation}");
        }
    }
    for item in &grouped.blockers {
        println!("  {} {}", crate::term::styled_error("FAIL"), item.message);
        if let Some(ref remediation) = item.remediation {
            println!("    remediation: {remediation}");
        }
    }
    println!(
        "\nSummary: {} passed, {} warnings, {} errors",
        grouped.summary.passed_checks,
        grouped.summary.actionable_warnings,
        grouped.summary.blockers
    );
}
