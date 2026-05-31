//! Shared readiness-check utilities used by planner, preflight, render, and
//! host-side doctor probes.

use crate::spec::ReadinessSpec;
use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use std::fs;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

/// Returns `true` when the host string matches a localhost address.
pub fn is_localhost_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Extracts the hostname from an HTTP/HTTPS URL.
///
/// Handles IPv6 bracket notation, userinfo (`user@host`), and port suffixes.
/// Returns `None` for malformed or empty authorities.
pub fn extract_http_host(url: &str) -> Option<&str> {
    let (_, after_scheme) = url.split_once("://")?;
    let authority = after_scheme.split('/').next()?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if authority.is_empty() {
        return None;
    }
    if authority.starts_with('[') {
        let end = authority.find(']')?;
        return Some(&authority[1..end]);
    }
    Some(authority.split(':').next().unwrap_or(authority))
}

/// Returns `true` when the readiness check relies on implicit localhost
/// semantics (TCP with no explicit host, or HTTP with a localhost URL).
pub fn readiness_uses_implicit_localhost(readiness: Option<&ReadinessSpec>) -> bool {
    match readiness {
        None | Some(ReadinessSpec::Sleep { .. } | ReadinessSpec::Log { .. }) => false,
        Some(ReadinessSpec::Tcp { host, .. }) => host.as_deref().is_none_or(is_localhost_host),
        Some(ReadinessSpec::Http { url, .. }) => {
            extract_http_host(url).is_none_or(is_localhost_host)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ReadinessProbeTarget {
    Sleep {
        seconds: u64,
    },
    Tcp {
        host: String,
        port: u16,
    },
    Log {
        pattern: String,
        log_file: Option<PathBuf>,
    },
    Http {
        url: String,
        expected_status: u16,
    },
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReadinessProbeDescription {
    pub(crate) probe_type: &'static str,
    pub(crate) target: ReadinessProbeTarget,
    pub(crate) timeout_seconds: u64,
    pub(crate) required_tool: Option<&'static str>,
    pub(crate) generated_behavior: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReadinessProbeResult {
    pub(crate) passed: bool,
    pub(crate) elapsed_seconds: f64,
    pub(crate) diagnostics: Vec<String>,
}

pub(crate) fn describe_readiness_probe(
    readiness: &ReadinessSpec,
    timeout_override: Option<u64>,
    log_file: Option<PathBuf>,
) -> ReadinessProbeDescription {
    match readiness {
        ReadinessSpec::Sleep { seconds } => {
            let timeout_seconds = timeout_override.unwrap_or(*seconds);
            ReadinessProbeDescription {
                probe_type: "sleep",
                target: ReadinessProbeTarget::Sleep {
                    seconds: timeout_seconds,
                },
                timeout_seconds,
                required_tool: None,
                generated_behavior: format!("wait_for_sleep \"$pid\" \"$name\" {seconds}"),
            }
        }
        ReadinessSpec::Tcp {
            host,
            port,
            timeout_seconds,
        } => {
            let host = host.as_deref().unwrap_or("127.0.0.1").to_string();
            let timeout_seconds = timeout_override.or(*timeout_seconds).unwrap_or(60);
            ReadinessProbeDescription {
                probe_type: "tcp",
                target: ReadinessProbeTarget::Tcp { host, port: *port },
                timeout_seconds,
                required_tool: Some("bash /dev/tcp in rendered jobs"),
                generated_behavior: format!(
                    "wait_for_tcp \"$pid\" \"$name\" {} {} {}",
                    host_for_display(readiness),
                    port,
                    timeout_seconds
                ),
            }
        }
        ReadinessSpec::Log {
            pattern,
            timeout_seconds,
        } => {
            let timeout_seconds = timeout_override.or(*timeout_seconds).unwrap_or(60);
            ReadinessProbeDescription {
                probe_type: "log",
                target: ReadinessProbeTarget::Log {
                    pattern: pattern.clone(),
                    log_file,
                },
                timeout_seconds,
                required_tool: Some("grep in rendered jobs"),
                generated_behavior: format!(
                    "wait_for_log \"$pid\" \"$name\" \"$LOG_DIR/<service>.log\" <pattern> {timeout_seconds}"
                ),
            }
        }
        ReadinessSpec::Http {
            url,
            status_code,
            timeout_seconds,
        } => {
            let timeout_seconds = timeout_override.or(*timeout_seconds).unwrap_or(60);
            ReadinessProbeDescription {
                probe_type: "http",
                target: ReadinessProbeTarget::Http {
                    url: url.clone(),
                    expected_status: *status_code,
                },
                timeout_seconds,
                required_tool: Some("curl"),
                generated_behavior: format!(
                    "wait_for_http \"$pid\" \"$name\" {url} {status_code} {timeout_seconds}"
                ),
            }
        }
    }
}

fn host_for_display(readiness: &ReadinessSpec) -> &str {
    match readiness {
        ReadinessSpec::Tcp { host, .. } => host.as_deref().unwrap_or("127.0.0.1"),
        _ => "",
    }
}

pub(crate) fn run_readiness_probe(
    description: &ReadinessProbeDescription,
) -> Result<ReadinessProbeResult> {
    let started = Instant::now();
    let mut diagnostics = Vec::new();
    let timeout = Duration::from_secs(description.timeout_seconds);
    let passed = match &description.target {
        ReadinessProbeTarget::Sleep { seconds } => {
            thread::sleep(Duration::from_secs(*seconds));
            diagnostics.push(format!("slept for {seconds}s"));
            true
        }
        ReadinessProbeTarget::Tcp { host, port } => {
            wait_for_tcp(host, *port, timeout, &mut diagnostics)
        }
        ReadinessProbeTarget::Http {
            url,
            expected_status,
        } => wait_for_http(url, *expected_status, timeout, &mut diagnostics),
        ReadinessProbeTarget::Log { pattern, log_file } => {
            let path = log_file
                .as_ref()
                .context("log readiness --run requires --log-file")?;
            wait_for_log(pattern, path, timeout, &mut diagnostics)?
        }
    };
    Ok(ReadinessProbeResult {
        passed,
        elapsed_seconds: started.elapsed().as_secs_f64(),
        diagnostics,
    })
}

fn wait_for_tcp(host: &str, port: u16, timeout: Duration, diagnostics: &mut Vec<String>) -> bool {
    let started = Instant::now();
    let address = (host, port);
    loop {
        match address.to_socket_addrs() {
            Ok(addrs) => {
                for addr in addrs {
                    if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
                        diagnostics.push(format!("connected to {host}:{port}"));
                        return true;
                    }
                }
            }
            Err(err) => diagnostics.push(format!("failed to resolve {host}:{port}: {err}")),
        }
        if started.elapsed() >= timeout {
            diagnostics.push(format!("timed out waiting for {host}:{port}"));
            return false;
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn wait_for_http(
    url: &str,
    expected_status: u16,
    timeout: Duration,
    diagnostics: &mut Vec<String>,
) -> bool {
    let started = Instant::now();
    loop {
        match Command::new("curl")
            .arg("--silent")
            .arg("--output")
            .arg("/dev/null")
            .arg("--write-out")
            .arg("%{http_code}")
            .arg("--max-time")
            .arg("2")
            .arg(url)
            .output()
        {
            Ok(output) => {
                let code = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if code == expected_status.to_string() {
                    diagnostics.push(format!("received HTTP {code} from {url}"));
                    return true;
                }
                if !code.is_empty() && code != "000" {
                    diagnostics.push(format!("received HTTP {code}; expected {expected_status}"));
                }
            }
            Err(err) => {
                diagnostics.push(format!("failed to execute curl: {err}"));
                return false;
            }
        }
        if started.elapsed() >= timeout {
            diagnostics.push(format!(
                "timed out waiting for HTTP {expected_status} from {url}"
            ));
            return false;
        }
        thread::sleep(Duration::from_millis(500));
    }
}

fn wait_for_log(
    pattern: &str,
    path: &PathBuf,
    timeout: Duration,
    diagnostics: &mut Vec<String>,
) -> Result<bool> {
    let regex = Regex::new(pattern)
        .with_context(|| format!("log readiness pattern '{pattern}' is not a valid regex"))?;
    let started = Instant::now();
    loop {
        match fs::read_to_string(path) {
            Ok(content) => {
                if regex.is_match(&content) {
                    diagnostics.push(format!("matched pattern in {}", path.display()));
                    return Ok(true);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => bail!(
                "failed to read readiness log file {}: {err}",
                path.display()
            ),
        }
        if started.elapsed() >= timeout {
            diagnostics.push(format!(
                "timed out waiting for pattern in {}",
                path.display()
            ));
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(250));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ReadinessSpec;
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn is_localhost_host_matches_known_addresses() {
        assert!(is_localhost_host("localhost"));
        assert!(is_localhost_host("127.0.0.1"));
        assert!(is_localhost_host("::1"));
        assert!(!is_localhost_host("192.168.1.1"));
        assert!(!is_localhost_host("example.com"));
        assert!(!is_localhost_host("0.0.0.0"));
    }

    #[test]
    fn extract_http_host_handles_standard_urls() {
        assert_eq!(
            extract_http_host("http://example.com/path"),
            Some("example.com")
        );
        assert_eq!(
            extract_http_host("https://example.com:8080/health"),
            Some("example.com")
        );
        assert_eq!(
            extract_http_host("http://127.0.0.1:3000/api"),
            Some("127.0.0.1")
        );
    }

    #[test]
    fn extract_http_host_handles_ipv6_brackets() {
        assert_eq!(extract_http_host("http://[::1]:8080/health"), Some("::1"));
        assert_eq!(
            extract_http_host("https://[2001:db8::1]/path"),
            Some("2001:db8::1")
        );
    }

    #[test]
    fn extract_http_host_handles_userinfo() {
        assert_eq!(
            extract_http_host("https://user@host.example.com/path"),
            Some("host.example.com")
        );
        assert_eq!(
            extract_http_host("https://user:pass@host.example.com/path"),
            Some("host.example.com")
        );
    }

    #[test]
    fn extract_http_host_returns_none_for_invalid() {
        assert_eq!(extract_http_host("not-a-url"), None);
        assert_eq!(extract_http_host(""), None);
    }

    #[test]
    fn readiness_uses_implicit_localhost_covers_all_variants() {
        assert!(!readiness_uses_implicit_localhost(None));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Sleep { seconds: 5 }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Log {
                pattern: "ready".into(),
                timeout_seconds: None,
            }
        )));

        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: None,
                port: 8080,
                timeout_seconds: None,
            }
        )));
        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: Some("localhost".into()),
                port: 8080,
                timeout_seconds: None,
            }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Tcp {
                host: Some("10.0.0.1".into()),
                port: 8080,
                timeout_seconds: None,
            }
        )));

        assert!(readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Http {
                url: "http://127.0.0.1:8080/health".into(),
                status_code: 200,
                timeout_seconds: None,
            }
        )));
        assert!(!readiness_uses_implicit_localhost(Some(
            &ReadinessSpec::Http {
                url: "http://10.0.0.1:8080/health".into(),
                status_code: 200,
                timeout_seconds: None,
            }
        )));
    }

    #[test]
    fn host_probe_runs_tcp_and_log_checks() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let port = listener.local_addr().expect("addr").port();
        let handle = std::thread::spawn(move || {
            let _ = listener.accept();
        });
        let description = describe_readiness_probe(
            &ReadinessSpec::Tcp {
                host: None,
                port,
                timeout_seconds: Some(1),
            },
            None,
            None,
        );
        assert!(run_readiness_probe(&description).expect("probe").passed);
        handle.join().expect("join");

        let mut file = tempfile::NamedTempFile::new().expect("temp log");
        writeln!(file, "server ready").expect("write");
        let description = describe_readiness_probe(
            &ReadinessSpec::Log {
                pattern: "server ready".into(),
                timeout_seconds: Some(1),
            },
            None,
            Some(file.path().to_path_buf()),
        );
        assert!(run_readiness_probe(&description).expect("probe").passed);
    }
}
