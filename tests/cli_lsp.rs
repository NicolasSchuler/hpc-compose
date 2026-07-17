use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::support::bin_path;

#[test]
fn lsp_stdio_publishes_diagnostics_for_did_open() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let compose_path = tmp.path().join("compose.yaml");
    let uri = format!("file://{}", compose_path.display());

    let mut child = Command::new(bin_path())
        .arg("lsp")
        .current_dir(tmp.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn hpc-compose lsp");
    let mut stdin = child.stdin.take().expect("lsp stdin");
    let stdout = child.stdout.take().expect("lsp stdout");
    let mut guard = ChildGuard { child };
    let messages = spawn_lsp_reader(stdout);

    write_lsp_message(
        &mut stdin,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "processId": null,
                "rootUri": null,
                "capabilities": {}
            }
        }),
    );
    let initialize = recv_lsp_message_matching(&messages, Duration::from_secs(5), |message| {
        message.get("id").and_then(Value::as_i64) == Some(1)
    });
    assert!(
        initialize.get("result").is_some(),
        "initialize should return server capabilities: {initialize}"
    );
    write_lsp_message(
        &mut stdin,
        json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        }),
    );

    write_lsp_message(
        &mut stdin,
        json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": uri,
                    "languageId": "yaml",
                    "version": 1,
                    "text": "services:\n  app:\n    image: alpine:3.20\n    ports: []\n"
                }
            }
        }),
    );
    let published = recv_lsp_message_matching(&messages, Duration::from_secs(5), |message| {
        message.get("method").and_then(Value::as_str) == Some("textDocument/publishDiagnostics")
    });
    let diagnostics = published["params"]["diagnostics"]
        .as_array()
        .expect("diagnostics array");
    assert_eq!(diagnostics.len(), 1, "published diagnostics: {published}");
    let diagnostic = &diagnostics[0];
    assert_eq!(diagnostic["source"], "hpc-compose");
    assert_eq!(diagnostic["code"], "hpc_compose::spec::unsupported_key");
    assert_eq!(diagnostic["data"]["field"], "services.app.ports");
    assert!(
        diagnostic["data"]["recommendation"]
            .as_str()
            .expect("recommendation")
            .contains("ports are not supported"),
        "diagnostic should carry agent recommendation data: {diagnostic}"
    );

    write_lsp_message(
        &mut stdin,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "shutdown",
            "params": null
        }),
    );
    let shutdown = recv_lsp_message_matching(&messages, Duration::from_secs(5), |message| {
        message.get("id").and_then(Value::as_i64) == Some(2)
    });
    assert!(
        shutdown.get("error").is_none(),
        "shutdown should succeed: {shutdown}"
    );
    write_lsp_message(
        &mut stdin,
        json!({
            "jsonrpc": "2.0",
            "method": "exit",
            "params": null
        }),
    );
    drop(stdin);

    let status = guard.wait(Duration::from_secs(5));
    assert!(status.success(), "lsp exited with {status}");
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn wait(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("poll lsp child") {
                return status;
            }
            if started.elapsed() >= timeout {
                let _ = self.child.kill();
                return self.child.wait().expect("wait killed lsp child");
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn spawn_lsp_reader(stdout: ChildStdout) -> Receiver<Result<Value, String>> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            match read_lsp_message(&mut reader) {
                Ok(message) => {
                    if tx.send(Ok(message)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                    break;
                }
            }
        }
    });
    rx
}

fn write_lsp_message(stdin: &mut ChildStdin, message: Value) {
    let body = message.to_string();
    write!(stdin, "Content-Length: {}\r\n\r\n{}", body.len(), body).expect("write lsp message");
    stdin.flush().expect("flush lsp message");
}

fn recv_lsp_message_matching(
    rx: &Receiver<Result<Value, String>>,
    timeout: Duration,
    mut predicate: impl FnMut(&Value) -> bool,
) -> Value {
    let started = Instant::now();
    loop {
        let remaining = timeout
            .checked_sub(started.elapsed())
            .unwrap_or(Duration::ZERO);
        assert!(
            !remaining.is_zero(),
            "timed out waiting for matching LSP message"
        );
        match rx.recv_timeout(remaining) {
            Ok(Ok(message)) if predicate(&message) => return message,
            Ok(Ok(_)) => continue,
            Ok(Err(error)) => panic!("failed to read LSP message: {error}"),
            Err(error) => panic!("timed out waiting for LSP message: {error}"),
        }
    }
}

fn read_lsp_message(reader: &mut impl BufRead) -> Result<Value, String> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        if bytes == 0 {
            return Err("unexpected EOF while reading LSP headers".to_string());
        }
        let header = line.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        if let Some(value) = header.strip_prefix("Content-Length:") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|error| error.to_string())?,
            );
        }
    }

    let len = content_length.ok_or("missing Content-Length header")?;
    let mut body = vec![0_u8; len];
    reader
        .read_exact(&mut body)
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&body).map_err(|error| error.to_string())
}
