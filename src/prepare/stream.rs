//! Prepare-specific subprocess output streaming and bounded diagnostics.

use std::collections::VecDeque;
use std::fs;
use std::io::{BufReader, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use super::{PrepareReporter, prepare_verbose_enabled};

/// Maximum stderr retained for a failing prepare subprocess. Output is still
/// drained and forwarded live; only the diagnostic copied into the final error
/// is bounded so a chatty tool cannot grow the CLI's memory without limit.
const PREPARE_STDERR_TAIL_BYTES: usize = 64 * 1024;
/// Raw-byte ceiling for one decoded progress item. Lossy UTF-8 expansion can
/// make the resulting `String` at most three times this size, so the bounded
/// channel has a finite byte ceiling even for newline-free binary output.
const PREPARE_OUTPUT_CHUNK_BYTES: usize = 16 * 1024;
const PREPARE_OUTPUT_QUEUE_LINES: usize = 256;

/// Live-progress context for a single subprocess invocation.
pub(super) struct StreamCtx<'a> {
    pub(super) reporter: &'a dyn PrepareReporter,
    pub(super) service: &'a str,
    pub(super) phase: &'a str,
    /// Artifact whose growing size is polled for best-effort byte progress.
    pub(super) target: Option<&'a Path>,
}

impl<'a> StreamCtx<'a> {
    /// A context that suppresses live progress (for fast/cleanup steps).
    pub(super) fn quiet(reporter: &'a dyn PrepareReporter, service: &'a str) -> Self {
        Self {
            reporter,
            service,
            phase: "",
            target: None,
        }
    }
}

/// True when an enroot/mksquashfs failure looks like a stale-NFS-handle or
/// squashfs-read error on the temporary extraction filesystem — the signature
/// of a shared filesystem that cannot sustain the extract-then-scan workload.
pub(super) fn is_stale_handle_error(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<StreamedCommandFailure>()
            .is_some_and(|failure| failure.signals.is_stale_handle())
    }) {
        return true;
    }
    let text = err.to_string().to_ascii_lowercase();
    text.contains("stale file handle")
        || text.contains("read failed because")
        || (text.contains("squashfs") && text.contains("read failed"))
}

/// True when an import failure looks like the registry rejecting the reference —
/// the image tag does not exist or the pull is unauthorized, rather than a
/// filesystem problem. This is the confusing case where a typo'd or non-existent
/// tag only surfaces deep inside `enroot import`.
pub(super) fn is_missing_image_error(err: &anyhow::Error) -> bool {
    if err.chain().any(|cause| {
        cause
            .downcast_ref::<StreamedCommandFailure>()
            .is_some_and(|failure| failure.signals.is_missing_image())
    }) {
        return true;
    }
    let text = err.to_string().to_ascii_lowercase();
    (text.contains("manifest") && (text.contains("unknown") || text.contains("not found")))
        || text.contains("401 unauthorized")
        || text.contains("access to the resource is denied")
}

/// Drains a byte stream in line-oriented, byte-bounded chunks, decoding lossily
/// so non-UTF-8 output never terminates the reader early. A tool can emit one
/// arbitrarily long line (or no newline at all), so `read_until` is deliberately
/// avoided: both the reader buffer and every item sent through the bounded
/// progress channel have a fixed maximum allocation. Newlines and a preceding
/// `\r` are stripped from ordinary lines.
pub(super) fn for_each_line_lossy<R: Read>(mut reader: R, mut on_line: impl FnMut(String)) {
    const READ_BUFFER_BYTES: usize = 8 * 1024;

    fn utf8_sequence_width(lead: u8) -> usize {
        match lead {
            0xC2..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF4 => 4,
            _ => 0,
        }
    }

    /// Number of trailing bytes that are a potentially valid but incomplete
    /// UTF-8 scalar. Invalid bytes are emitted lossily; only valid prefixes are
    /// carried into the next bounded chunk.
    fn incomplete_utf8_suffix_len(bytes: &[u8]) -> usize {
        let Some(last_index) = bytes.len().checked_sub(1) else {
            return 0;
        };
        if bytes[last_index].is_ascii() {
            return 0;
        }
        let mut lead_index = last_index;
        let mut continuation_count = 0;
        while lead_index > 0
            && bytes[lead_index] & 0b1100_0000 == 0b1000_0000
            && continuation_count < 3
        {
            lead_index -= 1;
            continuation_count += 1;
        }
        let lead = bytes[lead_index];
        if lead & 0b1100_0000 == 0b1000_0000 {
            return 0;
        }
        let width = utf8_sequence_width(lead);
        let available = bytes.len() - lead_index;
        if width > available { available } else { 0 }
    }

    fn emit_prefix(chunk: &mut Vec<u8>, prefix_len: usize, on_line: &mut impl FnMut(String)) {
        on_line(String::from_utf8_lossy(&chunk[..prefix_len]).into_owned());
        let remaining = chunk.len() - prefix_len;
        chunk.copy_within(prefix_len.., 0);
        chunk.truncate(remaining);
    }

    let mut read_buffer = [0_u8; READ_BUFFER_BYTES];
    let mut chunk = Vec::with_capacity(PREPARE_OUTPUT_CHUNK_BYTES);
    let mut emitted_for_line = false;
    let mut unterminated_line = false;
    loop {
        match reader.read(&mut read_buffer) {
            Ok(0) => break,
            Ok(read) => {
                for byte in &read_buffer[..read] {
                    if *byte == b'\n' {
                        if chunk.last() == Some(&b'\r') {
                            chunk.pop();
                        }
                        if !chunk.is_empty() || !emitted_for_line {
                            let len = chunk.len();
                            emit_prefix(&mut chunk, len, &mut on_line);
                        }
                        emitted_for_line = false;
                        unterminated_line = false;
                        continue;
                    }
                    unterminated_line = true;
                    chunk.push(*byte);
                    if chunk.len() >= PREPARE_OUTPUT_CHUNK_BYTES {
                        let carry = if chunk.last() == Some(&b'\r') {
                            1
                        } else {
                            incomplete_utf8_suffix_len(&chunk)
                        };
                        let emit_len = chunk.len() - carry;
                        if emit_len > 0 {
                            emit_prefix(&mut chunk, emit_len, &mut on_line);
                            emitted_for_line = true;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    if chunk.last() == Some(&b'\r') {
        chunk.pop();
    }
    if unterminated_line && (!chunk.is_empty() || !emitted_for_line) {
        let len = chunk.len();
        emit_prefix(&mut chunk, len, &mut on_line);
    }
}

const FAILURE_SIGNAL_OVERLAP_BYTES: usize = 64;

/// Bounded streaming classification state kept separately from the displayed
/// stderr tail. It remembers only booleans and a short overlap window, so an
/// early decisive marker survives tail eviction without retaining full output.
#[derive(Debug, Default)]
pub(super) struct StreamFailureSignals {
    overlap: Vec<u8>,
    saw_stale_file_handle: bool,
    saw_read_failed_because: bool,
    saw_squashfs: bool,
    saw_read_failed: bool,
    saw_manifest: bool,
    saw_unknown: bool,
    saw_not_found: bool,
    saw_unauthorized: bool,
    saw_access_denied: bool,
}

impl StreamFailureSignals {
    pub(super) fn observe(&mut self, text: &str) {
        fn contains(haystack: &[u8], needle: &[u8]) -> bool {
            haystack
                .windows(needle.len())
                .any(|window| window == needle)
        }

        let mut scan = Vec::with_capacity(self.overlap.len().saturating_add(text.len()));
        scan.extend_from_slice(&self.overlap);
        scan.extend(text.bytes().map(|byte| byte.to_ascii_lowercase()));
        self.saw_stale_file_handle |= contains(&scan, b"stale file handle");
        self.saw_read_failed_because |= contains(&scan, b"read failed because");
        self.saw_squashfs |= contains(&scan, b"squashfs");
        self.saw_read_failed |= contains(&scan, b"read failed");
        self.saw_manifest |= contains(&scan, b"manifest");
        self.saw_unknown |= contains(&scan, b"unknown");
        self.saw_not_found |= contains(&scan, b"not found");
        self.saw_unauthorized |= contains(&scan, b"401 unauthorized");
        self.saw_access_denied |= contains(&scan, b"access to the resource is denied");

        let keep_from = scan.len().saturating_sub(FAILURE_SIGNAL_OVERLAP_BYTES);
        self.overlap.clear();
        self.overlap.extend_from_slice(&scan[keep_from..]);
    }

    pub(super) fn is_stale_handle(&self) -> bool {
        self.saw_stale_file_handle
            || self.saw_read_failed_because
            || (self.saw_squashfs && self.saw_read_failed)
    }

    pub(super) fn is_missing_image(&self) -> bool {
        (self.saw_manifest && (self.saw_unknown || self.saw_not_found))
            || self.saw_unauthorized
            || self.saw_access_denied
    }
}

#[derive(Debug, Default)]
struct CapturedStderr {
    tail: BoundedStderrTail,
    signals: StreamFailureSignals,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to {context}: {diagnostic}")]
struct StreamedCommandFailure {
    context: String,
    diagnostic: String,
    signals: StreamFailureSignals,
}

/// Byte-bounded tail of stderr lines. The queue may begin in the middle of a
/// multibyte character after eviction; final lossy decoding keeps the failure
/// path total while preserving the most recent diagnostic bytes.
#[derive(Debug, Default)]
struct BoundedStderrTail {
    bytes: VecDeque<u8>,
}

impl BoundedStderrTail {
    fn push_line(&mut self, line: &str) {
        let line = line.as_bytes();
        let required = line.len().saturating_add(1);
        if required >= PREPARE_STDERR_TAIL_BYTES {
            self.bytes.clear();
            let keep = PREPARE_STDERR_TAIL_BYTES.saturating_sub(1);
            self.bytes
                .extend(line[line.len().saturating_sub(keep)..].iter().copied());
            self.bytes.push_back(b'\n');
            return;
        }
        let overflow = self
            .bytes
            .len()
            .saturating_add(required)
            .saturating_sub(PREPARE_STDERR_TAIL_BYTES);
        if overflow > 0 {
            self.bytes.drain(..overflow);
        }
        self.bytes.extend(line.iter().copied());
        self.bytes.push_back(b'\n');
    }

    fn into_string(self) -> String {
        let bytes = self.bytes.into_iter().collect::<Vec<_>>();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

/// Runs a prepare subprocess, forwarding its live stdout/stderr to the
/// reporter and best-effort byte progress of the target artifact, while
/// preserving the buffered stderr tail for the failure message.
///
/// Both pipes are drained on dedicated threads to avoid pipe-buffer deadlock on
/// chatty tools (enroot import prints a lot); the reporter is only ever called
/// on this (the calling) thread, so it need not be `Send`.
pub(super) fn run_streamed_command(
    mut command: Command,
    bin: &str,
    context: &str,
    stream: &StreamCtx<'_>,
) -> Result<()> {
    if !stream.phase.is_empty() {
        stream.reporter.step_started(stream.service, stream.phase);
    }
    // Verbose mode: hand the tool this process's stdout/stderr so its raw output
    // (including carriage-return progress bars) streams straight through.
    if prepare_verbose_enabled() {
        command.stdin(Stdio::null());
        let status = command
            .status()
            .with_context(|| format!("failed to execute '{bin}' while trying to {context}"))?;
        if !status.success() {
            bail!("failed to {} (see the streamed output above)", context);
        }
        return Ok(());
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to execute '{bin}' while trying to {context}"))?;

    // Bound cross-thread progress delivery as well as the retained stderr tail.
    // A slow renderer may apply backpressure to the child, but cannot make the
    // CLI retain an unbounded number of output lines in memory.
    let (tx, rx) = mpsc::sync_channel::<String>(PREPARE_OUTPUT_QUEUE_LINES);
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let tx_out = tx.clone();
    let stdout_handle = stdout.map(|pipe| {
        thread::spawn(move || {
            for_each_line_lossy(BufReader::new(pipe), |line| {
                let _ = tx_out.send(line);
            });
        })
    });
    let stderr_handle = stderr.map(|pipe| {
        thread::spawn(move || {
            let mut captured = CapturedStderr::default();
            for_each_line_lossy(BufReader::new(pipe), |line| {
                captured.signals.observe(&line);
                captured.tail.push_line(&line);
                let _ = tx.send(line);
            });
            captured
        })
    });
    // If a pipe was unexpectedly absent, make sure the sender side is dropped so
    // the channel can close (the moved `tx`/`tx_out` are otherwise owned by the
    // threads). Both pipes are piped above, so in practice both threads run.
    if stdout_handle.is_none() && stderr_handle.is_none() {
        // Nothing streams; fall through and just wait.
    }

    let forward = |line: &str, stream: &StreamCtx<'_>| {
        let trimmed = line.trim_end();
        if !trimmed.is_empty() && !stream.phase.is_empty() {
            stream.reporter.step_output(stream.service, trimmed);
        }
    };

    let mut last_bytes = 0u64;
    let status = loop {
        while let Ok(line) = rx.try_recv() {
            forward(&line, stream);
        }
        if let Some(target) = stream.target
            && let Ok(meta) = fs::metadata(target)
        {
            let len = meta.len();
            if len != last_bytes {
                last_bytes = len;
                stream.reporter.step_bytes(stream.service, len);
            }
        }
        match child
            .try_wait()
            .context("failed to poll prepare subprocess")?
        {
            Some(status) => break status,
            None => match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(line) => forward(&line, stream),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    thread::sleep(Duration::from_millis(25));
                }
            },
        }
    };

    // Child exited; keep draining the bounded queue until both readers reach
    // EOF. Joining first can deadlock when a reader is blocked sending into a
    // full queue and the caller is no longer receiving.
    while stdout_handle
        .as_ref()
        .is_some_and(|handle| !handle.is_finished())
        || stderr_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(line) => forward(&line, stream),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    while let Ok(line) = rx.try_recv() {
        forward(&line, stream);
    }

    if let Some(handle) = stdout_handle {
        let _ = handle.join();
    }
    let captured_stderr = stderr_handle
        .and_then(|handle| handle.join().ok())
        .unwrap_or_default();
    if !status.success() {
        return Err(StreamedCommandFailure {
            context: context.to_string(),
            diagnostic: captured_stderr.tail.into_string().trim().to_string(),
            signals: captured_stderr.signals,
        }
        .into());
    }
    Ok(())
}
