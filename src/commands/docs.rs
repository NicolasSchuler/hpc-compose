use anyhow::Result;
use hpc_compose::cli::OutputFormat;
use hpc_compose::docs_search::{DocsSearchOutput, search_docs};

use crate::output;

pub(crate) fn search(
    query_parts: Vec<String>,
    limit: usize,
    format: Option<OutputFormat>,
) -> Result<()> {
    let query = query_parts.join(" ");
    let report = search_docs(&query, limit);
    match output::resolve_output_format(format) {
        OutputFormat::Json => output::print_pretty_json(&report)?,
        OutputFormat::Text => print_text(&report),
    }
    Ok(())
}

fn print_text(report: &DocsSearchOutput) {
    write_text(&mut std::io::stdout(), report);
}

fn write_text(writer: &mut impl std::io::Write, report: &DocsSearchOutput) {
    output::docs::write_search_results(writer, report).expect("failed printing to stdout");
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed stdout"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn text_stdout_failure_retains_println_panic_semantics() {
        let report = search_docs("cache", 1);
        let panic = catch_unwind(AssertUnwindSafe(|| {
            write_text(&mut FailingWriter, &report);
        }));

        let payload = panic.expect_err("closed stdout must retain the historical panic");
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or_default();
        assert!(message.starts_with("failed printing to stdout: "));
        assert!(message.contains("closed stdout"));
    }
}
