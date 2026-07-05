//! Provenance annotation for rendered batch scripts.
//!
//! The renderer emits plain text; this module lets the cleanly-mapped emit
//! sites (SBATCH directives, feature-block sections, readiness gates, and
//! dependency waits) record which spec field produced which script lines, and
//! optionally interleave human-readable provenance comments. Comments always go
//! on their own line ABOVE the emitted content — never trailing on a `#SBATCH`
//! line, whose parser cannot be trusted with trailing comments — and are
//! preview-only: submission paths never enable them.

/// A provenance span mapping a range of rendered script lines back to the
/// compose spec field (or feature block) that produced them.
///
/// Line numbers are 1-based and inclusive. When annotation comments are
/// enabled, the comment line itself is part of the span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenanceSpan {
    /// Spec path that produced the lines, e.g. `x-slurm.mem` or
    /// `services.app.readiness.tcp`.
    pub source: String,
    /// Human-readable section name for feature-block banners, e.g.
    /// `artifact helpers`. `None` for single-field sites.
    pub section: Option<String>,
    /// First line of the span (1-based, inclusive).
    pub start_line: usize,
    /// Last line of the span (1-based, inclusive).
    pub end_line: usize,
}

/// Records [`ProvenanceSpan`]s while a script renders and, when annotation
/// comments are enabled, interleaves `# <- field` / `# --- section ---`
/// comment lines above the emitted content.
///
/// With comments disabled the wrappers are behaviorally inert: they call the
/// emit closure and record spans, leaving the rendered bytes untouched.
#[derive(Debug)]
pub(crate) struct Annotations {
    comments: bool,
    spans: Vec<ProvenanceSpan>,
    scanned_len: usize,
    scanned_lines: usize,
}

impl Annotations {
    /// Creates a recorder; `comments` controls whether provenance comment
    /// lines are interleaved into the rendered output.
    pub(crate) fn new(comments: bool) -> Self {
        Self {
            comments,
            spans: Vec::new(),
            scanned_len: 0,
            scanned_lines: 0,
        }
    }

    /// Consumes the recorder, returning every recorded span in emit order.
    pub(crate) fn into_spans(self) -> Vec<ProvenanceSpan> {
        self.spans
    }

    /// Annotates a single-field emit site: `# <- {source}` above the content.
    pub(crate) fn field(&mut self, out: &mut String, source: &str, emit: impl FnOnce(&mut String)) {
        self.record(out, &[source], None, emit);
    }

    /// Annotates a feature-block emit site with a banner naming the section
    /// and the spec field(s) that enabled it:
    /// `# --- {title} ({sources}) ---`. One span is recorded per source, all
    /// covering the same line range.
    pub(crate) fn section(
        &mut self,
        out: &mut String,
        title: &str,
        sources: &[&str],
        emit: impl FnOnce(&mut String),
    ) {
        self.record(out, sources, Some(title), emit);
    }

    fn record(
        &mut self,
        out: &mut String,
        sources: &[&str],
        section: Option<&str>,
        emit: impl FnOnce(&mut String),
    ) {
        let start_line = self.complete_lines(out) + 1;
        if self.comments {
            match section {
                Some(title) => {
                    out.push_str(&format!("# --- {title} ({}) ---\n", sources.join(", ")));
                }
                None => out.push_str(&format!("# <- {}\n", sources.join(", "))),
            }
        }
        emit(out);
        let end_line = self.complete_lines(out);
        if end_line < start_line {
            // The site emitted nothing; do not record an inverted span.
            return;
        }
        for source in sources {
            self.spans.push(ProvenanceSpan {
                source: (*source).to_string(),
                section: section.map(str::to_string),
                start_line,
                end_line,
            });
        }
    }

    /// Returns the number of complete (newline-terminated) lines in `out`.
    ///
    /// The renderer only ever appends, so the count advances incrementally
    /// over the newly added bytes instead of rescanning the whole script.
    fn complete_lines(&mut self, out: &str) -> usize {
        debug_assert!(
            out.len() >= self.scanned_len,
            "render output must only grow"
        );
        self.scanned_lines += out.as_bytes()[self.scanned_len..]
            .iter()
            .filter(|byte| **byte == b'\n')
            .count();
        self.scanned_len = out.len();
        self.scanned_lines
    }
}
