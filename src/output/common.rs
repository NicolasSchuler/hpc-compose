//! Output-format defaults shared across command families.

use hpc_compose::cli::{OutputFormat, StatsOutputFormat};

pub(crate) fn resolve_output_format(format: Option<OutputFormat>) -> OutputFormat {
    format.unwrap_or(OutputFormat::Text)
}

pub(crate) fn resolve_stats_output_format(
    format: Option<StatsOutputFormat>,
    json: bool,
) -> StatsOutputFormat {
    if json {
        StatsOutputFormat::Json
    } else {
        format.unwrap_or(StatsOutputFormat::Text)
    }
}
