use serde::Deserialize;

use super::stats::CollectorStatus;

#[derive(Debug, Deserialize)]
pub(super) struct SamplerMetaFile {
    pub(super) interval_seconds: u64,
    pub(super) collectors: Vec<CollectorStatus>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GpuDeviceSampleRow {
    pub(super) sampled_at: String,
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) rank: Option<String>,
    #[serde(default)]
    pub(super) local_rank: Option<String>,
    #[serde(default)]
    pub(super) service: Option<String>,
    #[serde(default)]
    pub(super) collector: Option<String>,
    pub(super) index: Option<String>,
    pub(super) uuid: Option<String>,
    pub(super) name: Option<String>,
    pub(super) utilization_gpu: Option<String>,
    pub(super) utilization_memory: Option<String>,
    pub(super) memory_used_mib: Option<String>,
    pub(super) memory_total_mib: Option<String>,
    pub(super) temperature_c: Option<String>,
    pub(super) power_draw_w: Option<String>,
    pub(super) power_limit_w: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct GpuProcessSampleRow {
    pub(super) sampled_at: String,
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) rank: Option<String>,
    #[serde(default)]
    pub(super) local_rank: Option<String>,
    #[serde(default)]
    pub(super) service: Option<String>,
    #[serde(default)]
    pub(super) collector: Option<String>,
    pub(super) gpu_uuid: Option<String>,
    pub(super) pid: Option<String>,
    pub(super) process_name: Option<String>,
    pub(super) used_memory_mib: Option<String>,
    /// Raw `/proc/<pid>/cgroup` content captured live by the sampler with
    /// newlines condensed to `;`. Parsed here (not in the job) so the cgroup
    /// v1/v2 layouts stay unit-testable against fixtures.
    #[serde(default)]
    pub(super) cgroup: Option<String>,
    /// `SLURM_PROCID` read live from `/proc/<pid>/environ`, if readable.
    #[serde(default)]
    pub(super) slurm_procid: Option<String>,
    /// `SLURM_LOCALID` read live from `/proc/<pid>/environ`, if readable.
    #[serde(default)]
    pub(super) slurm_localid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CpuSampleRow {
    #[serde(default)]
    pub(super) node: Option<String>,
    #[serde(default)]
    pub(super) cpu_util_pct: Option<f64>,
    #[serde(default)]
    pub(super) core_count: Option<u64>,
    #[serde(default)]
    pub(super) loadavg_1m: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SlurmSampleRow {
    pub(super) sampled_at: String,
    pub(super) step_id: Option<String>,
    pub(super) ntasks: Option<String>,
    pub(super) ave_cpu: Option<String>,
    pub(super) ave_rss: Option<String>,
    pub(super) max_rss: Option<String>,
    pub(super) alloc_tres: Option<String>,
    pub(super) tres_usage_in_ave: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        CpuSampleRow, GpuDeviceSampleRow, GpuProcessSampleRow, SamplerMetaFile, SlurmSampleRow,
    };

    #[test]
    fn sampler_wire_rows_preserve_required_and_optional_fields() {
        let meta = serde_json::from_str::<SamplerMetaFile>(
            r#"{
                "interval_seconds": 15,
                "collectors": [{
                    "name": "gpu",
                    "enabled": true,
                    "available": true,
                    "note": null,
                    "last_sampled_at": "2026-08-12T12:00:00Z"
                }]
            }"#,
        )
        .expect("sampler metadata should deserialize");
        assert_eq!(meta.interval_seconds, 15);
        assert_eq!(meta.collectors.len(), 1);
        assert_eq!(meta.collectors[0].name, "gpu");

        let gpu = serde_json::from_str::<GpuDeviceSampleRow>(
            r#"{"sampled_at":"2026-08-12T12:00:00Z","node":"n1","uuid":"GPU-1"}"#,
        )
        .expect("GPU device row should deserialize");
        assert_eq!(gpu.sampled_at, "2026-08-12T12:00:00Z");
        assert_eq!(gpu.node.as_deref(), Some("n1"));
        assert_eq!(gpu.uuid.as_deref(), Some("GPU-1"));

        let process = serde_json::from_str::<GpuProcessSampleRow>(
            r#"{"sampled_at":"2026-08-12T12:00:00Z","pid":"42","cgroup":"0::/job"}"#,
        )
        .expect("GPU process row should deserialize");
        assert_eq!(process.pid.as_deref(), Some("42"));
        assert_eq!(process.cgroup.as_deref(), Some("0::/job"));

        let cpu = serde_json::from_str::<CpuSampleRow>(
            r#"{"node":"n1","cpu_util_pct":37.5,"core_count":8,"loadavg_1m":2.5}"#,
        )
        .expect("CPU row should deserialize");
        assert_eq!(cpu.node.as_deref(), Some("n1"));
        assert_eq!(cpu.cpu_util_pct, Some(37.5));

        let slurm = serde_json::from_str::<SlurmSampleRow>(
            r#"{"sampled_at":"2026-08-12T12:00:00Z","step_id":"42.0","ntasks":"8"}"#,
        )
        .expect("Slurm row should deserialize");
        assert_eq!(slurm.step_id.as_deref(), Some("42.0"));
        assert_eq!(slurm.ntasks.as_deref(), Some("8"));
    }

    #[test]
    fn sampler_wire_rows_keep_current_missing_field_semantics() {
        let gpu =
            serde_json::from_str::<GpuDeviceSampleRow>(r#"{"sampled_at":"2026-08-12T12:00:00Z"}"#)
                .expect("optional GPU fields should default to null");
        assert!(gpu.node.is_none());
        assert!(gpu.uuid.is_none());

        let process =
            serde_json::from_str::<GpuProcessSampleRow>(r#"{"sampled_at":"2026-08-12T12:00:00Z"}"#)
                .expect("optional GPU process fields should default to null");
        assert!(process.pid.is_none());
        assert!(process.cgroup.is_none());

        let cpu = serde_json::from_str::<CpuSampleRow>("{}")
            .expect("all CPU fields should default to null");
        assert!(cpu.node.is_none());
        assert!(cpu.cpu_util_pct.is_none());

        assert!(serde_json::from_str::<GpuDeviceSampleRow>("{}").is_err());
        assert!(serde_json::from_str::<GpuProcessSampleRow>("{}").is_err());
        assert!(serde_json::from_str::<SlurmSampleRow>("{}").is_err());
        assert!(serde_json::from_str::<SamplerMetaFile>(r#"{"collectors":[]}"#).is_err());
    }
}
