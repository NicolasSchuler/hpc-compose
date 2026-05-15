#![allow(missing_docs)]

use std::ffi::{CStr, CString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::Serialize;

const NVML_LIBRARY_NAME: &str = "libnvidia-ml.so.1";
const NVML_SUCCESS: i32 = 0;
const NVML_ERROR_INSUFFICIENT_SIZE: i32 = 7;
const NVML_EVENT_TYPE_SINGLE_BIT_ECC_ERROR: u64 = 0x0000_0000_0000_0001;
const NVML_EVENT_TYPE_DOUBLE_BIT_ECC_ERROR: u64 = 0x0000_0000_0000_0002;
const NVML_EVENT_TYPE_PSTATE: u64 = 0x0000_0000_0000_0004;
const NVML_EVENT_TYPE_XID_CRITICAL_ERROR: u64 = 0x0000_0000_0000_0008;
const NVML_EVENT_TYPE_CLOCK: u64 = 0x0000_0000_0000_0010;
const NVML_EVENT_TYPE_POWER_SOURCE_CHANGE: u64 = 0x0000_0000_0000_0080;
const NVML_EVENT_TYPE_CLOCK_CHANGE: u64 = 0x0000_0000_0000_0100;

/// Options for the native metrics feasibility probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsProbeOptions {
    pub duration_seconds: u64,
    pub compare_nvidia_smi: bool,
}

/// Top-level report produced by `hpc-compose metrics-probe`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsProbeReport {
    pub schema_version: u32,
    pub generated_at_unix: u64,
    pub duration_seconds: u64,
    pub capabilities: MetricsProbeCapabilities,
    pub measurements: MetricsProbeMeasurements,
    pub recommendation: MetricsProbeRecommendation,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsProbeCapabilities {
    pub perf_event_open: PerfEventOpenCapability,
    pub nvml: NvmlCapability,
    pub tracepoints: TracepointCapability,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricsProbeMeasurements {
    pub perf: Option<PerfMeasurement>,
    pub nvml: Option<NvmlMeasurement>,
    pub nvidia_smi: Option<NvidiaSmiComparison>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetricsProbeRecommendation {
    PerfOnly,
    PerfNvmlPolling,
    AdminRequiredForEbpf,
    NotViable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PerfEventOpenCapability {
    pub available: bool,
    pub perf_event_paranoid: Option<String>,
    pub errno: Option<i32>,
    pub errno_name: Option<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PerfMeasurement {
    pub cycles: u64,
    pub instructions: u64,
    pub elapsed_ns: u128,
    pub time_enabled_ns: u64,
    pub time_running_ns: u64,
    pub ipc: Option<f64>,
    pub workload_iterations: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NvmlCapability {
    pub available: bool,
    pub library: String,
    pub device_count: Option<u32>,
    pub supported_event_mask: Option<u64>,
    pub supported_event_names: Vec<String>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NvmlMeasurement {
    pub devices: Vec<NvmlDeviceMeasurement>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NvmlDeviceMeasurement {
    pub index: u32,
    pub utilization_gpu_percent: Option<u32>,
    pub utilization_memory_percent: Option<u32>,
    pub memory_used_bytes: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub power_draw_mw: Option<u32>,
    pub process_count: Option<u32>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NvidiaSmiComparison {
    pub available: bool,
    pub elapsed_ns: Option<u128>,
    pub status: Option<i32>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TracepointCapability {
    pub tracing_root: Option<PathBuf>,
    pub available_events_readable: bool,
    pub unprivileged_bpf_disabled: Option<String>,
    pub selected_tracepoints: Vec<TracepointProbe>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TracepointProbe {
    pub name: String,
    pub available: bool,
    pub id: Option<u64>,
    pub note: Option<String>,
}

/// Builds a native/event-oriented metrics feasibility report.
pub fn build_metrics_probe_report(options: MetricsProbeOptions) -> Result<MetricsProbeReport> {
    let duration = Duration::from_secs(options.duration_seconds);
    let (perf_capability, perf_measurement) = probe_perf(duration);
    let (nvml_capability, nvml_measurement) = probe_nvml();
    let tracepoints = probe_tracepoints();
    let nvidia_smi = options.compare_nvidia_smi.then(measure_nvidia_smi_once);
    let recommendation = choose_recommendation(
        &perf_capability,
        &nvml_capability,
        &tracepoints,
        perf_measurement.as_ref(),
    );

    Ok(MetricsProbeReport {
        schema_version: 1,
        generated_at_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        duration_seconds: options.duration_seconds,
        capabilities: MetricsProbeCapabilities {
            perf_event_open: perf_capability,
            nvml: nvml_capability,
            tracepoints,
        },
        measurements: MetricsProbeMeasurements {
            perf: perf_measurement,
            nvml: nvml_measurement,
            nvidia_smi,
        },
        recommendation,
    })
}

fn choose_recommendation(
    perf: &PerfEventOpenCapability,
    nvml: &NvmlCapability,
    tracepoints: &TracepointCapability,
    perf_measurement: Option<&PerfMeasurement>,
) -> MetricsProbeRecommendation {
    if perf.available && nvml.available {
        MetricsProbeRecommendation::PerfNvmlPolling
    } else if perf.available || perf_measurement.is_some() {
        MetricsProbeRecommendation::PerfOnly
    } else if tracepoints.available_events_readable || tracepoints.tracing_root.is_some() {
        MetricsProbeRecommendation::AdminRequiredForEbpf
    } else {
        MetricsProbeRecommendation::NotViable
    }
}

#[cfg(target_os = "linux")]
fn perf_event_paranoid_value() -> Option<String> {
    read_trimmed(Path::new("/proc/sys/kernel/perf_event_paranoid"))
}

#[cfg(target_os = "linux")]
trait PerfEventSource {
    fn open_counter(
        &self,
        config: u64,
        group_fd: i32,
        disabled: bool,
    ) -> std::result::Result<i32, i32>;
}

#[cfg(target_os = "linux")]
struct LinuxPerfEventSource;

#[cfg(target_os = "linux")]
impl PerfEventSource for LinuxPerfEventSource {
    fn open_counter(
        &self,
        config: u64,
        group_fd: i32,
        disabled: bool,
    ) -> std::result::Result<i32, i32> {
        linux_perf::open_counter(config, group_fd, disabled)
    }
}

#[cfg(target_os = "linux")]
fn probe_perf(duration: Duration) -> (PerfEventOpenCapability, Option<PerfMeasurement>) {
    probe_perf_with_source(&LinuxPerfEventSource, duration, perf_event_paranoid_value())
}

#[cfg(target_os = "linux")]
fn probe_perf_with_source(
    source: &dyn PerfEventSource,
    duration: Duration,
    perf_event_paranoid: Option<String>,
) -> (PerfEventOpenCapability, Option<PerfMeasurement>) {
    let leader = match source.open_counter(linux_perf::PERF_COUNT_HW_CPU_CYCLES, -1, true) {
        Ok(fd) => fd,
        Err(errno) => {
            return (
                perf_unavailable_from_errno(errno, perf_event_paranoid, "CPU cycles counter"),
                None,
            );
        }
    };

    let instructions =
        match source.open_counter(linux_perf::PERF_COUNT_HW_INSTRUCTIONS, leader, false) {
            Ok(fd) => fd,
            Err(errno) => {
                linux_perf::close_fd(leader);
                return (
                    perf_unavailable_from_errno(errno, perf_event_paranoid, "instructions counter"),
                    None,
                );
            }
        };

    let measured = linux_perf::measure_group(leader, instructions, duration);
    linux_perf::close_fd(instructions);
    linux_perf::close_fd(leader);

    match measured {
        Ok(measurement) => (
            PerfEventOpenCapability {
                available: true,
                perf_event_paranoid,
                errno: None,
                errno_name: None,
                note: Some(
                    "opened grouped hardware counters for user-space cycles and instructions"
                        .to_string(),
                ),
            },
            Some(measurement),
        ),
        Err(errno) => (
            perf_unavailable_from_errno(errno, perf_event_paranoid, "counter read/ioctl"),
            None,
        ),
    }
}

#[cfg(not(target_os = "linux"))]
fn probe_perf(_duration: Duration) -> (PerfEventOpenCapability, Option<PerfMeasurement>) {
    (
        PerfEventOpenCapability {
            available: false,
            perf_event_paranoid: None,
            errno: None,
            errno_name: None,
            note: Some("perf_event_open is only available on Linux".to_string()),
        },
        None,
    )
}

#[cfg(target_os = "linux")]
fn perf_unavailable_from_errno(
    errno: i32,
    perf_event_paranoid: Option<String>,
    stage: &str,
) -> PerfEventOpenCapability {
    let errno_name = errno_name(errno).map(str::to_string);
    let note = match errno {
        libc::EACCES | libc::EPERM => format!(
            "perf_event_open denied while opening {stage}; check perf_event_paranoid or CAP_PERFMON"
        ),
        libc::ENOENT => format!("perf_event_open could not find the requested {stage} event"),
        libc::EOPNOTSUPP => format!("perf_event_open does not support the requested {stage} event"),
        libc::ENOSYS => "perf_event_open syscall is not available on this kernel".to_string(),
        libc::E2BIG => "perf_event_open rejected the perf_event_attr size".to_string(),
        _ => format!("perf_event_open failed while opening {stage}"),
    };
    PerfEventOpenCapability {
        available: false,
        perf_event_paranoid,
        errno: Some(errno),
        errno_name,
        note: Some(note),
    }
}

#[cfg(target_os = "linux")]
mod linux_perf {
    use std::io;
    use std::mem;
    use std::time::{Duration, Instant};

    use super::PerfMeasurement;

    pub(super) const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
    pub(super) const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;

    const PERF_TYPE_HARDWARE: u32 = 0;
    const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
    const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
    const PERF_FORMAT_GROUP: u64 = 1 << 3;
    const PERF_ATTR_DISABLED: u64 = 1 << 0;
    const PERF_ATTR_EXCLUDE_KERNEL: u64 = 1 << 5;
    const PERF_ATTR_EXCLUDE_HV: u64 = 1 << 6;
    const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 1 << 3;
    const PERF_IOC_FLAG_GROUP: libc::c_ulong = 1;
    const PERF_EVENT_IOC_ENABLE: libc::c_ulong = ioctl_none(b'$', 0);
    const PERF_EVENT_IOC_DISABLE: libc::c_ulong = ioctl_none(b'$', 1);
    const PERF_EVENT_IOC_RESET: libc::c_ulong = ioctl_none(b'$', 3);

    const fn ioctl_none(kind: u8, nr: u8) -> libc::c_ulong {
        ((kind as libc::c_ulong) << 8) | nr as libc::c_ulong
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct PerfEventAttr {
        type_: u32,
        size: u32,
        config: u64,
        sample_period_or_freq: u64,
        sample_type: u64,
        read_format: u64,
        flags: u64,
        wakeup_events_or_watermark: u32,
        bp_type: u32,
        config1: u64,
        config2: u64,
        branch_sample_type: u64,
        sample_regs_user: u64,
        sample_stack_user: u32,
        clockid: i32,
        sample_regs_intr: u64,
        aux_watermark: u32,
        sample_max_stack: u16,
        reserved_2: u16,
        aux_sample_size: u32,
        reserved_3: u32,
        sig_data: u64,
        config3: u64,
    }

    impl PerfEventAttr {
        fn hardware(config: u64, disabled: bool) -> Self {
            Self {
                type_: PERF_TYPE_HARDWARE,
                size: mem::size_of::<Self>() as u32,
                config,
                sample_period_or_freq: 0,
                sample_type: 0,
                read_format: PERF_FORMAT_GROUP
                    | PERF_FORMAT_TOTAL_TIME_ENABLED
                    | PERF_FORMAT_TOTAL_TIME_RUNNING,
                flags: (disabled as u64) | PERF_ATTR_EXCLUDE_KERNEL | PERF_ATTR_EXCLUDE_HV,
                wakeup_events_or_watermark: 0,
                bp_type: 0,
                config1: 0,
                config2: 0,
                branch_sample_type: 0,
                sample_regs_user: 0,
                sample_stack_user: 0,
                clockid: 0,
                sample_regs_intr: 0,
                aux_watermark: 0,
                sample_max_stack: 0,
                reserved_2: 0,
                aux_sample_size: 0,
                reserved_3: 0,
                sig_data: 0,
                config3: 0,
            }
        }
    }

    pub(super) fn open_counter(
        config: u64,
        group_fd: i32,
        disabled: bool,
    ) -> std::result::Result<i32, i32> {
        let mut attr = PerfEventAttr::hardware(config, disabled);
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &mut attr as *mut PerfEventAttr,
                0,
                -1,
                group_fd,
                PERF_FLAG_FD_CLOEXEC,
            )
        };
        if fd < 0 {
            Err(last_errno())
        } else {
            Ok(fd as i32)
        }
    }

    pub(super) fn measure_group(
        leader: i32,
        _instructions: i32,
        duration: Duration,
    ) -> std::result::Result<PerfMeasurement, i32> {
        if ioctl(leader, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP) < 0 {
            return Err(last_errno());
        }
        if ioctl(leader, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP) < 0 {
            return Err(last_errno());
        }
        let started = Instant::now();
        let iterations = run_probe_workload(duration);
        let elapsed_ns = started.elapsed().as_nanos();
        if ioctl(leader, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP) < 0 {
            return Err(last_errno());
        }

        let mut values = [0_u64; 5];
        let expected = mem::size_of_val(&values);
        let read_bytes =
            unsafe { libc::read(leader, values.as_mut_ptr().cast::<libc::c_void>(), expected) };
        if read_bytes < 0 {
            return Err(last_errno());
        }
        if read_bytes as usize != expected || values[0] < 2 {
            return Err(libc::EIO);
        }

        let cycles = values[3];
        let instructions = values[4];
        Ok(PerfMeasurement {
            cycles,
            instructions,
            elapsed_ns,
            time_enabled_ns: values[1],
            time_running_ns: values[2],
            ipc: (cycles > 0).then(|| instructions as f64 / cycles as f64),
            workload_iterations: iterations,
        })
    }

    fn run_probe_workload(duration: Duration) -> u64 {
        let started = Instant::now();
        let mut iterations = 0_u64;
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        loop {
            for _ in 0..1024 {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                std::hint::black_box(state);
                iterations = iterations.wrapping_add(1);
            }
            if duration.is_zero() || started.elapsed() >= duration {
                break;
            }
        }
        std::hint::black_box(state);
        iterations
    }

    fn ioctl(fd: i32, request: libc::c_ulong, arg: libc::c_ulong) -> i32 {
        unsafe { libc::ioctl(fd, request, arg) }
    }

    pub(super) fn close_fd(fd: i32) {
        unsafe {
            libc::close(fd);
        }
    }

    fn last_errno() -> i32 {
        io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO)
    }
}

trait DynamicLibraryLoader {
    fn open(&self, name: &CStr) -> std::result::Result<Box<dyn DynamicLibrary>, String>;
}

trait DynamicLibrary {
    unsafe fn symbol(&self, name: &str) -> std::result::Result<*mut libc::c_void, String>;
}

#[cfg(unix)]
struct UnixDynamicLibraryLoader;

#[cfg(unix)]
struct UnixDynamicLibrary {
    handle: *mut libc::c_void,
}

#[cfg(unix)]
impl Drop for UnixDynamicLibrary {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.handle);
        }
    }
}

#[cfg(unix)]
impl DynamicLibraryLoader for UnixDynamicLibraryLoader {
    fn open(&self, name: &CStr) -> std::result::Result<Box<dyn DynamicLibrary>, String> {
        let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW) };
        if handle.is_null() {
            Err(dlerror_message()
                .unwrap_or_else(|| format!("failed to dlopen {}", name.to_string_lossy())))
        } else {
            Ok(Box::new(UnixDynamicLibrary { handle }))
        }
    }
}

#[cfg(unix)]
impl DynamicLibrary for UnixDynamicLibrary {
    unsafe fn symbol(&self, name: &str) -> std::result::Result<*mut libc::c_void, String> {
        let name = CString::new(name).map_err(|err| err.to_string())?;
        unsafe {
            libc::dlerror();
        }
        let symbol = unsafe { libc::dlsym(self.handle, name.as_ptr()) };
        if symbol.is_null() {
            Err(dlerror_message()
                .unwrap_or_else(|| format!("missing symbol {}", name.to_string_lossy())))
        } else {
            Ok(symbol)
        }
    }
}

#[cfg(unix)]
fn dlerror_message() -> Option<String> {
    let error = unsafe { libc::dlerror() };
    if error.is_null() {
        None
    } else {
        Some(
            unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned(),
        )
    }
}

#[cfg(not(unix))]
struct UnsupportedDynamicLibraryLoader;

#[cfg(not(unix))]
impl DynamicLibraryLoader for UnsupportedDynamicLibraryLoader {
    fn open(&self, _name: &CStr) -> std::result::Result<Box<dyn DynamicLibrary>, String> {
        Err("dynamic library loading is not implemented for this platform".to_string())
    }
}

type NvmlDevice = *mut libc::c_void;
type NvmlEventSet = *mut libc::c_void;
type NvmlInit = unsafe extern "C" fn() -> i32;
type NvmlShutdown = unsafe extern "C" fn() -> i32;
type NvmlDeviceGetCount = unsafe extern "C" fn(*mut libc::c_uint) -> i32;
type NvmlDeviceGetHandleByIndex = unsafe extern "C" fn(libc::c_uint, *mut NvmlDevice) -> i32;
type NvmlDeviceGetUtilizationRates = unsafe extern "C" fn(NvmlDevice, *mut NvmlUtilization) -> i32;
type NvmlDeviceGetMemoryInfo = unsafe extern "C" fn(NvmlDevice, *mut NvmlMemory) -> i32;
type NvmlDeviceGetPowerUsage = unsafe extern "C" fn(NvmlDevice, *mut libc::c_uint) -> i32;
type NvmlDeviceGetComputeRunningProcesses =
    unsafe extern "C" fn(NvmlDevice, *mut libc::c_uint, *mut NvmlProcessInfo) -> i32;
type NvmlEventSetCreate = unsafe extern "C" fn(*mut NvmlEventSet) -> i32;
type NvmlDeviceRegisterEvents = unsafe extern "C" fn(NvmlDevice, u64, NvmlEventSet) -> i32;
type NvmlEventSetFree = unsafe extern "C" fn(NvmlEventSet) -> i32;

#[repr(C)]
#[derive(Default)]
struct NvmlUtilization {
    gpu: libc::c_uint,
    memory: libc::c_uint,
}

#[repr(C)]
#[derive(Default)]
struct NvmlMemory {
    total: libc::c_ulonglong,
    free: libc::c_ulonglong,
    used: libc::c_ulonglong,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NvmlProcessInfo {
    pid: libc::c_uint,
    used_gpu_memory: libc::c_ulonglong,
    gpu_instance_id: libc::c_uint,
    compute_instance_id: libc::c_uint,
}

struct NvmlSymbols {
    shutdown: NvmlShutdown,
    device_get_count: NvmlDeviceGetCount,
    device_get_handle_by_index: NvmlDeviceGetHandleByIndex,
    device_get_utilization_rates: Option<NvmlDeviceGetUtilizationRates>,
    device_get_memory_info: Option<NvmlDeviceGetMemoryInfo>,
    device_get_power_usage: Option<NvmlDeviceGetPowerUsage>,
    device_get_compute_running_processes: Option<NvmlDeviceGetComputeRunningProcesses>,
    event_set_create: Option<NvmlEventSetCreate>,
    device_register_events: Option<NvmlDeviceRegisterEvents>,
    event_set_free: Option<NvmlEventSetFree>,
}

#[cfg(unix)]
fn probe_nvml() -> (NvmlCapability, Option<NvmlMeasurement>) {
    probe_nvml_with_loader(&UnixDynamicLibraryLoader)
}

#[cfg(not(unix))]
fn probe_nvml() -> (NvmlCapability, Option<NvmlMeasurement>) {
    probe_nvml_with_loader(&UnsupportedDynamicLibraryLoader)
}

fn probe_nvml_with_loader(
    loader: &dyn DynamicLibraryLoader,
) -> (NvmlCapability, Option<NvmlMeasurement>) {
    let library_name = CString::new(NVML_LIBRARY_NAME).expect("static library name");
    let library = match loader.open(&library_name) {
        Ok(library) => library,
        Err(err) => {
            return (
                NvmlCapability {
                    available: false,
                    library: NVML_LIBRARY_NAME.to_string(),
                    device_count: None,
                    supported_event_mask: None,
                    supported_event_names: Vec::new(),
                    note: Some(format!("failed to load NVML dynamically: {err}")),
                },
                None,
            );
        }
    };

    let init: NvmlInit = match load_required_symbol(library.as_ref(), "nvmlInit_v2") {
        Ok(symbol) => symbol,
        Err(err) => {
            return (
                nvml_unavailable(format!("failed to load required NVML symbol: {err}")),
                None,
            );
        }
    };
    let symbols = match load_nvml_symbols(library.as_ref()) {
        Ok(symbols) => symbols,
        Err(err) => return (nvml_unavailable(err), None),
    };

    let init_rc = unsafe { init() };
    if init_rc != NVML_SUCCESS {
        return (
            nvml_unavailable(format!("nvmlInit_v2 returned {init_rc}")),
            None,
        );
    }

    let outcome = probe_initialized_nvml(&symbols);
    unsafe {
        (symbols.shutdown)();
    }
    outcome
}

fn load_nvml_symbols(library: &dyn DynamicLibrary) -> std::result::Result<NvmlSymbols, String> {
    Ok(NvmlSymbols {
        shutdown: load_required_symbol(library, "nvmlShutdown")?,
        device_get_count: load_required_symbol(library, "nvmlDeviceGetCount_v2")?,
        device_get_handle_by_index: load_required_symbol(library, "nvmlDeviceGetHandleByIndex_v2")?,
        device_get_utilization_rates: load_optional_symbol(
            library,
            "nvmlDeviceGetUtilizationRates",
        ),
        device_get_memory_info: load_optional_symbol(library, "nvmlDeviceGetMemoryInfo"),
        device_get_power_usage: load_optional_symbol(library, "nvmlDeviceGetPowerUsage"),
        device_get_compute_running_processes: load_optional_symbol(
            library,
            "nvmlDeviceGetComputeRunningProcesses_v3",
        )
        .or_else(|| load_optional_symbol(library, "nvmlDeviceGetComputeRunningProcesses")),
        event_set_create: load_optional_symbol(library, "nvmlEventSetCreate"),
        device_register_events: load_optional_symbol(library, "nvmlDeviceRegisterEvents"),
        event_set_free: load_optional_symbol(library, "nvmlEventSetFree"),
    })
}

fn load_required_symbol<T: Copy>(
    library: &dyn DynamicLibrary,
    name: &str,
) -> std::result::Result<T, String> {
    let symbol = unsafe { library.symbol(name)? };
    Ok(unsafe { std::mem::transmute_copy::<*mut libc::c_void, T>(&symbol) })
}

fn load_optional_symbol<T: Copy>(library: &dyn DynamicLibrary, name: &str) -> Option<T> {
    unsafe { library.symbol(name) }
        .ok()
        .map(|symbol| unsafe { std::mem::transmute_copy::<*mut libc::c_void, T>(&symbol) })
}

fn probe_initialized_nvml(symbols: &NvmlSymbols) -> (NvmlCapability, Option<NvmlMeasurement>) {
    let mut count = 0_u32;
    let count_rc = unsafe { (symbols.device_get_count)(&mut count) };
    if count_rc != NVML_SUCCESS {
        return (
            nvml_unavailable(format!("nvmlDeviceGetCount_v2 returned {count_rc}")),
            None,
        );
    }

    let mut devices = Vec::new();
    let mut supported_event_mask = 0_u64;
    let mut supported_event_names = Vec::new();
    for index in 0..count {
        let mut handle = std::ptr::null_mut();
        let handle_rc = unsafe { (symbols.device_get_handle_by_index)(index, &mut handle) };
        if handle_rc != NVML_SUCCESS {
            devices.push(NvmlDeviceMeasurement {
                index,
                utilization_gpu_percent: None,
                utilization_memory_percent: None,
                memory_used_bytes: None,
                memory_total_bytes: None,
                power_draw_mw: None,
                process_count: None,
                note: Some(format!(
                    "nvmlDeviceGetHandleByIndex_v2 returned {handle_rc}"
                )),
            });
            continue;
        }
        devices.push(probe_nvml_device(symbols, index, handle));
        let supported = probe_nvml_event_mask(symbols, handle);
        supported_event_mask |= supported;
    }
    for (name, mask) in nvml_event_candidates() {
        if supported_event_mask & mask != 0 {
            supported_event_names.push(name.to_string());
        }
    }

    (
        NvmlCapability {
            available: true,
            library: NVML_LIBRARY_NAME.to_string(),
            device_count: Some(count),
            supported_event_mask: Some(supported_event_mask),
            supported_event_names,
            note: Some(
                "NVML utilization, memory, and power are query-style metrics; event support is probed separately"
                    .to_string(),
            ),
        },
        Some(NvmlMeasurement { devices }),
    )
}

fn probe_nvml_device(
    symbols: &NvmlSymbols,
    index: u32,
    handle: NvmlDevice,
) -> NvmlDeviceMeasurement {
    let mut note = Vec::new();
    let mut utilization_gpu_percent = None;
    let mut utilization_memory_percent = None;
    if let Some(get_utilization) = symbols.device_get_utilization_rates {
        let mut utilization = NvmlUtilization::default();
        let rc = unsafe { get_utilization(handle, &mut utilization) };
        if rc == NVML_SUCCESS {
            utilization_gpu_percent = Some(utilization.gpu);
            utilization_memory_percent = Some(utilization.memory);
        } else {
            note.push(format!("nvmlDeviceGetUtilizationRates returned {rc}"));
        }
    }

    let mut memory_used_bytes = None;
    let mut memory_total_bytes = None;
    if let Some(get_memory) = symbols.device_get_memory_info {
        let mut memory = NvmlMemory::default();
        let rc = unsafe { get_memory(handle, &mut memory) };
        if rc == NVML_SUCCESS {
            memory_used_bytes = Some(memory.used);
            memory_total_bytes = Some(memory.total);
        } else {
            note.push(format!("nvmlDeviceGetMemoryInfo returned {rc}"));
        }
    }

    let mut power_draw_mw = None;
    if let Some(get_power) = symbols.device_get_power_usage {
        let mut power = 0_u32;
        let rc = unsafe { get_power(handle, &mut power) };
        if rc == NVML_SUCCESS {
            power_draw_mw = Some(power);
        } else {
            note.push(format!("nvmlDeviceGetPowerUsage returned {rc}"));
        }
    }

    let process_count = symbols
        .device_get_compute_running_processes
        .and_then(|get_processes| probe_nvml_process_count(get_processes, handle, &mut note));

    NvmlDeviceMeasurement {
        index,
        utilization_gpu_percent,
        utilization_memory_percent,
        memory_used_bytes,
        memory_total_bytes,
        power_draw_mw,
        process_count,
        note: (!note.is_empty()).then(|| note.join("; ")),
    }
}

fn probe_nvml_process_count(
    get_processes: NvmlDeviceGetComputeRunningProcesses,
    handle: NvmlDevice,
    note: &mut Vec<String>,
) -> Option<u32> {
    let mut count = 0_u32;
    let first_rc = unsafe { get_processes(handle, &mut count, std::ptr::null_mut()) };
    if first_rc == NVML_SUCCESS {
        return Some(count);
    }
    if first_rc != NVML_ERROR_INSUFFICIENT_SIZE {
        note.push(format!(
            "nvmlDeviceGetComputeRunningProcesses returned {first_rc}"
        ));
        return None;
    }
    if count == 0 {
        return Some(0);
    }
    let mut processes = vec![NvmlProcessInfo::default(); count as usize];
    let second_rc = unsafe { get_processes(handle, &mut count, processes.as_mut_ptr()) };
    if second_rc == NVML_SUCCESS || second_rc == NVML_ERROR_INSUFFICIENT_SIZE {
        Some(count)
    } else {
        note.push(format!(
            "nvmlDeviceGetComputeRunningProcesses returned {second_rc}"
        ));
        None
    }
}

fn probe_nvml_event_mask(symbols: &NvmlSymbols, handle: NvmlDevice) -> u64 {
    let (Some(create), Some(register), Some(free)) = (
        symbols.event_set_create,
        symbols.device_register_events,
        symbols.event_set_free,
    ) else {
        return 0;
    };

    let mut supported = 0_u64;
    for (_, mask) in nvml_event_candidates() {
        let mut event_set = std::ptr::null_mut();
        let create_rc = unsafe { create(&mut event_set) };
        if create_rc != NVML_SUCCESS || event_set.is_null() {
            continue;
        }
        let register_rc = unsafe { register(handle, mask, event_set) };
        unsafe {
            free(event_set);
        }
        if register_rc == NVML_SUCCESS {
            supported |= mask;
        }
    }
    supported
}

fn nvml_event_candidates() -> [(&'static str, u64); 7] {
    [
        ("single_bit_ecc_error", NVML_EVENT_TYPE_SINGLE_BIT_ECC_ERROR),
        ("double_bit_ecc_error", NVML_EVENT_TYPE_DOUBLE_BIT_ECC_ERROR),
        ("pstate", NVML_EVENT_TYPE_PSTATE),
        ("xid_critical_error", NVML_EVENT_TYPE_XID_CRITICAL_ERROR),
        ("clock", NVML_EVENT_TYPE_CLOCK),
        ("power_source_change", NVML_EVENT_TYPE_POWER_SOURCE_CHANGE),
        ("clock_change", NVML_EVENT_TYPE_CLOCK_CHANGE),
    ]
}

fn nvml_unavailable(note: String) -> NvmlCapability {
    NvmlCapability {
        available: false,
        library: NVML_LIBRARY_NAME.to_string(),
        device_count: None,
        supported_event_mask: None,
        supported_event_names: Vec::new(),
        note: Some(note),
    }
}

fn measure_nvidia_smi_once() -> NvidiaSmiComparison {
    let started = Instant::now();
    match Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,utilization.gpu,memory.used,power.draw",
            "--format=csv,noheader,nounits",
        ])
        .output()
    {
        Ok(output) => NvidiaSmiComparison {
            available: output.status.success(),
            elapsed_ns: Some(started.elapsed().as_nanos()),
            status: output.status.code(),
            note: (!output.status.success()).then(|| {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if stderr.is_empty() { stdout } else { stderr }
            }),
        },
        Err(err) => NvidiaSmiComparison {
            available: false,
            elapsed_ns: Some(started.elapsed().as_nanos()),
            status: None,
            note: Some(format!("failed to execute nvidia-smi: {err}")),
        },
    }
}

fn probe_tracepoints() -> TracepointCapability {
    let tracing_root = find_tracing_root();
    let unprivileged_bpf_disabled =
        read_trimmed(Path::new("/proc/sys/kernel/unprivileged_bpf_disabled"));
    let Some(root) = tracing_root else {
        return TracepointCapability {
            tracing_root: None,
            available_events_readable: false,
            unprivileged_bpf_disabled,
            selected_tracepoints: selected_tracepoint_names()
                .iter()
                .map(|name| TracepointProbe {
                    name: (*name).to_string(),
                    available: false,
                    id: None,
                    note: Some("tracing filesystem is not visible".to_string()),
                })
                .collect(),
            note: Some(
                "no tracing filesystem found at /sys/kernel/tracing or /sys/kernel/debug/tracing"
                    .to_string(),
            ),
        };
    };

    let available_events_path = root.join("available_events");
    let available_events = fs::read_to_string(&available_events_path);
    let available_events_readable = available_events.is_ok();
    let available_events = available_events.unwrap_or_default();
    let selected_tracepoints = selected_tracepoint_names()
        .iter()
        .map(|name| probe_tracepoint(&root, &available_events, name))
        .collect::<Vec<_>>();
    let note = (!available_events_readable).then(|| {
        format!(
            "could not read {}; tracepoint IDs may still reveal partial capability",
            available_events_path.display()
        )
    });

    TracepointCapability {
        tracing_root: Some(root),
        available_events_readable,
        unprivileged_bpf_disabled,
        selected_tracepoints,
        note,
    }
}

fn find_tracing_root() -> Option<PathBuf> {
    [
        Path::new("/sys/kernel/tracing"),
        Path::new("/sys/kernel/debug/tracing"),
    ]
    .into_iter()
    .find(|path| path.is_dir())
    .map(Path::to_path_buf)
}

fn selected_tracepoint_names() -> [&'static str; 6] {
    [
        "syscalls/sys_enter_read",
        "syscalls/sys_exit_read",
        "block/block_rq_issue",
        "block/block_rq_complete",
        "net/net_dev_queue",
        "net/netif_receive_skb",
    ]
}

fn probe_tracepoint(root: &Path, available_events: &str, name: &str) -> TracepointProbe {
    let event_name = name.replace('/', ":");
    let listed = available_events
        .lines()
        .any(|line| line.trim() == event_name);
    let id_path = root.join("events").join(name).join("id");
    let id = read_trimmed(&id_path).and_then(|value| value.parse::<u64>().ok());
    let available = listed || id.is_some();
    TracepointProbe {
        name: name.to_string(),
        available,
        id,
        note: (!available).then(|| {
            format!(
                "not found in available_events and no readable id at {}",
                id_path.display()
            )
        }),
    }
}

fn read_trimmed(path: &Path) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(target_os = "linux")]
fn errno_name(errno: i32) -> Option<&'static str> {
    match errno {
        libc::EACCES => Some("EACCES"),
        libc::EPERM => Some("EPERM"),
        libc::ENOENT => Some("ENOENT"),
        libc::EOPNOTSUPP => Some("EOPNOTSUPP"),
        libc::ENOSYS => Some("ENOSYS"),
        libc::E2BIG => Some("E2BIG"),
        libc::EIO => Some("EIO"),
        _ => None,
    }
}

/// Serializes a metrics probe report as pretty JSON.
pub fn serialize_metrics_probe_report(report: &MetricsProbeReport) -> Result<String> {
    serde_json::to_string_pretty(report).context("failed to serialize metrics probe report")
}

/// Validates metrics probe options before running the probe.
pub fn validate_metrics_probe_options(options: MetricsProbeOptions) -> Result<()> {
    if options.duration_seconds > 3_600 {
        bail!("metrics-probe --duration-seconds must be at most 3600");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_serializes_all_availability_states() {
        let report = MetricsProbeReport {
            schema_version: 1,
            generated_at_unix: 1,
            duration_seconds: 0,
            capabilities: MetricsProbeCapabilities {
                perf_event_open: PerfEventOpenCapability {
                    available: true,
                    perf_event_paranoid: Some("2".into()),
                    errno: None,
                    errno_name: None,
                    note: Some("ok".into()),
                },
                nvml: NvmlCapability {
                    available: false,
                    library: NVML_LIBRARY_NAME.into(),
                    device_count: None,
                    supported_event_mask: None,
                    supported_event_names: Vec::new(),
                    note: Some("missing".into()),
                },
                tracepoints: TracepointCapability {
                    tracing_root: None,
                    available_events_readable: false,
                    unprivileged_bpf_disabled: Some("1".into()),
                    selected_tracepoints: vec![TracepointProbe {
                        name: "syscalls/sys_enter_read".into(),
                        available: false,
                        id: None,
                        note: Some("missing".into()),
                    }],
                    note: Some("no tracing fs".into()),
                },
            },
            measurements: MetricsProbeMeasurements {
                perf: Some(PerfMeasurement {
                    cycles: 10,
                    instructions: 20,
                    elapsed_ns: 30,
                    time_enabled_ns: 40,
                    time_running_ns: 40,
                    ipc: Some(2.0),
                    workload_iterations: 50,
                }),
                nvml: None,
                nvidia_smi: Some(NvidiaSmiComparison {
                    available: false,
                    elapsed_ns: Some(10),
                    status: None,
                    note: Some("missing".into()),
                }),
            },
            recommendation: MetricsProbeRecommendation::PerfOnly,
        };

        let json = serialize_metrics_probe_report(&report).expect("json");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(value["capabilities"]["perf_event_open"]["available"], true);
        assert_eq!(value["recommendation"], "perf_only");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn perf_errno_mapping_marks_permission_denied_unavailable() {
        let capability =
            perf_unavailable_from_errno(libc::EACCES, Some("3".to_string()), "CPU cycles counter");

        assert!(!capability.available);
        assert_eq!(capability.errno, Some(libc::EACCES));
        assert_eq!(capability.errno_name.as_deref(), Some("EACCES"));
        assert!(
            capability
                .note
                .as_deref()
                .expect("note")
                .contains("perf_event_paranoid")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn perf_probe_uses_structured_unavailability_from_source() {
        struct DeniedSource;

        impl PerfEventSource for DeniedSource {
            fn open_counter(
                &self,
                _config: u64,
                _group_fd: i32,
                _disabled: bool,
            ) -> std::result::Result<i32, i32> {
                Err(libc::EPERM)
            }
        }

        let (capability, measurement) =
            probe_perf_with_source(&DeniedSource, Duration::ZERO, Some("2".into()));
        assert!(!capability.available);
        assert_eq!(capability.errno_name.as_deref(), Some("EPERM"));
        assert!(measurement.is_none());
    }

    #[test]
    fn nvml_loader_failure_is_structured_unavailability() {
        struct MissingLoader;

        impl DynamicLibraryLoader for MissingLoader {
            fn open(&self, _name: &CStr) -> std::result::Result<Box<dyn DynamicLibrary>, String> {
                Err("not installed".to_string())
            }
        }

        let (capability, measurement) = probe_nvml_with_loader(&MissingLoader);
        assert!(!capability.available);
        assert!(
            capability
                .note
                .as_deref()
                .unwrap_or("")
                .contains("not installed")
        );
        assert!(measurement.is_none());
    }
}
