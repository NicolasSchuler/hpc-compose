#![no_main]

use hpc_compose::spec::ComposeSpec;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let raw = String::from_utf8_lossy(data);
    let _ = ComposeSpec::load_fuzz_root_from_str(&raw);
});
