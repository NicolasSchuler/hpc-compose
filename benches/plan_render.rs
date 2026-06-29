use std::env;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::Result;
use hpc_compose::planner::build_plan;
use hpc_compose::prepare::build_runtime_plan;
use hpc_compose::render::render_script;
use hpc_compose::spec::ComposeSpec;

const DEFAULT_ITERATIONS: usize = 200;

fn main() -> Result<()> {
    let iterations = env::var("HPC_COMPOSE_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_ITERATIONS);
    let compose_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/dev-python-app.yaml");

    let script_len = bench_plan_render(&compose_path, iterations)?;
    println!("plan_render: {iterations} iterations, last script bytes={script_len}");
    Ok(())
}

fn bench_plan_render(compose_path: &Path, iterations: usize) -> Result<usize> {
    let start = Instant::now();
    let mut script_len = 0;

    for _ in 0..iterations {
        let spec = ComposeSpec::load(compose_path)?;
        let plan = build_plan(compose_path, spec)?;
        let runtime_plan = build_runtime_plan(&plan);
        let script = render_script(&runtime_plan)?;
        script_len = black_box(script.len());
        black_box(runtime_plan);
    }

    let elapsed = start.elapsed();
    let nanos_per_iter = elapsed.as_nanos() / iterations as u128;
    println!("plan_render: elapsed={elapsed:?}, ns/iter={nanos_per_iter}");

    Ok(script_len)
}
