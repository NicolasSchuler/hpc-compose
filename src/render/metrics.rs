pub(super) fn render_metrics_helpers(out: &mut String) {
    out.push_str("json_bool_from_flag() {\n");
    out.push_str("  if [[ \"${1:-0}\" == \"1\" ]]; then\n");
    out.push_str("    printf true\n");
    out.push_str("  else\n");
    out.push_str("    printf false\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("metrics_timestamp() {\n");
    out.push_str("  date -u +%Y-%m-%dT%H:%M:%SZ\n");
    out.push_str("}\n\n");

    out.push_str("metrics_warning_once() {\n");
    out.push_str("  local collector=$1\n");
    out.push_str("  local message=$2\n");
    out.push_str("  case \"$collector\" in\n");
    out.push_str("    gpu)\n");
    out.push_str("      [[ \"$GPU_WARNING_EMITTED\" == \"1\" ]] && return 0\n");
    out.push_str("      GPU_WARNING_EMITTED=1\n");
    out.push_str("      ;;\n");
    out.push_str("    slurm)\n");
    out.push_str("      [[ \"$SLURM_WARNING_EMITTED\" == \"1\" ]] && return 0\n");
    out.push_str("      SLURM_WARNING_EMITTED=1\n");
    out.push_str("      ;;\n");
    out.push_str("    cpu)\n");
    out.push_str("      [[ \"$CPU_WARNING_EMITTED\" == \"1\" ]] && return 0\n");
    out.push_str("      CPU_WARNING_EMITTED=1\n");
    out.push_str("      ;;\n");
    out.push_str("    steps)\n");
    out.push_str("      [[ \"$STEPS_WARNING_EMITTED\" == \"1\" ]] && return 0\n");
    out.push_str("      STEPS_WARNING_EMITTED=1\n");
    out.push_str("      ;;\n");
    out.push_str("  esac\n");
    out.push_str("  echo \"metrics warning [$collector]: $message\" >&2\n");
    out.push_str("}\n\n");

    out.push_str("write_metrics_meta() {\n");
    out.push_str("  local tmp_meta=\"$METRICS_META_FILE.tmp\"\n");
    out.push_str("  {\n");
    out.push_str("    printf '{\\n'\n");
    out.push_str(
        "    printf '  \"sampler_pid\": %s,\\n' \"$(json_number_or_null \"$SAMPLER_PID\")\"\n",
    );
    out.push_str("    printf '  \"interval_seconds\": %s,\\n' \"$METRICS_INTERVAL_SECONDS\"\n");
    out.push_str("    printf '  \"collectors\": [\\n'\n");
    out.push_str("    printf '    {\"name\":\"gpu\",\"enabled\":%s,\"available\":%s,\"note\":%s,\"last_sampled_at\":%s,\"coverage\":{\"scope\":%s,\"expected_nodes\":%s,\"observed_nodes\":%s,\"degraded\":%s,\"reason\":%s}},\\n' \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$GPU_COLLECTOR_ENABLED\")\" \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$GPU_COLLECTOR_AVAILABLE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$GPU_COLLECTOR_NOTE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$GPU_COLLECTOR_LAST_SAMPLED_AT\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$GPU_COVERAGE_SCOPE\")\" \\\n");
    out.push_str("      \"$GPU_COVERAGE_EXPECTED_NODES\" \\\n");
    out.push_str("      \"$GPU_COVERAGE_OBSERVED_NODES\" \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$GPU_COVERAGE_DEGRADED\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$GPU_COVERAGE_REASON\")\"\n");
    out.push_str("    printf '    {\"name\":\"slurm\",\"enabled\":%s,\"available\":%s,\"note\":%s,\"last_sampled_at\":%s},\\n' \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$SLURM_COLLECTOR_ENABLED\")\" \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$SLURM_COLLECTOR_AVAILABLE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$SLURM_COLLECTOR_NOTE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$SLURM_COLLECTOR_LAST_SAMPLED_AT\")\"\n");
    out.push_str("    printf '    {\"name\":\"cpu\",\"enabled\":%s,\"available\":%s,\"note\":%s,\"last_sampled_at\":%s,\"coverage\":{\"scope\":%s,\"expected_nodes\":%s,\"observed_nodes\":%s,\"degraded\":%s,\"reason\":%s}}\\n' \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$CPU_COLLECTOR_ENABLED\")\" \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$CPU_COLLECTOR_AVAILABLE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$CPU_COLLECTOR_NOTE\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$CPU_COLLECTOR_LAST_SAMPLED_AT\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$CPU_COVERAGE_SCOPE\")\" \\\n");
    out.push_str("      \"$CPU_COVERAGE_EXPECTED_NODES\" \\\n");
    out.push_str("      \"$CPU_COVERAGE_OBSERVED_NODES\" \\\n");
    out.push_str("      \"$(json_bool_from_flag \"$CPU_COVERAGE_DEGRADED\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$CPU_COVERAGE_REASON\")\"\n");
    out.push_str("    printf '  ]\\n}\\n'\n");
    out.push_str("  } > \"$tmp_meta\"\n");
    out.push_str("  mv \"$tmp_meta\" \"$METRICS_META_FILE\"\n");
    out.push_str("}\n\n");

    out.push_str("mark_gpu_collector_unavailable() {\n");
    out.push_str("  GPU_COLLECTOR_AVAILABLE=0\n");
    out.push_str("  GPU_COLLECTOR_NOTE=$1\n");
    out.push_str("  GPU_COVERAGE_SCOPE=unknown\n");
    out.push_str("  GPU_COVERAGE_OBSERVED_NODES=0\n");
    out.push_str("  GPU_COVERAGE_DEGRADED=1\n");
    out.push_str("  GPU_COVERAGE_REASON=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("  metrics_warning_once gpu \"$1\"\n");
    out.push_str("}\n\n");

    out.push_str("mark_gpu_collector_success() {\n");
    out.push_str("  GPU_COLLECTOR_AVAILABLE=1\n");
    out.push_str("  GPU_COLLECTOR_LAST_SAMPLED_AT=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("}\n\n");

    out.push_str("mark_slurm_collector_unavailable() {\n");
    out.push_str("  SLURM_COLLECTOR_AVAILABLE=0\n");
    out.push_str("  SLURM_COLLECTOR_NOTE=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("  metrics_warning_once slurm \"$1\"\n");
    out.push_str("}\n\n");

    out.push_str("mark_slurm_collector_success() {\n");
    out.push_str("  SLURM_COLLECTOR_AVAILABLE=1\n");
    out.push_str("  SLURM_COLLECTOR_NOTE=\"\"\n");
    out.push_str("  SLURM_COLLECTOR_LAST_SAMPLED_AT=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("}\n\n");

    out.push_str("mark_cpu_collector_unavailable() {\n");
    out.push_str("  CPU_COLLECTOR_AVAILABLE=0\n");
    out.push_str("  CPU_COLLECTOR_NOTE=$1\n");
    out.push_str("  CPU_COVERAGE_SCOPE=unknown\n");
    out.push_str("  CPU_COVERAGE_OBSERVED_NODES=0\n");
    out.push_str("  CPU_COVERAGE_DEGRADED=1\n");
    out.push_str("  CPU_COVERAGE_REASON=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("  metrics_warning_once cpu \"$1\"\n");
    out.push_str("}\n\n");

    out.push_str("mark_cpu_collector_success() {\n");
    out.push_str("  CPU_COLLECTOR_AVAILABLE=1\n");
    out.push_str("  CPU_COLLECTOR_LAST_SAMPLED_AT=$1\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("}\n\n");

    out.push_str("set_gpu_current_node_coverage() {\n");
    out.push_str("  GPU_COVERAGE_OBSERVED_NODES=1\n");
    out.push_str("  if (( GPU_COVERAGE_EXPECTED_NODES > 1 )); then\n");
    out.push_str("    GPU_COVERAGE_SCOPE=batch_node\n");
    out.push_str("    GPU_COVERAGE_DEGRADED=1\n");
    out.push_str(
        "    GPU_COVERAGE_REASON=\"multi-node allocation sampled on the batch node only\"\n",
    );
    out.push_str("  else\n");
    out.push_str("    GPU_COVERAGE_SCOPE=allocation\n");
    out.push_str("    GPU_COVERAGE_DEGRADED=0\n");
    out.push_str("    GPU_COVERAGE_REASON=\"\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("set_cpu_current_node_coverage() {\n");
    out.push_str("  CPU_COVERAGE_OBSERVED_NODES=1\n");
    out.push_str("  if (( CPU_COVERAGE_EXPECTED_NODES > 1 )); then\n");
    out.push_str("    CPU_COVERAGE_SCOPE=batch_node\n");
    out.push_str("    CPU_COVERAGE_DEGRADED=1\n");
    out.push_str(
        "    CPU_COVERAGE_REASON=\"multi-node allocation sampled on the batch node only\"\n",
    );
    out.push_str("  else\n");
    out.push_str("    CPU_COVERAGE_SCOPE=allocation\n");
    out.push_str("    CPU_COVERAGE_DEGRADED=0\n");
    out.push_str("    CPU_COVERAGE_REASON=\"\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    // Best-effort per-PID attribution probes (cgroup + task environment).
    // Shared verbatim with the self-contained per-node fanout script.
    out.push_str(gpu_attribution_helpers_body());
    out.push('\n');

    // Captures the live Slurm step-id -> step-name map through squeue so
    // post-processing (`hpc-compose stats`) can resolve a sampled GPU-process
    // cgroup step back to the hpc-compose service that launched it. This must
    // run while the job is alive: neither the step list nor /proc/<pid> survive
    // job teardown. Rows are appended to steps.jsonl only when the map changes.
    // Every failure path is best-effort (warn-once, never affects the job).
    out.push_str("capture_step_map() {\n");
    out.push_str("  [[ \"$GPU_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  [[ -n \"${SLURM_JOB_ID:-}\" ]] || return 0\n");
    out.push_str("  if ! command -v squeue >/dev/null 2>&1; then\n");
    out.push_str("    metrics_warning_once steps \"squeue is not available; per-service GPU attribution will stay null\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    out.push_str("  local output\n");
    out.push_str("  if ! output=$(squeue --noheader --steps --jobs \"$SLURM_JOB_ID\" --format='%i|%j' 2>&1); then\n");
    out.push_str("    metrics_warning_once steps \"squeue step query failed; per-service GPU attribution will stay null: $(trim_whitespace \"${output//$'\\n'/; }\")\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  [[ \"$output\" == \"$LAST_STEP_MAP\" ]] && return 0\n");
    out.push_str("  LAST_STEP_MAP=$output\n");
    out.push_str("  local line\n");
    out.push_str("  while IFS= read -r line; do\n");
    out.push_str("    [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str("    local step_id step_name\n");
    out.push_str("    step_id=$(trim_whitespace \"${line%%|*}\")\n");
    out.push_str("    step_name=$(trim_whitespace \"${line#*|}\")\n");
    out.push_str("    [[ -z \"$step_id\" ]] && continue\n");
    out.push_str("    printf '{\"sampled_at\":\"%s\",\"step_id\":%s,\"step_name\":%s}\\n' \\\n");
    out.push_str("      \"$(json_escape \"$sampled_at\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$step_id\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$step_name\")\" >> \"$STEP_MAP_FILE\"\n");
    out.push_str("  done <<< \"$output\"\n");
    out.push_str("  return 0\n");
    out.push_str("}\n\n");

    out.push_str("sample_gpu_metrics_current_node() {\n");
    out.push_str("  [[ \"$GPU_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  GPU_COLLECTOR_NOTE=\"\"\n");
    out.push_str("  if ! command -v nvidia-smi >/dev/null 2>&1; then\n");
    out.push_str(
        "    mark_gpu_collector_unavailable \"nvidia-smi is not available on this node\"\n",
    );
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    out.push_str("  local sample_node=\"${HPC_COMPOSE_GPU_SAMPLE_NODE:-${SLURMD_NODENAME:-${HOSTNAME:-}}}\"\n");
    out.push_str("  local output\n");
    out.push_str("  if ! output=$(nvidia-smi --query-gpu=index,uuid,name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw,power.limit --format=csv,noheader,nounits 2>&1); then\n");
    out.push_str("    mark_gpu_collector_unavailable \"nvidia-smi GPU query failed: $(trim_whitespace \"${output//$'\\n'/; }\")\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  set_gpu_current_node_coverage\n");
    out.push_str("  local line\n");
    out.push_str("  while IFS= read -r line; do\n");
    out.push_str("    [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str("    IFS=',' read -r raw_index raw_uuid raw_name raw_util_gpu raw_util_mem raw_mem_used raw_mem_total raw_temp raw_power_draw raw_power_limit <<< \"$line\"\n");
    out.push_str("    local index uuid name util_gpu util_mem mem_used mem_total temperature power_draw power_limit\n");
    out.push_str("    index=$(trim_whitespace \"$raw_index\")\n");
    out.push_str("    uuid=$(trim_whitespace \"$raw_uuid\")\n");
    out.push_str("    name=$(trim_whitespace \"$raw_name\")\n");
    out.push_str("    util_gpu=$(trim_whitespace \"$raw_util_gpu\")\n");
    out.push_str("    util_mem=$(trim_whitespace \"$raw_util_mem\")\n");
    out.push_str("    mem_used=$(trim_whitespace \"$raw_mem_used\")\n");
    out.push_str("    mem_total=$(trim_whitespace \"$raw_mem_total\")\n");
    out.push_str("    temperature=$(trim_whitespace \"$raw_temp\")\n");
    out.push_str("    power_draw=$(trim_whitespace \"$raw_power_draw\")\n");
    out.push_str("    power_limit=$(trim_whitespace \"$raw_power_limit\")\n");
    out.push_str("    printf '{\"sampled_at\":\"%s\",\"node\":%s,\"rank\":null,\"local_rank\":null,\"service\":null,\"collector\":\"nvidia-smi\",\"index\":%s,\"uuid\":%s,\"name\":%s,\"utilization_gpu\":%s,\"utilization_memory\":%s,\"memory_used_mib\":%s,\"memory_total_mib\":%s,\"temperature_c\":%s,\"power_draw_w\":%s,\"power_limit_w\":%s}\\n' \\\n");
    out.push_str("      \"$(json_escape \"$sampled_at\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$sample_node\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$index\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$uuid\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$name\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$util_gpu\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$util_mem\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$mem_used\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$mem_total\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$temperature\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$power_draw\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$power_limit\")\" >> \"$GPU_METRICS_FILE\"\n");
    out.push_str("  done <<< \"$output\"\n");
    out.push_str("  local process_output\n");
    out.push_str("  if process_output=$(nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits 2>&1); then\n");
    out.push_str("    while IFS= read -r line; do\n");
    out.push_str("      [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str("      IFS=',' read -r raw_gpu_uuid raw_pid raw_process_name raw_used_memory <<< \"$line\"\n");
    out.push_str("      local gpu_uuid pid process_name used_memory\n");
    out.push_str("      gpu_uuid=$(trim_whitespace \"$raw_gpu_uuid\")\n");
    out.push_str("      pid=$(trim_whitespace \"$raw_pid\")\n");
    out.push_str("      process_name=$(trim_whitespace \"$raw_process_name\")\n");
    out.push_str("      used_memory=$(trim_whitespace \"$raw_used_memory\")\n");
    // Raw attribution facts captured while the PID is alive; the mapping to a
    // service happens in `hpc-compose stats` (null there when unresolvable).
    out.push_str("      local proc_cgroup proc_slurm_procid proc_slurm_localid\n");
    out.push_str("      proc_cgroup=$(gpu_process_cgroup \"$pid\")\n");
    out.push_str("      proc_slurm_procid=$(gpu_process_environ_value \"$pid\" SLURM_PROCID)\n");
    out.push_str("      proc_slurm_localid=$(gpu_process_environ_value \"$pid\" SLURM_LOCALID)\n");
    out.push_str("      printf '{\"sampled_at\":\"%s\",\"node\":%s,\"rank\":null,\"local_rank\":null,\"service\":null,\"collector\":\"nvidia-smi\",\"gpu_uuid\":%s,\"pid\":%s,\"process_name\":%s,\"used_memory_mib\":%s,\"cgroup\":%s,\"slurm_procid\":%s,\"slurm_localid\":%s}\\n' \\\n");
    out.push_str("        \"$(json_escape \"$sampled_at\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$sample_node\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$gpu_uuid\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$pid\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$process_name\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$used_memory\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$proc_cgroup\")\" \\\n");
    out.push_str("        \"$(json_string_or_null \"$proc_slurm_procid\")\" \\\n");
    out.push_str(
        "        \"$(json_string_or_null \"$proc_slurm_localid\")\" >> \"$GPU_PROCESSES_FILE\"\n",
    );
    out.push_str("    done <<< \"$process_output\"\n");
    out.push_str("  else\n");
    out.push_str("    GPU_COLLECTOR_AVAILABLE=1\n");
    out.push_str("    GPU_COLLECTOR_NOTE=\"nvidia-smi compute process query failed: $(trim_whitespace \"${process_output//$'\\n'/; }\")\"\n");
    out.push_str("    GPU_COLLECTOR_LAST_SAMPLED_AT=$sampled_at\n");
    out.push_str("    write_metrics_meta\n");
    out.push_str("    metrics_warning_once gpu \"$GPU_COLLECTOR_NOTE\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  mark_gpu_collector_success \"$sampled_at\"\n");
    out.push_str("}\n\n");

    out.push_str("write_gpu_sample_node_script() {\n");
    out.push_str("  local script_path=\"$METRICS_DIR/gpu-sample-node.sh\"\n");
    out.push_str("  cat > \"$script_path\" <<'HPC_COMPOSE_GPU_SAMPLE_NODE'\n");
    out.push_str("#!/bin/bash\n");
    out.push_str("set -euo pipefail\n");
    out.push_str("sampled_at=$1\n");
    out.push_str("output_root=$2\n");
    out.push_str("node=\"${SLURMD_NODENAME:-${HOSTNAME:-}}\"\n");
    out.push_str("if [[ -z \"$node\" ]]; then node=$(hostname); fi\n");
    out.push_str("node_dir=\"$output_root/$node\"\n");
    out.push_str("mkdir -p \"$node_dir\"\n");
    out.push_str("json_escape() {\n");
    out.push_str("  local value=$1\n");
    out.push_str("  value=${value//\\\\/\\\\\\\\}\n");
    out.push_str("  value=${value//\\\"/\\\\\\\"}\n");
    out.push_str("  value=${value//$'\\n'/\\\\n}\n");
    out.push_str("  value=${value//$'\\r'/\\\\r}\n");
    out.push_str("  value=${value//$'\\t'/\\\\t}\n");
    out.push_str("  printf '%s' \"$value\"\n");
    out.push_str("}\n");
    out.push_str("json_string_or_null() {\n");
    out.push_str("  local value=${1-}\n");
    out.push_str("  if [[ -z \"$value\" ]]; then printf null; else printf '\"%s\"' \"$(json_escape \"$value\")\"; fi\n");
    out.push_str("}\n");
    out.push_str("trim_whitespace() {\n");
    out.push_str("  local value=${1-}\n");
    out.push_str("  value=${value#\"${value%%[![:space:]]*}\"}\n");
    out.push_str("  value=${value%\"${value##*[![:space:]]}\"}\n");
    out.push_str("  printf '%s' \"$value\"\n");
    out.push_str("}\n");
    out.push_str(gpu_attribution_helpers_body());
    out.push_str("if ! command -v nvidia-smi >/dev/null 2>&1; then\n");
    out.push_str(
        "  printf 'nvidia-smi unavailable on %s\\n' \"$node\" > \"$node_dir/status.txt\"\n",
    );
    out.push_str("  exit 0\n");
    out.push_str("fi\n");
    out.push_str("output=\"\"\n");
    out.push_str("if output=$(nvidia-smi --query-gpu=index,uuid,name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw,power.limit --format=csv,noheader,nounits 2>&1); then\n");
    out.push_str("  while IFS= read -r line; do\n");
    out.push_str("    [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str("    IFS=',' read -r raw_index raw_uuid raw_name raw_util_gpu raw_util_mem raw_mem_used raw_mem_total raw_temp raw_power_draw raw_power_limit <<< \"$line\"\n");
    out.push_str("    index=$(trim_whitespace \"$raw_index\")\n");
    out.push_str("    uuid=$(trim_whitespace \"$raw_uuid\")\n");
    out.push_str("    name=$(trim_whitespace \"$raw_name\")\n");
    out.push_str("    util_gpu=$(trim_whitespace \"$raw_util_gpu\")\n");
    out.push_str("    util_mem=$(trim_whitespace \"$raw_util_mem\")\n");
    out.push_str("    mem_used=$(trim_whitespace \"$raw_mem_used\")\n");
    out.push_str("    mem_total=$(trim_whitespace \"$raw_mem_total\")\n");
    out.push_str("    temperature=$(trim_whitespace \"$raw_temp\")\n");
    out.push_str("    power_draw=$(trim_whitespace \"$raw_power_draw\")\n");
    out.push_str("    power_limit=$(trim_whitespace \"$raw_power_limit\")\n");
    out.push_str("    printf '{\"sampled_at\":\"%s\",\"node\":%s,\"rank\":null,\"local_rank\":null,\"service\":null,\"collector\":\"nvidia-smi\",\"index\":%s,\"uuid\":%s,\"name\":%s,\"utilization_gpu\":%s,\"utilization_memory\":%s,\"memory_used_mib\":%s,\"memory_total_mib\":%s,\"temperature_c\":%s,\"power_draw_w\":%s,\"power_limit_w\":%s}\\n' \\\n");
    out.push_str("      \"$(json_escape \"$sampled_at\")\" \"$(json_string_or_null \"$node\")\" \"$(json_string_or_null \"$index\")\" \"$(json_string_or_null \"$uuid\")\" \"$(json_string_or_null \"$name\")\" \"$(json_string_or_null \"$util_gpu\")\" \"$(json_string_or_null \"$util_mem\")\" \"$(json_string_or_null \"$mem_used\")\" \"$(json_string_or_null \"$mem_total\")\" \"$(json_string_or_null \"$temperature\")\" \"$(json_string_or_null \"$power_draw\")\" \"$(json_string_or_null \"$power_limit\")\" >> \"$node_dir/gpu.jsonl\"\n");
    out.push_str("  done <<< \"$output\"\n");
    out.push_str("else\n");
    out.push_str("  printf 'nvidia-smi GPU query failed on %s: %s\\n' \"$node\" \"$(trim_whitespace \"${output//$'\\n'/; }\")\" > \"$node_dir/status.txt\"\n");
    out.push_str("fi\n");
    out.push_str("process_output=\"\"\n");
    out.push_str("if process_output=$(nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits 2>&1); then\n");
    out.push_str("  while IFS= read -r line; do\n");
    out.push_str("    [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str(
        "    IFS=',' read -r raw_gpu_uuid raw_pid raw_process_name raw_used_memory <<< \"$line\"\n",
    );
    out.push_str("    gpu_uuid=$(trim_whitespace \"$raw_gpu_uuid\")\n");
    out.push_str("    pid=$(trim_whitespace \"$raw_pid\")\n");
    out.push_str("    process_name=$(trim_whitespace \"$raw_process_name\")\n");
    out.push_str("    used_memory=$(trim_whitespace \"$raw_used_memory\")\n");
    out.push_str("    proc_cgroup=$(gpu_process_cgroup \"$pid\")\n");
    out.push_str("    proc_slurm_procid=$(gpu_process_environ_value \"$pid\" SLURM_PROCID)\n");
    out.push_str("    proc_slurm_localid=$(gpu_process_environ_value \"$pid\" SLURM_LOCALID)\n");
    out.push_str("    printf '{\"sampled_at\":\"%s\",\"node\":%s,\"rank\":null,\"local_rank\":null,\"service\":null,\"collector\":\"nvidia-smi\",\"gpu_uuid\":%s,\"pid\":%s,\"process_name\":%s,\"used_memory_mib\":%s,\"cgroup\":%s,\"slurm_procid\":%s,\"slurm_localid\":%s}\\n' \\\n");
    out.push_str("      \"$(json_escape \"$sampled_at\")\" \"$(json_string_or_null \"$node\")\" \"$(json_string_or_null \"$gpu_uuid\")\" \"$(json_string_or_null \"$pid\")\" \"$(json_string_or_null \"$process_name\")\" \"$(json_string_or_null \"$used_memory\")\" \"$(json_string_or_null \"$proc_cgroup\")\" \"$(json_string_or_null \"$proc_slurm_procid\")\" \"$(json_string_or_null \"$proc_slurm_localid\")\" >> \"$node_dir/gpu_processes.jsonl\"\n");
    out.push_str("  done <<< \"$process_output\"\n");
    out.push_str("fi\n");
    out.push_str("HPC_COMPOSE_GPU_SAMPLE_NODE\n");
    out.push_str("  chmod +x \"$script_path\"\n");
    out.push_str("  printf '%s' \"$script_path\"\n");
    out.push_str("}\n\n");

    out.push_str("sample_gpu_metrics_all_nodes() {\n");
    out.push_str("  [[ \"$GPU_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    // Stable per-job scratch dir reused every interval: cleared then recreated
    // in place so the transient per-node sample files do not accumulate one
    // directory per sample tick. Cumulative data still lands in $GPU_METRICS_FILE.
    out.push_str("  local sample_root=\"$METRICS_DIR/gpu-node-samples\"\n");
    out.push_str("  rm -rf \"$sample_root\"\n");
    out.push_str("  mkdir -p \"$sample_root\"\n");
    out.push_str("  local script_path\n");
    out.push_str("  script_path=$(write_gpu_sample_node_script)\n");
    out.push_str("  if ! srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 --exact --overlap bash \"$script_path\" \"$sampled_at\" \"$sample_root\" >/dev/null 2>&1; then\n");
    // srun fanout failure does not mean the collector is dead: the batch node
    // can still sample its own GPUs. Degrade to the single-node path and record
    // the degradation (warn-once) instead of marking the collector unavailable.
    out.push_str("    metrics_warning_once gpu \"multi-node GPU fanout failed through srun; sampling the batch node only\"\n");
    out.push_str("    sample_gpu_metrics_current_node\n");
    out.push_str("    local fallback_status=$?\n");
    out.push_str(
        "    if (( fallback_status == 0 )) && [[ \"$GPU_COLLECTOR_AVAILABLE\" == \"1\" ]]; then\n",
    );
    out.push_str(
        "      GPU_COLLECTOR_NOTE=\"multi-node GPU fanout degraded to batch-node sampling\"\n",
    );
    out.push_str("      GPU_COVERAGE_SCOPE=batch_node\n");
    out.push_str("      GPU_COVERAGE_OBSERVED_NODES=1\n");
    out.push_str("      GPU_COVERAGE_DEGRADED=1\n");
    out.push_str("      GPU_COVERAGE_REASON=\"multi-node GPU fanout failed through srun\"\n");
    out.push_str("      write_metrics_meta\n");
    out.push_str("    fi\n");
    out.push_str("    return \"$fallback_status\"\n");
    out.push_str("  fi\n");
    out.push_str("  shopt -s nullglob\n");
    out.push_str("  local gpu_files=(\"$sample_root\"/*/gpu.jsonl)\n");
    out.push_str("  local proc_files=(\"$sample_root\"/*/gpu_processes.jsonl)\n");
    out.push_str("  local status_files=(\"$sample_root\"/*/status.txt)\n");
    out.push_str("  if (( ${#gpu_files[@]} == 0 )); then\n");
    out.push_str("    mark_gpu_collector_unavailable \"nvidia-smi produced no GPU samples on allocation nodes\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  GPU_COVERAGE_SCOPE=allocation\n");
    out.push_str("  GPU_COVERAGE_OBSERVED_NODES=${#gpu_files[@]}\n");
    out.push_str("  GPU_COVERAGE_DEGRADED=0\n");
    out.push_str("  GPU_COVERAGE_REASON=\"\"\n");
    out.push_str("  GPU_COLLECTOR_NOTE=\"\"\n");
    out.push_str("  if (( GPU_COVERAGE_OBSERVED_NODES < GPU_COVERAGE_EXPECTED_NODES || ${#status_files[@]} > 0 )); then\n");
    out.push_str("    GPU_COVERAGE_DEGRADED=1\n");
    out.push_str("    GPU_COVERAGE_REASON=\"GPU samples covered ${GPU_COVERAGE_OBSERVED_NODES}/${GPU_COVERAGE_EXPECTED_NODES} allocation nodes\"\n");
    out.push_str("  fi\n");
    out.push_str("  cat \"${gpu_files[@]}\" >> \"$GPU_METRICS_FILE\"\n");
    out.push_str("  if (( ${#proc_files[@]} > 0 )); then cat \"${proc_files[@]}\" >> \"$GPU_PROCESSES_FILE\"; fi\n");
    out.push_str("  if (( ${#status_files[@]} > 0 )); then GPU_COLLECTOR_NOTE=\"$(paste -sd '; ' \"${status_files[@]}\" 2>/dev/null || true)\"; GPU_COVERAGE_REASON=\"$GPU_COLLECTOR_NOTE\"; fi\n");
    out.push_str("  mark_gpu_collector_success \"$sampled_at\"\n");
    out.push_str("}\n\n");

    out.push_str("sample_gpu_metrics() {\n");
    // The step map is job-global, so one batch-node squeue capture covers every
    // node's process rows (including fanout rows on multi-node allocations).
    out.push_str("  if [[ \"${BACKEND:-slurm}\" == \"slurm\" ]]; then\n");
    out.push_str("    capture_step_map\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ \"${BACKEND:-slurm}\" == \"slurm\" && \"${HPC_COMPOSE_NODE_COUNT:-1}\" -gt 1 ]]; then\n");
    out.push_str("    sample_gpu_metrics_all_nodes\n");
    out.push_str("  else\n");
    out.push_str("    sample_gpu_metrics_current_node\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("sample_slurm_metrics() {\n");
    out.push_str("  [[ \"$SLURM_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  if ! command -v sstat >/dev/null 2>&1; then\n");
    out.push_str("    mark_slurm_collector_unavailable \"sstat is not available on this node\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    out.push_str("  local output\n");
    // AllocTRES is a sacct (allocation) field that sstat rejects with "Invalid
    // field requested"; sstat reports live step usage only. Sampling usage and
    // leaving allocation to accounting keeps this collector working across Slurm
    // builds. `alloc_tres` stays in the sample schema as null for compatibility.
    out.push_str("  if ! output=$(sstat --allsteps --jobs \"$SLURM_JOB_ID\" --parsable2 --noconvert --format=JobID,NTasks,AveCPU,AveRSS,MaxRSS,TRESUsageInAve 2>&1); then\n");
    out.push_str("    mark_slurm_collector_unavailable \"sstat query failed: $(trim_whitespace \"${output//$'\\n'/; }\")\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  local line\n");
    out.push_str("  while IFS= read -r line; do\n");
    out.push_str("    [[ -z \"$(trim_whitespace \"$line\")\" ]] && continue\n");
    out.push_str("    [[ \"$line\" == JobID* ]] && continue\n");
    out.push_str("    local -a fields=()\n");
    out.push_str("    IFS='|' read -r -a fields <<< \"$line\"\n");
    out.push_str("    if (( ${#fields[@]} != 6 )); then\n");
    out.push_str("      mark_slurm_collector_unavailable \"malformed sstat output while sampling metrics\"\n");
    out.push_str("      return 0\n");
    out.push_str("    fi\n");
    out.push_str("    local step_id\n");
    out.push_str("    step_id=$(trim_whitespace \"${fields[0]}\")\n");
    out.push_str("    if [[ ! \"$step_id\" =~ ^${SLURM_JOB_ID}\\.[0-9]+$ ]]; then\n");
    out.push_str("      continue\n");
    out.push_str("    fi\n");
    out.push_str("    local ntasks ave_cpu ave_rss max_rss tres_usage_in_ave\n");
    out.push_str("    ntasks=$(trim_whitespace \"${fields[1]}\")\n");
    out.push_str("    ave_cpu=$(trim_whitespace \"${fields[2]}\")\n");
    out.push_str("    ave_rss=$(trim_whitespace \"${fields[3]}\")\n");
    out.push_str("    max_rss=$(trim_whitespace \"${fields[4]}\")\n");
    out.push_str("    tres_usage_in_ave=$(trim_whitespace \"${fields[5]}\")\n");
    out.push_str("    printf '{\"sampled_at\":\"%s\",\"step_id\":%s,\"ntasks\":%s,\"ave_cpu\":%s,\"ave_rss\":%s,\"max_rss\":%s,\"alloc_tres\":null,\"tres_usage_in_ave\":%s}\\n' \\\n");
    out.push_str("      \"$(json_escape \"$sampled_at\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$step_id\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$ntasks\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$ave_cpu\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$ave_rss\")\" \\\n");
    out.push_str("      \"$(json_string_or_null \"$max_rss\")\" \\\n");
    out.push_str(
        "      \"$(json_string_or_null \"$tres_usage_in_ave\")\" >> \"$SLURM_METRICS_FILE\"\n",
    );
    out.push_str("  done <<< \"$output\"\n");
    out.push_str("  mark_slurm_collector_success \"$sampled_at\"\n");
    out.push_str("}\n\n");

    // CPU utilization collector. Reads /proc/stat (the aggregate `cpu` line)
    // and keeps the previous tick's counters in a per-node state file so the
    // sample function computes a non-idle/total delta without an extra sleep.
    // The first sample for a given state file has no prior counters and emits a
    // null `cpu_util_pct`. /proc/stat is Linux-only: a missing/unreadable path
    // marks the collector unavailable through warn-once diagnostics instead of
    // failing the job.
    out.push_str(cpu_emit_fn_body());
    out.push('\n');

    out.push_str("sample_cpu_metrics_current_node() {\n");
    out.push_str("  [[ \"$CPU_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  CPU_COLLECTOR_NOTE=\"\"\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    out.push_str("  local sample_node=\"${HPC_COMPOSE_CPU_SAMPLE_NODE:-${SLURMD_NODENAME:-${HOSTNAME:-}}}\"\n");
    out.push_str("  local stat_path=\"${HPC_COMPOSE_PROC_STAT_PATH:-/proc/stat}\"\n");
    out.push_str("  local loadavg_path=\"${HPC_COMPOSE_PROC_LOADAVG_PATH:-/proc/loadavg}\"\n");
    out.push_str("  local state_dir=\"$METRICS_DIR/cpu-state\"\n");
    out.push_str("  mkdir -p \"$state_dir\"\n");
    out.push_str("  if ! emit_cpu_sample_row \"$sampled_at\" \"$sample_node\" \"$stat_path\" \"$loadavg_path\" \"$state_dir/batch-node.state\" \"$CPU_METRICS_FILE\"; then\n");
    out.push_str(
        "    mark_cpu_collector_unavailable \"/proc/stat is not readable on this node\"\n",
    );
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  set_cpu_current_node_coverage\n");
    out.push_str("  mark_cpu_collector_success \"$sampled_at\"\n");
    out.push_str("}\n\n");

    // Per-node CPU sampler script fanned out through srun on multi-node jobs, so
    // cpu.jsonl rows carry a `node` field the way GPU rows do. Transient per-node
    // cpu.jsonl outputs land under `output_root` (cleared each tick); the delta
    // state files live under a separate persistent `state_root` so per-node
    // counters survive between ticks.
    out.push_str("write_cpu_sample_node_script() {\n");
    out.push_str("  local script_path=\"$METRICS_DIR/cpu-sample-node.sh\"\n");
    out.push_str("  cat > \"$script_path\" <<'HPC_COMPOSE_CPU_SAMPLE_NODE'\n");
    out.push_str("#!/bin/bash\n");
    out.push_str("set -euo pipefail\n");
    out.push_str("sampled_at=$1\n");
    out.push_str("output_root=$2\n");
    out.push_str("state_root=$3\n");
    out.push_str("node=\"${SLURMD_NODENAME:-${HOSTNAME:-}}\"\n");
    out.push_str("if [[ -z \"$node\" ]]; then node=$(hostname); fi\n");
    out.push_str("node_dir=\"$output_root/$node\"\n");
    out.push_str("state_dir=\"$state_root/$node\"\n");
    out.push_str("mkdir -p \"$node_dir\" \"$state_dir\"\n");
    out.push_str(cpu_node_json_helpers());
    out.push_str(cpu_emit_fn_body());
    out.push_str("emit_cpu_sample_row \"$sampled_at\" \"$node\" \"/proc/stat\" \"/proc/loadavg\" \"$state_dir/cpu.state\" \"$node_dir/cpu.jsonl\" || printf 'proc-stat unavailable on %s\\n' \"$node\" > \"$node_dir/status.txt\"\n");
    out.push_str("HPC_COMPOSE_CPU_SAMPLE_NODE\n");
    out.push_str("  chmod +x \"$script_path\"\n");
    out.push_str("  printf '%s' \"$script_path\"\n");
    out.push_str("}\n\n");

    out.push_str("sample_cpu_metrics_all_nodes() {\n");
    out.push_str("  [[ \"$CPU_COLLECTOR_ENABLED\" == \"1\" ]] || return 0\n");
    out.push_str("  local sampled_at\n");
    out.push_str("  sampled_at=$(metrics_timestamp)\n");
    // Transient per-node output is cleared each tick (like the GPU fanout); the
    // delta state root is kept so per-node counters persist between ticks.
    out.push_str("  local sample_root=\"$METRICS_DIR/cpu-node-samples\"\n");
    out.push_str("  local state_root=\"$METRICS_DIR/cpu-node-state\"\n");
    out.push_str("  rm -rf \"$sample_root\"\n");
    out.push_str("  mkdir -p \"$sample_root\" \"$state_root\"\n");
    out.push_str("  local script_path\n");
    out.push_str("  script_path=$(write_cpu_sample_node_script)\n");
    out.push_str("  if ! srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 --exact --overlap bash \"$script_path\" \"$sampled_at\" \"$sample_root\" \"$state_root\" >/dev/null 2>&1; then\n");
    // srun fanout failure does not kill the collector: the batch node can still
    // sample its own /proc/stat. Degrade to the single-node path and record it.
    out.push_str("    metrics_warning_once cpu \"multi-node CPU fanout failed through srun; sampling the batch node only\"\n");
    out.push_str("    sample_cpu_metrics_current_node\n");
    out.push_str("    local fallback_status=$?\n");
    out.push_str(
        "    if (( fallback_status == 0 )) && [[ \"$CPU_COLLECTOR_AVAILABLE\" == \"1\" ]]; then\n",
    );
    out.push_str(
        "      CPU_COLLECTOR_NOTE=\"multi-node CPU fanout degraded to batch-node sampling\"\n",
    );
    out.push_str("      CPU_COVERAGE_SCOPE=batch_node\n");
    out.push_str("      CPU_COVERAGE_OBSERVED_NODES=1\n");
    out.push_str("      CPU_COVERAGE_DEGRADED=1\n");
    out.push_str("      CPU_COVERAGE_REASON=\"multi-node CPU fanout failed through srun\"\n");
    out.push_str("      write_metrics_meta\n");
    out.push_str("    fi\n");
    out.push_str("    return \"$fallback_status\"\n");
    out.push_str("  fi\n");
    out.push_str("  shopt -s nullglob\n");
    out.push_str("  local cpu_files=(\"$sample_root\"/*/cpu.jsonl)\n");
    out.push_str("  local status_files=(\"$sample_root\"/*/status.txt)\n");
    out.push_str("  if (( ${#cpu_files[@]} == 0 )); then\n");
    out.push_str("    mark_cpu_collector_unavailable \"/proc/stat produced no CPU samples on allocation nodes\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  CPU_COVERAGE_SCOPE=allocation\n");
    out.push_str("  CPU_COVERAGE_OBSERVED_NODES=${#cpu_files[@]}\n");
    out.push_str("  CPU_COVERAGE_DEGRADED=0\n");
    out.push_str("  CPU_COVERAGE_REASON=\"\"\n");
    out.push_str("  CPU_COLLECTOR_NOTE=\"\"\n");
    out.push_str("  if (( CPU_COVERAGE_OBSERVED_NODES < CPU_COVERAGE_EXPECTED_NODES || ${#status_files[@]} > 0 )); then\n");
    out.push_str("    CPU_COVERAGE_DEGRADED=1\n");
    out.push_str("    CPU_COVERAGE_REASON=\"CPU samples covered ${CPU_COVERAGE_OBSERVED_NODES}/${CPU_COVERAGE_EXPECTED_NODES} allocation nodes\"\n");
    out.push_str("  fi\n");
    out.push_str("  cat \"${cpu_files[@]}\" >> \"$CPU_METRICS_FILE\"\n");
    out.push_str("  if (( ${#status_files[@]} > 0 )); then CPU_COLLECTOR_NOTE=\"$(paste -sd '; ' \"${status_files[@]}\" 2>/dev/null || true)\"; CPU_COVERAGE_REASON=\"$CPU_COLLECTOR_NOTE\"; fi\n");
    out.push_str("  mark_cpu_collector_success \"$sampled_at\"\n");
    out.push_str("}\n\n");

    out.push_str("sample_cpu_metrics() {\n");
    out.push_str("  if [[ \"${BACKEND:-slurm}\" == \"slurm\" && \"${HPC_COMPOSE_NODE_COUNT:-1}\" -gt 1 ]]; then\n");
    out.push_str("    sample_cpu_metrics_all_nodes\n");
    out.push_str("  else\n");
    out.push_str("    sample_cpu_metrics_current_node\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("write_metrics_diagnostics_node_script() {\n");
    out.push_str("  local script_path=\"$METRICS_DIR/diagnostics-node.sh\"\n");
    out.push_str("  cat > \"$script_path\" <<'HPC_COMPOSE_DIAGNOSTICS_NODE'\n");
    out.push_str("#!/bin/bash\n");
    out.push_str("set -euo pipefail\n");
    out.push_str("root=$1\n");
    out.push_str("node=\"${SLURMD_NODENAME:-${HOSTNAME:-}}\"\n");
    out.push_str("if [[ -z \"$node\" ]]; then node=$(hostname); fi\n");
    out.push_str("dir=\"$root/nodes/$node\"\n");
    out.push_str("mkdir -p \"$dir\"\n");
    out.push_str("env | sort | grep -E '^(NCCL|UCX|FI|CUDA|ROCR|HIP|OMPI|PMI|PMIX|I_MPI)_' > \"$dir/env.txt\" 2>/dev/null || true\n");
    out.push_str("if command -v nvidia-smi >/dev/null 2>&1; then nvidia-smi topo -m > \"$dir/nvidia-smi-topo.txt\" 2>&1 || true; nvidia-smi -q > \"$dir/nvidia-smi-q.txt\" 2>&1 || true; fi\n");
    out.push_str("if command -v ibstat >/dev/null 2>&1; then ibstat > \"$dir/ibstat.txt\" 2>&1 || true; fi\n");
    out.push_str("if command -v ibv_devinfo >/dev/null 2>&1; then ibv_devinfo > \"$dir/ibv_devinfo.txt\" 2>&1 || true; fi\n");
    out.push_str("if command -v ucx_info >/dev/null 2>&1; then ucx_info -v > \"$dir/ucx_info.txt\" 2>&1 || true; fi\n");
    out.push_str("if command -v fi_info >/dev/null 2>&1; then fi_info > \"$dir/fi_info.txt\" 2>&1 || true; fi\n");
    out.push_str("HPC_COMPOSE_DIAGNOSTICS_NODE\n");
    out.push_str("  chmod +x \"$script_path\"\n");
    out.push_str("  printf '%s' \"$script_path\"\n");
    out.push_str("}\n\n");

    out.push_str("capture_metrics_diagnostics() {\n");
    out.push_str("  mkdir -p \"$METRICS_DIAGNOSTICS_DIR\"\n");
    out.push_str("  local script_path\n");
    out.push_str("  script_path=$(write_metrics_diagnostics_node_script)\n");
    out.push_str("  if [[ \"${BACKEND:-slurm}\" == \"slurm\" && \"${HPC_COMPOSE_NODE_COUNT:-1}\" -gt 1 ]]; then\n");
    out.push_str("    srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 --exact --overlap bash \"$script_path\" \"$METRICS_DIAGNOSTICS_DIR\" >/dev/null 2>&1 || true\n");
    out.push_str("  else\n");
    out.push_str(
        "    bash \"$script_path\" \"$METRICS_DIAGNOSTICS_DIR\" >/dev/null 2>&1 || true\n",
    );
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("sample_metrics_once() {\n");
    out.push_str("  sample_gpu_metrics\n");
    out.push_str("  sample_slurm_metrics\n");
    out.push_str("  sample_cpu_metrics\n");
    out.push_str("}\n\n");

    out.push_str("metrics_sampler_loop() {\n");
    out.push_str("  while true; do\n");
    out.push_str("    sleep \"$METRICS_INTERVAL_SECONDS\"\n");
    out.push_str("    sample_metrics_once\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("start_metrics_sampler() {\n");
    out.push_str("  mkdir -p \"$METRICS_DIR\"\n");
    out.push_str("  GPU_COVERAGE_EXPECTED_NODES=${HPC_COMPOSE_NODE_COUNT:-1}\n");
    out.push_str("  CPU_COVERAGE_EXPECTED_NODES=${HPC_COMPOSE_NODE_COUNT:-1}\n");
    out.push_str("  if (( GPU_COVERAGE_EXPECTED_NODES > 1 )); then GPU_COVERAGE_DEGRADED=1; GPU_COVERAGE_REASON=\"no successful GPU sample yet\"; fi\n");
    out.push_str("  if (( CPU_COVERAGE_EXPECTED_NODES > 1 )); then CPU_COVERAGE_DEGRADED=1; CPU_COVERAGE_REASON=\"no successful CPU sample yet\"; fi\n");
    out.push_str("  : > \"$GPU_METRICS_FILE\"\n");
    out.push_str("  : > \"$GPU_PROCESSES_FILE\"\n");
    out.push_str("  : > \"$SLURM_METRICS_FILE\"\n");
    out.push_str("  : > \"$CPU_METRICS_FILE\"\n");
    out.push_str("  : > \"$STEP_MAP_FILE\"\n");
    out.push_str("  capture_metrics_diagnostics\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("  sample_metrics_once\n");
    out.push_str("  metrics_sampler_loop &\n");
    out.push_str("  SAMPLER_PID=$!\n");
    out.push_str("  write_metrics_meta\n");
    out.push_str("}\n\n");

    // Capture the window between the last periodic tick and job end with one
    // extra synchronous sample. The sample runs in the background so the wait
    // can be bounded: a hung nvidia-smi/sstat must never delay job teardown.
    // The generated script does not otherwise assume coreutils `timeout`, so
    // the ~10s budget is enforced with a portable kill-after-deadline loop.
    out.push_str("final_metrics_sample() {\n");
    out.push_str("  local budget_seconds=10\n");
    out.push_str("  sample_metrics_once &\n");
    out.push_str("  local sample_pid=$!\n");
    out.push_str("  local start\n");
    out.push_str("  start=$(date +%s)\n");
    out.push_str("  while kill -0 \"$sample_pid\" 2>/dev/null; do\n");
    out.push_str("    if (( $(date +%s) - start >= budget_seconds )); then\n");
    out.push_str("      kill \"$sample_pid\" 2>/dev/null || true\n");
    out.push_str("      break\n");
    out.push_str("    fi\n");
    out.push_str("    sleep 1\n");
    out.push_str("  done\n");
    out.push_str("  wait \"$sample_pid\" 2>/dev/null || true\n");
    out.push_str("}\n\n");

    out.push_str("stop_metrics_sampler() {\n");
    out.push_str("  [[ -n \"$SAMPLER_PID\" ]] || return 0\n");
    // Flush a final sample before tearing the loop down so cleanup only
    // proceeds once the last-tick..job-end window has been recorded.
    out.push_str("  final_metrics_sample\n");
    out.push_str("  if kill -0 \"$SAMPLER_PID\" 2>/dev/null; then\n");
    out.push_str("    kill \"$SAMPLER_PID\" 2>/dev/null || true\n");
    out.push_str("    wait \"$SAMPLER_PID\" 2>/dev/null || true\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
}

/// Bash body of `emit_cpu_sample_row`, shared verbatim between the in-process
/// helpers and the self-contained per-node fanout script.
///
/// Reads the aggregate `cpu` line from `stat_path`, counts per-core lines for
/// `core_count`, computes the non-idle/total delta against the counters stored
/// in `state_file` (empty util on the first sample for that state file), reads
/// the 1-minute load from `loadavg_path`, then appends one JSON row to
/// `output_file`. Returns non-zero when `stat_path` is unreadable or has no
/// aggregate line so callers can mark the collector unavailable.
fn cpu_emit_fn_body() -> &'static str {
    r##"emit_cpu_sample_row() {
  local sampled_at=$1
  local node=$2
  local stat_path=$3
  local loadavg_path=$4
  local state_file=$5
  local output_file=$6
  [[ -r "$stat_path" ]] || return 1
  local cpu_line=""
  local cores=0
  local line
  while IFS= read -r line; do
    case "$line" in
      "cpu "*) if [[ -z "$cpu_line" ]]; then cpu_line=$line; fi ;;
      cpu[0-9]*) cores=$((cores + 1)) ;;
    esac
  done < "$stat_path"
  [[ -n "$cpu_line" ]] || return 1
  local -a fields=()
  read -r -a fields <<< "$cpu_line"
  local user=${fields[1]:-0}
  local nice_time=${fields[2]:-0}
  local system=${fields[3]:-0}
  local idle=${fields[4]:-0}
  local iowait=${fields[5]:-0}
  local irq=${fields[6]:-0}
  local softirq=${fields[7]:-0}
  local steal=${fields[8]:-0}
  local idle_all=$((idle + iowait))
  local non_idle=$((user + nice_time + system + irq + softirq + steal))
  local total=$((idle_all + non_idle))
  local util=""
  if [[ -r "$state_file" ]]; then
    local prev_total=0
    local prev_idle=0
    read -r prev_total prev_idle < "$state_file" || true
    local dt=$((total - ${prev_total:-0}))
    local di=$((idle_all - ${prev_idle:-0}))
    if (( dt > 0 )); then
      util=$(LC_ALL=C awk -v dt="$dt" -v di="$di" 'BEGIN { u = (dt - di) * 100.0 / dt; if (u < 0) u = 0; if (u > 100) u = 100; printf "%.1f", u }')
    fi
  fi
  printf '%s %s\n' "$total" "$idle_all" > "$state_file"
  local load=""
  if [[ -r "$loadavg_path" ]]; then
    read -r load _ < "$loadavg_path" || load=""
  fi
  printf '{"sampled_at":"%s","node":%s,"cpu_util_pct":%s,"core_count":%s,"loadavg_1m":%s}\n' \
    "$(json_escape "$sampled_at")" \
    "$(json_string_or_null "$node")" \
    "$(json_number_or_null "$util")" \
    "$(json_number_or_null "$cores")" \
    "$(json_number_or_null "$load")" >> "$output_file"
  return 0
}
"##
}

/// Bash bodies of `gpu_process_cgroup` and `gpu_process_environ_value`, shared
/// verbatim between the in-process GPU sampler and the self-contained per-node
/// fanout script.
///
/// Both helpers are strictly best-effort: they print the raw value when it is
/// readable and print nothing otherwise, and they always return 0 so a failed
/// probe can never break a sampler tick (the fanout script runs under
/// `set -euo pipefail`). `gpu_process_cgroup` captures the raw
/// `/proc/<pid>/cgroup` content with newlines condensed to `;` — parsing the
/// Slurm job/step out of it happens in Rust post-processing, where the cgroup
/// v1/v2 layouts are unit-tested against fixtures. `gpu_process_environ_value`
/// extracts one variable from `/proc/<pid>/environ` (same-user processes only,
/// which covers every process this job can own).
fn gpu_attribution_helpers_body() -> &'static str {
    r##"gpu_process_cgroup() {
  local pid=${1-}
  if [[ -n "$pid" && -r "/proc/$pid/cgroup" ]]; then
    tr '\n' ';' < "/proc/$pid/cgroup" 2>/dev/null || true
  fi
  return 0
}
gpu_process_environ_value() {
  local pid=${1-}
  local name=${2-}
  if [[ -z "$pid" || -z "$name" || ! -r "/proc/$pid/environ" ]]; then
    return 0
  fi
  local entry
  entry=$( (tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -m1 "^$name=") 2>/dev/null || true)
  if [[ -n "$entry" ]]; then
    printf '%s' "${entry#*=}"
  fi
  return 0
}
"##
}

/// JSON escaping helpers embedded verbatim in the self-contained per-node CPU
/// fanout script (which runs in a fresh `bash` and cannot see the launcher's
/// helper definitions).
fn cpu_node_json_helpers() -> &'static str {
    r##"json_escape() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  printf '%s' "$value"
}
json_string_or_null() {
  local value=${1-}
  if [[ -z "$value" ]]; then printf null; else printf '"%s"' "$(json_escape "$value")"; fi
}
json_number_or_null() {
  local value=${1-}
  if [[ -z "$value" ]]; then printf null; else printf '%s' "$value"; fi
}
"##
}
