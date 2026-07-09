use super::bash_array_literal;
use crate::cache::dataset::{HfArtifactRef, render_hf_stage_command, staged_input_dir};
use crate::runtime_plan::RuntimePlan;
use crate::spec::{StageInConfig, StageMode, StageOutWhen};

/// Whether any stage-in entry stages a `hf://` HuggingFace source.
pub(super) fn has_hf_stage_in(plan: &RuntimePlan) -> bool {
    plan.slurm.stage_in.iter().any(|entry| entry.hf.is_some())
}

pub(super) fn render_stage_helpers(out: &mut String, plan: &RuntimePlan) {
    // Filesystem-path stage-in entries only; `hf://` sources stage via a
    // separate cluster-side download step (see `render_hf_stage_in`).
    let path_entries: Vec<&StageInConfig> = plan
        .slurm
        .stage_in
        .iter()
        .filter(|entry| entry.from.is_some())
        .collect();
    let stage_in_from = path_entries
        .iter()
        .map(|entry| entry.from.clone().unwrap_or_default())
        .collect::<Vec<_>>();
    let stage_in_to = path_entries
        .iter()
        .map(|entry| entry.to.clone())
        .collect::<Vec<_>>();
    let stage_in_modes = path_entries
        .iter()
        .map(|entry| stage_mode_label(entry.mode).to_string())
        .collect::<Vec<_>>();
    let stage_out_from = plan
        .slurm
        .stage_out
        .iter()
        .map(|entry| entry.from.clone())
        .collect::<Vec<_>>();
    let stage_out_to = plan
        .slurm
        .stage_out
        .iter()
        .map(|entry| entry.to.clone())
        .collect::<Vec<_>>();
    let stage_out_modes = plan
        .slurm
        .stage_out
        .iter()
        .map(|entry| stage_mode_label(entry.mode).to_string())
        .collect::<Vec<_>>();
    let stage_out_when = plan
        .slurm
        .stage_out
        .iter()
        .map(|entry| stage_out_when_label(entry.when).to_string())
        .collect::<Vec<_>>();

    out.push_str(&format!(
        "STAGE_IN_FROM={}\n",
        bash_array_literal(&stage_in_from)
    ));
    out.push_str(&format!(
        "STAGE_IN_TO={}\n",
        bash_array_literal(&stage_in_to)
    ));
    out.push_str(&format!(
        "STAGE_IN_MODES={}\n",
        bash_array_literal(&stage_in_modes)
    ));
    out.push_str(&format!(
        "STAGE_OUT_FROM={}\n",
        bash_array_literal(&stage_out_from)
    ));
    out.push_str(&format!(
        "STAGE_OUT_TO={}\n",
        bash_array_literal(&stage_out_to)
    ));
    out.push_str(&format!(
        "STAGE_OUT_MODES={}\n",
        bash_array_literal(&stage_out_modes)
    ));
    out.push_str(&format!(
        "STAGE_OUT_WHEN={}\n\n",
        bash_array_literal(&stage_out_when)
    ));

    out.push_str("scratch_host_path_for() {\n");
    out.push_str("  local path=$1\n");
    out.push_str("  if [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\" ]]; then\n");
    out.push_str("    printf '%s' \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  elif [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\"/* ]]; then\n");
    out.push_str(
        "    printf '%s/%s' \"$SCRATCH_HOST_PATH\" \"${path#\"$SCRATCH_CONTAINER_PATH\"/}\"\n",
    );
    out.push_str("  else\n");
    out.push_str("    printf '%s' \"$path\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("stage_copy_path() {\n");
    out.push_str("  local from=$1\n");
    out.push_str("  local to=$2\n");
    out.push_str("  local mode=$3\n");
    out.push_str("  mkdir -p \"$(dirname \"$to\")\"\n");
    out.push_str("  if [[ \"$mode\" == \"rsync\" ]]; then\n");
    out.push_str("    if command -v rsync >/dev/null 2>&1; then\n");
    out.push_str("      rsync -a \"$from\" \"$to\"\n");
    out.push_str("    else\n");
    out.push_str("      cp -R \"$from\" \"$to\"\n");
    out.push_str("    fi\n");
    out.push_str("  else\n");
    out.push_str("    cp -R \"$from\" \"$to\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("scratch_requires_node_fanout() {\n");
    out.push_str("  [[ \"${SCRATCH_SCOPE:-}\" == \"node_local\" ]] || return 1\n");
    out.push_str("  [[ \"${BACKEND:-slurm}\" == \"slurm\" ]] || return 1\n");
    out.push_str("  (( ${HPC_COMPOSE_NODE_COUNT:-1} > 1 ))\n");
    out.push_str("}\n\n");

    out.push_str("run_scratch_command_on_each_node() {\n");
    out.push_str("  local command=$1\n");
    out.push_str("  srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash -lc \"$command\" bash \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("}\n\n");

    out.push_str("init_scratch() {\n");
    out.push_str("  [[ -n \"${SCRATCH_HOST_PATH:-}\" ]] || return 0\n");
    out.push_str("  mkdir -p \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  if scratch_requires_node_fanout; then\n");
    out.push_str("    run_scratch_command_on_each_node 'mkdir -p \"$1\"'\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("stage_in_paths_on_current_node() {\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!STAGE_IN_FROM[@]}\"; do\n");
    out.push_str("    local from=${STAGE_IN_FROM[i]}\n");
    out.push_str("    local to\n");
    out.push_str("    to=$(scratch_host_path_for \"${STAGE_IN_TO[i]}\")\n");
    out.push_str("    echo \"Staging in $from -> $to\"\n");
    out.push_str("    stage_copy_path \"$from\" \"$to\" \"${STAGE_IN_MODES[i]}\"\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("write_stage_in_node_script() {\n");
    out.push_str("  local script_path=\"$JOB_TMP/stage-in-node.sh\"\n");
    out.push_str("  cat > \"$script_path\" <<'HPC_COMPOSE_STAGE_IN_NODE'\n");
    out.push_str("#!/bin/bash\n");
    out.push_str("set -euo pipefail\n");
    out.push_str("SCRATCH_CONTAINER_PATH=$1\n");
    out.push_str("SCRATCH_HOST_PATH=$2\n");
    out.push_str(&format!(
        "STAGE_IN_FROM={}\n",
        bash_array_literal(&stage_in_from)
    ));
    out.push_str(&format!(
        "STAGE_IN_TO={}\n",
        bash_array_literal(&stage_in_to)
    ));
    out.push_str(&format!(
        "STAGE_IN_MODES={}\n\n",
        bash_array_literal(&stage_in_modes)
    ));
    out.push_str("scratch_host_path_for() {\n");
    out.push_str("  local path=$1\n");
    out.push_str("  if [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\" ]]; then\n");
    out.push_str("    printf '%s' \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  elif [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\"/* ]]; then\n");
    out.push_str(
        "    printf '%s/%s' \"$SCRATCH_HOST_PATH\" \"${path#\"$SCRATCH_CONTAINER_PATH\"/}\"\n",
    );
    out.push_str("  else\n");
    out.push_str("    printf '%s' \"$path\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
    out.push_str("stage_copy_path() {\n");
    out.push_str("  local from=$1\n");
    out.push_str("  local to=$2\n");
    out.push_str("  local mode=$3\n");
    out.push_str("  mkdir -p \"$(dirname \"$to\")\"\n");
    out.push_str("  if [[ \"$mode\" == \"rsync\" ]]; then\n");
    out.push_str("    if command -v rsync >/dev/null 2>&1; then\n");
    out.push_str("      rsync -a \"$from\" \"$to\"\n");
    out.push_str("    else\n");
    out.push_str("      cp -R \"$from\" \"$to\"\n");
    out.push_str("    fi\n");
    out.push_str("  else\n");
    out.push_str("    cp -R \"$from\" \"$to\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
    out.push_str("stage_in_paths_on_current_node() {\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!STAGE_IN_FROM[@]}\"; do\n");
    out.push_str("    local from=${STAGE_IN_FROM[i]}\n");
    out.push_str("    local to\n");
    out.push_str("    to=$(scratch_host_path_for \"${STAGE_IN_TO[i]}\")\n");
    out.push_str("    echo \"Staging in $from -> $to\"\n");
    out.push_str("    stage_copy_path \"$from\" \"$to\" \"${STAGE_IN_MODES[i]}\"\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");
    out.push_str("stage_in_paths_on_current_node\n");
    out.push_str("HPC_COMPOSE_STAGE_IN_NODE\n");
    out.push_str("  chmod +x \"$script_path\"\n");
    out.push_str("  printf '%s' \"$script_path\"\n");
    out.push_str("}\n\n");

    out.push_str("stage_in_paths() {\n");
    out.push_str("  (( ${#STAGE_IN_FROM[@]} > 0 )) || return 0\n");
    out.push_str("  if scratch_requires_node_fanout; then\n");
    out.push_str("    local stage_in_node_script\n");
    out.push_str("    stage_in_node_script=$(write_stage_in_node_script)\n");
    out.push_str("    srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash \"$stage_in_node_script\" \"$SCRATCH_CONTAINER_PATH\" \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  else\n");
    out.push_str("    stage_in_paths_on_current_node\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("stage_out_paths_on_current_node() {\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  local outcome=success\n");
    out.push_str("  (( exit_code != 0 )) && outcome=failure\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!STAGE_OUT_FROM[@]}\"; do\n");
    out.push_str("    local when=${STAGE_OUT_WHEN[i]}\n");
    out.push_str("    if [[ \"$when\" == \"on_success\" && \"$outcome\" != \"success\" ]]; then continue; fi\n");
    out.push_str("    if [[ \"$when\" == \"on_failure\" && \"$outcome\" != \"failure\" ]]; then continue; fi\n");
    out.push_str("    local from\n");
    out.push_str("    from=$(scratch_host_path_for \"${STAGE_OUT_FROM[i]}\")\n");
    out.push_str("    local to=${STAGE_OUT_TO[i]}\n");
    out.push_str("    echo \"Staging out $from -> $to\"\n");
    out.push_str("    stage_copy_path \"$from\" \"$to\" \"${STAGE_OUT_MODES[i]}\"\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");

    out.push_str("write_stage_out_node_script() {\n");
    out.push_str("  local script_path=\"$JOB_TMP/stage-out-node.sh\"\n");
    out.push_str("  cat > \"$script_path\" <<'HPC_COMPOSE_STAGE_OUT_NODE'\n");
    out.push_str("#!/bin/bash\n");
    out.push_str("set -euo pipefail\n");
    out.push_str("exit_code=${1:-0}\n");
    out.push_str("SCRATCH_CONTAINER_PATH=$2\n");
    out.push_str("SCRATCH_HOST_PATH=$3\n");
    out.push_str(&format!(
        "STAGE_OUT_FROM={}\n",
        bash_array_literal(&stage_out_from)
    ));
    out.push_str(&format!(
        "STAGE_OUT_TO={}\n",
        bash_array_literal(&stage_out_to)
    ));
    out.push_str(&format!(
        "STAGE_OUT_MODES={}\n",
        bash_array_literal(&stage_out_modes)
    ));
    out.push_str(&format!(
        "STAGE_OUT_WHEN={}\n\n",
        bash_array_literal(&stage_out_when)
    ));
    out.push_str("scratch_host_path_for() {\n");
    out.push_str("  local path=$1\n");
    out.push_str("  if [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\" ]]; then\n");
    out.push_str("    printf '%s' \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  elif [[ -n \"${SCRATCH_CONTAINER_PATH:-}\" && -n \"${SCRATCH_HOST_PATH:-}\" && \"$path\" == \"$SCRATCH_CONTAINER_PATH\"/* ]]; then\n");
    out.push_str(
        "    printf '%s/%s' \"$SCRATCH_HOST_PATH\" \"${path#\"$SCRATCH_CONTAINER_PATH\"/}\"\n",
    );
    out.push_str("  else\n");
    out.push_str("    printf '%s' \"$path\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
    out.push_str("stage_copy_path() {\n");
    out.push_str("  local from=$1\n");
    out.push_str("  local to=$2\n");
    out.push_str("  local mode=$3\n");
    out.push_str("  mkdir -p \"$(dirname \"$to\")\"\n");
    out.push_str("  if [[ \"$mode\" == \"rsync\" ]]; then\n");
    out.push_str("    if command -v rsync >/dev/null 2>&1; then\n");
    out.push_str("      rsync -a \"$from\" \"$to\"\n");
    out.push_str("    else\n");
    out.push_str("      cp -R \"$from\" \"$to\"\n");
    out.push_str("    fi\n");
    out.push_str("  else\n");
    out.push_str("    cp -R \"$from\" \"$to\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
    out.push_str("stage_out_paths_on_current_node() {\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  local outcome=success\n");
    out.push_str("  (( exit_code != 0 )) && outcome=failure\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!STAGE_OUT_FROM[@]}\"; do\n");
    out.push_str("    local when=${STAGE_OUT_WHEN[i]}\n");
    out.push_str("    if [[ \"$when\" == \"on_success\" && \"$outcome\" != \"success\" ]]; then continue; fi\n");
    out.push_str("    if [[ \"$when\" == \"on_failure\" && \"$outcome\" != \"failure\" ]]; then continue; fi\n");
    out.push_str("    local from\n");
    out.push_str("    from=$(scratch_host_path_for \"${STAGE_OUT_FROM[i]}\")\n");
    out.push_str("    local to=${STAGE_OUT_TO[i]}\n");
    out.push_str("    echo \"Staging out $from -> $to\"\n");
    out.push_str("    stage_copy_path \"$from\" \"$to\" \"${STAGE_OUT_MODES[i]}\"\n");
    out.push_str("  done\n");
    out.push_str("}\n\n");
    out.push_str("stage_out_paths_on_current_node \"$exit_code\"\n");
    out.push_str("HPC_COMPOSE_STAGE_OUT_NODE\n");
    out.push_str("  chmod +x \"$script_path\"\n");
    out.push_str("  printf '%s' \"$script_path\"\n");
    out.push_str("}\n\n");

    out.push_str("stage_out_paths() {\n");
    out.push_str("  (( ${#STAGE_OUT_FROM[@]} > 0 )) || return 0\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  if scratch_requires_node_fanout; then\n");
    out.push_str("    local stage_out_node_script\n");
    out.push_str("    stage_out_node_script=$(write_stage_out_node_script)\n");
    out.push_str("    srun --nodes=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks=\"$HPC_COMPOSE_NODE_COUNT\" --ntasks-per-node=1 bash \"$stage_out_node_script\" \"$exit_code\" \"$SCRATCH_CONTAINER_PATH\" \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  else\n");
    out.push_str("    stage_out_paths_on_current_node \"$exit_code\"\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");

    out.push_str("cleanup_scratch() {\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  case \"$SCRATCH_CLEANUP_POLICY\" in\n");
    out.push_str("    never) return 0 ;;\n");
    out.push_str("    on_success) (( exit_code == 0 )) || return 0 ;;\n");
    out.push_str("  esac\n");
    out.push_str("  rm -rf \"$SCRATCH_HOST_PATH\"\n");
    out.push_str("  if scratch_requires_node_fanout; then\n");
    out.push_str("    run_scratch_command_on_each_node 'rm -rf \"$1\"'\n");
    out.push_str("  fi\n");
    out.push_str("}\n\n");
}

/// Emits the cluster-side HuggingFace download steps for every `hf://`
/// stage-in entry, fetching into each entry's content-addressed directory under
/// the shared cache dir. The download runs INSIDE the Slurm allocation; the
/// rendered script contains a guarded `huggingface-cli download` line and never
/// a literal `hf://` mount argument.
pub(super) fn render_hf_stage_in(out: &mut String, plan: &RuntimePlan, huggingface_cli_bin: &str) {
    let hf_entries: Vec<&StageInConfig> = plan
        .slurm
        .stage_in
        .iter()
        .filter(|entry| entry.hf.is_some())
        .collect();
    if hf_entries.is_empty() {
        return;
    }

    out.push_str("# Stage in HuggingFace artifacts (downloaded inside the allocation).\n");
    out.push_str("stage_in_huggingface_artifacts() {\n");
    for entry in hf_entries {
        let hf = entry.hf.as_ref().expect("filtered to hf entries");
        let kind = hf.as_staged_input_kind();
        let spec = hf.uri();
        let staged_spec =
            crate::cache::dataset::StagedInputSpec::new(kind, spec, Some(hf.revision.clone()));
        let key = crate::cache::dataset::dataset_cache_key(&staged_spec);
        let cas_dir = staged_input_dir(&plan.cache_dir, kind, &key);
        let reference = HfArtifactRef {
            repo: hf.repo.clone(),
            revision: hf.revision.clone(),
            kind,
        };
        let command =
            render_hf_stage_command(&reference, &cas_dir.to_string_lossy(), huggingface_cli_bin);
        for line in command.lines() {
            out.push_str("  ");
            out.push_str(line);
            out.push('\n');
        }
        // Materialize the staged artifact into the in-job destination so the
        // service sees it at the spec'd `to` path, reusing the path-copy helper.
        let to = shell_single_quote(&entry.to);
        out.push_str("  local hf_stage_to\n");
        out.push_str(&format!("  hf_stage_to=$(scratch_host_path_for {to})\n"));
        out.push_str("  stage_copy_path \"$HF_STAGE_TARGET\"/. \"$hf_stage_to\" copy\n");
    }
    out.push_str("}\n\n");
}

/// Single-quotes a value for safe embedding in the rendered shell step.
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn stage_mode_label(mode: StageMode) -> &'static str {
    match mode {
        StageMode::Rsync => "rsync",
        StageMode::Copy => "copy",
    }
}

fn stage_out_when_label(when: StageOutWhen) -> &'static str {
    match when {
        StageOutWhen::Always => "always",
        StageOutWhen::OnSuccess => "on_success",
        StageOutWhen::OnFailure => "on_failure",
    }
}
