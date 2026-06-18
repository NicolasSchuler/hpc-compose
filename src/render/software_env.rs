use super::shell_quote;
use crate::spec::SoftwareEnvConfig;

pub(super) fn render_software_env_helpers(out: &mut String) {
    out.push_str("hpc_compose_module() {\n");
    out.push_str("  if command -v module >/dev/null 2>&1; then\n");
    out.push_str("    module \"$@\"\n");
    out.push_str("    return $?\n");
    out.push_str("  fi\n");
    out.push_str("  if [[ -f /etc/profile.d/modules.sh ]]; then\n");
    out.push_str("    # shellcheck disable=SC1091\n");
    out.push_str("    source /etc/profile.d/modules.sh\n");
    out.push_str("    module \"$@\"\n");
    out.push_str("    return $?\n");
    out.push_str("  fi\n");
    out.push_str(
        "  echo \"environment modules are requested but the module command is unavailable\" >&2\n",
    );
    out.push_str("  return 127\n");
    out.push_str("}\n\n");
}

pub(super) fn render_apply_software_env(out: &mut String, env: &SoftwareEnvConfig, indent: &str) {
    if env.modules.purge {
        out.push_str(indent);
        out.push_str("hpc_compose_module purge\n");
    }
    for module in &env.modules.load {
        out.push_str(indent);
        out.push_str("hpc_compose_module load ");
        out.push_str(&shell_quote(module));
        out.push('\n');
    }
    if let Some(spack) = &env.spack {
        let view = shell_quote(&spack.view);
        out.push_str(indent);
        out.push_str(&format!(
            "if [[ -d {view}/bin ]]; then export PATH={view}/bin:\"$PATH\"; fi\n"
        ));
        out.push_str(indent);
        out.push_str(&format!("if [[ -d {view}/lib ]]; then export LD_LIBRARY_PATH={view}/lib:\"${{LD_LIBRARY_PATH:-}}\"; fi\n"));
        out.push_str(indent);
        out.push_str(&format!("if [[ -d {view}/lib64 ]]; then export LD_LIBRARY_PATH={view}/lib64:\"${{LD_LIBRARY_PATH:-}}\"; fi\n"));
        out.push_str(indent);
        out.push_str(&format!("for hpc_compose_py_site in {view}/lib/python*/site-packages {view}/lib64/python*/site-packages; do if [[ -d \"$hpc_compose_py_site\" ]]; then export PYTHONPATH=\"$hpc_compose_py_site:${{PYTHONPATH:-}}\"; fi; done\n"));
    }
    for (key, value) in &env.env {
        out.push_str(indent);
        out.push_str("export ");
        out.push_str(key);
        out.push('=');
        out.push_str(&shell_quote(value));
        out.push('\n');
    }
}

pub(super) fn software_env_export_names(
    global: &SoftwareEnvConfig,
    service: &SoftwareEnvConfig,
) -> Vec<String> {
    let mut names = global.env.keys().cloned().collect::<Vec<_>>();
    names.extend(service.env.keys().cloned());
    names.sort();
    names.dedup();
    names
}

pub(super) fn effective_software_env_pairs(
    global: &SoftwareEnvConfig,
    service: &SoftwareEnvConfig,
) -> Vec<String> {
    let mut env = global.env.clone();
    env.extend(service.env.clone());
    env.into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}
