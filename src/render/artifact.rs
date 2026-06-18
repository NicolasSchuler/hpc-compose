pub(super) fn render_artifact_helpers(out: &mut String) {
    out.push_str("artifact_timestamp() {\n");
    out.push_str("  date -u +%Y-%m-%dT%H:%M:%SZ\n");
    out.push_str("}\n\n");

    out.push_str("write_json_string_array() {\n");
    out.push_str("  local label=$1\n");
    out.push_str("  shift\n");
    out.push_str("  printf '  \"%s\": [' \"$label\"\n");
    out.push_str("  local first=1\n");
    out.push_str("  local item\n");
    out.push_str("  for item in \"$@\"; do\n");
    out.push_str("    if (( first == 0 )); then\n");
    out.push_str("      printf ','\n");
    out.push_str("    fi\n");
    out.push_str("    printf '\\n    \"%s\"' \"$(json_escape \"$item\")\"\n");
    out.push_str("    first=0\n");
    out.push_str("  done\n");
    out.push_str("  if (( first == 0 )); then\n");
    out.push_str("    printf '\\n'\n");
    out.push_str("  fi\n");
    out.push_str("  printf '  ]'\n");
    out.push_str("}\n\n");

    out.push_str("write_bundle_pattern_array() {\n");
    out.push_str("  local bundle=$1\n");
    out.push_str("  local label=$2\n");
    out.push_str("  printf '      \"%s\": [' \"$label\"\n");
    out.push_str("  local first=1\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!ARTIFACT_SOURCE_PATTERNS[@]}\"; do\n");
    out.push_str("    [[ \"${ARTIFACT_PATTERN_BUNDLES[i]}\" == \"$bundle\" ]] || continue\n");
    out.push_str("    if (( first == 0 )); then\n");
    out.push_str("      printf ','\n");
    out.push_str("    fi\n");
    out.push_str(
        "    printf '\\n        \"%s\"' \"$(json_escape \"${ARTIFACT_SOURCE_PATTERNS[i]}\")\"\n",
    );
    out.push_str("    first=0\n");
    out.push_str("  done\n");
    out.push_str("  if (( first == 0 )); then\n");
    out.push_str("    printf '\\n'\n");
    out.push_str("  fi\n");
    out.push_str("  printf '      ]'\n");
    out.push_str("}\n\n");

    out.push_str("write_bundle_record_array() {\n");
    out.push_str("  local bundle=$1\n");
    out.push_str("  local label=$2\n");
    out.push_str("  shift 2\n");
    out.push_str("  printf '      \"%s\": [' \"$label\"\n");
    out.push_str("  local first=1\n");
    out.push_str("  local record\n");
    out.push_str("  local record_bundle\n");
    out.push_str("  local record_value\n");
    out.push_str("  for record in \"$@\"; do\n");
    out.push_str("    record_bundle=${record%%$'\\t'*}\n");
    out.push_str("    record_value=${record#*$'\\t'}\n");
    out.push_str("    [[ \"$record_bundle\" == \"$bundle\" ]] || continue\n");
    out.push_str("    if (( first == 0 )); then\n");
    out.push_str("      printf ','\n");
    out.push_str("    fi\n");
    out.push_str("    printf '\\n        \"%s\"' \"$(json_escape \"$record_value\")\"\n");
    out.push_str("    first=0\n");
    out.push_str("  done\n");
    out.push_str("  if (( first == 0 )); then\n");
    out.push_str("    printf '\\n'\n");
    out.push_str("  fi\n");
    out.push_str("  printf '      ]'\n");
    out.push_str("}\n\n");

    out.push_str("write_artifact_bundles_json() {\n");
    out.push_str("  local first=1\n");
    out.push_str("  local bundle\n");
    out.push_str("  printf '  \"bundles\": {'\n");
    out.push_str("  for bundle in \"${ARTIFACT_BUNDLE_NAMES[@]}\"; do\n");
    out.push_str("    if (( first == 0 )); then\n");
    out.push_str("      printf ','\n");
    out.push_str("    fi\n");
    out.push_str("    printf '\\n    \"%s\": {\\n' \"$(json_escape \"$bundle\")\"\n");
    out.push_str("    write_bundle_pattern_array \"$bundle\" \"declared_source_patterns\"\n");
    out.push_str("    printf ',\\n'\n");
    out.push_str(
        "    write_bundle_record_array \"$bundle\" \"matched_source_paths\" \"${ARTIFACT_BUNDLE_MATCH_RECORDS[@]}\"\n",
    );
    out.push_str("    printf ',\\n'\n");
    out.push_str(
        "    write_bundle_record_array \"$bundle\" \"copied_relative_paths\" \"${ARTIFACT_BUNDLE_COPIED_RECORDS[@]}\"\n",
    );
    out.push_str("    printf ',\\n'\n");
    out.push_str(
        "    write_bundle_record_array \"$bundle\" \"warnings\" \"${ARTIFACT_BUNDLE_WARNING_RECORDS[@]}\"\n",
    );
    out.push_str("    printf '\\n    }'\n");
    out.push_str("    first=0\n");
    out.push_str("  done\n");
    out.push_str("  if (( first == 0 )); then\n");
    out.push_str("    printf '\\n'\n");
    out.push_str("  fi\n");
    out.push_str("  printf '  }'\n");
    out.push_str("}\n\n");

    out.push_str("write_artifact_manifest() {\n");
    out.push_str("  local job_outcome=$1\n");
    out.push_str("  shift\n");
    out.push_str("  local -a matched_source_paths=(\"$@\")\n");
    out.push_str("  local -a copied_relative_paths=(\"${ARTIFACT_COPIED_RELATIVE_PATHS[@]}\")\n");
    out.push_str("  local -a warnings=(\"${ARTIFACT_WARNINGS[@]}\")\n");
    out.push_str("  local tmp_manifest=\"$ARTIFACTS_MANIFEST_FILE.tmp\"\n");
    out.push_str("  {\n");
    out.push_str("    printf '{\\n'\n");
    out.push_str("    printf '  \"schema_version\": 3,\\n'\n");
    out.push_str("    printf '  \"job_id\": \"%s\",\\n' \"$(json_escape \"$SLURM_JOB_ID\")\"\n");
    out.push_str("    printf '  \"collect_policy\": \"%s\",\\n' \"$(json_escape \"$ARTIFACTS_COLLECT_POLICY\")\"\n");
    out.push_str("    printf '  \"collected_at\": \"%s\",\\n' \"$(json_escape \"$(artifact_timestamp)\")\"\n");
    out.push_str(
        "    printf '  \"job_outcome\": \"%s\",\\n' \"$(json_escape \"$job_outcome\")\"\n",
    );
    out.push_str("    if [[ \"$RESUME_ENABLED\" == \"1\" ]]; then\n");
    out.push_str("      printf '  \"attempt\": %s,\\n' \"$ATTEMPT\"\n");
    out.push_str("      printf '  \"is_resume\": %s,\\n' \"$(if [[ \"$IS_RESUME\" == \"1\" ]]; then printf true; else printf false; fi)\"\n");
    out.push_str(
        "      printf '  \"resume_dir\": \"%s\",\\n' \"$(json_escape \"$RESUME_HOST_PATH\")\"\n",
    );
    out.push_str("    else\n");
    out.push_str("      printf '  \"attempt\": null,\\n'\n");
    out.push_str("      printf '  \"is_resume\": null,\\n'\n");
    out.push_str("      printf '  \"resume_dir\": null,\\n'\n");
    out.push_str("    fi\n");
    out.push_str("    write_json_string_array \"declared_source_patterns\" \"${ARTIFACT_SOURCE_PATTERNS[@]}\"\n");
    out.push_str("    printf ',\\n'\n");
    out.push_str(
        "    write_json_string_array \"matched_source_paths\" \"${matched_source_paths[@]}\"\n",
    );
    out.push_str("    printf ',\\n'\n");
    out.push_str(
        "    write_json_string_array \"copied_relative_paths\" \"${copied_relative_paths[@]}\"\n",
    );
    out.push_str("    printf ',\\n'\n");
    out.push_str("    write_json_string_array \"warnings\" \"${warnings[@]}\"\n");
    out.push_str("    printf ',\\n'\n");
    out.push_str("    write_artifact_bundles_json\n");
    out.push_str("    printf '\\n}\\n'\n");
    out.push_str("  } > \"$tmp_manifest\"\n");
    out.push_str("  mv \"$tmp_manifest\" \"$ARTIFACTS_MANIFEST_FILE\"\n");
    out.push_str("}\n\n");

    out.push_str("collect_artifacts() {\n");
    out.push_str("  local exit_code=${1:-0}\n");
    out.push_str("  local job_outcome=success\n");
    out.push_str("  local should_collect=0\n");
    out.push_str("  local declared_pattern\n");
    out.push_str("  local host_pattern\n");
    out.push_str("  local matched\n");
    out.push_str("  local container_match\n");
    out.push_str("  local relative_path\n");
    out.push_str("  local destination\n");
    out.push_str("  local copy_output\n");
    out.push_str("  local shopt_state\n");
    out.push_str("  local pattern_matched\n");
    out.push_str("  local bundle_name\n");
    out.push_str("  local bundle_match_key\n");
    out.push_str("  local bundle_copy_key\n");
    out.push_str("  local bundle_warning_key\n");
    out.push_str("  local -a matched_source_paths=()\n");
    out.push_str("  ARTIFACT_COPIED_RELATIVE_PATHS=()\n");
    out.push_str("  ARTIFACT_BUNDLE_MATCH_RECORDS=()\n");
    out.push_str("  ARTIFACT_BUNDLE_COPIED_RECORDS=()\n");
    out.push_str("  ARTIFACT_BUNDLE_WARNING_RECORDS=()\n");
    out.push_str("  ARTIFACT_WARNINGS=()\n");
    out.push_str("  local -A seen_matches=()\n");
    out.push_str("  local -A seen_copied=()\n");
    out.push_str("  local -A seen_bundle_matches=()\n");
    out.push_str("  local -A seen_bundle_copied=()\n");
    out.push_str("  local -A seen_bundle_warnings=()\n");
    out.push_str("  if (( exit_code != 0 )); then\n");
    out.push_str("    job_outcome=failure\n");
    out.push_str("  fi\n");
    out.push_str("  case \"$ARTIFACTS_COLLECT_POLICY\" in\n");
    out.push_str("    always)\n");
    out.push_str("      should_collect=1\n");
    out.push_str("      ;;\n");
    out.push_str("    on_success)\n");
    out.push_str("      [[ \"$job_outcome\" == \"success\" ]] && should_collect=1\n");
    out.push_str("      ;;\n");
    out.push_str("    on_failure)\n");
    out.push_str("      [[ \"$job_outcome\" == \"failure\" ]] && should_collect=1\n");
    out.push_str("      ;;\n");
    out.push_str("  esac\n");
    out.push_str("  mkdir -p \"$ARTIFACTS_DIR\"\n");
    out.push_str("  rm -rf \"$ARTIFACTS_PAYLOAD_DIR\"\n");
    out.push_str("  mkdir -p \"$ARTIFACTS_PAYLOAD_DIR\"\n");
    out.push_str("  if (( should_collect == 0 )); then\n");
    out.push_str("    ARTIFACT_WARNINGS+=(\"collection skipped because job outcome '$job_outcome' does not match policy '$ARTIFACTS_COLLECT_POLICY'\")\n");
    out.push_str("    write_artifact_manifest \"$job_outcome\"\n");
    out.push_str("    return 0\n");
    out.push_str("  fi\n");
    out.push_str("  shopt_state=$(shopt -p nullglob globstar dotglob)\n");
    out.push_str("  shopt -s nullglob globstar dotglob\n");
    out.push_str("  local i\n");
    out.push_str("  for i in \"${!ARTIFACT_SOURCE_PATTERNS[@]}\"; do\n");
    out.push_str("    declared_pattern=\"${ARTIFACT_SOURCE_PATTERNS[i]}\"\n");
    out.push_str("    bundle_name=\"${ARTIFACT_PATTERN_BUNDLES[i]}\"\n");
    out.push_str("    pattern_matched=0\n");
    out.push_str("    host_pattern=\"$JOB_TMP${declared_pattern#/hpc-compose/job}\"\n");
    out.push_str("    while IFS= read -r matched; do\n");
    out.push_str("      [[ -n \"$matched\" ]] || continue\n");
    out.push_str("      pattern_matched=1\n");
    out.push_str("      if [[ -n \"${seen_matches[\"$matched\"]+x}\" ]]; then\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      seen_matches[\"$matched\"]=1\n");
    out.push_str("      container_match=\"/hpc-compose/job${matched#\"$JOB_TMP\"}\"\n");
    out.push_str("      matched_source_paths+=(\"$container_match\")\n");
    out.push_str("      bundle_match_key=\"$bundle_name\"$'\\t'\"$container_match\"\n");
    out.push_str("      if [[ -z \"${seen_bundle_matches[\"$bundle_match_key\"]+x}\" ]]; then\n");
    out.push_str("        seen_bundle_matches[\"$bundle_match_key\"]=1\n");
    out.push_str("        ARTIFACT_BUNDLE_MATCH_RECORDS+=(\"$bundle_match_key\")\n");
    out.push_str("      fi\n");
    out.push_str("      if [[ \"$matched\" == \"$JOB_TMP\" ]]; then\n");
    out.push_str("        ARTIFACT_WARNINGS+=(\"skipped reserved root path '/hpc-compose/job'; collect a child path instead\")\n");
    out.push_str("        bundle_warning_key=\"$bundle_name\"$'\\t'\"skipped reserved root path '/hpc-compose/job'; collect a child path instead\"\n");
    out.push_str(
        "        if [[ -z \"${seen_bundle_warnings[\"$bundle_warning_key\"]+x}\" ]]; then\n",
    );
    out.push_str("          seen_bundle_warnings[\"$bundle_warning_key\"]=1\n");
    out.push_str("          ARTIFACT_BUNDLE_WARNING_RECORDS+=(\"$bundle_warning_key\")\n");
    out.push_str("        fi\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      relative_path=${matched#\"$JOB_TMP\"/}\n");
    out.push_str(
        "      if [[ \"$relative_path\" == \"$matched\" || -z \"$relative_path\" ]]; then\n",
    );
    out.push_str(
        "        ARTIFACT_WARNINGS+=(\"skipped unsupported artifact path '$container_match'\")\n",
    );
    out.push_str("        bundle_warning_key=\"$bundle_name\"$'\\t'\"skipped unsupported artifact path '$container_match'\"\n");
    out.push_str(
        "        if [[ -z \"${seen_bundle_warnings[\"$bundle_warning_key\"]+x}\" ]]; then\n",
    );
    out.push_str("          seen_bundle_warnings[\"$bundle_warning_key\"]=1\n");
    out.push_str("          ARTIFACT_BUNDLE_WARNING_RECORDS+=(\"$bundle_warning_key\")\n");
    out.push_str("        fi\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      bundle_copy_key=\"$bundle_name\"$'\\t'\"$relative_path\"\n");
    out.push_str("      if [[ -n \"${seen_copied[\"$relative_path\"]+x}\" ]]; then\n");
    out.push_str("        if [[ -z \"${seen_bundle_copied[\"$bundle_copy_key\"]+x}\" ]]; then\n");
    out.push_str("          seen_bundle_copied[\"$bundle_copy_key\"]=1\n");
    out.push_str("          ARTIFACT_BUNDLE_COPIED_RECORDS+=(\"$bundle_copy_key\")\n");
    out.push_str("        fi\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      destination=\"$ARTIFACTS_PAYLOAD_DIR/$relative_path\"\n");
    out.push_str("      if [[ -d \"$matched\" ]]; then\n");
    out.push_str("        mkdir -p \"$destination\"\n");
    out.push_str("        if copy_output=$(cp -R \"$matched\"/. \"$destination\" 2>&1); then\n");
    out.push_str("          seen_copied[\"$relative_path\"]=1\n");
    out.push_str("          ARTIFACT_COPIED_RELATIVE_PATHS+=(\"$relative_path\")\n");
    out.push_str("          if [[ -z \"${seen_bundle_copied[\"$bundle_copy_key\"]+x}\" ]]; then\n");
    out.push_str("            seen_bundle_copied[\"$bundle_copy_key\"]=1\n");
    out.push_str("            ARTIFACT_BUNDLE_COPIED_RECORDS+=(\"$bundle_copy_key\")\n");
    out.push_str("          fi\n");
    out.push_str("        else\n");
    out.push_str("          ARTIFACT_WARNINGS+=(\"failed to copy '$container_match': $(trim_whitespace \"${copy_output//$'\\n'/; }\")\")\n");
    out.push_str("          bundle_warning_key=\"$bundle_name\"$'\\t'\"failed to copy '$container_match': $(trim_whitespace \"${copy_output//$'\\n'/; }\")\"\n");
    out.push_str(
        "          if [[ -z \"${seen_bundle_warnings[\"$bundle_warning_key\"]+x}\" ]]; then\n",
    );
    out.push_str("            seen_bundle_warnings[\"$bundle_warning_key\"]=1\n");
    out.push_str("            ARTIFACT_BUNDLE_WARNING_RECORDS+=(\"$bundle_warning_key\")\n");
    out.push_str("          fi\n");
    out.push_str("        fi\n");
    out.push_str("        continue\n");
    out.push_str("      fi\n");
    out.push_str("      mkdir -p \"$(dirname \"$destination\")\"\n");
    out.push_str("      if copy_output=$(cp -R \"$matched\" \"$destination\" 2>&1); then\n");
    out.push_str("        seen_copied[\"$relative_path\"]=1\n");
    out.push_str("        ARTIFACT_COPIED_RELATIVE_PATHS+=(\"$relative_path\")\n");
    out.push_str("        if [[ -z \"${seen_bundle_copied[\"$bundle_copy_key\"]+x}\" ]]; then\n");
    out.push_str("          seen_bundle_copied[\"$bundle_copy_key\"]=1\n");
    out.push_str("          ARTIFACT_BUNDLE_COPIED_RECORDS+=(\"$bundle_copy_key\")\n");
    out.push_str("        fi\n");
    out.push_str("      else\n");
    out.push_str("        ARTIFACT_WARNINGS+=(\"failed to copy '$container_match': $(trim_whitespace \"${copy_output//$'\\n'/; }\")\")\n");
    out.push_str("        bundle_warning_key=\"$bundle_name\"$'\\t'\"failed to copy '$container_match': $(trim_whitespace \"${copy_output//$'\\n'/; }\")\"\n");
    out.push_str(
        "        if [[ -z \"${seen_bundle_warnings[\"$bundle_warning_key\"]+x}\" ]]; then\n",
    );
    out.push_str("          seen_bundle_warnings[\"$bundle_warning_key\"]=1\n");
    out.push_str("          ARTIFACT_BUNDLE_WARNING_RECORDS+=(\"$bundle_warning_key\")\n");
    out.push_str("        fi\n");
    out.push_str("      fi\n");
    out.push_str("    done < <(compgen -G \"$host_pattern\" || true)\n");
    out.push_str("    if (( pattern_matched == 0 )); then\n");
    out.push_str(
        "      ARTIFACT_WARNINGS+=(\"pattern '$declared_pattern' did not match any paths\")\n",
    );
    out.push_str("      bundle_warning_key=\"$bundle_name\"$'\\t'\"pattern '$declared_pattern' did not match any paths\"\n");
    out.push_str(
        "      if [[ -z \"${seen_bundle_warnings[\"$bundle_warning_key\"]+x}\" ]]; then\n",
    );
    out.push_str("        seen_bundle_warnings[\"$bundle_warning_key\"]=1\n");
    out.push_str("        ARTIFACT_BUNDLE_WARNING_RECORDS+=(\"$bundle_warning_key\")\n");
    out.push_str("      fi\n");
    out.push_str("    fi\n");
    out.push_str("  done\n");
    out.push_str("  eval \"$shopt_state\"\n");
    out.push_str("  write_artifact_manifest \"$job_outcome\" \"${matched_source_paths[@]}\"\n");
    out.push_str("}\n\n");
}
