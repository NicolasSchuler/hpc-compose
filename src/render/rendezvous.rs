pub(super) fn render_rendezvous_helpers(out: &mut String) {
    out.push_str(
        r#"rdzv_env_token() {
  local value=$1
  value=${value^^}
  value=${value//[^A-Z0-9]/_}
  printf '%s' "$value"
}

rdzv_json_string_field() {
  local file=$1
  local field=$2
  sed -n "s/^[[:space:]]*\"$field\"[[:space:]]*:[[:space:]]*\"\(.*\)\"[,]*/\1/p" "$file" | head -n 1
}

rdzv_json_number_field() {
  local file=$1
  local field=$2
  sed -n "s/^[[:space:]]*\"$field\"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\)[,]*/\1/p" "$file" | head -n 1
}

rdzv_export_record_env() {
  local name=$1
  local file=$2
  local token
  token=$(rdzv_env_token "$name")
  local url host port protocol path job_id service
  url=$(rdzv_json_string_field "$file" url)
  host=$(rdzv_json_string_field "$file" host)
  port=$(rdzv_json_number_field "$file" port)
  protocol=$(rdzv_json_string_field "$file" protocol)
  path=$(rdzv_json_string_field "$file" path)
  job_id=$(rdzv_json_string_field "$file" job_id)
  service=$(rdzv_json_string_field "$file" service)
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_NAME=$name")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_URL=$url")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_HOST=$host")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_PORT=$port")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_PROTOCOL=$protocol")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_PATH=$path")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_JOB_ID=$job_id")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_SERVICE=$service")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_NAME=$name")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_URL=$url")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_HOST=$host")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_PORT=$port")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_PROTOCOL=$protocol")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_PATH=$path")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_JOB_ID=$job_id")
  RDZV_LAUNCH_ENV+=("HPC_COMPOSE_RDZV_${token}_SERVICE=$service")
}

resolve_rendezvous_dependencies() {
  RDZV_LAUNCH_ENV=()
  local name file registered_at ttl now start
  start=$(date +%s)
  for name in "${RDZV_CLIENT_NAMES[@]:-}"; do
    file="$CACHE_ROOT/rendezvous/$name/latest.json"
    while true; do
      if [[ -f "$file" ]]; then
        registered_at=$(rdzv_json_number_field "$file" registered_at)
        ttl=$(rdzv_json_number_field "$file" ttl_seconds)
        now=$(date +%s)
        if [[ -n "$registered_at" && -n "$ttl" && $(( now - registered_at )) -lt "$ttl" ]]; then
          rdzv_export_record_env "$name" "$file"
          break
        fi
      fi
      if (( $(date +%s) - start >= RDZV_CLIENT_TIMEOUT_SECONDS )); then
        if [[ "$RDZV_CLIENT_REQUIRED" == "1" ]]; then
          echo "Timed out resolving rendezvous '$name' under $CACHE_ROOT/rendezvous" >&2
          return 1
        fi
        echo "warning: rendezvous '$name' not resolved before timeout" >&2
        break
      fi
      sleep 1
    done
  done
}

register_service_rendezvous_by_index() {
  local index=$1
  local rdzv_name=${SERVICE_RDZV_NAMES[index]:-}
  [[ -z "$rdzv_name" ]] && return 0
  [[ "${SERVICE_RDZV_REGISTERED[index]:-0}" == "1" ]] && return 0
  local service_name=${SERVICE_NAMES[index]:-unknown}
  local host
  host=$(first_word "${SERVICE_STEP_NODELIST[index]:-$HPC_COMPOSE_PRIMARY_NODE}")
  local port=${SERVICE_RDZV_PORTS[index]:-}
  local protocol=${SERVICE_RDZV_PROTOCOLS[index]:-http}
  local path=${SERVICE_RDZV_PATHS[index]:-}
  local ttl=${SERVICE_RDZV_TTLS[index]:-3600}
  local metadata_json=${SERVICE_RDZV_METADATA_JSON[index]:-}
  [[ -z "$metadata_json" ]] && metadata_json='{}'
  local url="${protocol}://${host}:${port}${path}"
  local dir="$CACHE_ROOT/rendezvous/$rdzv_name"
  local token
  token=$(printf '%s-%s' "$SLURM_JOB_ID" "$service_name" | tr -c 'A-Za-z0-9_.-' '_')
  local record="$dir/$token.json"
  # Unique per-writer temp names so concurrent multi-node registrations to the
  # same rendezvous name never share (and clobber) a temp file mid-write.
  local record_tmp="$dir/.$token.$$.tmp"
  local latest_tmp="$dir/.latest.$token.$$.tmp"
  mkdir -p "$dir"
  cat > "$record_tmp" <<HPC_COMPOSE_RDZV_JSON
{
  "schema_version": 1,
  "name": "$(json_escape "$rdzv_name")",
  "job_id": "$(json_escape "$SLURM_JOB_ID")",
  "service": "$(json_escape "$service_name")",
  "host": "$(json_escape "$host")",
  "port": $port,
  "protocol": "$(json_escape "$protocol")",
  "path": "$(json_escape "$path")",
  "url": "$(json_escape "$url")",
  "registered_at": $(date +%s),
  "ttl_seconds": $ttl,
  "cache_dir": "$(json_escape "$CACHE_ROOT")",
  "metadata": $metadata_json
}
HPC_COMPOSE_RDZV_JSON
  mv "$record_tmp" "$record"
  cp "$record" "$latest_tmp"
  mv "$latest_tmp" "$dir/latest.json"
  SERVICE_RDZV_REGISTERED[index]="1"
  echo "Registered rendezvous '$rdzv_name' for service '$service_name' at $url"
}

deregister_rendezvous_records() {
  local i
  for i in "${!SERVICE_NAMES[@]}"; do
    local rdzv_name=${SERVICE_RDZV_NAMES[i]:-}
    [[ -z "$rdzv_name" ]] && continue
    local latest="$CACHE_ROOT/rendezvous/$rdzv_name/latest.json"
    [[ -f "$latest" ]] || continue
    local owner
    owner=$(rdzv_json_string_field "$latest" job_id)
    if [[ "$owner" == "$SLURM_JOB_ID" ]]; then
      rm -f "$latest"
    fi
  done
}

"#,
    );
}
