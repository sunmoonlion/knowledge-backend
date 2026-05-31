#!/usr/bin/env bash

redis_admin_auth_args() {
  if redis-cli --help 2>&1 | grep -q -- '--user'; then
    printf -- '--user %s -a %s' "${REDIS_ADMIN_USER}" "${REDIS_ADMIN_PASSWORD}"
    return 0
  fi

  die "redis-cli does not support --user. Please install redis-cli >= 6."
}

redis_use_k8s_client() {
  [[ "${TARGET:-}" == "k8s" ]] && ! bool_true "${DBCTL_K8S_USE_LOCAL_CLIENT:-false}"
}

redis_client_namespace() {
  if [[ -n "${REDIS_CLIENT_NAMESPACE:-}" ]]; then
    printf '%s\n' "${REDIS_CLIENT_NAMESPACE}"
  elif [[ "${DB_HOST}" =~ \.([a-z0-9-]+)\.svc(\.|$) ]]; then
    printf '%s\n' "${BASH_REMATCH[1]}"
  else
    printf '%s\n' "${K8S_PRECHECK_NAMESPACE:-${NAMESPACE:-default}}"
  fi
}

redis_client_image() {
  printf '%s\n' "${REDIS_CLIENT_IMAGE:-harbor.sunmoonai.com:30443/k8s-images/redis:8.2.1-debian-12-r0}"
}

redis_run_k8s_client() {
  # 从 stdin 读脚本；非交互 Pod，避免 --rm -i 的 "pod deleted" 误判
  local pod_name="$1"
  local namespace image timeout term_exit logs
  local pull_secret overrides script_content script_b64
  namespace="$(redis_client_namespace)"
  image="$(redis_client_image)"
  timeout="${REDIS_CLIENT_POD_RUNNING_TIMEOUT:-5m0s}"
  pull_secret="${REDIS_CLIENT_IMAGE_PULL_SECRET:-harbor-registry-secret}"
  overrides="$(printf '{"spec":{"imagePullSecrets":[{"name":"%s"}]}}' "${pull_secret}")"

  require_cmd "kubectl"
  script_content="$(cat)"
  script_b64="$(printf '%s' "${script_content}" | base64 -w0 2>/dev/null || printf '%s' "${script_content}" | base64)"

  log "[redis] using temporary Redis client pod: ${namespace}/${pod_name} (${image})"
  if ! kubectl run "${pod_name}" --restart=Never -n "${namespace}" \
    --image="${image}" \
    --overrides="${overrides}" \
    --command -- bash -c "echo '${script_b64}' | base64 -d | bash -se" >/dev/null 2>&1; then
    kubectl delete pod "${pod_name}" -n "${namespace}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    return 1
  fi

  if kubectl wait --for=jsonpath='{.status.phase}'=Succeeded -n "${namespace}" "pod/${pod_name}" --timeout="${timeout}" >/dev/null 2>&1; then
    term_exit=0
  elif kubectl wait --for=jsonpath='{.status.phase}'=Failed -n "${namespace}" "pod/${pod_name}" --timeout="${timeout}" >/dev/null 2>&1; then
    term_exit="$(kubectl get pod "${pod_name}" -n "${namespace}" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || echo 1)"
  else
    term_exit=1
  fi

  logs="$(kubectl logs "${pod_name}" -n "${namespace}" 2>/dev/null || true)"
  kubectl delete pod "${pod_name}" -n "${namespace}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [[ -n "${logs}" ]]; then
    printf '%s\n' "${logs}"
  fi
  return "${term_exit:-1}"
}

redis_validate() {
  require_non_empty "DB_HOST" "${DB_HOST:-}"
  require_non_empty "DB_PORT" "${DB_PORT:-}"
  require_non_empty "REDIS_DB_INDEX" "${REDIS_DB_INDEX:-}"

  if bool_true "${REDIS_AUTH_ONLY:-false}"; then
    # Password-only auth (no ACL user management). Useful for apps that don't support Redis username.
    require_non_empty "REDIS_PASSWORD" "${REDIS_PASSWORD:-}"
    return 0
  fi

  require_non_empty "REDIS_ADMIN_USER" "${REDIS_ADMIN_USER:-}"
  require_non_empty "REDIS_ADMIN_PASSWORD" "${REDIS_ADMIN_PASSWORD:-}"
  require_non_empty "APP_DB_USER" "${APP_DB_USER:-}"
  require_non_empty "APP_DB_PASSWORD" "${APP_DB_PASSWORD:-}"
}

redis_provision() {
  if bool_true "${REDIS_AUTH_ONLY:-false}"; then
    log "Provision Redis (auth-only): db=${REDIS_DB_INDEX}"
    if bool_true "${DRY_RUN:-false}"; then
      log "DRY_RUN=true, skip executing redis-cli"
      return 0
    fi
    wait_k8s_pods_ready
    if redis_use_k8s_client; then
      local pod_name
      pod_name="$(dbctl_k8s_client_pod_name "redis-auth" "${SERVICE_NAME:-app}")"
      redis_run_k8s_client "${pod_name}" <<EOF
REDISCLI_AUTH='${REDIS_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' -n '${REDIS_DB_INDEX}' PING >/dev/null
EOF
      [[ $? -eq 0 ]] || die "Redis k8s auth client pod failed"
    else
      require_cmd "redis-cli"
      redis_precheck
    fi
    APP_DB_URI="redis://:${REDIS_PASSWORD}@${DB_HOST}:${DB_PORT}/${REDIS_DB_INDEX}"
    require_non_empty "APP_DB_URI(redis)" "${APP_DB_URI}"
    return 0
  fi

  # 多前缀：设 REDIS_KEY_PREFIX_SEP（如 |）则在 REDIS_KEY_PREFIX 内用该字符分段；未设则按空白分段。
  # env 里若 SEP 为 |，须写成 REDIS_KEY_PREFIX_SEP="|"，否则 bash source 会把 | 当成管道。
  local key_spec="${REDIS_KEY_PREFIX:-${SERVICE_NAME:-app}:*}"
  local key_sep="${REDIS_KEY_PREFIX_SEP:-}"
  # +@connection：PING/CLIENT 等，ioredis 连接就绪检查需要；勿省略
  # +@pubsub：Bull/NodeBull worker 需要 subscribe/psubscribe
  local category="${REDIS_ACL_CATEGORY:-+@read +@write +@connection +@hash +@string +@list +@set +@sortedset}"
  local channel_spec="${REDIS_CHANNEL_PREFIX:-}"
  local redis_all_channels="false"
  if [[ "${category}" == *"+@pubsub"* ]]; then
    if bool_true "${REDIS_ALL_CHANNELS:-true}"; then
      redis_all_channels="true"
    elif [[ -z "${channel_spec}" ]]; then
      channel_spec="${key_spec}"
    fi
  fi

  local channel_log="none"
  if [[ "${redis_all_channels}" == "true" ]]; then
    channel_log="allchannels"
  elif [[ -n "${channel_spec}" ]]; then
    channel_log="${channel_spec}"
  fi

  log "Provision Redis ACL user: user=${APP_DB_USER}, db=${REDIS_DB_INDEX}, keyPrefixes=${key_spec} (sep=${key_sep:-whitespace}), channelAccess=${channel_log}"
  if bool_true "${DRY_RUN:-false}"; then
    log "DRY_RUN=true, skip executing redis-cli"
    return 0
  fi

  wait_k8s_pods_ready

  if redis_use_k8s_client; then
    local pod_name
    pod_name="$(dbctl_k8s_client_pod_name "redis-provision" "${SERVICE_NAME:-app}")"
    redis_run_k8s_client "${pod_name}" <<EOF
key_spec='${key_spec}'
key_sep='${key_sep}'
channel_spec='${channel_spec}'
category='${category}'
redis_all_channels='${redis_all_channels}'
key_args=()
channel_args=()
if [[ -n "\${key_sep}" ]]; then
  IFS="\${key_sep}" read -ra prefix_parts <<< "\${key_spec}"
else
  read -ra prefix_parts <<< "\${key_spec}"
fi
for pat in "\${prefix_parts[@]}"; do
  [[ -z "\${pat}" ]] && continue
  key_args+=( "~\${pat}" )
done
if [[ "\${redis_all_channels}" != "true" && -n "\${channel_spec}" ]]; then
  if [[ -n "\${key_sep}" ]]; then
    IFS="\${key_sep}" read -ra channel_parts <<< "\${channel_spec}"
  else
    read -ra channel_parts <<< "\${channel_spec}"
  fi
  for pat in "\${channel_parts[@]}"; do
    [[ -z "\${pat}" ]] && continue
    channel_args+=( "&\${pat}" )
  done
fi
REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' PING >/dev/null
if [[ "\${redis_all_channels}" == "true" ]]; then
  REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' ACL SETUSER '${APP_DB_USER}' on '>${APP_DB_PASSWORD}' "\${key_args[@]}" allchannels \${category} -@dangerous >/dev/null
else
  REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' ACL SETUSER '${APP_DB_USER}' on '>${APP_DB_PASSWORD}' "\${key_args[@]}" "\${channel_args[@]}" \${category} -@dangerous >/dev/null
fi
echo '[redis-client] ACL user upserted: ${APP_DB_USER}'
EOF
    [[ $? -eq 0 ]] || die "Redis k8s provision client pod failed"
    APP_DB_URI="redis://${APP_DB_USER}:${APP_DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${REDIS_DB_INDEX}"
    require_non_empty "APP_DB_URI(redis)" "${APP_DB_URI}"
    return 0
  fi

  require_cmd "redis-cli"
  redis_precheck

  local auth_args
  auth_args="$(redis_admin_auth_args)"
  local -a key_args=()
  local -a channel_args=()
  local -a prefix_parts=()
  local -a channel_parts=()
  local IFS_save="${IFS}"
  if [[ -n "${key_sep}" ]]; then
    IFS="${key_sep}"
  else
    IFS=$' \t\n'
  fi
  # shellcheck disable=SC2162
  read -ra prefix_parts <<< "${key_spec}"
  IFS="${IFS_save}"
  local pat
  for pat in "${prefix_parts[@]}"; do
    [[ -z "${pat}" ]] && continue
    key_args+=( "~${pat}" )
  done
  if [[ "${redis_all_channels}" != "true" && -n "${channel_spec}" ]]; then
    if [[ -n "${key_sep}" ]]; then
      IFS="${key_sep}"
    else
      IFS=$' \t\n'
    fi
    # shellcheck disable=SC2162
    read -ra channel_parts <<< "${channel_spec}"
    IFS="${IFS_save}"
    for pat in "${channel_parts[@]}"; do
      [[ -z "${pat}" ]] && continue
      channel_args+=( "&${pat}" )
    done
  fi
  if [[ "${redis_all_channels}" == "true" ]]; then
    # shellcheck disable=SC2086
    redis-cli -h "${DB_HOST}" -p "${DB_PORT}" ${auth_args} ACL SETUSER "${APP_DB_USER}" on ">${APP_DB_PASSWORD}" "${key_args[@]}" allchannels ${category} -@dangerous >/dev/null
  else
    # shellcheck disable=SC2086
    redis-cli -h "${DB_HOST}" -p "${DB_PORT}" ${auth_args} ACL SETUSER "${APP_DB_USER}" on ">${APP_DB_PASSWORD}" "${key_args[@]}" "${channel_args[@]}" ${category} -@dangerous >/dev/null
  fi
  log "[redis] ACL user upserted: ${APP_DB_USER}"

  APP_DB_URI="redis://${APP_DB_USER}:${APP_DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${REDIS_DB_INDEX}"
  require_non_empty "APP_DB_URI(redis)" "${APP_DB_URI}"
}

redis_deprovision() {
  if bool_true "${REDIS_AUTH_ONLY:-false}"; then
    log "Deprovision Redis (auth-only): nothing to do server-side"
    APP_DB_URI=""
    return 0
  fi

  log "Deprovision Redis ACL user: user=${APP_DB_USER}"
  if bool_true "${DRY_RUN:-false}"; then
    log "DRY_RUN=true, skip executing redis-cli"
    APP_DB_URI=""
    return 0
  fi

  wait_k8s_pods_ready

  if redis_use_k8s_client; then
    local pod_name
    pod_name="$(dbctl_k8s_client_pod_name "redis-deprovision" "${SERVICE_NAME:-app}")"
    redis_run_k8s_client "${pod_name}" <<EOF
REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' PING >/dev/null
REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' ACL DELUSER '${APP_DB_USER}' >/dev/null || true
if [[ '${DEPROVISION_DROP_DATABASE:-false}' == 'true' || '${DEPROVISION_DROP_DATABASE:-false}' == '1' ]]; then
  if [[ '${REDIS_ALLOW_FLUSH_DB:-false}' == 'true' || '${REDIS_ALLOW_FLUSH_DB:-false}' == '1' ]]; then
    REDISCLI_AUTH='${REDIS_ADMIN_PASSWORD}' redis-cli -h '${DB_HOST}' -p '${DB_PORT}' --user '${REDIS_ADMIN_USER}' -n '${REDIS_DB_INDEX}' FLUSHDB ASYNC >/dev/null
  else
    echo '[redis-client][warn] DEPROVISION_DROP_DATABASE=true but REDIS_ALLOW_FLUSH_DB!=true, skip FLUSHDB for safety'
  fi
fi
echo '[redis-client] dropped ACL user if exists: ${APP_DB_USER}'
EOF
    APP_DB_URI=""
    return 0
  fi

  require_cmd "redis-cli"
  redis_precheck
  local auth_args
  auth_args="$(redis_admin_auth_args)"
  # shellcheck disable=SC2086
  redis-cli -h "${DB_HOST}" -p "${DB_PORT}" ${auth_args} ACL DELUSER "${APP_DB_USER}" >/dev/null || true
  log "[redis] dropped ACL user if exists: ${APP_DB_USER}"
  if bool_true "${DEPROVISION_DROP_DATABASE:-false}"; then
    if bool_true "${REDIS_ALLOW_FLUSH_DB:-false}"; then
      # shellcheck disable=SC2086
      redis-cli -h "${DB_HOST}" -p "${DB_PORT}" ${auth_args} -n "${REDIS_DB_INDEX}" FLUSHDB ASYNC >/dev/null
      log "[redis] flushed db index: ${REDIS_DB_INDEX}"
    else
      warn "DEPROVISION_DROP_DATABASE=true but REDIS_ALLOW_FLUSH_DB!=true, skip FLUSHDB for safety"
    fi
  fi
  APP_DB_URI=""
}

redis_precheck() {
  local timeout="${DB_PRECHECK_TIMEOUT_SECONDS:-60}"
  local interval="${DB_PRECHECK_INTERVAL_SECONDS:-3}"

  if ! precheck_enabled; then
    log "Redis precheck disabled"
    return 0
  fi

  if redis_use_k8s_client; then
    return 0
  fi

  require_cmd "redis-cli"
  log "Redis precheck: waiting for readiness (timeout=${timeout}s, interval=${interval}s)"
  if bool_true "${REDIS_AUTH_ONLY:-false}"; then
    if wait_until "${timeout}" "${interval}" redis-cli -h "${DB_HOST}" -p "${DB_PORT}" -a "${REDIS_PASSWORD}" PING >/dev/null 2>&1; then
      log "Redis precheck passed"
      return 0
    fi
  else
    local auth_args
    auth_args="$(redis_admin_auth_args)"
    # shellcheck disable=SC2086
    if wait_until "${timeout}" "${interval}" redis-cli -h "${DB_HOST}" -p "${DB_PORT}" ${auth_args} PING >/dev/null 2>&1; then
      log "Redis precheck passed"
      return 0
    fi
  fi

  die "Redis precheck failed: service not ready or unreachable"
}
