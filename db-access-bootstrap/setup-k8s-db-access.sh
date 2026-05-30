#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/config"
LIB_DIR="${SCRIPT_DIR}/lib"

# shellcheck disable=SC1091
source "${CONFIG_DIR}/common.env"
# shellcheck disable=SC1091
source "${LIB_DIR}/k8s-deploy-context.sh"

PG_CONFIG="${PG_K8S_CONFIG:-${CONFIG_DIR}/postgresql.k8s.env}"
REDIS_CONFIG="${REDIS_K8S_CONFIG:-${CONFIG_DIR}/redis.k8s.env}"
MONGO_CONFIG="${MONGO_K8S_CONFIG:-${CONFIG_DIR}/mongodb.k8s.env}"

OUT="${K8S_OUT_ENV:-${SCRIPT_DIR}/.env.local.k8s.db}"

log() { printf '[db-access-bootstrap][k8s] %s\n' "$*"; }
die() { printf '[db-access-bootstrap][k8s][error] %s\n' "$*" >&2; exit 1; }

bool_true() {
  case "${1:-false}" in 1|true|TRUE|yes|YES|on|ON) return 0 ;; *) return 1 ;; esac
}

require_file() { [[ -f "$1" ]] || die "Missing file: $1"; }

replace_env_value() {
  local file="$1" key="$2" value="$3"
  if grep -q "^${key}=" "$file"; then
    sed -i "s#^${key}=.*#${key}=${value}#g" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

write_k8s_out() {
  local runtime_config=""

  printf '# k8s 场景连接串（内部 Service DNS），由 setup-k8s-db-access.sh 生成。\n' > "${OUT}"
  printf '# 仅在 Pod 内部可用，不可直接用于本机开发。\n' >> "${OUT}"
  printf 'APP_ENV=production\n' >> "${OUT}"

  if bool_true "${ENABLE_POSTGRESQL:-false}" && [[ -f "${PG_CONFIG}" ]]; then
    runtime_config="$(k8s_bootstrap_prepare_config "${PG_CONFIG}")"
    # shellcheck disable=SC1090
    source "${runtime_config}"
    rm -f "${runtime_config}"
    replace_env_value "${OUT}" "DATABASE_URL" \
      "postgresql://${APP_DB_USER}:${APP_DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${APP_DB_NAME}"
  fi

  if bool_true "${ENABLE_REDIS:-false}" && [[ -f "${REDIS_CONFIG}" ]]; then
    runtime_config="$(k8s_bootstrap_prepare_config "${REDIS_CONFIG}")"
    # shellcheck disable=SC1090
    source "${runtime_config}"
    rm -f "${runtime_config}"
    replace_env_value "${OUT}" "REDIS_HOST"     "${DB_HOST}"
    replace_env_value "${OUT}" "REDIS_PORT"     "${DB_PORT}"
    replace_env_value "${OUT}" "REDIS_DB"       "${REDIS_DB_INDEX}"
    replace_env_value "${OUT}" "REDIS_USER"     "${APP_DB_USER}"
    replace_env_value "${OUT}" "REDIS_PASSWORD" "${APP_DB_PASSWORD}"
  fi

  log "k8s env written: ${OUT}"
}

main() {
  [[ -x "${DBCTL_BIN}" ]] || die "DBCTL_BIN not executable: ${DBCTL_BIN}"
  command -v kubectl >/dev/null 2>&1 || die "Missing kubectl"

  k8s_bootstrap_resolve_context
  log "deploy context: app=${APP_NAMESPACE}, data=${DATA_NAMESPACE}, cluster=${CLUSTER:-${K8S_TARGET_MODE:-unknown}}"

  bool_true "${ENABLE_POSTGRESQL:-false}" && require_file "${PG_CONFIG}" && k8s_bootstrap_run_dbctl "${DBCTL_BIN}" "${PG_CONFIG}"
  bool_true "${ENABLE_MONGODB:-false}" && require_file "${MONGO_CONFIG}" && k8s_bootstrap_run_dbctl "${DBCTL_BIN}" "${MONGO_CONFIG}"
  bool_true "${ENABLE_REDIS:-false}" && require_file "${REDIS_CONFIG}" && k8s_bootstrap_run_dbctl "${DBCTL_BIN}" "${REDIS_CONFIG}"

  write_k8s_out

  log "Done. k8s secrets applied in namespace: ${APP_NAMESPACE}"
}

main "$@"
