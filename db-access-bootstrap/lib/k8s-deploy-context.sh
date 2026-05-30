#!/usr/bin/env bash
# db-access-bootstrap：解析 K8s 部署上下文，并为 dbctl 生成运行时配置。
#
# deploy 脚本应传入：
#   APP_NAMESPACE  应用 Pod 所在命名空间（如 app-platform-dev）
#   DATA_NAMESPACE 数据组件命名空间（可选；默认由 app-platform-* 推导为 data-platform-*）
#   CLUSTER        集群标识（C1/C2/KIND，供 client 镜像解析）

k8s_bootstrap_resolve_context() {
  APP_NAMESPACE="${APP_NAMESPACE:-${NAMESPACE:-app-platform-dev}}"
  NAMESPACE="${APP_NAMESPACE}"

  if [[ -z "${DATA_NAMESPACE:-}" ]]; then
    if [[ "${APP_NAMESPACE}" =~ ^app-platform-(.+)$ ]]; then
      DATA_NAMESPACE="data-platform-${BASH_REMATCH[1]}"
    else
      DATA_NAMESPACE="data-platform-dev"
    fi
  fi

  PG_SERVICE="${PG_SERVICE:-postgresql-sunmoonai}"
  REDIS_SERVICE="${REDIS_SERVICE:-redis-sunmoonai-master}"
  NODEBULL_REDIS_SERVICE="${NODEBULL_REDIS_SERVICE:-redis-nodebull-master}"
  MONGO_SERVICE="${MONGO_SERVICE:-mongodb-sunmoonai}"

  export APP_NAMESPACE NAMESPACE DATA_NAMESPACE
  export PG_SERVICE REDIS_SERVICE NODEBULL_REDIS_SERVICE MONGO_SERVICE
}

_k8s_bootstrap_sed_escape() {
  printf '%s' "$1" | sed -e 's/[\\/&|]/\\&/g'
}

# 根据部署上下文生成 dbctl 运行时配置（stdout 输出临时文件路径）
k8s_bootstrap_prepare_config() {
  local src="$1"
  [[ -f "$src" ]] || return 1

  local dst pg_host redis_host nodebull_host mongo_host app_ns
  dst="$(mktemp "${TMPDIR:-/tmp}/db-bootstrap.XXXXXX")"
  cp "$src" "$dst"

  pg_host="$(_k8s_bootstrap_sed_escape "${PG_SERVICE}.${DATA_NAMESPACE}.svc.cluster.local")"
  redis_host="$(_k8s_bootstrap_sed_escape "${REDIS_SERVICE}.${DATA_NAMESPACE}.svc.cluster.local")"
  nodebull_host="$(_k8s_bootstrap_sed_escape "${NODEBULL_REDIS_SERVICE}.${DATA_NAMESPACE}.svc.cluster.local")"
  mongo_host="$(_k8s_bootstrap_sed_escape "${MONGO_SERVICE}.${DATA_NAMESPACE}.svc.cluster.local")"
  app_ns="$(_k8s_bootstrap_sed_escape "${APP_NAMESPACE}")"

  sed -i -e "s|^OUTPUT_NAMESPACE=.*|OUTPUT_NAMESPACE=${app_ns}|" "$dst"

  case "$(basename "$src")" in
    redis.k8s.env)
      sed -i -e "s|^DB_HOST=.*|DB_HOST=${redis_host}|" "$dst"
      ;;
    nodebull-redis.k8s.env)
      sed -i -e "s|^DB_HOST=.*|DB_HOST=${nodebull_host}|" "$dst"
      ;;
    postgresql.k8s.env)
      sed -i -e "s|^DB_HOST=.*|DB_HOST=${pg_host}|" "$dst"
      ;;
    mongodb.k8s.env)
      sed -i -e "s|^DB_HOST=.*|DB_HOST=${mongo_host}|" "$dst"
      ;;
  esac

  printf '%s\n' "$dst"
}

k8s_bootstrap_require_service() {
  local host="$1"
  local svc ns

  svc="${host%%.*}"
  ns="${host#*.}"
  ns="${ns%%.svc*}"

  if ! kubectl get svc -n "${ns}" "${svc}" >/dev/null 2>&1; then
    printf '[db-access-bootstrap][k8s][error] 缺少依赖 Service: %s/%s（请先部署 data-platform，例如 deploy-data-platform-all 或 deploy-redis.sh deploy nodebull）\n' \
      "${ns}" "${svc}" >&2
    return 1
  fi
}

k8s_bootstrap_run_dbctl() {
  local dbctl_bin="$1"
  local config_src="$2"
  local runtime_config=""
  local db_host=""
  local rc=0

  runtime_config="$(k8s_bootstrap_prepare_config "$config_src")" || return 1

  # shellcheck disable=SC1090
  source "${runtime_config}"
  db_host="${DB_HOST:-}"
  if [[ -n "${db_host}" ]]; then
    k8s_bootstrap_require_service "${db_host}" || { rm -f "${runtime_config}"; return 1; }
  fi

  printf '[db-access-bootstrap][k8s] dbctl provision: %s -> namespace=%s, data=%s\n' \
    "$(basename "$config_src")" "${APP_NAMESPACE}" "${DATA_NAMESPACE}"
  "${dbctl_bin}" --config "${runtime_config}" --target k8s --action provision || rc=$?
  rm -f "${runtime_config}"
  return "${rc}"
}
