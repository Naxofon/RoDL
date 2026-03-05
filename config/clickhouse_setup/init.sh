#!/bin/bash
set -e


SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

APP_USER="${CLICKHOUSE_APP_USER:-${CLICKHOUSE_USER:-default_user}}"
APP_PASSWORD="${CLICKHOUSE_APP_PASSWORD:-${CLICKHOUSE_PASSWORD:-tteesstt}}"
ACCESS_USER="${CLICKHOUSE_APP_ACCESS_USER:-${CLICKHOUSE_ACCESS_USER:-access_user}}"
ACCESS_PASSWORD="${CLICKHOUSE_APP_ACCESS_PASSWORD:-${CLICKHOUSE_ACCESS_PASSWORD:-accesspass}}"

ADMIN_USER="${CLICKHOUSE_ADMIN_USER:-default}"
ADMIN_PASSWORD="${CLICKHOUSE_ADMIN_PASSWORD:-}"
ADMIN_HOST="${CLICKHOUSE_HOST:-localhost}"

if [ "$APP_USER" = "default" ]; then
  echo "ERROR: APP_USER/CLICKHOUSE_USER must not be 'default'." >&2
  exit 1
fi

escape_sql() {
  printf "%s" "$1" | sed "s/'/''/g"
}

APP_PASSWORD_ESCAPED="$(escape_sql "$APP_PASSWORD")"
ACCESS_PASSWORD_ESCAPED="$(escape_sql "$ACCESS_PASSWORD")"

parse_config() {
  awk '
    BEGIN { in_top_db = 0; in_loader_db = 0 }
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    {
      line=$0
      sub(/\r$/, "", line)

      match(line, /^[[:space:]]*/)
      indent = RLENGTH

      colon_pos = index(line, ":")
      if (colon_pos == 0) { next }
      key = substr(line, 1, colon_pos - 1)
      val = substr(line, colon_pos + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", val)
      if (key == "") { next }

      if (indent == 0) {
        in_top_db = (key == "databases") ? 1 : 0
        in_loader_db = 0
        next
      }
      if (indent == 2 && in_top_db && val != "") { print key "=" val; next }
      if (indent == 4 && key == "databases") { in_loader_db = 1; next }
      if (indent == 4 && key != "databases") { in_loader_db = 0 }
      if (indent == 6 && in_loader_db && val != "") { print key "=" val }
    }
  ' "$1"
}

CFG_ENTRIES=""
CONFIG_PATH=""
for candidate in "${LOADER_CONFIG_PATH:-}" "${SCRIPT_DIR}/../loaders.yaml"; do
  [ -z "$candidate" ] && continue
  if [ -f "$candidate" ]; then
    CFG_ENTRIES="$(parse_config "$candidate")"
    [ -n "$CFG_ENTRIES" ] && CONFIG_PATH="$candidate" && break
  fi
done

if [ -z "$CFG_ENTRIES" ]; then
  echo "ERROR: No loaders.yaml found or empty. Set LOADER_CONFIG_PATH or provide config/loaders.yaml." >&2
  exit 1
fi

DB_DEFAULT=""
DB_ACCESS=""
DATA_DBS=""

validate_identifier() {
  if ! echo "$1" | grep -qE '^[a-zA-Z0-9_]+$'; then
    echo "ERROR: Invalid identifier '$1' (only [a-zA-Z0-9_] allowed)" >&2
    exit 1
  fi
}

add_data_db() {
  db="$1"
  validate_identifier "$db"
  case " $DATA_DBS " in
    *" $db "*) return ;;
    *) DATA_DBS="$DATA_DBS $db" ;;
  esac
}

echo "Using DB config from $CONFIG_PATH"
echo "CFG entries:"
while IFS= read -r line; do
  [ -z "$line" ] && continue
  key="${line%%=*}"
  val="${line#*=}"
  echo "  $key=$val"
  case "$key" in
    default) DB_DEFAULT="$val" ;;
    access) DB_ACCESS="$val" ;;
    *) add_data_db "$val" ;;
  esac
done <<< "$CFG_ENTRIES"

if [ -z "$DB_DEFAULT" ]; then
  echo "ERROR: loaders.yaml missing required key: databases.default" >&2
  exit 1
fi

[ -z "$DB_ACCESS" ] && DB_ACCESS="$DB_DEFAULT" && echo "WARN: 'access' not set; defaulting to $DB_ACCESS" >&2

validate_identifier "$DB_DEFAULT"
validate_identifier "$DB_ACCESS"
validate_identifier "$APP_USER"
validate_identifier "$ACCESS_USER"

echo "default DB: $DB_DEFAULT"
echo "access DB: $DB_ACCESS"
echo "data DBs:$DATA_DBS"

SQL_STMTS="CREATE DATABASE IF NOT EXISTS ${DB_DEFAULT};
CREATE DATABASE IF NOT EXISTS ${DB_ACCESS};
"
for db in $DATA_DBS; do
  SQL_STMTS="${SQL_STMTS}CREATE DATABASE IF NOT EXISTS ${db};
"
done

SQL_STMTS="${SQL_STMTS}CREATE USER IF NOT EXISTS ${APP_USER} IDENTIFIED BY '${APP_PASSWORD_ESCAPED}' HOST ANY;
CREATE USER IF NOT EXISTS ${ACCESS_USER} IDENTIFIED BY '${ACCESS_PASSWORD_ESCAPED}' HOST ANY;

CREATE TABLE IF NOT EXISTS ${DB_ACCESS}.Accesses
(
    login Nullable(String),
    token Nullable(String),
    container Nullable(String),
    type Nullable(String)
) ENGINE = MergeTree
ORDER BY (type, container, login)
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS ${DB_ACCESS}.AdminUsers
(
    id UInt64,
    name String,
    role LowCardinality(String)
) ENGINE = ReplacingMergeTree
ORDER BY id;

GRANT ACCESS MANAGEMENT ON *.* TO ${APP_USER};
"

for db in $DATA_DBS; do
  SQL_STMTS="${SQL_STMTS}GRANT SELECT, INSERT, ALTER, CREATE, TRUNCATE, SHOW TABLES ON ${db}.* TO ${APP_USER};
"
done

SQL_STMTS="${SQL_STMTS}REVOKE ALL ON ${DB_ACCESS}.Accesses FROM ${APP_USER};
GRANT SELECT, INSERT, ALTER, CREATE, TRUNCATE, SHOW TABLES ON ${DB_ACCESS}.Accesses TO ${ACCESS_USER};
GRANT SELECT, INSERT, ALTER, SHOW TABLES ON ${DB_ACCESS}.AdminUsers TO ${ACCESS_USER};
"

for db in $DATA_DBS; do
  SQL_STMTS="${SQL_STMTS}REVOKE ALL ON ${db}.* FROM ${ACCESS_USER};
"
done

clickhouse client \
  --host "${ADMIN_HOST}" \
  --user "${ADMIN_USER}" \
  --password "${ADMIN_PASSWORD}" \
  --multiquery <<<"$SQL_STMTS"
