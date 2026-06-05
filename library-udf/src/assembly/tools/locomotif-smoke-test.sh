#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
DEFAULT_ALL_BIN_HOME="$REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-all-bin/apache-iotdb-2.0.7-SNAPSHOT-all-bin"
DEFAULT_CLI_BIN_HOME="$REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-cli-bin/apache-iotdb-2.0.7-SNAPSHOT-cli-bin"
DEFAULT_LIBRARY_UDF_BIN_HOME="$REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-library-udf-bin/apache-iotdb-2.0.7-SNAPSHOT-library-udf-bin"

host="${host:-127.0.0.1}"
rpcPort="${rpcPort:-6667}"
user="${user:-root}"
pass="${pass:-root}"

IOTDB_ALL_BIN_HOME="${IOTDB_ALL_BIN_HOME:-$DEFAULT_ALL_BIN_HOME}"
IOTDB_CLI_BIN_HOME="${IOTDB_CLI_BIN_HOME:-$DEFAULT_CLI_BIN_HOME}"
LIBRARY_UDF_BIN_HOME="${LIBRARY_UDF_BIN_HOME:-$DEFAULT_LIBRARY_UDF_BIN_HOME}"
AUTO_START="${AUTO_START:-true}"
AUTO_STOP="${AUTO_STOP:-true}"
INSTALL_UDF_JAR="${INSTALL_UDF_JAR:-true}"

SETUP_SQL="${SETUP_SQL:-$SCRIPT_DIR/locomotif-smoke-setup.sql}"
QUERY_SQL="${QUERY_SQL:-$SCRIPT_DIR/locomotif-smoke-query.sql}"
OUTPUT_DIR="${OUTPUT_DIR:-$PWD/locomotif-smoke-output}"
OUTPUT_FILE="$OUTPUT_DIR/locomotif-smoke-$(date +%Y%m%d%H%M%S).log"

if [[ -z "${UDF_JAR:-}" ]]; then
  if [[ -f "$LIBRARY_UDF_BIN_HOME/library-udf.jar" ]]; then
    UDF_JAR="$LIBRARY_UDF_BIN_HOME/library-udf.jar"
  else
    UDF_JAR=""
    for candidate in "$REPO_ROOT"/library-udf/target/library-udf-*-jar-with-dependencies.jar "$REPO_ROOT"/library-udf/target/library-udf-*.jar; do
      if [[ -f "$candidate" && "$candidate" != *-sources.jar ]]; then
        UDF_JAR="$candidate"
        break
      fi
    done
  fi
fi

if [[ -n "${IOTDB_CLI:-}" ]]; then
  CLI="$IOTDB_CLI"
elif [[ -x "$IOTDB_CLI_BIN_HOME/sbin/start-cli.sh" ]]; then
  CLI="$IOTDB_CLI_BIN_HOME/sbin/start-cli.sh"
elif [[ -n "${IOTDB_HOME:-}" && -x "$IOTDB_HOME/sbin/start-cli.sh" ]]; then
  CLI="$IOTDB_HOME/sbin/start-cli.sh"
elif [[ -x "$SCRIPT_DIR/../sbin/start-cli.sh" ]]; then
  CLI="$SCRIPT_DIR/../sbin/start-cli.sh"
elif [[ -x "$SCRIPT_DIR/../../sbin/start-cli.sh" ]]; then
  CLI="$SCRIPT_DIR/../../sbin/start-cli.sh"
else
  echo "Cannot find start-cli.sh. Set IOTDB_HOME or IOTDB_CLI before running this script." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

install_udf_jar() {
  if [[ "$INSTALL_UDF_JAR" != "true" ]]; then
    return
  fi
  if [[ -z "${UDF_JAR:-}" || ! -f "$UDF_JAR" ]]; then
    echo "Cannot find library-udf jar. Set UDF_JAR before running this script." >&2
    exit 1
  fi
  if [[ ! -d "$IOTDB_ALL_BIN_HOME" ]]; then
    echo "Cannot find IoTDB all-bin home: $IOTDB_ALL_BIN_HOME" >&2
    exit 1
  fi
  mkdir -p "$IOTDB_ALL_BIN_HOME/ext/udf"
  cp "$UDF_JAR" "$IOTDB_ALL_BIN_HOME/ext/udf/library-udf.jar"
  echo "Installed UDF jar: $UDF_JAR -> $IOTDB_ALL_BIN_HOME/ext/udf/library-udf.jar"
}

start_iotdb() {
  if [[ "$AUTO_START" != "true" ]]; then
    return
  fi
  if [[ ! -x "$IOTDB_ALL_BIN_HOME/sbin/start-standalone.sh" ]]; then
    echo "Cannot find start-standalone.sh under $IOTDB_ALL_BIN_HOME" >&2
    exit 1
  fi
  echo "Starting IoTDB standalone from $IOTDB_ALL_BIN_HOME"
  (cd "$IOTDB_ALL_BIN_HOME" && bash sbin/start-standalone.sh)
}

stop_iotdb() {
  if [[ "$AUTO_START" != "true" || "$AUTO_STOP" != "true" ]]; then
    return
  fi
  if [[ -x "$IOTDB_ALL_BIN_HOME/sbin/stop-standalone.sh" ]]; then
    echo "Stopping IoTDB standalone from $IOTDB_ALL_BIN_HOME"
    (cd "$IOTDB_ALL_BIN_HOME" && bash sbin/stop-standalone.sh) || true
  fi
}

wait_iotdb_ready() {
  if [[ "$AUTO_START" != "true" ]]; then
    return
  fi
  local log="$OUTPUT_DIR/iotdb-ready-check.log"
  for _ in $(seq 1 60); do
    if "$CLI" -h "$host" -p "$rpcPort" -u "$user" -pw "$pass" -e "show version" >"$log" 2>&1; then
      echo "IoTDB is ready."
      return
    fi
    sleep 2
  done
  echo "Timed out waiting for IoTDB to become ready. Last CLI output:" >&2
  cat "$log" >&2 || true
  exit 1
}

run_sql() {
  local sql="$1"
  echo ">>> $sql"
  "$CLI" -h "$host" -p "$rpcPort" -u "$user" -pw "$pass" -e "$sql"
}

run_sql_allow_failure() {
  local sql="$1"
  echo ">>> $sql"
  "$CLI" -h "$host" -p "$rpcPort" -u "$user" -pw "$pass" -e "$sql" || true
}

run_sql_file() {
  local file="$1"
  while IFS= read -r line || [[ -n "$line" ]]; do
    local sql
    sql="$(printf '%s' "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    if [[ -z "$sql" || "$sql" == --* ]]; then
      continue
    fi
    run_sql "$sql"
  done < "$file"
}

run_query_file() {
  local file="$1"
  while IFS= read -r line || [[ -n "$line" ]]; do
    local sql
    sql="$(printf '%s' "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
    if [[ -z "$sql" || "$sql" == --* ]]; then
      continue
    fi
    {
      echo ">>> $sql"
      "$CLI" -h "$host" -p "$rpcPort" -u "$user" -pw "$pass" -e "$sql"
    } 2>&1 | tee -a "$OUTPUT_FILE"
  done < "$file"
}

echo "Using CLI: $CLI"
echo "Writing smoke-test output to $OUTPUT_FILE"

trap stop_iotdb EXIT

install_udf_jar
start_iotdb
wait_iotdb_ready

if [[ "${SKIP_REGISTER:-false}" != "true" ]]; then
  run_sql_allow_failure "drop function locomotif"
  run_sql "create function locomotif as 'org.apache.iotdb.library.dmatch.UDTFLoCoMotif'"
fi

run_sql_allow_failure "drop database root.locomotif"
run_sql_file "$SETUP_SQL"
run_query_file "$QUERY_SQL"

if grep -q '"motifId"' "$OUTPUT_FILE"; then
  echo "LoCoMotif smoke test passed."
else
  echo "LoCoMotif smoke test failed: query output does not contain motifId." >&2
  exit 1
fi
