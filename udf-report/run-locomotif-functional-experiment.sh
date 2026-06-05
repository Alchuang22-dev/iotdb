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
IOTDB_REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="$(cd "$IOTDB_REPO_ROOT/.." && pwd)"

DEFAULT_ALL_BIN_HOME="$IOTDB_REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-all-bin/apache-iotdb-2.0.7-SNAPSHOT-all-bin"
DEFAULT_CLI_BIN_HOME="$IOTDB_REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-cli-bin/apache-iotdb-2.0.7-SNAPSHOT-cli-bin"
DEFAULT_LIBRARY_UDF_BIN_HOME="$IOTDB_REPO_ROOT/distribution/target/apache-iotdb-2.0.7-SNAPSHOT-library-udf-bin/apache-iotdb-2.0.7-SNAPSHOT-library-udf-bin"

host="${host:-127.0.0.1}"
rpcPort="${rpcPort:-6667}"
user="${user:-root}"
pass="${pass:-root}"

IOTDB_ALL_BIN_HOME="${IOTDB_ALL_BIN_HOME:-$DEFAULT_ALL_BIN_HOME}"
IOTDB_CLI_BIN_HOME="${IOTDB_CLI_BIN_HOME:-$DEFAULT_CLI_BIN_HOME}"
LIBRARY_UDF_BIN_HOME="${LIBRARY_UDF_BIN_HOME:-$DEFAULT_LIBRARY_UDF_BIN_HOME}"
DATASET_ROOT="${DATASET_ROOT:-$WORKSPACE_ROOT/datasets/locomotif-udf/small}"
AUTO_START="${AUTO_START:-true}"
AUTO_STOP="${AUTO_STOP:-true}"
INSTALL_UDF_JAR="${INSTALL_UDF_JAR:-true}"

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
OUT_ROOT="${OUT_ROOT:-$SCRIPT_DIR/locomotif-functional}"
RUN_DIR="$OUT_ROOT/$RUN_ID"
GENERATED_DIR="$RUN_DIR/generated"
RESULT_DIR="$RUN_DIR/results"
LOG_DIR="$RUN_DIR/logs"
SUMMARY_CSV="$RUN_DIR/summary.csv"
REPORT_MD="$RUN_DIR/report.md"
LATEST_REPORT="$SCRIPT_DIR/locomotif_functional_report.md"

if [[ -z "${UDF_JAR:-}" ]]; then
  UDF_JAR=""
  for candidate in "$IOTDB_REPO_ROOT"/library-udf/target/library-udf-*-jar-with-dependencies.jar "$LIBRARY_UDF_BIN_HOME/library-udf.jar" "$IOTDB_REPO_ROOT"/library-udf/target/library-udf-*.jar; do
    if [[ -f "$candidate" && "$candidate" != *-sources.jar ]]; then
      UDF_JAR="$candidate"
      break
    fi
  done
fi

if [[ -n "${IOTDB_CLI:-}" ]]; then
  CLI="$IOTDB_CLI"
elif [[ -x "$IOTDB_CLI_BIN_HOME/sbin/start-cli.sh" ]]; then
  CLI="$IOTDB_CLI_BIN_HOME/sbin/start-cli.sh"
else
  echo "Cannot find start-cli.sh. Set IOTDB_CLI_BIN_HOME or IOTDB_CLI before running this script." >&2
  exit 1
fi

mkdir -p "$GENERATED_DIR" "$RESULT_DIR" "$LOG_DIR"

run_cli() {
  "$CLI" -h "$host" -p "$rpcPort" -u "$user" -pw "$pass" "$@"
}

install_udf_jar() {
  if [[ "$INSTALL_UDF_JAR" != "true" ]]; then
    return
  fi
  if [[ -z "${UDF_JAR:-}" || ! -f "$UDF_JAR" ]]; then
    echo "Cannot find library-udf jar. Set UDF_JAR before running this script." >&2
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
  local log="$LOG_DIR/iotdb-ready-check.log"
  for _ in $(seq 1 60); do
    if run_cli -e "show version" >"$log" 2>&1; then
      echo "IoTDB is ready."
      return
    fi
    sleep 2
  done
  echo "Timed out waiting for IoTDB to become ready. Last CLI output:" >&2
  cat "$log" >&2 || true
  exit 1
}

append_tsmd_series() {
  local file="$1"
  local device="$2"
  local limit="$3"
  awk -F, -v device="$device" -v limit="$limit" '
    NR > 1 && count < limit {
      printf "INSERT INTO %s(timestamp,s1,label) VALUES(%d,%s,%s)\n", device, count + 1, $2, $3
      count++
    }
  ' "$file" >>"$GENERATED_DIR/setup.sql"
}

append_tsmd_refit_slice() {
  local file="$1"
  local start_index="$2"
  local limit="$3"
  awk -F, -v start_index="$start_index" -v limit="$limit" '
    NR > 1 && $1 >= start_index && count < limit {
      printf "INSERT INTO root.locomotif_exp.tsmd_refit(timestamp,s1,label) VALUES(%d,%s,%s)\n", count + 1, $2, $3
      count++
    }
  ' "$file" >>"$GENERATED_DIR/setup.sql"
}

append_pamap2_sample() {
  local file="$1"
  local limit="$2"
  awk -F, -v limit="$limit" '
    NR > 1 && count < limit {
      for (i = 1; i <= NF; i++) {
        if ($i == "NaN" || $i == "") {
          next
        }
      }
      printf "INSERT INTO root.locomotif_exp.pamap2(timestamp,activity_id,hand_acc_x,hand_acc_y,hand_acc_z,chest_acc_x,chest_acc_y,chest_acc_z,ankle_acc_x,ankle_acc_y,ankle_acc_z) VALUES(%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)\n", count + 1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
      count++
    }
  ' "$file" >>"$GENERATED_DIR/setup.sql"
}

append_refit_house1_sample() {
  local file="$1"
  local limit="$2"
  awk -F, -v limit="$limit" '
    NR > 1 && count < limit {
      printf "INSERT INTO root.locomotif_exp.refit_house1(timestamp,aggregate,app1,app2,app3) VALUES(%d,%s,%s,%s,%s)\n", count + 1, $3, $4, $5, $6
      count++
    }
  ' "$file" >>"$GENERATED_DIR/setup.sql"
}

generate_setup_sql() {
  local sql="$GENERATED_DIR/setup.sql"
  {
    echo "create function locomotif as 'org.apache.iotdb.library.dmatch.UDTFLoCoMotif'"
    echo "create database root.locomotif_exp"
    echo "create timeseries root.locomotif_exp.synthetic.s1 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.synthetic.s2 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_mitdb1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_mitdb1.label with datatype=INT32, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_ppg.s1 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_ppg.label with datatype=INT32, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_refit.s1 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.tsmd_refit.label with datatype=INT32, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.activity_id with datatype=INT32, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.hand_acc_x with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.hand_acc_y with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.hand_acc_z with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.chest_acc_x with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.chest_acc_y with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.chest_acc_z with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.ankle_acc_x with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.ankle_acc_y with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.pamap2.ankle_acc_z with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.refit_house1.aggregate with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.refit_house1.app1 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.refit_house1.app2 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
    echo "create timeseries root.locomotif_exp.refit_house1.app3 with datatype=DOUBLE, encoding=PLAIN, compression=UNCOMPRESSED"
  } >"$sql"

  local s1=(0 1 2 1 0 -1 -2 -1 0 1 0 -1)
  local s2=(1 2 1 0 -1 -2 -1 0 1 0 -1 0)
  local t=1
  for _ in $(seq 1 4); do
    for i in $(seq 0 11); do
      echo "INSERT INTO root.locomotif_exp.synthetic(timestamp,s1,s2) VALUES($t,${s1[$i]},${s2[$i]})" >>"$sql"
      t=$((t + 1))
    done
  done

  append_tsmd_series "$DATASET_ROOT/tsmd-real/mitdb1/mitdb1_0.csv" root.locomotif_exp.tsmd_mitdb1 5000
  append_tsmd_series "$DATASET_ROOT/tsmd-real/ptt-ppg/ppg_0.csv" root.locomotif_exp.tsmd_ppg 5000
  append_tsmd_refit_slice "$DATASET_ROOT/tsmd-real/refit/refit_0.csv" 130000 7000
  append_pamap2_sample "$DATASET_ROOT/pamap2/subject101_activity_sample.csv" 3000
  append_refit_house1_sample "$DATASET_ROOT/refit/CLEAN_House1_head_20000.csv" 5000

  echo "quit" >>"$sql"
}

run_setup_sql() {
  echo "Running setup SQL: $GENERATED_DIR/setup.sql"
  : >"$LOG_DIR/setup.log"
  run_cli -e "drop function locomotif" >>"$LOG_DIR/setup.log" 2>&1 || true
  run_cli -e "drop database root.locomotif_exp" >>"$LOG_DIR/setup.log" 2>&1 || true
  run_cli <"$GENERATED_DIR/setup.sql" >>"$LOG_DIR/setup.log" 2>&1
}

run_query() {
  local name="$1"
  local description="$2"
  local sql="$3"
  local output="$RESULT_DIR/$name.log"
  local start
  local end
  local query_exit=0
  start=$(date +%s)
  {
    echo ">>> $sql"
    run_cli -e "$sql"
  } >"$output" 2>&1 || query_exit=$?
  local status="PASS"
  if [[ "$query_exit" != "0" ]] || ! grep -q '"motifId"' "$output"; then
    status="FAIL"
  fi
  end=$(date +%s)
  local motif_rows
  motif_rows=$( (grep -o '"motifId"' "$output" || true) | wc -l | tr -d ' ')
  local cli_cost
  cli_cost=$(grep 'It costs' "$output" | tail -1 | sed 's/.*It costs //; s/[[:space:]]*$//')
  local sample_json
  sample_json=$(grep -m 1 '{"motifId"' "$output" | sed 's/^.*|{"motifId"/{"motifId"/; s/|$//' || true)
  printf '%s,%s,%s,%s,%s,"%s"\n' "$name" "$status" "$motif_rows" "$((end - start))" "${cli_cost:-unknown}" "$description" >>"$SUMMARY_CSV"
  printf '%s\n' "$sample_json" >"$RESULT_DIR/$name.sample.json"
}

generate_report() {
  {
    echo "# LoCoMotif UDF 功能实验结果"
    echo
    echo "## 实验准备"
    echo
    echo "- Apache IoTDB: 2.0.7-SNAPSHOT 本地 all-bin/cli-bin"
    echo "- UDF: \`org.apache.iotdb.library.dmatch.UDTFLoCoMotif\`，注册名 \`locomotif\`"
    echo "- 输出目录: \`$RUN_DIR\`"
    echo "- 数据集:"
    echo "  - TSMD Benchmark Real Collection: \`mitdb1_0.csv\`, \`ppg_0.csv\`, \`refit_0.csv\` slice"
    echo "  - PAMAP2: \`subject101_activity_sample.csv\`，活动 ID 4/5/12/13 的 IMU 加速度子集"
    echo "  - REFIT: \`CLEAN_House1_head_20000.csv\` 中前 5000 行"
    echo
    echo "## 实验结果"
    echo
    echo "| 实验 | 状态 | 返回 motif 行数 | Shell 耗时(s) | CLI 统计耗时 | 说明 |"
    echo "|---|---:|---:|---:|---:|---|"
    tail -n +2 "$SUMMARY_CSV" | awk -F, '{gsub(/^"|"$/, "", $6); printf "| `%s` | %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5, $6}'
    echo
    echo "## 结论"
    echo
    echo "- 合成重复片段用于验证基础 UDF 注册、窗口访问和 JSON 输出链路。"
    echo "- TSMD ECG/PPG 用于验证 LoCoMotif 在真实生理周期信号上发现重复 motif 的能力。"
    echo "- PAMAP2 使用 9 个 IMU 加速度通道，验证多变量输入路径。"
    echo "- REFIT 使用电力负载序列，验证长周期/稀疏重复模式场景。"
    echo
    echo "## 结果文件"
    echo
    echo "- Setup SQL: \`generated/setup.sql\`"
    echo "- Setup log: \`logs/setup.log\`"
    echo "- Summary CSV: \`summary.csv\`"
    echo "- Query logs: \`results/*.log\`"
  } >"$REPORT_MD"
  cp "$REPORT_MD" "$LATEST_REPORT"
}

echo "Writing LoCoMotif functional experiment to $RUN_DIR"
trap stop_iotdb EXIT

generate_setup_sql
install_udf_jar
start_iotdb
wait_iotdb_ready
run_setup_sql

printf 'name,status,motif_rows,shell_seconds,cli_cost,description\n' >"$SUMMARY_CSV"
run_query synthetic_pair "合成数据；4 次重复长度 12 motif；双变量；关闭 warping" \
  'select locomotif(s1, s2, "l_min"="12", "l_max"="12", "rho"="0.6", "nb"="3", "window"="48", "step"="48", "warping"="false") from root.locomotif_exp.synthetic'
run_query tsmd_mitdb1_ecg "TSMD mitdb1_0；单变量 ECG；窗口 5000" \
  'select locomotif(s1, "l_min"="216", "l_max"="450", "rho"="0.6", "nb"="2", "window"="5000", "step"="5000", "max_points"="6000", "warping"="true") from root.locomotif_exp.tsmd_mitdb1'
run_query tsmd_ppg "TSMD ppg_0；单变量 PPG；窗口 5000" \
  'select locomotif(s1, "l_min"="250", "l_max"="450", "rho"="0.6", "nb"="2", "window"="5000", "step"="5000", "max_points"="6000", "warping"="true") from root.locomotif_exp.tsmd_ppg'
run_query pamap2_multivariate "PAMAP2 subject101；9 维 IMU 加速度；窗口 3000" \
  'select locomotif(hand_acc_x, hand_acc_y, hand_acc_z, chest_acc_x, chest_acc_y, chest_acc_z, ankle_acc_x, ankle_acc_y, ankle_acc_z, "l_min"="80", "l_max"="240", "rho"="0.55", "nb"="2", "window"="3000", "step"="3000", "max_points"="3500", "warping"="true") from root.locomotif_exp.pamap2'
run_query tsmd_refit_load "TSMD REFIT refit_0 motif 区间；单变量负载；窗口 7000" \
  'select locomotif(s1, "l_min"="280", "l_max"="450", "rho"="0.25", "nb"="3", "window"="7000", "step"="7000", "max_points"="7500", "warping"="true") from root.locomotif_exp.tsmd_refit'
run_query refit_house1_load "REFIT House1 原始清洗数据前 5000 行；总功率和 3 个电器通道" \
  'select locomotif(aggregate, app1, app2, app3, "l_min"="120", "l_max"="600", "rho"="0.3", "nb"="2", "window"="5000", "step"="5000", "max_points"="5500", "warping"="true") from root.locomotif_exp.refit_house1'

generate_report
echo "LoCoMotif functional experiment report: $REPORT_MD"
