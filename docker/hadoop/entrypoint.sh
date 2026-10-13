#!/usr/bin/env bash
set -Eeuo pipefail

DATA_ROOT="${HADOOP_DATA_ROOT:-/data/hadoop}"
NAME_DIR="${DATA_ROOT}/name"

mkdir -p \
  "${DATA_ROOT}/tmp" \
  "${DATA_ROOT}/name" \
  "${DATA_ROOT}/data" \
  "${DATA_ROOT}/yarn-local" \
  "${DATA_ROOT}/yarn-logs" \
  "${HADOOP_LOG_DIR}" \
  "${HADOOP_PID_DIR}"

if [[ ! -f "${NAME_DIR}/current/VERSION" ]]; then
  echo "Formatting the empty HDFS NameNode volume for its first run."
  hdfs namenode -format -nonInteractive
fi

hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

shutdown_hadoop() {
  echo "Stopping Hadoop daemons."
  yarn --daemon stop nodemanager || true
  yarn --daemon stop resourcemanager || true
  hdfs --daemon stop datanode || true
  hdfs --daemon stop namenode || true
}

trap shutdown_hadoop EXIT TERM INT

ready=false
for attempt in $(seq 1 90); do
  if hdfs dfsadmin -report >/dev/null 2>&1 \
    && yarn node -list 2>/dev/null | grep -q RUNNING; then
    ready=true
    break
  fi
  sleep 2
done

if [[ "${ready}" != true ]]; then
  echo "Hadoop did not become ready. Inspect the container logs with 'docker compose logs hadoop'." >&2
  exit 1
fi

hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /user/nexus /datalake/raw /datalake/processed /datalake/analytics
echo "Hadoop 3.5.0 is ready. HDFS UI: http://localhost:9870/ ; YARN UI: http://localhost:8088/"

tail -F "${HADOOP_LOG_DIR}"/*.log &
log_tail_pid=$!
wait "${log_tail_pid}"
