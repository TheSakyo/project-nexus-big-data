export JAVA_HOME=/opt/temurin-17.0.20.1
export YARN_LOG_DIR=/home/turinguser/nexus-hadoop-data/yarn-logs
export YARN_HEAPSIZE=96
export YARN_RESOURCEMANAGER_OPTS="-Xmx96m ${YARN_RESOURCEMANAGER_OPTS:-}"
export YARN_NODEMANAGER_OPTS="-Xmx96m ${YARN_NODEMANAGER_OPTS:-}"
