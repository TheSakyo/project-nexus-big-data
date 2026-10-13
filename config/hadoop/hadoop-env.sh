export JAVA_HOME=/opt/temurin-17.0.20.1
export HADOOP_LOG_DIR=/home/turinguser/nexus-hadoop-data/logs
export HADOOP_HEAPSIZE_MAX=128
export HDFS_NAMENODE_OPTS="-Xmx128m ${HDFS_NAMENODE_OPTS:-}"
export HDFS_DATANODE_OPTS="-Xmx96m ${HDFS_DATANODE_OPTS:-}"
