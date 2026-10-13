FROM eclipse-temurin:17-jdk-jammy

ARG HADOOP_VERSION=3.5.0
ARG HADOOP_ARCHIVE_BASE=https://archive.apache.org/dist/hadoop/common
ARG TARGETARCH

ENV JAVA_HOME=/opt/java/openjdk \
    HADOOP_HOME=/opt/hadoop-3.5.0 \
    HADOOP_CONF_DIR=/opt/nexus-hadoop-conf \
    HADOOP_MAPRED_HOME=/opt/hadoop-3.5.0 \
    HADOOP_LOG_DIR=/data/hadoop/logs \
    HADOOP_PID_DIR=/tmp/hadoop-pids \
    PATH=/opt/hadoop-3.5.0/bin:/opt/hadoop-3.5.0/sbin:/opt/java/openjdk/bin:${PATH}

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) HADOOP_ARCHIVE="hadoop-${HADOOP_VERSION}.tar.gz" ;; \
         arm64) HADOOP_ARCHIVE="hadoop-${HADOOP_VERSION}-aarch64.tar.gz" ;; \
         *) echo "Unsupported Docker architecture: ${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl --fail --location --retry 3 --output /tmp/hadoop.tar.gz \
       "${HADOOP_ARCHIVE_BASE}/hadoop-${HADOOP_VERSION}/${HADOOP_ARCHIVE}" \
    && curl --fail --location --retry 3 --output /tmp/hadoop.tar.gz.sha512 \
       "${HADOOP_ARCHIVE_BASE}/hadoop-${HADOOP_VERSION}/${HADOOP_ARCHIVE}.sha512" \
    && cd /tmp \
    && expected_checksum="$(awk 'NR == 1 { print $NF; exit }' hadoop.tar.gz.sha512)" \
    && printf '%s  %s\n' "${expected_checksum}" /tmp/hadoop.tar.gz | sha512sum --check --status \
    && tar -xzf hadoop.tar.gz -C /opt \
    && rm -f /tmp/hadoop.tar.gz /tmp/hadoop.tar.gz.sha512 \
    && groupadd --system nexus \
    && useradd --system --gid nexus --home-dir /home/nexus --create-home --shell /usr/sbin/nologin nexus \
    && mkdir -p /opt/nexus-hadoop-conf /data/hadoop /workspace \
    && chown -R nexus:nexus /data/hadoop /home/nexus

COPY docker/hadoop/conf/ /opt/nexus-hadoop-conf/
COPY docker/hadoop/entrypoint.sh /usr/local/bin/nexus-hadoop-entrypoint

RUN chmod 0755 /usr/local/bin/nexus-hadoop-entrypoint \
    && chown -R root:root /opt/nexus-hadoop-conf \
    && chmod -R a+rX /opt/nexus-hadoop-conf

WORKDIR /workspace
USER nexus

ENTRYPOINT ["/usr/local/bin/nexus-hadoop-entrypoint"]
