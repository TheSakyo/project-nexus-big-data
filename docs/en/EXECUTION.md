# Installing and running the project

This guide separates building the code from running MapReduce jobs. Building the JAR does not require Hadoop; running a job always requires a Hadoop cluster with HDFS and YARN.

## Choose a mode

| Mode | Install on the workstation | Where Hadoop runs | Status in this repository |
|---|---|---|---|
| Manual | JDK 17, Python 3.9+, Hadoop 3.5.0 and its dependencies | Linux, or Linux under WSL2 on Windows | Available on the maintainer's WSL. A different remote cluster requires SSH access and adaptation of server-specific paths. |
| Docker Compose | Docker Desktop on Windows/macOS, or Docker Engine + Compose on Linux; Python 3.9+ and JDK 17 for local commands/build | A local Hadoop 3.5.0 container with HDFS/YARN | Available through `compose.yaml` and the Compose backend in `nexus.py`. No access to the maintainer's WSL is needed. |

Compose is the recommended route for collaborators: it starts a local pseudo-distributed cluster and stores HDFS data in a named Docker volume. The JAR is built on the workstation with the Maven Wrapper; Hadoop does not need to be installed there.

The image, empty cluster, and scripts can be prepared and started while the dataset downloads. The container does not mount the download directory or read source data.

## Shared prerequisites

- Git to clone the repository.
- Java 17 to build the JAR on the host. Java 21 also works for compilation; the Hadoop 3.5.0 server must use Java 17.
- Python 3.9 or later for `scripts/nexus.py`.
- For Compose mode: Docker Engine with Docker Compose v2.17 or later, a running engine, and about 6 GiB of memory available to the container.
- The Maven Wrapper is included (`mvnw` and `mvnw.cmd`), so a separate Maven installation is not required. It downloads Maven and dependencies on the first build, so Internet access is needed.
- Input data in the expected format, then loaded into HDFS. Docker Compose does not provide the dataset.

Check the tools:

```sh
git --version
java -version
python --version
```

On Linux/macOS, use `python3 --version` if needed.

## Method 1 — manual installation

### Platform support

| Operating system | Build and inventory | Manual Hadoop for jobs |
|---|---|---|
| Windows | Directly in PowerShell with JDK 17+ and Python 3.9+ | Use WSL2 with a Linux distribution. Native Windows Hadoop is not the workflow supported by Apache or by this repository's scripts. |
| GNU/Linux | JDK 17+, Python 3.9+, and OpenSSH if the cluster is remote | Install and configure Hadoop 3.5.0 on Linux. This is the recommended manual route. |
| macOS | JDK 17+ and Python 3.9+ | Hadoop may need machine-specific adjustments; this repository does not provide a native macOS configuration. Prefer a Linux VM or Docker Compose. |

The Apache [single-node cluster setup](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html) describes pseudo-distributed HDFS/YARN installation. Hadoop 3.5.0 requires Java 17 server-side according to the [Apache release notes](https://hadoop.apache.org/docs/r3.5.0/).

On Debian/Ubuntu (including WSL), the base dependencies can be installed with:

```sh
sudo apt update
sudo apt install openjdk-17-jdk python3 openssh-client
```

Then download the Hadoop 3.5.0 distribution from Apache, verify its SHA-512 checksum, extract it into a Linux path, set `JAVA_HOME`/`HADOOP_HOME`, and configure the four HDFS/YARN XML files. Follow the Apache guide, including the one-time HDFS initialization. The maintainer's settings under `config/hadoop/` are specific to their WSL environment and are not a ready-to-use configuration for another OS.

### Build and inventory

From the repository root:

```sh
python scripts/nexus.py build
python scripts/nexus.py inventory --root /path/to/dataset
```

On Windows, the dataset path can look like `D:\\datasets\\nexus`. The build automatically uses `mvnw.cmd`. On Linux/macOS/WSL, the script uses `./mvnw`.

Inventory works without Hadoop and does not read file contents unless `--sha256` is requested. Follow the [dataset preparation guide](PREPARATION.md); do not inventory the directory while it is downloading.

### Run on a manually configured cluster

To use the current `cluster` and `job` commands, a collaborator needs:

1. a Hadoop 3.5.0 cluster with Java 17, HDFS, and YARN running;
2. an SSH account on that host and the OpenSSH `ssh` and `scp` clients locally;
3. an SSH target configured in `~/.ssh/config`;
4. the remote configuration, data, and installation paths expected by the script, or changes to the script for that environment.

Example personal SSH target (replace the host and account):

```sshconfig
Host my-hadoop
    HostName hadoop.example.org
    User my-user
    IdentityFile ~/.ssh/id_ed25519
```

Then, from the repository:

```sh
python scripts/nexus.py cluster status --target my-hadoop
python scripts/nexus.py job activity --target my-hadoop \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

**Current limitation:** `--target` changes only the SSH host. `scripts/nexus.py` also contains remote paths and Java/Hadoop installation locations specific to the maintainer's environment (`/home/turinguser/...`, `/opt/...`). Changing only `--target` does not make the script work with an arbitrary cluster. Those constants/configuration must be adapted, or made configurable in a future change.

If Hadoop is installed locally in a Linux distribution rather than on an SSH host, the current SSH commands are not the right backend. Start Hadoop locally and submit the JAR with that cluster's `hdfs`/`hadoop` commands.

## Method 2 — Docker Compose

### Planned dependencies

- Windows or macOS: Docker Desktop with its engine running; Compose is included.
- GNU/Linux: Docker Engine, Docker CLI, and the Docker Compose plugin.
- Python 3.9+ and JDK 17 are used on the host by `nexus.py` and the Maven Wrapper. Hadoop, its binaries, HDFS, and YARN run in the container.
- The service requests 6 GiB of memory from Docker by default. Adjust `NEXUS_HADOOP_MEMORY_LIMIT` and YARN resources only if you know how to lower those requirements.
- The dataset still has to be downloaded and loaded into HDFS separately; it is not needed to build or start the empty cluster.

The official [Compose installation guide](https://docs.docker.com/compose/install/) covers each platform. On Windows, Docker Desktop can use WSL2 as its backend, but collaborators will not need to install Hadoop inside their WSL distribution: Hadoop will run in containers.

### Start, build, and stop

From the repository root, the first command builds the Hadoop 3.5.0 Linux image, verifies the Apache archive's SHA-512 checksum, starts HDFS/YARN, and waits for the service health check:

```sh
python scripts/nexus.py cluster start --backend compose
```

Check the service and build the JAR on the host:

```sh
python scripts/nexus.py cluster status --backend compose
python scripts/nexus.py build
```

The local web interfaces are `http://localhost:9870/` for HDFS and `http://localhost:8088/` for YARN. They bind to `127.0.0.1` and are not exposed to the local network.

The named Docker volume preserves HDFS when the container is stopped. Stop the service without removing its data:

On first startup, the entrypoint formats only an empty NameNode volume; subsequent starts reuse its metadata and HDFS files.

```sh
python scripts/nexus.py cluster stop --backend compose
```

This command stops the container but keeps the built image and HDFS volume. On the next `cluster start`, Compose normally reuses cached image layers and the same HDFS files.

**Warning:** do not use `docker compose down --volumes` or remove the project's Docker volume if you want to keep HDFS files. The `--volumes` option deletes the data persisted in that volume.

### Check both backends one at a time

These commands check the build and startup of both environments without running their Hadoop clusters simultaneously. They do not submit jobs and do not require the dataset. Run each command separately and wait for the terminal prompt before continuing. Do not stop the SSH cluster while a job is active.

From the repository root, first check Docker (the output must include `Client` and `Server`) and build the JAR:

```sh
docker version
python scripts/nexus.py build
```

Check and then stop the maintainer's WSL Hadoop services:

```sh
python scripts/nexus.py cluster status --backend ssh
python scripts/nexus.py cluster stop --backend ssh
```

Start, check, and then stop the Compose cluster. Stopping it preserves its HDFS volume:

```sh
python scripts/nexus.py cluster start --backend compose
python scripts/nexus.py cluster status --backend compose
python scripts/nexus.py cluster stop --backend compose
```

Finally, restart and check the WSL Hadoop services:

```sh
python scripts/nexus.py cluster start --backend ssh
python scripts/nexus.py cluster status --backend ssh
```

The SSH/WSL and Compose HDFS stores are independent: a file in one is not automatically available in the other. The SSH status command may take a few seconds to report an unavailable service; wait for it to finish before entering another command.

### Load inputs into HDFS

After the download finishes, copy an input directory into the container and then put it in HDFS. Adapt the example to the actual path:

```sh
docker compose cp "LOCAL_TELEMETRY_PATH" hadoop:/tmp/telemetry
docker compose exec -T hadoop hdfs dfs -put /tmp/telemetry /datalake/raw/telemetry
```

For a very large source, this copy adds a staging step; check local free space before duplicating data. Keep downloaded originals outside Git.

### Submit a job

Business arguments stay the same; `--backend compose` executes the Java stages in the container instead of using SSH/SCP:

```sh
python scripts/nexus.py job activity --backend compose \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

The Compose cluster will be a single-node pseudo-distributed environment for learning and development runs. It does not replace a distributed cluster or, by itself, support a multi-node scalability measurement.

## Are the Python scripts still useful with Compose?

Yes, but cluster-access commands need an adapter:

| Command/capability | Current manual SSH mode | With Compose after integration |
|---|---|---|
| `inventory` | Usable from the host | Usable from the host; raw data remains outside the image/container. |
| `build` | Builds the JAR with the local JDK | Can build inside the container to avoid a local JDK. |
| `cluster status/start/stop` | Manages the maintainer's cluster over SSH | Manages the container with Docker Compose; `stop` preserves the HDFS volume. |
| `job` | Copies the JAR over SCP and submits it to the SSH host | Submits the same stages in the container; date/path validation and existing-output checks are shared. |

The scripts remain the shared entry point. Inventory runs locally, build uses the workstation JDK, and each cluster/job command explicitly selects `--backend ssh` or `--backend compose`. Transport details are isolated by mode.

## Shared limitations

- The real dataset is external to the repository; users must obtain it, inventory its formats, and load inputs into HDFS.
- The jobs expect player profiles in JSONL. If MongoDB arrives only as BSON, export it to JSONL after inspecting the delivered data.
- A local single-node cluster is for educational development, not for exposing HDFS/YARN to the Internet or guaranteeing production performance.
- Never run `hdfs namenode -format` on a volume that contains existing data: formatting resets NameNode metadata.

## Resources

- [Official Apache guide — Hadoop 3.5.0 single-node cluster](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Official Apache guide — Hadoop Docker](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/HadoopDocker.html)
- [Official Docker documentation — Compose installation](https://docs.docker.com/compose/install/)
- [Dataset preparation guide](PREPARATION.md)
