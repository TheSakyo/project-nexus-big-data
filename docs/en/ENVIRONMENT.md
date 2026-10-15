# Development environment

Initial status recorded on October 13, 2026; Compose and dataset location updated on October 15, 2026.

## Workstation

| Component | Status | Use |
|---|---|---|
| Windows Java / `javac` | OpenJDK 21.0.1 | Local tools and compilation. |
| Maven Wrapper | Maven 3.9.9, wrapper 3.3.4 | Use `mvnw.cmd` on Windows or `./mvnw` on Linux/macOS/WSL. |
| WSL `debian-default` | Debian 13, OpenJDK 21.0.12, Maven 3.9.9 | Linux build environment is ready. |
| Docker Desktop | Hadoop 3.5.0 Compose cluster was healthy when checked on October 15, 2026. | Local Compose cluster runtime. |

A Docker Compose Hadoop setup at the repository root was started and verified healthy on October 15, 2026. Docker Desktop must be running to start or manage the cluster.

The C: system drive lost free space during WSL installations and downloads. Avoid downloading or extracting Hadoop or the dataset there. The download is complete; the local copy is under `D:\Workspaces\java-workspace\JavaHadoop\data\source\` and is excluded from Git.

## Maintainer's local Hadoop cluster

The Debian cluster runs in the maintainer's local WSL environment. `turing-server` is their personal SSH alias to that environment; it is not a shared remote server and collaborators cannot use it as-is.

| Component | Status |
|---|---|
| Server Java | Eclipse Temurin 17.0.20.1 at `/opt/temurin-17.0.20.1`. |
| Hadoop | Apache Hadoop 3.5.0 at `/opt/hadoop-3.5.0`. |
| Configuration | `/home/turinguser/hadoop-setup/conf`; version-controlled reference in `config/hadoop/`. |
| HDFS | NameNode and DataNode running; `/datalake/raw`, `/datalake/processed`, and `/datalake/analytics` zones created. |
| YARN | ResourceManager and NodeManager running; one node is `RUNNING`. |
| Network | Service interfaces are bound to `127.0.0.1`. |
| HDFS storage | About 113 GiB was available at the time of inspection. |
| End-to-end MapReduce smoke job | Synthetic WordCount succeeded on October 13, 2026; output: `hadoop 2`, `mapreduce 1`, `nexus 2`. |

The VM has only about 848 MiB of RAM. YARN resources are limited and do not indicate expected performance on the real dataset. MapReduce sort buffering is set to 16 MiB so the synthetic job can run with the available Java heap. This smoke job validates HDFS/YARN/MapReduce, not processing of the project data.

Checksum-verified installation archives are kept in `/home/turinguser/hadoop-setup`. Do not remove archives or data directories without instruction.

## Data handling rules

- The download is complete; the project source copy is in `data/source/`.
- The local inventory is stored under `reports/local/`; full quality checks and HDFS ingestion remain to be completed.
- Check available storage before ingesting into HDFS; keep the source copy unchanged and excluded from Git.
- Keep downloaded data out of Git; `data/`, `datalake/`, `datasets/`, and `reports/local/` are ignored.
