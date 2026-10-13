# Project Nexus — Big Data

An educational project for processing Nexus Interactive data with Hadoop and Java MapReduce.

## Goals

- Preserve original sources in a `RAW` zone.
- Clean and prepare data in `PROCESSED`.
- Produce metrics in `ANALYTICS`.
- Implement at least two meaningful Java MapReduce jobs, including a cross-source join.
- Document data-quality checks, results, limitations, and a measured scalability experiment.

## Current status

- The Maven skeleton and wrapper are ready; Hadoop configuration is versioned under `config/hadoop/`.
- The dataset is still downloading to a directory outside the repository.
- Sources and sizes will be inventoried after the user confirms the download is complete.
- Java MapReduce jobs for DAU/WAU, retention, and technical incidents are prepared; no project-data outputs have been calculated.
- Hadoop 3.5.0, HDFS, and YARN are installed in the maintainer's local WSL Linux environment, reachable through their `turing-server` SSH alias; an end-to-end WordCount smoke job succeeded on synthetic data. This alias is not shared with collaborators.
- HDFS zones `/datalake/raw`, `/datalake/processed`, and `/datalake/analytics` have been created.
- Cross-platform inventory and cluster-management tools are available in `scripts/nexus.py`.

Analytical findings will only be documented after they have been run and verified against the real data.

## Guides

- [Cross-platform installation and execution](EXECUTION.md)
- [Full project plan](PLAN.md)
- [Environment status](ENVIRONMENT.md)
- [Preparation during download](PREPARATION.md)
- [Official data catalog](DATA_CATALOG.md)
- [Data catalog template](DATA_CATALOG_TEMPLATE.md)
- [Final report template](REPORT_TEMPLATE.md)
- [10-minute defense outline](PRESENTATION_OUTLINE.md)

## Build and run

Use the Maven Wrapper: `./mvnw` on Linux/macOS/WSL or `mvnw.cmd` on Windows. The project targets Java 17 bytecode for compatibility with the selected Hadoop services.

The preparation cluster has only about 848 MiB of RAM. Its YARN and MapReduce settings support small smoke jobs; capacity for real processing will be assessed from the dataset inventory.

## Data and secrets

Do not add raw data, passwords, tokens, or large exports to the repository. Keep credentials in a local secret store and document their use without publishing their values.
