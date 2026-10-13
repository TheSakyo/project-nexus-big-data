# Project plan — Project Nexus

This plan follows the assignment requirements and the [official data catalog](DATA_CATALOG.md). The delivered files, their volumes, and discrepancies from the catalog still need to be checked after the download.

## Status and working rule

The dataset is downloading outside the repository. Do not read, move, rename, or process data files until the user confirms the download is complete. The separate official `DATA_CATALOG.md` has been reviewed and copied into the repository. Code, documentation, and configuration work takes place in `D:\Workspaces\java-workspace\JavaHadoop`.

## Phases and exit criteria

| Phase | Actions | Exit criteria |
|---|---|---|
| 1. Baseline | Review the repository, assignment, Java, Maven, Docker/Compose, WSL, and Hadoop options. | Current state is documented and pre-existing Git changes are preserved. |
| 2. Data inventory | After confirmation, record files, formats, sizes, partitions, empty files, and errors; compare against the official catalog. | Python tool is ready (`python scripts/nexus.py inventory`); the real inventory and discrepancies are established. |
| 3. Environment | Compare available options by memory, storage, network, and Hadoop/Java compatibility. | Prepared: Debian `turing-server`, Hadoop 3.5.0, server Java 17, and a successful HDFS/YARN/MapReduce smoke job. |
| 4. Data lake and ingestion | Define `RAW`, `PROCESSED`, and `ANALYTICS`; prepare restartable ingestion that preserves originals. | Every batch is traceable; processing never changes `RAW`. |
| 5. Quality and preparation | Define rules from observed schemas: missing fields, dates, formats, duplicates, and identifiers; count rejects and normalizations. | `PROCESSED` outputs have documented checks and rules. |
| 6. MapReduce jobs | Build at least two meaningful, reproducible Java jobs. | Jobs run with explicit inputs and outputs and verified results. |
| 7. Analysis and joins | Study activity/retention, choose a business question after inspection, join at least two sources, and perform open-ended exploration. | Definitions, populations, windows, results, limitations, and confidence are documented. |
| 8. Scalability | Set a Java memory limit, process more data than that limit, and record Hadoop counters and logs. | Volume, limit, duration, and metrics are recorded; no performance claims without measurement. |
| 9. Deliverables | Finalize README, scripts, configuration, verified results, report, and presentation. | Templates are ready in [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md) and [PRESENTATION_OUTLINE.md](PRESENTATION_OUTLINE.md); another person can reproduce the workflow and understand the recommendations. |

Dataset-independent preparation is complete: Maven Wrapper, Hadoop configuration, and the Python inventory/cluster tool. See [PREPARATION.md](PREPARATION.md).

## Analysis tracks to define

### Track 1 — Activity and retention

Starting point from the catalog: define an active player as having at least one `session_start` on a UTC day. Measure DAU/WAU and retention by installation week (`players.created_at`), joining telemetry `player_id`/`pid` to `players._id`. Normalize telemetry schema v1/v2 and timestamp formats; check missing IDs, unmatched events, and incomplete final days. These choices remain provisional until the real inventory.

### Track 2 — Business question

Recommended technical track: compare `crash`/`disconnect` events by game version (`game_version`/`ver`) and hardware profile (`players.profile.platform`, `gpu_vendor`, `gpu_model`, `os`), joining on player ID. Inform which versions or configurations to investigate first. This is a descriptive association, not causal proof; account for group sizes, schema versions, and exposure differences. Confirm fields and volumes during inventory.

### Track 3 — Open-ended exploration

Candidate: abandonment (`level_abandon` or `match_end.outcome=abandon`) by map, mode, region, or version. Explore the announced fields (`map_id`, `mode`, `region`, `duration_sec`) after checking denominators and invalid values. Keep this track open if the files or group sizes do not support a reliable comparison.

## Decisions still pending

- The preparation environment is Hadoop 3.5.0 on `turing-server` in single-node pseudo-distributed mode; this is not production capacity.
- Select the physical source path and ingestion strategy after the user confirms the download is complete.
- HDFS zones `/datalake/raw`, `/datalake/processed`, and `/datalake/analytics` exist; internal partitioning depends on the observed sources.
- Check delivered files against the formats and schema variants announced in the catalog.
- Confirm the availability and quality of join keys in the actual data.
- Determine required memory and the volume for the scale experiment.

The synthetic smoke job validates HDFS → YARN → MapReduce, but does not use project data. The VM has about 848 MiB RAM; real jobs may require another host or more resources.

## Reproducibility safeguards

- Never put passwords or tokens in the repository, logs, or versioned commands.
- Keep large files and generated outputs out of Git.
- Make jobs restartable without overwriting `RAW`.
- Record commands, parameters, versions, volumes, and observed results.
- Never present synthetic or miniature results as findings from the real dataset.
