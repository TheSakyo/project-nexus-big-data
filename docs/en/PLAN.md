# Project plan — Project Nexus

This plan follows the assignment requirements and the [official data catalog](DATA_CATALOG.md). The download and file inventory are complete; detailed schema and quality checks remain to be performed.

## Status and working rule

The complete dataset is under `data/source/`, which is excluded from Git. The local inventory is stored under `reports/local/`. The official `DATA_CATALOG.md` was reviewed and copied into the repository. Code, documentation, and configuration work takes place in `D:\Workspaces\java-workspace\JavaHadoop`.

## Phases and exit criteria

| Phase | Actions | Exit criteria |
|---|---|---|
| 1. Baseline | Review the repository, assignment, Java, Maven, Docker/Compose, WSL, and Hadoop options. | Current state is documented and pre-existing Git changes are preserved. |
| 2. Data inventory | Record files, formats, sizes, partitions, empty files, and errors; compare against the official catalog. | Local inventory is complete and manifests were compared; detailed data-quality rules remain to be validated during processing. |
| 3. Environment | Compare available options by memory, storage, network, and Hadoop/Java compatibility. | WSL environment verified with a smoke job; Docker Compose Hadoop 3.5.0 cluster reported healthy on October 15, 2026. These local environments are not shared. |
| 4. Data lake and ingestion | Define `RAW`, `PROCESSED`, and `ANALYTICS`; prepare restartable ingestion that preserves originals. | Every batch is traceable; processing never changes `RAW`. |
| 5. Quality and preparation | Define rules from observed schemas: missing fields, dates, formats, duplicates, and identifiers; count rejects and normalizations. | `PROCESSED` outputs have documented checks and rules. |
| 6. MapReduce jobs | Normalize announced telemetry schemas; produce daily active players, DAU/WAU, Dn retention, and profile-joined technical analysis. | Java jobs are present and compile; real executions and results still need to be produced on the dataset. |
| 7. Analysis and joins | Study activity/retention, choose a business question after inspection, join at least two sources, and perform open-ended exploration. | Definitions, populations, windows, results, limitations, and confidence are documented. |
| 8. Scalability | Set a Java memory limit, process more data than that limit, and record Hadoop counters and logs. | Volume, limit, duration, and metrics are recorded; no performance claims without measurement. |
| 9. Deliverables | Finalize README, scripts, configuration, verified results, report, and presentation. | Templates are ready in [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md) and [PRESENTATION_OUTLINE.md](PRESENTATION_OUTLINE.md); another person can reproduce the workflow and understand the recommendations. |

## Stable work completed without the data

- A Java parser normalizes announced telemetry v1/v2 fields and timestamp formats; jobs count invalid input records.
- `DailyActivePlayersJob` deduplicates players by UTC day using `session_start`.
- `ActivityMetricsJob` produces DAU and rolling seven-calendar-day UTC WAU; it excludes windows before the supplied observation start date.
- `PlayerCohortJoinJob` joins `players._id` to telemetry `player_id`/`pid` and attaches activity dates to `created_at`. `CohortRetentionMetricsJob` computes Dn with a denominator limited to players whose Dn is fully observable.
- `TechnicalIncidentsByProfileJob` joins `session_start`, `crash`, and `disconnect` events to player profiles; `TechnicalIncidentAggregationJob` calculates counts and the `crash/session_start` ratio by day, version, platform, and GPU vendor.
- `python scripts/nexus.py build` packages the JAR; `python scripts/nexus.py job ...` submits pipelines to the cluster over SSH. These preparation commands do not transfer or read dataset files.

The jobs expect player JSONL snapshots. If the delivery contains BSON only, a MongoDB-to-JSONL export still needs to be prepared for the tools and access actually available. Commands and metric limitations are documented in [PREPARATION.md](PREPARATION.md).

## Analysis tracks to define

| Track | Question and decision to inform | Stakeholders / sources | Expected result and possible action | Main risk |
|---|---|---|---|---|
| Activity / retention | Is activity or retention changing across days and cohorts enough to warrant product investigation? | Product and studio leadership; `session_start` telemetry, player `created_at`. | DAU/WAU and Dn retention curves; investigate a break or weak cohort. | Incomplete coverage, missing events, or a biased activity definition. |
| Technical | Do crashes vary by version and hardware profile in a way that can prioritize QA investigations? | Engineering and QA; telemetry and player `profile`. | Crash/session counts by date, version, platform, and GPU; reproduce the most frequent cases. | Different exposure, small groups, missing profiles, and duplicate events. |
| Open-ended exploration | Do abandonment rates vary unusually by map or game mode? | Gameplay design and product; `match_start`, `match_end`, `level_abandon`, map/level references. | Rates by map/mode with explicit denominators; investigate the most unusual encounters. | Incomplete denominator, uninstrumented abandonment, or confusing association with causation. |

### Track 1 — Activity and retention

Implemented definitions: an active player has at least one `session_start` per UTC day; cohorts are grouped by the Monday of the UTC week of `players.created_at`; Dn means activity on the exact calendar day n after account creation. Dn days that are not yet observable at the end date are excluded from the denominator. The actual telemetry start date is needed to exclude players installed before observed coverage.

### Track 2 — Business question

Prepared technical track: compare `crash`/`disconnect` counts and the crash / `session_start` ratio by game version (`game_version`/`ver`), platform, and GPU vendor (`players.profile.platform`, `gpu_vendor`), joined by player ID. This is a descriptive association, not causal proof. The code does not discard repeated `event_id` values: first measure how IDs are reused, then choose a justified deduplication rule.

### Track 3 — Open-ended exploration

Candidate: abandonment (`level_abandon` or `match_end.outcome=abandon`) by map, mode, region, or version. Explore the announced fields (`map_id`, `mode`, `region`, `duration_sec`) after checking denominators and invalid values. Keep this track open if the files or group sizes do not support a reliable comparison.

## Decisions still pending

- The maintainer's preparation environment uses Hadoop 3.5.0 under WSL in single-node pseudo-distributed mode; the `turing-server` alias is local and this environment is not shared. This is not production capacity.
- Source path is confirmed as `data/source/`; the HDFS ingestion strategy remains to be defined.
- HDFS zones `/datalake/raw`, `/datalake/processed`, and `/datalake/analytics` exist; internal partitioning depends on the observed sources.
- Check delivered files against the catalog's formats and schema variants; the prepared jobs require player JSONL snapshots.
- Confirm join-key availability and quality in actual data, and determine whether BSON-to-JSONL conversion is needed.
- Determine required memory and the volume for the scale experiment.

The synthetic smoke job validates HDFS → YARN → MapReduce, but does not use project data. The VM has about 848 MiB RAM; real jobs may require another host or more resources.

## Reproducibility safeguards

- Never put passwords or tokens in the repository, logs, or versioned commands.
- Keep large files and generated outputs out of Git.
- Make jobs restartable without overwriting `RAW`.
- Record commands, parameters, versions, volumes, and observed results.
- Never present synthetic or miniature results as findings from the real dataset.
