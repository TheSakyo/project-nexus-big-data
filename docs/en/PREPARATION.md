# Dataset-independent preparation

The tools below can be prepared while the dataset downloads. Do not run them against the source directory until the user confirms the download is complete.

## Reproducible inventory

From a terminal with Python 3.9 or later, at the repository root:

```sh
python scripts/nexus.py inventory --root 'D:\path\to\the\dataset'
```

On Linux or macOS, use `python3` if `python` is unavailable. The tool records relative paths, extensions, sizes, modification times, and empty files. It writes the CSV to `reports/local/`, which Git ignores, and refuses to write the report inside the source directory. The basic inventory does not read file contents.

To also calculate SHA-256 for every file:

```sh
python scripts/nexus.py inventory --root 'D:\path\to\the\dataset' --sha256
```

SHA-256 reads every byte and may take a while for tens of GiB; it is optional. An inventory with access errors returns a failure code and must not be used for capacity planning.

## Managing the preparation cluster

Python and the OpenSSH client (`ssh` and `scp`) must be available. The `turing-server` SSH alias can be overridden with `--target`, which must point to the same server configured for this project:

```sh
python scripts/nexus.py cluster status
python scripts/nexus.py cluster start
python scripts/nexus.py cluster stop
```

Starting synchronizes the version-controlled configuration and starts the services. It never formats HDFS. It assumes the NameNode has already been formatted and the SSH account can start Hadoop. `stop` stops services in reverse order; it does not delete HDFS data.

## After the download is confirmed

1. Generate the inventory and compare sources with the delivered manifest and catalog.
2. Inspect a representative sample of each format without modifying originals.
3. Choose a transfer method that avoids an unnecessary full duplicate on the server.
4. Check available storage and reserve headroom for `PROCESSED`, results, and temporary files.
5. Import into `RAW` with a batch manifest and a restartable command that does not overwrite an existing ingestion.
6. Compare files with parser assumptions and catalog schemas, then adjust Map and Reduce functions if needed.

## Prepared Java jobs

### Code layout

- `mapreduce/common` contains shared telemetry/player parsing and the composite player join key.
- `mapreduce/activity` contains the player-day and DAU/WAU jobs.
- `mapreduce/retention` contains the player/activity join and Dn retention aggregation.
- `mapreduce/technical` contains the technical-event/profile join and final incident aggregation.

The packages follow the analytical domains. Each job keeps Hadoop mapper/reducer stages explicit, while parsing and repeated join mechanics live in shared components.

Package the JAR without running tests:

```sh
python scripts/nexus.py build
```

The following commands submit jobs to `turing-server` over SSH. Input paths must already exist in HDFS. Work and output paths must be new for each run; the script refuses to overwrite an existing output. Replace dates with boundaries established during inventory.

DAU and rolling seven-day UTC WAU:

```sh
python scripts/nexus.py job activity \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

Dn retention with weekly cohorts based on `players.created_at`:

```sh
python scripts/nexus.py job retention \
  --players /datalake/raw/mongo/players \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-retention-001 \
  --output /datalake/analytics/retention-001 \
  --start-date YYYY-MM-DD \
  --as-of-date YYYY-MM-DD \
  --max-day 30
```

Technical incidents joined to player profiles:

```sh
python scripts/nexus.py job technical \
  --players /datalake/raw/mongo/players \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-technical-001 \
  --output /datalake/analytics/technical-001
```

The JAR expects player JSONL snapshots. If MongoDB is delivered as BSON only, these commands are not ready to run; prepare a JSONL export after checking the delivered files and available tools.

### Output definitions and limits

- Telemetry normalizes `event_type`/`evt`, `timestamp`/`ts`, `player_id`/`pid`, `session_id`/`session`, and `game_version`/`ver`. Timestamps without a timezone are interpreted as UTC; confirm this at sampling time.
- `DailyActivePlayersJob` writes `date<TAB>player_id`, one row per active player per UTC day. `ActivityMetricsJob` writes `DAU<TAB>date<TAB>count` and `WAU<TAB>window_start<TAB>count`. WAU spans seven calendar days starting on that date. Dates with no activity are omitted, equivalent to zero.
- Retention groups installation dates by Monday UTC and measures activity on exact day Dn. Each Dn denominator includes only players installed on or after the observed period start whose Dn is on or before `--as-of-date`.
- The technical track writes date, version, platform, GPU vendor, sessions, crashes, disconnects, and crashes/session. It covers events joined to a player record. Events without an ID or player record are counted by Hadoop counters and excluded from hardware groups.
- Raw technical events are not deduplicated by `event_id` because the catalog warns that this identifier may be reused. Reassess counts and rates after examining actual duplicates.
- Hadoop counters distinguish invalid input lines and missing identifiers. These jobs have not been run on project data.

The assignment's 40 GiB estimate is not verified. Final capacity planning will use observed sizes, compression, and intermediate storage needs. The server has only about 848 MiB RAM; if useful jobs cannot run within that limit, another host or a suitable processing strategy will be needed, followed by a measured scale experiment.

## Work that depends on the data

The official catalog is available at `D:\gdau8rrtb2czlzgbh4uocjqb.51.75.28.54.sslip.io\DATA_CATALOG.md`. Its repository copy is the [Data Catalog](DATA_CATALOG.md). It already describes the announced sources, schemas, join keys, events, and expected quality issues. This is enough to prepare processing and analysis designs.

The post-download inventory is still needed to confirm the actual files, formats, volumes, and discrepancies from the catalog. Use the [data catalog template](DATA_CATALOG_TEMPLATE.md) to record observations made against delivered files; it does not replace the official catalog.
