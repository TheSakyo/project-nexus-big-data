# Project Nexus — Project report

> Complete this outline after inventory, implementation, and execution. Do not present placeholders or synthetic examples as real findings.

## Executive summary

- Business question: `To be completed`
- Sources and period actually analyzed: `To be completed`
- Verified findings: `Complete after execution`
- Recommendation and confidence level: `To be completed`

## 1. Context and business question

- Studio context and decision to support.
- Analysis questions and populations of interest.
- Operational definitions (active player, abandonment, incident, etc.).

## 2. Data and provenance

- Reference the completed [data catalog](DATA_CATALOG_TEMPLATE.md).
- Inventory: logical paths, formats, sizes, period, and partitions.
- Provenance, access limits, sensitive data, and retention rules.
- Differences from the delivered manifest or catalog.

## 3. Architecture and data flow

- Diagram of source → `RAW` → `PROCESSED` → `ANALYTICS`.
- Hadoop environment, versions, topology, and resource constraints.
- Ingestion strategy, batch traceability, and restart procedure.

## 4. Quality and preparation

| Check | Rule | Records affected | Action | Observed count |
|---|---|---:|---|---:|
| To be completed | To be completed | To be measured | To be documented | To be measured |

Document missing or invalid values, duplicates, corrections, and rejected records. Keep `RAW` sources unchanged.

## 5. Java MapReduce jobs

For each job, describe its question, inputs/outputs, keys, Mapper, Reducer, partitioning, restart behavior, and result checks.

### Job 1 — Activity and retention

`Define after inspecting events and timestamps.`

### Job 2 — Business analysis and cross-source join

`Choose after confirming available fields and join keys.`

## 6. Analyses and findings

### Activity, DAU, WAU, and retention

Definition, population, period, cohorts, windows, measured results, incomplete data, and limitations.

### Business question

Question, joined sources, method, observed result, possible decision, and interpretation risks.

### Open-ended exploration

Observed phenomenon, checks of alternatives, descriptive finding, correlation, hypothesis, and any demonstrated causal evidence.

Do not present any finding until it has been recalculated and verified.

## 7. Scalability experiment

| Item | Measured value |
|---|---|
| Job and code version | To be measured |
| Data volume and file count | To be measured |
| Java heap limit | To be measured |
| Duration and resources | To be measured |
| Map/Reduce tasks and Hadoop counters | To be measured |
| Result check | To be verified |

Explain how the data volume exceeds the memory of one Java process and which work Hadoop distributes. Distinguish synthetic experiments from real-data processing.

## 8. Limitations, risks, and recommendations

Separate coverage, data quality, method, and infrastructure limits. Distinguish statistical association from causality. Link each recommendation to a verified observation.

## 9. Reproduction

- Java, Hadoop, Maven, and Python versions.
- Inventory, ingestion, build, and execution commands.
- Parameters, logical paths, and output locations.
- Expected checks and restart procedure.

## Appendices

Full catalog, schemas, dictionary, non-secret configuration, commands, counters, sample outputs, and recorded decisions.
