# Data catalog — Project Nexus

> Fill in this template from the official catalog and delivered files. Keep unknown values as `To be determined`; do not infer them from file names.

## Overview

| Source | Listed in manifest | Actual format | Raw size | Period | Candidate keys | Intended use |
|---|---|---|---:|---|---|---|
| Gameplay telemetry | To be determined | To be determined | To be determined | To be determined | To be determined | Activity, gameplay |
| Technical logs | To be determined | To be determined | To be determined | To be determined | To be determined | Incidents, versions |
| MongoDB players / transactions | To be determined | To be determined | To be determined | To be determined | To be determined | Segmentation, value |
| CSV reference data | To be determined | Confirm CSV | To be determined | To be determined | To be determined | Enrichment |

## File or homogeneous source profile

Repeat this section for every homogeneous source.

### Identity and provenance

- Logical name: `To be determined`
- Relative path: `To be determined`
- Source / producer: `To be determined`
- Format, compression, and encoding: `To be determined`
- Size, file count, and partition layout: `To be determined`
- Coverage period and delivery frequency: `To be determined`
- Manifest, checksum, or documentation: `To be determined`

### Observed schema

| Field | Observed type | Nullable | Anonymized example | Confirmed meaning | Quality note |
|---|---|---|---|---|---|
| To be determined | To be determined | To be determined | To be determined | To be determined | To be determined |

### Semantics and quality

- Player identifier(s) and uniqueness scope: `To be determined`
- Event / transaction / session identifier(s): `To be determined`
- Timestamp, timezone, and precision: `To be determined`
- Events used to define an active player: `To be determined`
- Rules for missing, invalid, or duplicate values: `To be determined`
- Sensitive data and usage constraints: `To be determined`
- Checks and counters to publish: `To be determined`

## Potential joins

| Left source | Left field | Right source | Right field | Observed cardinality | Risk / check |
|---|---|---|---|---|---|
| To be determined | To be determined | To be determined | To be determined | To be determined | To be determined |

## Analysis decisions after inventory

- Population and activity definition: `To be determined`
- DAU / WAU / retention windows and cohorts: `To be determined`
- Business question for the second track: `To be determined`
- Open-ended exploration and hypothesis checks: `To be determined`
- Volume and memory limit for the scale test: `To be determined`

## Traceability

- Inventory date: `To be determined`
- Manifest/catalog version: `To be determined`
- Local inventory report: `reports/local/dataset-inventory.csv`
- Samples and anonymization method: `To be determined`
- Decisions, assumptions, and business sign-off: `To be determined`
