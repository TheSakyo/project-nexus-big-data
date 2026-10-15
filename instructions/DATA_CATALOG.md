# Nexus Interactive — data catalog

Use this catalog to design ingestion, joins, and quality checks. Field names are in English (studio telemetry convention).

## Sources

| Source | System | Format | Typical volume | Grain |
|---|---|---|---|---|
| Players | MongoDB `players` | BSON | thousands → hundreds of thousands | 1 document / player |
| Transactions | MongoDB `transactions` | BSON | small / medium | 1 document / payment |
| Gameplay telemetry | files | JSONL, partitioned by day | **dominant** | 1 line / event |
| Technical logs | files | mixed text + JSON | large | 1 line / log |
| Business repositories | files | CSV | tiny | 1 row / entity |

**Players and payments originate in MongoDB.** Depending on the distribution
method, the instructor may provide read-only JSONL snapshots in `raw/mongo`.
These snapshots are transport copies of the MongoDB collections, not an
additional source.

Suggested RAW layout after ingestion (example):

```
/datalake/raw/mongo/players/...          # your export from MongoDB
/datalake/raw/mongo/transactions/...
/datalake/raw/telemetry/dt=2026-08-01/...
/datalake/raw/logs/dt=2026-08-01/...
/datalake/raw/references/...
```

## Join keys

| Concept | Fields |
|---|---|
| Player | `players._id` = `player_id` = telemetry `player_id` / `pid` = logs `player` / `player_id` = `transactions.player_id` |
| Session | telemetry `session_id` / `session` = logs `session` / `session_id` |
| Match | `payload.match_id` |
| Map / level / weapon / SKU / region / version | ids in CSV repositories |
| Calendar | telemetry `timestamp` / `ts`, transaction `ts`, player `created_at` / `last_seen_at` |

Not every event can be joined. Treat unlinkable identifiers as a quality topic.

## MongoDB — `players`

| Field | Type | Notes |
|---|---|---|
| `_id` | string | `P00001234` |
| `username` | string | |
| `email` | string | |
| `created_at` | datetime | account creation / install |
| `country` | string | ISO-3166 alpha-2 |
| `acquisition.channel` | string | see `acquisition_channels.csv` |
| `acquisition.campaign` | string | |
| `acquisition.install_at` | datetime | |
| `profile.platform` | string | `pc`, `console`, `mobile` |
| `profile.os` | string | |
| `profile.gpu_vendor` | string | `NVIDIA`, `AMD`, `Intel`, `console`, `adreno`, `unknown` |
| `profile.gpu_model` | string | |
| `profile.driver_version` | string | |
| `profile.cpu` | string | |
| `profile.ram_gb` | int | |
| `preferred_region` | string | matchmaking region |
| `status` | string | `active`, `churned`, `banned` |
| `last_seen_at` | datetime | |
| `level` | int | |
| `xp` | int | |

There is **no** explicit `is_bot` flag. Behaviour has to be inferred.

## MongoDB — `transactions`

Real-money payments (not in-game credit grants).

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | string | |
| `player_id` | string | |
| `ts` | datetime | |
| `sku` | string | see `shop_items.csv` |
| `amount_eur` | float | already converted to EUR |
| `currency` | string | `EUR` |
| `method` | string | `card`, `paypal`, `mobile_store` |
| `status` | string | `settled`, `refunded`, `chargeback` |
| `country` | string | |
| `card_last4` | string \| null | present for card payments |
| `campaign` | string | campaign at purchase time (copied from acquisition) |

## Telemetry JSONL

Partition: `telemetry/dt=YYYY-MM-DD/part-XXXXX.jsonl`

Two schema versions coexist.

### Schema v1 (`schema_version: 1`)

| Field | Type |
|---|---|
| `event_id` | string |
| `event_type` | string |
| `timestamp` | string or number (see quality) |
| `player_id` | string |
| `session_id` | string \| null |
| `game_version` | string |
| `schema_version` | 1 |
| `platform` | string |
| `region` | string |
| `payload` | object |

### Schema v2 (`schema_version: 2`) — appears later in the period

| v2 field | v1 equivalent |
|---|---|
| `evt` | `event_type` |
| `ts` | `timestamp` |
| `pid` | `player_id` |
| `session` | `session_id` |
| `ver` | `game_version` |

### Event types

| `event_type` | Meaning | Useful payload fields |
|---|---|---|
| `session_start` | client session begins | `platform`, `os` |
| `session_end` | client session ends | `reason`: `logout` / `crash` |
| `tutorial_complete` | first-day tutorial | `skipped` |
| `match_start` | match begins | `match_id`, `map_id`, `level_id`, `mode`, `region`, `ping_ms` |
| `weapon_equip` | primary weapon | `weapon_id` |
| `level_enter` | map / encounter entered | `level_id`, `map_id` |
| `player_death` | death | `killer_weapon`, `time_alive_sec`, `spawn_near_enemy` |
| `player_kill` | elimination | `weapon_id`, `headshot` |
| `level_complete` | encounter finished | |
| `level_abandon` | player left | `reason` |
| `crash` | client crash | `code`, GPU fields, `game_version` |
| `disconnect` | network drop | `ping_ms`, `region`, `code` |
| `match_end` | match result | `outcome`, `duration_sec`, `kills`, `deaths`, `weapon_primary`, `fps_avg` |

`match_end.outcome`: `win`, `loss`, `abandon`, `disconnect`.

Suggested **active-player** definition: at least one `session_start` on that UTC day. Justify any other choice.

## Technical logs

Partition: `logs/dt=YYYY-MM-DD/part-XXXXX.log`

Two encodings are mixed:

1. Syslog-like text
   `2026-08-12T14:22:03.112Z ERROR matchmaking region=sa-east-1 code=MM_TIMEOUT player=P00001234 session=S... msg="..."`
2. JSON
   `{ "ts", "level", "service", "code", "player_id", "session_id", "region", "msg" }`

Services: `matchmaking`, `game-server`, `auth`, `payment`, `anticheat`.
Codes are described in `error_codes.csv`.

## CSV repositories

| File | Key | Role |
|---|---|---|
| `weapons.csv` | `weapon_id` | name, type, damage, TTK |
| `maps.csv` | `map_id` | map metadata |
| `levels.csv` | `level_id` | encounter, links to `map_id` |
| `acquisition_channels.csv` | `channel_id` | family, paid flag, nominal quality |
| `regions.csv` | `region_id` | continent, typical ping |
| `game_versions.csv` | `version` | release date, notes |
| `shop_items.csv` | `sku` | catalog price |
| `error_codes.csv` | `code` | service, severity |
| `countries.csv` | `country` | continent, default region |

## Known quality issues (non-exhaustive)

You are expected to **discover and document** issues. Examples you may encounter:

- missing, blank, or unknown `player_id`
- events that do not match any Mongo player
- duplicate lines / reused `event_id`
- mixed timestamp formats (ISO-8601 `Z`, offset `+02:00`, `dd/mm/yyyy`, Unix milliseconds, space-separated)
- two telemetry schemas
- null `session_id` on some crash events
- occasional negative `duration_sec`
- mixed log formats
- incomplete last days of the window (recent cohorts not fully observed)
- populations that are small in some countries / platforms

A field can still be used if you state the impact on the metric.

## Metric hints (not prescriptions)

- **DAU**: distinct `player_id` with an activity event on a UTC day
- **WAU**: distinct `player_id` over 7 UTC days
- **Cohort**: players sharing the same install week (`players.created_at` truncated)
- **Retention Dn**: share of a cohort with activity on day n after install
- **Abandon rate**: `level_abandon` or `match_end.outcome=abandon` over matches
- **Crash rate**: `crash` events over sessions or matches, sliced by `game_version` and hardware
- **Weapon share**: kills or match wins where `weapon_primary` / `weapon_id` equals a given id
- **Channel LTV**: settled `amount_eur` minus refunds / chargebacks, joined to `acquisition.channel`
