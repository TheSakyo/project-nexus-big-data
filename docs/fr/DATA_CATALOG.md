# Catalogue des données — Nexus Interactive

Ce catalogue officiel décrit les sources, les schémas annoncés, les clés de jointure, des indicateurs possibles et des problèmes de qualité à rechercher. Il sert à préparer l’ingestion et les analyses. L’inventaire après téléchargement doit confirmer les fichiers réellement livrés, leurs formats et leurs volumes.

## Sources

| Source | Système | Format annoncé | Volume indicatif | Granularité |
|---|---|---|---|---|
| Joueurs | MongoDB `players` | BSON | milliers à centaines de milliers | 1 document par joueur |
| Transactions | MongoDB `transactions` | BSON | petit / moyen | 1 document par paiement |
| Télémétrie de jeu | fichiers | JSONL, partitionné par jour | **dominant** | 1 ligne par événement |
| Journaux techniques | fichiers | texte et JSON mélangés | élevé | 1 ligne par journal |
| Référentiels métier | fichiers | CSV | très faible | 1 ligne par entité |

Les joueurs et paiements proviennent de MongoDB. Selon le mode de distribution, l’enseignant peut fournir des instantanés JSONL en lecture seule dans `raw/mongo`. Ce sont des copies de transport des collections MongoDB, pas des sources supplémentaires.

Exemple de disposition RAW envisagée :

```text
/datalake/raw/mongo/players/...
/datalake/raw/mongo/transactions/...
/datalake/raw/telemetry/dt=2026-08-01/...
/datalake/raw/logs/dt=2026-08-01/...
/datalake/raw/references/...
```

## Clés de jointure

| Concept | Champs |
|---|---|
| Joueur | `players._id` = `player_id` = télémétrie `player_id` / `pid` = journaux `player` / `player_id` = `transactions.player_id` |
| Session | télémétrie `session_id` / `session` = journaux `session` / `session_id` |
| Partie | `payload.match_id` |
| Carte / niveau / arme / SKU / région / version | identifiants dans les référentiels CSV |
| Calendrier | télémétrie `timestamp` / `ts`, transaction `ts`, joueur `created_at` / `last_seen_at` |

Tous les événements ne pourront pas forcément être joints. Les identifiants non raccordables peuvent faire l’objet d’une analyse qualité.

## MongoDB — `players`

| Champ | Type | Notes |
|---|---|---|
| `_id` | chaîne | Exemple : `P00001234` |
| `username` | chaîne | |
| `email` | chaîne | Donnée personnelle ; éviter de la propager aux sorties analytiques. |
| `created_at` | date-heure | Création du compte / installation |
| `country` | chaîne | ISO-3166 alpha-2 |
| `acquisition.channel` | chaîne | Voir `acquisition_channels.csv` |
| `acquisition.campaign` | chaîne | |
| `acquisition.install_at` | date-heure | |
| `profile.platform` | chaîne | `pc`, `console`, `mobile` |
| `profile.os` | chaîne | |
| `profile.gpu_vendor` | chaîne | `NVIDIA`, `AMD`, `Intel`, `console`, `adreno`, `unknown` |
| `profile.gpu_model` | chaîne | |
| `profile.driver_version` | chaîne | |
| `profile.cpu` | chaîne | |
| `profile.ram_gb` | entier | |
| `preferred_region` | chaîne | Région de matchmaking |
| `status` | chaîne | `active`, `churned`, `banned` |
| `last_seen_at` | date-heure | |
| `level` | entier | |
| `xp` | entier | |

Il n’existe pas de champ explicite `is_bot`. Tout comportement de bot doit être inféré et présenté comme tel.

## MongoDB — `transactions`

Paiements en monnaie réelle (les crédits du jeu n’en font pas partie).

| Champ | Type | Notes |
|---|---|---|
| `transaction_id` | chaîne | |
| `player_id` | chaîne | |
| `ts` | date-heure | |
| `sku` | chaîne | Voir `shop_items.csv` |
| `amount_eur` | flottant | Déjà converti en EUR |
| `currency` | chaîne | `EUR` |
| `method` | chaîne | `card`, `paypal`, `mobile_store` |
| `status` | chaîne | `settled`, `refunded`, `chargeback` |
| `country` | chaîne | |
| `card_last4` | chaîne ou nul | Présent pour les paiements par carte ; ne pas propager aux sorties analytiques. |
| `campaign` | chaîne | Campagne d’acquisition copiée au moment de l’achat |

## Télémétrie JSONL

Partitions annoncées : `telemetry/dt=YYYY-MM-DD/part-XXXXX.jsonl`.
Deux versions de schéma coexistent.

### Schéma v1 (`schema_version: 1`)

| Champ | Type |
|---|---|
| `event_id` | chaîne |
| `event_type` | chaîne |
| `timestamp` | chaîne ou nombre (voir qualité) |
| `player_id` | chaîne |
| `session_id` | chaîne ou nul |
| `game_version` | chaîne |
| `schema_version` | 1 |
| `platform` | chaîne |
| `region` | chaîne |
| `payload` | objet |

### Schéma v2 (`schema_version: 2`)

| Champ v2 | Équivalent v1 |
|---|---|
| `evt` | `event_type` |
| `ts` | `timestamp` |
| `pid` | `player_id` |
| `session` | `session_id` |
| `ver` | `game_version` |

### Types d’événement

| `event_type` | Signification | Champs utiles de `payload` |
|---|---|---|
| `session_start` | Début d’une session client | `platform`, `os` |
| `session_end` | Fin d’une session client | `reason`: `logout` / `crash` |
| `tutorial_complete` | Tutoriel du premier jour terminé | `skipped` |
| `match_start` | Début d’une partie | `match_id`, `map_id`, `level_id`, `mode`, `region`, `ping_ms` |
| `weapon_equip` | Arme principale équipée | `weapon_id` |
| `level_enter` | Carte / rencontre commencée | `level_id`, `map_id` |
| `player_death` | Mort du joueur | `killer_weapon`, `time_alive_sec`, `spawn_near_enemy` |
| `player_kill` | Élimination | `weapon_id`, `headshot` |
| `level_complete` | Rencontre terminée | |
| `level_abandon` | Rencontre quittée | `reason` |
| `crash` | Plantage du client | `code`, champs GPU, `game_version` |
| `disconnect` | Perte réseau | `ping_ms`, `region`, `code` |
| `match_end` | Résultat d’une partie | `outcome`, `duration_sec`, `kills`, `deaths`, `weapon_primary`, `fps_avg` |

`match_end.outcome` peut valoir `win`, `loss`, `abandon`, `disconnect`.
Définition suggérée d’un joueur actif : au moins un `session_start` durant le jour UTC. Toute autre définition doit être justifiée.

## Journaux techniques

Partitions annoncées : `logs/dt=YYYY-MM-DD/part-XXXXX.log`. Deux encodages sont mélangés :

1. Texte de type syslog : `2026-08-12T14:22:03.112Z ERROR matchmaking region=sa-east-1 code=MM_TIMEOUT player=P00001234 session=S... msg="..."`
2. JSON : `{ "ts", "level", "service", "code", "player_id", "session_id", "region", "msg" }`

Services : `matchmaking`, `game-server`, `auth`, `payment`, `anticheat`. Les codes sont décrits dans `error_codes.csv`.

## Référentiels CSV

| Fichier | Clé | Rôle |
|---|---|---|
| `weapons.csv` | `weapon_id` | nom, type, dégâts, TTK |
| `maps.csv` | `map_id` | métadonnées des cartes |
| `levels.csv` | `level_id` | rencontre, lien vers `map_id` |
| `acquisition_channels.csv` | `channel_id` | famille, canal payant, qualité nominale |
| `regions.csv` | `region_id` | continent, latence habituelle |
| `game_versions.csv` | `version` | date de sortie, notes |
| `shop_items.csv` | `sku` | prix catalogue |
| `error_codes.csv` | `code` | service, gravité |
| `countries.csv` | `country` | continent, région par défaut |

## Problèmes de qualité à rechercher

La liste n’est pas exhaustive :

- `player_id` absent, vide ou inconnu ;
- événements sans joueur correspondant dans MongoDB ;
- lignes dupliquées ou `event_id` réutilisés ;
- formats d’horodatage mélangés : ISO-8601 `Z`, décalage `+02:00`, `dd/mm/yyyy`, millisecondes Unix, date-heure séparée par un espace ;
- deux versions du schéma télémétrique ;
- `session_id` nul sur certains plantages ;
- `duration_sec` parfois négatif ;
- formats de journaux mélangés ;
- derniers jours incomplets et cohortes récentes non observables sur toute la fenêtre ;
- petites populations pour certains pays ou plateformes.

Un champ peut rester exploitable si l’impact sur l’indicateur est expliqué.

## Indications d’indicateurs (non prescriptives)

- **DAU** : nombre distinct de `player_id` avec un événement d’activité par jour UTC.
- **WAU** : nombre distinct de joueurs sur sept jours UTC.
- **Cohorte** : joueurs regroupés par semaine d’installation (`players.created_at` tronqué à la semaine).
- **Rétention Dn** : part d’une cohorte active au jour n après l’installation.
- **Taux d’abandon** : `level_abandon` ou `match_end.outcome=abandon` rapporté aux parties.
- **Taux de crash** : événements `crash` rapportés aux sessions ou parties, par `game_version` et matériel.
- **Part d’arme** : éliminations ou victoires où `weapon_primary` / `weapon_id` correspond à l’arme.
- **LTV par canal** : `amount_eur` réglé moins remboursements / rétrofacturations, joint à `acquisition.channel`.

Ces formules sont des pistes. Documenter les dénominateurs, la période, les règles de déduplication et les limites avant de tirer des conclusions.
