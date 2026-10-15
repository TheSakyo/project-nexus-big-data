# Plan de réalisation — Project Nexus

Ce plan suit les exigences du sujet et le [catalogue officiel des données](DATA_CATALOG.md). Le téléchargement et l’inventaire sont terminés ; les contrôles approfondis de schéma et de qualité restent à effectuer.

## État et règle de travail

Le dataset complet est dans `data/source/`, exclu de Git. L’inventaire local est enregistré dans `reports/local/`. Le catalogue officiel `DATA_CATALOG.md` a été consulté et copié dans le dépôt. Le code, les documents et la configuration se travaillent dans `D:\Workspaces\java-workspace\JavaHadoop`.

## Étapes et critères de sortie

| Étape | Actions | Critère de sortie |
|---|---|---|
| 1. État des lieux | Examiner le dépôt, les consignes, Java, Maven, Docker/Compose, WSL et les possibilités Hadoop. | L’état est décrit et les changements Git préexistants sont préservés. |
| 2. Inventaire des données | Relever fichiers, formats, volumes, partitions, fichiers vides et erreurs ; comparer au catalogue officiel. | Inventaire local terminé et manifestes comparés ; les règles de qualité détaillées restent à valider pendant le traitement. |
| 3. Choix d’environnement | Comparer les options disponibles selon mémoire, stockage, réseau et compatibilité Hadoop/Java. | Environnement WSL vérifié sur un smoke test ; cluster Docker Compose Hadoop 3.5.0 déclaré healthy le 15 octobre 2026. Ces environnements locaux ne sont pas partagés. |
| 4. Data lake et ingestion | Définir les zones `RAW`, `PROCESSED`, `ANALYTICS` ; préparer une ingestion relançable qui conserve les originaux. | Chaque lot est traçable ; `RAW` reste inchangé par les traitements. |
| 5. Qualité et préparation | Définir les règles à partir des vrais schémas : champs manquants, dates, formats, doublons et identifiants ; compter rejets et normalisations. | Les sorties `PROCESSED` ont des contrôles et règles documentés. |
| 6. Traitements MapReduce | Normaliser les schémas télémétriques annoncés ; produire les joueurs actifs/jour, DAU/WAU, la rétention Dn et l’analyse technique jointe aux profils. | Jobs Java présents et compilables ; exécutions et résultats réels restent à produire sur le dataset. |
| 7. Analyses et jointures | Étudier activité/rétention, choisir un axe métier après inspection, croiser au moins deux sources et mener une exploration libre. | Définitions, populations, fenêtres, résultats, limites et confiance documentés. |
| 8. Passage à l’échelle | Fixer une limite mémoire Java, dépasser cette limite en volume, relever compteurs et journaux Hadoop. | Volume, limite, durée et indicateurs consignés ; aucune performance sans mesure. |
| 9. Livrables | Finaliser README, scripts, configuration, résultats vérifiés, rapport et présentation. | Trames prêtes dans [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md) et [PRESENTATION_OUTLINE.md](PRESENTATION_OUTLINE.md) ; une autre personne peut reproduire le parcours et comprendre les recommandations. |

## Travail stable effectué sans les données

- Un parseur Java normalise les champs télémétriques v1/v2 et les horodatages annoncés ; les entrées invalides sont comptées par les jobs.
- `DailyActivePlayersJob` déduplique les joueurs par jour UTC à partir de `session_start`.
- `ActivityMetricsJob` produit DAU et WAU glissant sur sept jours calendaires UTC ; il exclut les fenêtres antérieures à la date de début fournie.
- `PlayerCohortJoinJob` joint `players._id` à `player_id`/`pid` et associe les dates d’activité à `created_at`. `CohortRetentionMetricsJob` calcule Dn avec un dénominateur limité aux joueurs dont la journée Dn est complètement observable.
- `TechnicalIncidentsByProfileJob` joint les événements `session_start`, `crash` et `disconnect` au profil joueur ; `TechnicalIncidentAggregationJob` calcule les comptes et le ratio `crash/session_start` par jour, version, plateforme et fabricant GPU.
- `python scripts/nexus.py build` construit le JAR ; `python scripts/nexus.py job ...` soumet les pipelines au cluster via SSH. Aucune donnée n’est transférée ni lue par ces commandes de préparation.

Les jobs attendent des snapshots joueurs JSONL. Si la livraison ne contient que du BSON, l’export MongoDB vers JSONL reste à préparer selon les outils et accès effectivement disponibles. Les exemples de commandes et les limites des métriques sont dans [PREPARATION.md](PREPARATION.md).

## Analyses à cadrer

| Axe | Question et décision à éclairer | Acteurs / sources | Résultat et action envisagés | Risque principal |
|---|---|---|---|---|
| Activité / rétention | L’activité ou la rétention change-t-elle selon les jours et cohortes, au point de justifier une investigation produit ? | Produit et direction ; télémétrie `session_start`, joueurs `created_at`. | Courbes DAU/WAU et rétention Dn ; investiguer une rupture ou une cohorte faible. | Couverture incomplète, événements manquants ou définition d’activité biaisée. |
| Technique | Les crashs varient-ils selon version et profil matériel, ce qui aiderait à prioriser les investigations QA ? | Ingénierie et QA ; télémétrie, joueurs `profile`. | Comptes crash/session par date, version, plateforme et GPU ; reproduire les cas les plus fréquents. | Exposition différente, faibles effectifs, profils absents et doublons d’événements. |
| Exploration libre | Les abandons varient-ils de manière inhabituelle selon les cartes ou modes ? | Design gameplay et produit ; `match_start`, `match_end`, `level_abandon`, référentiels de cartes/niveaux. | Taux par carte/mode avec dénominateur explicite ; examiner les rencontres les plus atypiques. | Dénominateur incomplet, abandons non instrumentés ou confusion entre association et causalité. |

### Axe 1 — Activité et rétention

Définition implémentée : joueur actif avec au moins un `session_start` par jour UTC ; cohortes par lundi de la semaine UTC de `players.created_at` ; Dn par activité au jour calendaire exact n après création. Les jours Dn non encore observables à la date de fin sont exclus du dénominateur. La date de début réelle de la télémétrie est nécessaire pour exclure les cohortes installées avant la couverture observée.

### Axe 2 — Problématique métier au choix

Axe technique préparé : comparer les comptes `crash`/`disconnect` et le ratio crash / `session_start` par version (`game_version`/`ver`), plateforme et fabricant GPU (`players.profile.platform`, `gpu_vendor`), après jointure sur l’identifiant joueur. C’est une association descriptive, pas une preuve de causalité. Le code ne supprime pas les `event_id` répétés : il faut d’abord mesurer leur réutilisation réelle pour choisir une règle de déduplication justifiée.

### Axe 3 — Exploration libre

Piste candidate : abandon (`level_abandon` ou `match_end.outcome=abandon`) selon la carte, le mode, la région ou la version. Explorer les champs annoncés (`map_id`, `mode`, `region`, `duration_sec`) après vérification des dénominateurs et valeurs invalides. Garder cette piste ouverte si les fichiers ou effectifs ne permettent pas une comparaison fiable.

## Décisions en attente

- L’environnement de préparation du mainteneur utilise Hadoop 3.5.0 sous WSL, en mode pseudo-distribué mono-nœud ; l’alias `turing-server` est local et cet environnement n’est pas partagé. Ce n’est pas une capacité de production.
- Chemin source confirmé : `data/source/` ; stratégie d’ingestion HDFS encore à définir.
- Les zones HDFS `/datalake/raw`, `/datalake/processed` et `/datalake/analytics` existent ; le partitionnement reste à définir selon les sources.
- Concordance des fichiers livrés avec les formats et variantes annoncés dans le catalogue ; les snapshots joueurs JSONL sont requis par les jobs préparés.
- Disponibilité et qualité des clés de jointure dans les données réelles, et éventuelle conversion BSON vers JSONL.
- Mémoire nécessaire et volume retenu pour l’expérience d’échelle.

Le smoke test synthétique valide HDFS → YARN → MapReduce, mais n’utilise pas les données du projet. La VM a environ 848 Mio de RAM ; les traitements réels peuvent nécessiter une autre machine ou davantage de ressources.

## Précautions de reproductibilité

- Ne jamais enregistrer de mot de passe ou jeton dans le dépôt, les journaux ou les commandes versionnées.
- Garder les gros fichiers et sorties générées hors de Git.
- Rendre les traitements relançables sans écraser `RAW`.
- Conserver commandes, paramètres, versions, volumes et résultats observés.
- Ne pas présenter de résultat synthétique ou miniature comme un résultat du dataset réel.
