# Plan de réalisation — Project Nexus

Ce plan suit les exigences du sujet. Il s’appuie sur le [catalogue officiel des données](DATA_CATALOG.md) ; les fichiers réellement livrés, leurs volumes et les écarts au catalogue restent à vérifier après le téléchargement.

## État et règle de travail

Le téléchargement des données est en cours hors du dépôt. Ne pas lire, déplacer, renommer ou traiter les fichiers de données avant confirmation de sa fin. Le catalogue officiel `DATA_CATALOG.md`, distinct des données, a été consulté et copié dans le dépôt. Le code, les documents et la configuration se travaillent dans `D:\Workspaces\java-workspace\JavaHadoop`.

## Étapes et critères de sortie

| Étape | Actions | Critère de sortie |
|---|---|---|
| 1. État des lieux | Examiner le dépôt, les consignes, Java, Maven, Docker/Compose, WSL et les possibilités Hadoop. | L’état est décrit et les changements Git préexistants sont préservés. |
| 2. Inventaire des données | Après confirmation, relever fichiers, formats, volumes, partitions, fichiers vides et erreurs ; comparer au catalogue officiel. | Outil Python prêt (`python scripts/nexus.py inventory`) ; inventaire réel et écarts établis. |
| 3. Choix d’environnement | Comparer les options disponibles selon mémoire, stockage, réseau et compatibilité Hadoop/Java. | Préparé : serveur Debian `turing-server`, Hadoop 3.5.0, Java serveur 17 et smoke test HDFS/YARN/MapReduce réussi. |
| 4. Data lake et ingestion | Définir les zones `RAW`, `PROCESSED`, `ANALYTICS` ; préparer une ingestion relançable qui conserve les originaux. | Chaque lot est traçable ; `RAW` reste inchangé par les traitements. |
| 5. Qualité et préparation | Définir les règles à partir des vrais schémas : champs manquants, dates, formats, doublons et identifiants ; compter rejets et normalisations. | Les sorties `PROCESSED` ont des contrôles et règles documentés. |
| 6. Traitements MapReduce | Construire au moins deux jobs Java significatifs et reproductibles. | Jobs exécutables avec entrées/sorties explicites et résultats vérifiés. |
| 7. Analyses et jointures | Étudier activité/rétention, choisir un axe métier après inspection, croiser au moins deux sources et mener une exploration libre. | Définitions, populations, fenêtres, résultats, limites et confiance documentés. |
| 8. Passage à l’échelle | Fixer une limite mémoire Java, dépasser cette limite en volume, relever compteurs et journaux Hadoop. | Volume, limite, durée et indicateurs consignés ; aucune performance sans mesure. |
| 9. Livrables | Finaliser README, scripts, configuration, résultats vérifiés, rapport et présentation. | Trames prêtes dans [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md) et [PRESENTATION_OUTLINE.md](PRESENTATION_OUTLINE.md) ; une autre personne peut reproduire le parcours et comprendre les recommandations. |

Les préparations indépendantes des données sont faites : Maven Wrapper, configuration Hadoop, outil Python d’inventaire et de gestion du cluster. Voir [PREPARATION.md](PREPARATION.md).

## Analyses à cadrer

### Axe 1 — Activité et rétention

Base de travail issue du catalogue : définir un joueur actif par au moins un `session_start` sur un jour UTC. Mesurer DAU/WAU et rétention par semaine d’installation (`players.created_at`), avec jointure entre `player_id`/`pid` de la télémétrie et `players._id`. Normaliser les deux schémas télémétriques v1/v2 et les formats d’horodatage ; vérifier les identifiants absents, les événements non rattachés et les derniers jours incomplets. Ces choix sont provisoires jusqu’à l’inventaire réel.

### Axe 2 — Problématique métier au choix

Axe technique recommandé : comparer les événements `crash`/`disconnect` par version du jeu (`game_version`/`ver`) et profil matériel (`players.profile.platform`, `gpu_vendor`, `gpu_model`, `os`), avec jointure sur l’identifiant joueur. Éclairer la priorisation des versions ou configurations à investiguer. Ce sera une association descriptive, pas une preuve de causalité ; contrôler les tailles de groupes, versions de schéma et différences d’exposition. Confirmer la faisabilité des champs et volumes à l’inventaire.

### Axe 3 — Exploration libre

Piste candidate : abandon (`level_abandon` ou `match_end.outcome=abandon`) selon la carte, le mode, la région ou la version. Explorer les champs annoncés (`map_id`, `mode`, `region`, `duration_sec`) après vérification des dénominateurs et valeurs invalides. Garder cette piste ouverte si les fichiers ou effectifs ne permettent pas une comparaison fiable.

## Décisions en attente

- L’environnement de préparation est fixé à Hadoop 3.5.0 sur `turing-server`, en mode pseudo-distribué mono-nœud ; ce n’est pas une capacité de production.
- Chemin physique et stratégie d’ingestion à décider après confirmation du téléchargement.
- Les zones HDFS `/datalake/raw`, `/datalake/processed` et `/datalake/analytics` existent ; le partitionnement reste à définir selon les sources.
- Concordance des fichiers livrés avec les formats et variantes annoncés dans le catalogue.
- Disponibilité et qualité des clés de jointure dans les données réelles.
- Mémoire nécessaire et volume retenu pour l’expérience d’échelle.

Le smoke test synthétique valide HDFS → YARN → MapReduce, mais n’utilise pas les données du projet. La VM a environ 848 Mio de RAM ; les traitements réels peuvent nécessiter une autre machine ou davantage de ressources.

## Précautions de reproductibilité

- Ne jamais enregistrer de mot de passe ou jeton dans le dépôt, les journaux ou les commandes versionnées.
- Garder les gros fichiers et sorties générées hors de Git.
- Rendre les traitements relançables sans écraser `RAW`.
- Conserver commandes, paramètres, versions, volumes et résultats observés.
- Ne pas présenter de résultat synthétique ou miniature comme un résultat du dataset réel.
