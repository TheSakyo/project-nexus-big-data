# Préparation indépendante du dataset

Les outils ci-dessous peuvent être préparés pendant le téléchargement. Ne pas les lancer sur le dossier source avant confirmation de la fin du téléchargement.

## Inventaire reproductible

Depuis un terminal avec Python 3.9 ou plus récent, à la racine du dépôt :

```sh
python scripts/nexus.py inventory --root 'D:\chemin\vers\le\jeu-de-donnees'
```

Sous Linux ou macOS, utiliser `python3` si la commande `python` n’existe pas. L’outil recense les chemins relatifs, extensions, tailles, dates de modification et fichiers vides. Il écrit le CSV dans `reports/local/`, ignoré par Git, et refuse de créer le rapport dans le dossier source. L’inventaire de base ne relit pas le contenu des fichiers.

Pour calculer également SHA-256 de chaque fichier :

```sh
python scripts/nexus.py inventory --root 'D:\chemin\vers\le\jeu-de-donnees' --sha256
```

Le calcul SHA-256 relit chaque octet et peut prendre du temps sur plusieurs dizaines de Gio ; il est facultatif. Un inventaire incomplet en cas d’erreur d’accès renvoie un code d’échec et ne doit pas servir au dimensionnement.

## Gestion du cluster de préparation

Cette section concerne uniquement l’environnement WSL local du mainteneur. L’alias `turing-server` n’est pas fourni aux collaborateurs. Le mode SSH actuel et ses limites sont détaillés dans le [guide d’installation et d’exécution](EXECUTION.md).

Pour démarrer le cluster Docker local, utiliser plutôt `python scripts/nexus.py cluster start --backend compose`, puis `cluster status --backend compose`. Les commandes d’analyse doivent recevoir `--backend compose`. Le guide [Installation et exécution](EXECUTION.md) décrit l’import des données, la persistance HDFS et l’arrêt du conteneur.

Python et le client OpenSSH (`ssh` et `scp`) doivent être disponibles. L’alias SSH `turing-server` peut être remplacé avec `--target`, qui doit désigner le même serveur configuré pour le projet :

```sh
python scripts/nexus.py cluster status
python scripts/nexus.py cluster start
python scripts/nexus.py cluster stop
```

Le démarrage synchronise la configuration versionnée et démarre les services. Il ne formate jamais HDFS. Il suppose que le NameNode a déjà été initialisé et que le compte SSH peut démarrer Hadoop. `stop` arrête les services dans l’ordre inverse ; les données HDFS ne sont pas supprimées.

## Étapes après confirmation du téléchargement

1. Générer l’inventaire et comparer les sources au manifeste et au catalogue réellement livrés.
2. Inspecter un échantillon représentatif de chaque format sans modifier les originaux.
3. Choisir un transfert qui n’entraîne pas une copie complète inutile sur le serveur.
4. Vérifier l’espace disponible et réserver de la marge pour `PROCESSED`, les résultats et les fichiers temporaires.
5. Importer en `RAW` avec un manifeste de lot et une commande relançable qui ne remplace pas une ingestion existante.
6. Comparer les fichiers aux hypothèses du parseur et aux schémas du catalogue, puis ajuster les fonctions Map et Reduce si nécessaire.

## Jobs Java préparés

### Organisation du code

- `mapreduce/common` regroupe le parsing partagé des événements et profils joueurs, ainsi que la clé composite de jointure.
- `mapreduce/activity` contient la déduplication joueur-jour et les jobs DAU/WAU.
- `mapreduce/retention` contient la jointure profils/activité et l’agrégation de rétention Dn.
- `mapreduce/technical` contient la jointure télémétrie technique/profils et l’agrégation finale des incidents.

Les packages suivent les domaines analytiques. Les étapes Hadoop Map et Reduce restent explicites dans chaque job, tandis que le parsing et les mécanismes de jointure communs sont factorisés.

Construire le JAR sans lancer de tests :

```sh
python scripts/nexus.py build
```

Sans option de backend, les commandes suivantes utilisent l’alias SSH du mainteneur `turing-server`. Pour exécuter ces mêmes pipelines dans le cluster Docker local, ajouter `--backend compose`. Les chemins doivent déjà exister dans HDFS pour les entrées. Les chemins de travail et de sortie doivent être nouveaux à chaque exécution ; le script refuse d’écraser une sortie existante. Remplacer chaque date par les bornes déterminées lors de l’inventaire.

DAU et WAU glissant sur sept jours UTC :

```sh
python scripts/nexus.py job activity --backend compose \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

Rétention Dn, cohortes hebdomadaires basées sur `players.created_at` :

```sh
python scripts/nexus.py job retention --backend compose \
  --players /datalake/raw/mongo/players \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-retention-001 \
  --output /datalake/analytics/retention-001 \
  --start-date YYYY-MM-DD \
  --as-of-date YYYY-MM-DD \
  --max-day 30
```

Incidents techniques joints au profil du joueur :

```sh
python scripts/nexus.py job technical --backend compose \
  --players /datalake/raw/mongo/players \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-technical-001 \
  --output /datalake/analytics/technical-001
```

Le JAR utilise des snapshots de joueurs JSONL. Si MongoDB n’est livré qu’en BSON, ces commandes ne sont pas encore exécutables : préparer un export JSONL après avoir vérifié les fichiers et les outils disponibles.

### Définitions et limites des sorties

- La télémétrie normalise `event_type`/`evt`, `timestamp`/`ts`, `player_id`/`pid`, `session_id`/`session` et `game_version`/`ver`. Les horodatages sans fuseau sont interprétés en UTC ; confirmer cette règle à l’échantillonnage.
- `DailyActivePlayersJob` sort `date<TAB>player_id`, une ligne par joueur actif et par jour UTC. `ActivityMetricsJob` sort `DAU<TAB>date<TAB>count` et `WAU<TAB>début_de_fenêtre<TAB>count`. WAU couvre sept jours calendaires à partir de cette date. Les dates sans activité sont absentes, ce qui équivaut à zéro.
- La rétention regroupe les installations par lundi UTC et calcule l’activité au jour exact Dn. Le dénominateur de chaque Dn ne comprend que les joueurs installés depuis le début de la période observée et dont le jour Dn est antérieur ou égal à `--as-of-date`.
- L’axe technique sort date, version, plateforme, fabricant GPU, sessions, crashs, déconnexions et crashs/session. Il porte sur les événements rattachés à une fiche joueur. Les événements sans identifiant ou sans fiche sont comptés dans les compteurs Hadoop et exclus des groupes matériels.
- Les événements techniques bruts ne sont pas dédupliqués par `event_id`, car le catalogue signale que cet identifiant peut être réutilisé. Les taux et comptages devront être réévalués après examen des doublons réels.
- Les compteurs Hadoop distinguent les lignes invalides et les identifiants absents. Aucun résultat de ces jobs n’a été exécuté sur les données du projet.

Les 40 Gio annoncés dans le sujet sont une estimation. Le dimensionnement final utilisera les tailles observées, la compression et l’espace nécessaire aux zones intermédiaires. Le serveur n’a qu’environ 848 Mio de RAM ; si les traitements utiles ne peuvent pas tenir dans cette contrainte, il faudra une autre machine ou une stratégie adaptée, puis mesurer réellement le passage à l’échelle.

## Travail dépendant des données

Le catalogue officiel est disponible dans `D:\gdau8rrtb2czlzgbh4uocjqb.51.75.28.54.sslip.io\DATA_CATALOG.md`. Sa copie de référence dans le dépôt est [Catalogue des données](DATA_CATALOG.md). Il décrit déjà les sources, schémas annoncés, clés de jointure, événements et problèmes de qualité attendus. Cela permet de préparer les traitements et les analyses.

L’inventaire après téléchargement reste nécessaire pour confirmer les fichiers, formats, volumes et écarts réels par rapport au catalogue. Le [modèle de catalogue](DATA_CATALOG_TEMPLATE.md) sert à consigner les observations faites sur les fichiers livrés ; il ne remplace pas le catalogue officiel.
