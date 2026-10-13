# Project Nexus — Big Data

**Language / Langue:** [English](#english) · [Français](#français)

Project Nexus is an educational Big Data project using Hadoop and Java MapReduce. This root README is the cross-platform entry point; detailed guides are available in both languages.

## English

### Documentation

| Guide | Purpose |
|---|---|
| [English project overview](docs/en/README.md) | Goals, current status, and project conventions. |
| [Project plan](docs/en/PLAN.md) | Phases, exit criteria, analysis tracks, and pending decisions. |
| [Environment status](docs/en/ENVIRONMENT.md) | Workstation and Hadoop cluster setup and verified state. |
| [Preparation guide](docs/en/PREPARATION.md) | Cross-platform tools and steps to take after the download completes. |
| [Data catalog template](docs/en/DATA_CATALOG_TEMPLATE.md) | Record observed schemas, quality rules, and join keys. |
| [Official data catalog](docs/en/DATA_CATALOG.md) | Announced sources, schemas, join keys, metrics, and quality risks. |
| [Report template](docs/en/REPORT_TEMPLATE.md) | Structure for the final report, including quality, analyses, and scale measurements. |
| [Defense outline](docs/en/PRESENTATION_OUTLINE.md) | Suggested 10-minute presentation structure per student. |

### Quick start

- Build with the Maven Wrapper: `mvnw.cmd` on Windows, or `./mvnw` on Linux, macOS, and WSL.
- Preparation utilities require Python 3.9 or later. Use `python` or `python3`, depending on the operating system.
- Inventory command: `python scripts/nexus.py inventory --root PATH_TO_DATASET`.
- Cluster commands: `python scripts/nexus.py cluster status`, `start`, or `stop`. Cluster management also requires OpenSSH and an SSH alias configured for the project server.

The Python utility only supports inventory and cluster management. Assessed data processing and analysis jobs will be implemented in Java MapReduce on Hadoop.

## Français

### Documentation

| Guide | Contenu |
|---|---|
| [Présentation en français](docs/fr/README.md) | Objectifs, état actuel et conventions du projet. |
| [Plan du projet](docs/fr/PLAN.md) | Étapes, critères de sortie, axes d’analyse et décisions en attente. |
| [État de l’environnement](docs/fr/ENVIRONMENT.md) | Poste de travail, cluster Hadoop et vérifications réalisées. |
| [Guide de préparation](docs/fr/PREPARATION.md) | Outils multiplateformes et étapes après la fin du téléchargement. |
| [Modèle de catalogue des données](docs/fr/DATA_CATALOG_TEMPLATE.md) | Consigner les schémas observés, les règles de qualité et les clés de jointure. |
| [Catalogue officiel des données](docs/fr/DATA_CATALOG.md) | Sources, schémas annoncés, clés de jointure, indicateurs et risques qualité. |
| [Trame du rapport](docs/fr/REPORT_TEMPLATE.md) | Structure du rapport final, de la qualité aux mesures d’échelle. |
| [Déroulé de soutenance](docs/fr/PRESENTATION_OUTLINE.md) | Proposition de présentation de 10 minutes par étudiant. |

### Démarrage rapide

- Construire avec le Maven Wrapper : `mvnw.cmd` sous Windows ou `./mvnw` sous Linux, macOS et WSL.
- Les outils de préparation nécessitent Python 3.9 ou plus récent. Utiliser `python` ou `python3` selon le système.
- Inventaire : `python scripts/nexus.py inventory --root CHEMIN_DU_DATASET`.
- Cluster : `python scripts/nexus.py cluster status`, `start` ou `stop`. La gestion du cluster nécessite aussi OpenSSH et un alias SSH configuré pour le serveur du projet.

L’outil Python sert uniquement à l’inventaire et à la gestion du cluster. Les traitements et analyses évalués seront développés en Java MapReduce sur Hadoop.

## Current project status / État actuel

- Hadoop 3.5.0, HDFS, and YARN are installed on the preparation server; a synthetic WordCount smoke job succeeded. / Hadoop 3.5.0, HDFS et YARN sont installés sur le serveur de préparation ; un test WordCount synthétique a réussi.
- The real dataset is still downloading. Do not inspect, move, or ingest its data files until the user confirms completion. The official catalog has been reviewed separately. / Le dataset réel est toujours en cours de téléchargement. Ne pas inspecter, déplacer ou ingérer ses fichiers de données avant confirmation de la fin. Le catalogue officiel a été consulté séparément.
- No real-data results have been produced. / Aucun résultat sur les données réelles n’a encore été produit.

Keep datasets, credentials, and generated local inventory reports out of Git. See the language-specific guides for details.
