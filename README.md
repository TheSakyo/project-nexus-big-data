# Project Nexus — Big Data

**Language / Langue:** [English](#english) · [Français](#français)

Project Nexus is an educational Big Data project using Hadoop and Java MapReduce. This root README is the cross-platform entry point; detailed guides are available in both languages.

## English

### Documentation

| Guide | Purpose |
|---|---|
| [English project overview](docs/en/README.md) | Goals, current status, and project conventions. |
| [Execution setup](docs/en/EXECUTION.md) | Manual and Docker Compose setup, OS prerequisites, and script responsibilities. |
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
- Package the Java jobs with `python scripts/nexus.py build`; submit activity, retention, or technical pipelines with `python scripts/nexus.py job --help`.
- Cluster commands: `python scripts/nexus.py cluster status --backend compose`, `start --backend compose`, or `stop --backend compose`. The SSH backend remains available for the maintainer's WSL environment.

The Python utility handles inventory, cluster management, packaging, and job submission. Data processing and analysis are implemented in Java MapReduce on Hadoop. See the language-specific preparation guide for commands and metric definitions.

### Prerequisites and execution modes

- **Local build:** install a JDK 17 or newer and Python 3.9 or newer. The Maven Wrapper downloads and runs the configured Maven version, so a separate Maven installation is not required. WSL, Docker, and Hadoop are not required just to package the Java jobs.
- **Run jobs with Compose:** Docker Desktop/Engine and Compose start a local Hadoop 3.5.0 cluster; Python 3.9+ and JDK 17 are still used locally by the helper and Maven Wrapper. No Hadoop installation, WSL account, or SSH access to the maintainer's machine is required. See the execution guide for setup and commands.
- **Run jobs with manual SSH:** a Hadoop cluster is required. The current `turing-server` SSH alias points to the maintainer's own WSL environment; it is not a shared server. Another cluster requires its own SSH access and adaptation of machine-specific paths in the script.
- **Run jobs on a different cluster:** configure Hadoop 3.5.0, Java 17, HDFS/YARN, the project configuration, and an SSH account on that cluster. Update the target and HDFS paths for that environment.
- **Dataset:** the real dataset is not stored in this repository. It must be obtained separately, inventoried, and loaded into HDFS before the analysis jobs can run.
- **Docker Compose:** the repository includes a single-node local Hadoop environment. Docker Desktop/Engine, sufficient local memory/storage, and separate dataset ingestion into HDFS are still required.

## Français

### Documentation

| Guide | Contenu |
|---|---|
| [Présentation en français](docs/fr/README.md) | Objectifs, état actuel et conventions du projet. |
| [Installation et exécution](docs/fr/EXECUTION.md) | Parcours manuel et Docker Compose, prérequis par OS et rôle des scripts. |
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
- Cluster : `python scripts/nexus.py cluster status --backend compose`, `start --backend compose` ou `stop --backend compose`. Le backend SSH reste disponible pour l’environnement WSL du mainteneur.

L’outil Python gère l’inventaire, le build et la soumission des jobs ; les traitements et analyses sont développés en Java MapReduce sur Hadoop.

### Prérequis et modes d’exécution

- **Construire le projet localement :** installer un JDK 17 ou plus récent et Python 3.9 ou plus récent. Le Maven Wrapper télécharge et lance la version Maven configurée ; une installation séparée de Maven n’est donc pas requise. WSL, Docker et Hadoop ne sont pas nécessaires pour compiler le JAR.
- **Exécuter les jobs avec Compose :** Docker Desktop/Engine et Compose démarrent un cluster Hadoop 3.5.0 local ; Python 3.9+ et JDK 17 sont utilisés localement par l’outil et le Maven Wrapper. Pas besoin d’installer Hadoop, d’avoir un accès WSL au mainteneur ou d’utiliser SSH. Voir le guide d’exécution.
- **Exécuter les jobs par SSH manuel :** un cluster Hadoop reste nécessaire. L’alias `turing-server` pointe vers l’environnement WSL local du mainteneur ; ce n’est pas un serveur partagé. Un autre cluster nécessite son propre accès SSH et l’adaptation des chemins spécifiques à la machine dans le script.
- **Utiliser un autre cluster :** y configurer Hadoop 3.5.0, Java 17, HDFS/YARN, la configuration du projet et un compte SSH. Adapter la cible et les chemins HDFS à cet environnement.
- **Jeu de données :** le dataset réel n’est pas inclus dans ce dépôt. Il doit être obtenu séparément, inventorié, puis chargé dans HDFS avant l’exécution des analyses.
- **Docker Compose :** le dépôt fournit un environnement Hadoop local mono-nœud. Docker Desktop/Engine, assez de mémoire/espace disque et l’ingestion séparée du dataset dans HDFS restent nécessaires.

## Current project status / État actuel

- Hadoop 3.5.0, HDFS, and YARN are installed in the maintainer's local WSL environment; a synthetic WordCount smoke job succeeded there. This environment is not shared with repository collaborators. / Hadoop 3.5.0, HDFS et YARN sont installés dans l’environnement WSL local du mainteneur ; un test WordCount synthétique y a réussi. Cet environnement n’est pas partagé avec les collaborateurs du dépôt.
- Java MapReduce jobs for daily activity, retention, and profile-joined technical incidents are prepared. They have not been run on project data. / Les jobs Java MapReduce pour l’activité, la rétention et les incidents techniques joints aux profils sont préparés. Ils n’ont pas été exécutés sur les données du projet.
- The real dataset is still downloading. Do not inspect, move, or ingest its data files until the user confirms completion. The official catalog has been reviewed separately. / Le dataset réel est toujours en cours de téléchargement. Ne pas inspecter, déplacer ou ingérer ses fichiers de données avant confirmation de la fin. Le catalogue officiel a été consulté séparément.
- No real-data results have been produced. / Aucun résultat sur les données réelles n’a encore été produit.

Keep datasets, credentials, and generated local inventory reports out of Git. See the language-specific guides for details.
