# Environnement de développement

État relevé le 13 octobre 2026.

## Poste de travail

| Composant | État | Usage |
|---|---|---|
| Java / `javac` Windows | OpenJDK 21.0.1 | Outils et compilation locale. |
| Maven Wrapper | Maven 3.9.9, wrapper 3.3.4 | Utiliser `mvnw.cmd` sous Windows ou `./mvnw` sous Linux/macOS/WSL. |
| WSL `debian-default` | Debian 13, OpenJDK 21.0.12 et Maven 3.9.9 | Environnement Linux de build déjà prêt. |
| Docker Desktop | CLI installée, moteur arrêté lors du relevé | Non utilisé pour Hadoop. |

Le disque système C: a diminué pendant les installations et téléchargements WSL. Éviter d’y télécharger ou d’y extraire Hadoop et le jeu de données. Le répertoire du jeu de données est séparé du dépôt et ne doit pas être utilisé avant confirmation de la fin du téléchargement.

## Cluster Hadoop de préparation

Un serveur Debian accessible par l’alias SSH `turing-server` a été retenu afin de préserver l’espace du poste Windows/WSL.

| Composant | État |
|---|---|
| Java serveur | Eclipse Temurin 17.0.20.1 dans `/opt/temurin-17.0.20.1`. |
| Hadoop | Apache Hadoop 3.5.0 dans `/opt/hadoop-3.5.0`. |
| Configuration | `/home/turinguser/hadoop-setup/conf`, référence versionnée dans `config/hadoop/`. |
| HDFS | NameNode et DataNode démarrés ; zones `/datalake/raw`, `/datalake/processed`, `/datalake/analytics` créées. |
| YARN | ResourceManager et NodeManager démarrés ; un nœud `RUNNING`. |
| Réseau | Interfaces des services liées à `127.0.0.1`. |
| Stockage HDFS | Environ 113 Gio disponibles lors de la vérification. |
| Test MapReduce de bout en bout | WordCount synthétique réussi le 13 octobre 2026 ; sortie : `hadoop 2`, `mapreduce 1`, `nexus 2`. |

La VM ne dispose que d’environ 848 Mio de RAM. Les ressources YARN sont limitées et ne préjugent pas des performances sur le jeu réel. Le tampon de tri MapReduce est réglé à 16 Mio pour permettre au test synthétique de fonctionner avec le tas Java disponible. Ce test valide HDFS/YARN/MapReduce, pas le traitement des données du projet.

Les archives d’installation vérifiées par checksum sont conservées dans `/home/turinguser/hadoop-setup`. Ne pas supprimer les archives ou répertoires de données sans instruction.

## Règles concernant les données

- Le téléchargement se poursuit dans `D:\gdau8rrtb2czlzgbh4uocjqb.51.75.28.54.sslip.io`.
- Ne pas déplacer, renommer, inspecter ou importer ce répertoire avant confirmation de la fin du téléchargement.
- Une fois prêt, mesurer sa taille et l’espace disponible avant toute copie vers HDFS ; le dépôt reste dans `D:\Workspaces\java-workspace\JavaHadoop`.
- Garder les données téléchargées hors Git ; `data/`, `datalake/`, `datasets/` et `reports/local/` sont ignorés.
