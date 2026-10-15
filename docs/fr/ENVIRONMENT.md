# Environnement de développement

État initial relevé le 13 octobre 2026 ; configuration Compose et emplacement des données actualisés le 15 octobre 2026.

## Poste de travail

| Composant | État | Usage |
|---|---|---|
| Java / `javac` Windows | OpenJDK 21.0.1 | Outils et compilation locale. |
| Maven Wrapper | Maven 3.9.9, wrapper 3.3.4 | Utiliser `mvnw.cmd` sous Windows ou `./mvnw` sous Linux/macOS/WSL. |
| WSL `debian-default` | Debian 13, OpenJDK 21.0.12 et Maven 3.9.9 | Environnement Linux de build déjà prêt. |
| Docker Desktop | Cluster Hadoop Compose 3.5.0 healthy lors de la vérification du 15 octobre 2026. | Exécution locale du cluster Compose. |

La configuration Hadoop Docker Compose à la racine du dépôt a été démarrée et vérifiée healthy le 15 octobre 2026. Docker Desktop doit être actif pour démarrer ou gérer le cluster.

Le disque système C: a diminué pendant les installations et téléchargements WSL. Éviter d’y télécharger ou d’y extraire Hadoop et le jeu de données. Le téléchargement est terminé ; la copie locale se trouve dans `D:\Workspaces\java-workspace\JavaHadoop\data\source\` et est exclue de Git.

## Cluster Hadoop local du mainteneur

Le cluster Debian tourne dans l’environnement WSL local du mainteneur. `turing-server` est son alias SSH personnel vers cet environnement ; ce n’est pas un serveur distant partagé et les collaborateurs ne peuvent pas l’utiliser tel quel.

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

- Le téléchargement est terminé ; la copie source utilisée par le projet est `data/source/`.
- L’inventaire local est enregistré dans `reports/local/` ; les contrôles complets de qualité et l’ingestion HDFS restent à réaliser.
- Mesurer l’espace disponible avant l’ingestion vers HDFS ; la copie source reste inchangée et exclue de Git.
- Garder les données téléchargées hors Git ; `data/`, `datalake/`, `datasets/` et `reports/local/` sont ignorés.
