# Installer et exécuter le projet

Ce guide distingue la compilation du code et l’exécution de jobs MapReduce. Compiler le JAR ne nécessite pas Hadoop ; exécuter un job nécessite toujours un cluster Hadoop avec HDFS et YARN.

## Choisir un mode

| Mode | À installer sur le poste | Où Hadoop s’exécute | État dans ce dépôt |
|---|---|---|---|
| Manuel | JDK 17, Python 3.9+, Hadoop 3.5.0 et ses dépendances | Linux, ou Linux dans WSL2 sous Windows | Disponible sur le WSL du mainteneur. Un autre cluster distant exige de configurer son accès SSH et d’adapter les chemins spécifiques au serveur. |
| Docker Compose | Docker Desktop sous Windows/macOS, ou Docker Engine + Compose sous Linux ; Python 3.9+ et JDK 17 pour les commandes/build locaux | Un conteneur local Hadoop 3.5.0 avec HDFS/YARN | Disponible avec `compose.yaml` et le backend Compose de `nexus.py`. Aucun compte sur le WSL du mainteneur n’est nécessaire. |

Compose est la voie recommandée pour les collaborateurs : elle lance un cluster pseudo-distribué local et conserve les données HDFS dans un volume Docker nommé. Le build du JAR se fait sur le poste avec le Maven Wrapper ; Hadoop n’a pas à y être installé.

L’image, le cluster vide et les scripts peuvent être préparés et démarrés pendant le téléchargement du dataset. Le conteneur ne monte pas le dossier de téléchargement et ne lit aucune donnée source.

## Prérequis communs

- Git pour cloner le dépôt.
- Java 17 pour construire le JAR sur l’hôte. Java 21 convient aussi à la compilation ; le cluster Hadoop 3.5.0 doit utiliser Java 17.
- Python 3.9 ou plus récent pour `scripts/nexus.py`.
- Pour le mode Compose : Docker Engine avec Docker Compose v2.17 ou plus récent, moteur démarré et environ 6 Gio de mémoire disponibles pour ce conteneur.
- Le Maven Wrapper est inclus (`mvnw` et `mvnw.cmd`) : aucune installation séparée de Maven n’est nécessaire. Il télécharge Maven et les dépendances au premier build ; une connexion Internet est donc requise.
- Des données d’entrée au format attendu, puis leur import dans HDFS. Docker Compose ne fournira pas le dataset.

Vérifier les outils :

```sh
git --version
java -version
python --version
```

Sous Linux/macOS, `python3 --version` peut remplacer la dernière commande.

## Méthode 1 — installation manuelle

### Compatibilité par système

| Système | Build et inventaire | Hadoop manuel pour les jobs |
|---|---|---|
| Windows | Directement dans PowerShell : JDK 17+ et Python 3.9+ | Utiliser WSL2 avec une distribution Linux. Le Hadoop natif Windows n’est pas le parcours pris en charge par Apache ni par les scripts du dépôt. |
| GNU/Linux | JDK 17+, Python 3.9+ et OpenSSH si le cluster est distant | Installer et configurer Hadoop 3.5.0 sur Linux. C’est la voie manuelle recommandée. |
| macOS | JDK 17+ et Python 3.9+ | Hadoop peut demander des ajustements propres à la machine ; ce dépôt ne fournit pas de configuration macOS native. Préférer une VM Linux ou Docker Compose. |

Le guide Apache de [configuration d’un cluster à un nœud](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html) décrit l’installation pseudo-distribuée HDFS/YARN. Hadoop 3.5.0 requiert Java 17 côté serveur selon les [notes de version Apache](https://hadoop.apache.org/docs/r3.5.0/).

Sur Debian/Ubuntu (y compris WSL), les dépendances de base peuvent être installées avec :

```sh
sudo apt update
sudo apt install openjdk-17-jdk python3 openssh-client
```

Télécharger ensuite la distribution Hadoop 3.5.0 depuis Apache, vérifier son SHA-512, la décompresser dans un chemin Linux, définir `JAVA_HOME`/`HADOOP_HOME`, puis configurer les quatre XML de HDFS/YARN. Suivre les étapes du guide Apache, notamment l’initialisation HDFS une seule fois. Les réglages du mainteneur dans `config/hadoop/` sont propres à son WSL et ne constituent pas une configuration prête à l’emploi sur un autre OS.

### Build et inventaire

À la racine du dépôt :

```sh
python scripts/nexus.py build
python scripts/nexus.py inventory --root /chemin/vers/dataset
```

Sous Windows, le chemin du dataset peut être par exemple `D:\\datasets\\nexus`. Le build utilise `mvnw.cmd` automatiquement. Sous Linux/macOS/WSL, le script utilise `./mvnw`.

L’inventaire fonctionne sans Hadoop et sans lire le contenu des fichiers, sauf si `--sha256` est demandé. Respecter les règles de [préparation du dataset](PREPARATION.md) et ne pas inventorier le dossier pendant le téléchargement.

### Exécuter sur un cluster manuel

Pour qu’un utilisateur exécute les commandes `cluster` et `job` actuelles, il faut :

1. un cluster Hadoop 3.5.0 avec Java 17, HDFS et YARN en fonctionnement ;
2. un compte SSH sur l’hôte et les commandes OpenSSH `ssh` et `scp` sur le poste ;
3. une cible SSH configurée dans `~/.ssh/config` ;
4. les chemins de configuration, de données et d’installation attendus par le script, ou une adaptation du script pour l’environnement.

Exemple de cible SSH personnelle (remplacer l’hôte et le compte) :

```sshconfig
Host my-hadoop
    HostName hadoop.example.org
    User my-user
    IdentityFile ~/.ssh/id_ed25519
```

Puis, depuis le dépôt :

```sh
python scripts/nexus.py cluster status --target my-hadoop
python scripts/nexus.py job activity --target my-hadoop \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

**Limite actuelle :** `--target` ne configure que l’hôte SSH. `scripts/nexus.py` contient aussi des chemins distants et des emplacements Java/Hadoop propres à l’environnement du mainteneur (`/home/turinguser/...`, `/opt/...`). Changer `--target` seul ne rend donc pas le script compatible avec un cluster arbitraire. Il faut adapter ces constantes/configurations, ou faire évoluer le script pour les rendre configurables.

Si Hadoop est installé localement dans une distribution Linux et non sur un hôte SSH, les commandes SSH actuelles ne sont pas le bon backend. Il faut lancer Hadoop localement et soumettre le JAR avec les commandes `hdfs`/`hadoop` du cluster.

## Méthode 2 — Docker Compose

### Dépendances prévues

- Windows ou macOS : Docker Desktop avec son moteur démarré ; Compose est inclus.
- GNU/Linux : Docker Engine, Docker CLI et le plugin Docker Compose.
- Python 3.9+ et JDK 17 sont utilisés sur l’hôte par `nexus.py` et le Maven Wrapper. Hadoop, ses binaires, HDFS et YARN sont dans le conteneur.
- Le service demande 6 Gio de mémoire au moteur Docker par défaut. Ajuster `NEXUS_HADOOP_MEMORY_LIMIT` et les ressources YARN seulement si l’on sait réduire ces besoins.
- Le dataset reste à télécharger et à importer dans HDFS séparément ; il n’est pas nécessaire pour construire ou démarrer le cluster vide.

La documentation officielle explique l’[installation de Compose](https://docs.docker.com/compose/install/). Sous Windows, Docker Desktop peut utiliser WSL2 comme backend, mais les collaborateurs n’ont pas à installer Hadoop dans leur distribution WSL : Hadoop tourne dans les conteneurs.

### Démarrer, compiler et arrêter

Depuis la racine du dépôt, la première commande construit l’image Linux Hadoop 3.5.0, vérifie le checksum SHA-512 de l’archive Apache, démarre HDFS/YARN et attend que le service soit sain :

```sh
python scripts/nexus.py cluster start --backend compose
```

Les commandes suivantes vérifient le service et construisent le JAR sur l’hôte :

```sh
python scripts/nexus.py cluster status --backend compose
python scripts/nexus.py build
```

Les interfaces locales sont `http://localhost:9870/` pour HDFS et `http://localhost:8088/` pour YARN. Elles sont liées à `127.0.0.1`, pas exposées au réseau local.

Un volume Docker nommé conserve HDFS quand le conteneur est arrêté. Arrêter le service sans supprimer les données :

Au premier démarrage, le script initialise uniquement un volume NameNode vide ; les démarrages suivants réutilisent ses métadonnées et ses fichiers HDFS.

```sh
python scripts/nexus.py cluster stop --backend compose
```

Cette commande arrête le conteneur, mais conserve l’image construite et le volume HDFS. Au prochain `cluster start`, Compose réutilise normalement les couches d’image en cache et les mêmes fichiers HDFS.

**Attention :** ne pas utiliser `docker compose down --volumes` ni supprimer le volume Docker du projet si l’on veut garder les fichiers HDFS. `--volumes` efface les données persistées dans ce volume.

### Vérifier les deux backends l’un après l’autre

Ces commandes vérifient la compilation et le démarrage des deux environnements sans faire tourner leurs clusters Hadoop en même temps. Elles ne lancent pas de job et ne nécessitent pas le dataset. Exécuter chaque commande séparément et attendre le retour de l’invite du terminal avant de continuer. Ne pas arrêter le cluster SSH pendant un job actif.

Depuis la racine du dépôt, vérifier d’abord Docker (la sortie doit contenir `Client` et `Server`) et construire le JAR :

```sh
docker version
python scripts/nexus.py build
```

Vérifier puis arrêter les services Hadoop WSL du mainteneur :

```sh
python scripts/nexus.py cluster status --backend ssh
python scripts/nexus.py cluster stop --backend ssh
```

Démarrer, vérifier puis arrêter le cluster Compose. L’arrêt conserve son volume HDFS :

```sh
python scripts/nexus.py cluster start --backend compose
python scripts/nexus.py cluster status --backend compose
python scripts/nexus.py cluster stop --backend compose
```

Enfin, redémarrer et vérifier les services Hadoop WSL :

```sh
python scripts/nexus.py cluster start --backend ssh
python scripts/nexus.py cluster status --backend ssh
```

Les HDFS SSH/WSL et Compose sont indépendants : un fichier présent dans l’un n’est pas automatiquement disponible dans l’autre. Le statut SSH peut prendre quelques secondes à signaler qu’un service est indisponible ; attendre la fin de la commande avant d’en saisir une autre.

### Importer des entrées dans HDFS

Une fois le téléchargement terminé, copier un dossier d’entrée dans le conteneur puis le déposer dans HDFS. Exemple à adapter au chemin réel :

```sh
docker compose cp "CHEMIN_LOCAL_TELEMETRY" hadoop:/tmp/telemetry
docker compose exec -T hadoop hdfs dfs -put /tmp/telemetry /datalake/raw/telemetry
```

Pour une très grande source, cette copie ajoute une étape de transit ; mesurer le disque local avant de dupliquer les données. Les originaux téléchargés restent hors du dépôt Git.

### Soumettre un job

Les arguments métier restent identiques ; `--backend compose` exécute les étapes Java dans le conteneur au lieu d’utiliser SSH/SCP :

```sh
python scripts/nexus.py job activity --backend compose \
  --telemetry /datalake/raw/telemetry \
  --work /datalake/processed/run-activity-001 \
  --output /datalake/analytics/activity-001 \
  --start-date YYYY-MM-DD
```

Le cluster Compose sera un environnement local mono-nœud pseudo-distribué, utile pour apprendre et exécuter des essais. Il ne remplace pas un cluster distribué et ne permet pas, à lui seul, de mesurer un passage à l’échelle multi-nœuds.

## Les scripts Python servent-ils encore avec Compose ?

Oui, mais les commandes d’accès au cluster doivent être adaptées :

| Commande/capacité | Manuel SSH actuel | Avec Compose après intégration |
|---|---|---|
| `inventory` | Utilisable depuis l’hôte | Utilisable depuis l’hôte ; les données brutes restent hors image/conteneur. |
| `build` | Compile le JAR avec le JDK local | Peut compiler dans le conteneur afin d’éviter un JDK local. |
| `cluster status/start/stop` | Gère le cluster mainteneur par SSH | Gère le conteneur via Docker Compose ; `stop` préserve le volume HDFS. |
| `job` | Copie le JAR par SCP et soumet sur l’hôte SSH | Soumet les mêmes étapes dans le conteneur ; les validations de dates/chemins et la prévention des sorties existantes sont communes. |

Les scripts restent le point d’entrée commun. L’inventaire fonctionne localement, le build utilise le JDK du poste et chaque commande de cluster/job choisit explicitement `--backend ssh` ou `--backend compose`. Les détails de transport sont isolés selon le mode.

## Limites communes

- Le jeu réel est externe au dépôt ; l’utilisateur doit obtenir les fichiers, inventorier les formats puis charger les entrées dans HDFS.
- Les jobs attendent les profils joueurs en JSONL. Un export BSON MongoDB devra être converti après inspection des données réellement livrées.
- Le cluster local mono-nœud est fait pour le développement pédagogique, pas pour exposer HDFS/YARN sur Internet ni pour garantir des performances de production.
- Ne jamais lancer `hdfs namenode -format` sur un volume contenant des données existantes : le formatage réinitialise les métadonnées du NameNode.

## Ressources

- [Guide officiel Apache — Hadoop 3.5.0, cluster mono-nœud](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Guide officiel Apache — Docker Hadoop](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/HadoopDocker.html)
- [Documentation officielle Docker — installation de Compose](https://docs.docker.com/compose/install/)
- [Guide de préparation du dataset](PREPARATION.md)
