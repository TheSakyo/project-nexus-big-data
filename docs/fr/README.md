# Project Nexus — Big Data

Projet pédagogique de traitement de données de Nexus Interactive avec Hadoop et Java MapReduce.

## Objectifs

- conserver les sources originales dans une zone `RAW` ;
- nettoyer et préparer les données dans `PROCESSED` ;
- produire les indicateurs dans `ANALYTICS` ;
- implémenter au moins deux traitements Java MapReduce significatifs, dont un croisement de sources ;
- documenter les contrôles de qualité, les résultats, les limites et une mesure de passage à l’échelle.

## État actuel

- Squelette Maven prêt avec Maven Wrapper ; configuration Hadoop versionnée dans `config/hadoop/`.
- Le téléchargement est terminé ; la copie locale du dataset se trouve dans `data/source/`, dossier exclu de Git.
- L’inventaire des fichiers est disponible dans `reports/local/`. Les contrôles approfondis de schéma et de qualité restent à réaliser sur HDFS.
- Les jobs Java MapReduce pour DAU/WAU, rétention et incidents techniques sont préparés ; aucune sortie n’a été calculée sur les données du projet.
- Hadoop 3.5.0, HDFS et YARN sont installés dans l’environnement WSL Linux local du mainteneur, accessible par son alias SSH `turing-server` ; le test WordCount de bout en bout a réussi sur des données synthétiques. Cet alias n’est pas partagé avec les collaborateurs.
- Les zones HDFS `/datalake/raw`, `/datalake/processed` et `/datalake/analytics` sont créées.
- Les outils multiplateformes d’inventaire et de gestion du cluster sont disponibles dans `scripts/nexus.py`.

Les résultats analytiques ne seront documentés qu’après exécution et vérification sur les données réelles.

## Guides

- [Installation et exécution multiplateformes](EXECUTION.md)
- [Plan complet](PLAN.md)
- [État de l’environnement](ENVIRONMENT.md)
- [Préparation des données](PREPARATION.md)
- [Catalogue officiel des données](DATA_CATALOG.md)
- [Modèle de catalogue de données](DATA_CATALOG_TEMPLATE.md)
- [Trame du rapport final](REPORT_TEMPLATE.md)
- [Déroulé de soutenance de 10 minutes](PRESENTATION_OUTLINE.md)

## Installation et exécution

Le build utilise le Maven Wrapper : `./mvnw` sous Linux/macOS/WSL ou `mvnw.cmd` sous Windows. Le projet produit des classes Java 17, compatibles avec les services Hadoop retenus.

Le cluster de préparation ne dispose que d’environ 848 Mio de RAM. Ses paramètres YARN et MapReduce conviennent aux petits essais ; la capacité pour les traitements réels sera évaluée à partir de l’inventaire du dataset.

## Données et secrets

Ne pas ajouter de données brutes, mots de passe, jetons ou exports volumineux au dépôt. Conserver les identifiants localement et documenter leur utilisation sans inclure leurs valeurs.
