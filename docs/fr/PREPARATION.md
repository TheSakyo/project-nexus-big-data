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
6. Définir schémas et contrôles avant d’écrire les fonctions Map et Reduce métier.

Les 40 Gio annoncés dans le sujet sont une estimation. Le dimensionnement final utilisera les tailles observées, la compression et l’espace nécessaire aux zones intermédiaires. Le serveur n’a qu’environ 848 Mio de RAM ; si les traitements utiles ne peuvent pas tenir dans cette contrainte, il faudra une autre machine ou une stratégie adaptée, puis mesurer réellement le passage à l’échelle.

## Travail dépendant des données

Le catalogue officiel est disponible dans `D:\gdau8rrtb2czlzgbh4uocjqb.51.75.28.54.sslip.io\DATA_CATALOG.md`. Sa copie de référence dans le dépôt est [Catalogue des données](DATA_CATALOG.md). Il décrit déjà les sources, schémas annoncés, clés de jointure, événements et problèmes de qualité attendus. Cela permet de préparer les traitements et les analyses.

L’inventaire après téléchargement reste nécessaire pour confirmer les fichiers, formats, volumes et écarts réels par rapport au catalogue. Le [modèle de catalogue](DATA_CATALOG_TEMPLATE.md) sert à consigner les observations faites sur les fichiers livrés ; il ne remplace pas le catalogue officiel.
