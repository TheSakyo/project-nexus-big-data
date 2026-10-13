# Plan de réalisation — Project Nexus

Ce plan suit les exigences du sujet. Les choix qui dépendent des formats ou du contenu restent ouverts jusqu’à l’inventaire après téléchargement.

## État et règle de travail

Le téléchargement des données est en cours hors du dépôt. Ne pas lire, déplacer, renommer ou traiter ces fichiers avant confirmation de sa fin. Le code, les documents et la configuration se travaillent dans `D:\Workspaces\java-workspace\JavaHadoop`.

## Étapes et critères de sortie

| Étape | Actions | Critère de sortie |
|---|---|---|
| 1. État des lieux | Examiner le dépôt, les consignes, Java, Maven, Docker/Compose, WSL et les possibilités Hadoop. | L’état est décrit et les changements Git préexistants sont préservés. |
| 2. Inventaire des données | Après confirmation, relever fichiers, formats, volumes, partitions, fichiers vides et erreurs ; comparer au manifeste et au catalogue. | Outil Python prêt (`python scripts/nexus.py inventory`) ; inventaire réel et écarts établis sans supposer la présence d’une source. |
| 3. Choix d’environnement | Comparer les options disponibles selon mémoire, stockage, réseau et compatibilité Hadoop/Java. | Préparé : serveur Debian `turing-server`, Hadoop 3.5.0, Java serveur 17 et smoke test HDFS/YARN/MapReduce réussi. |
| 4. Data lake et ingestion | Définir les zones `RAW`, `PROCESSED`, `ANALYTICS` ; préparer une ingestion relançable qui conserve les originaux. | Chaque lot est traçable ; `RAW` reste inchangé par les traitements. |
| 5. Qualité et préparation | Définir les règles à partir des vrais schémas : champs manquants, dates, formats, doublons et identifiants ; compter rejets et normalisations. | Les sorties `PROCESSED` ont des contrôles et règles documentés. |
| 6. Traitements MapReduce | Construire au moins deux jobs Java significatifs et reproductibles. | Jobs exécutables avec entrées/sorties explicites et résultats vérifiés. |
| 7. Analyses et jointures | Étudier activité/rétention, choisir un axe métier après inspection, croiser au moins deux sources et mener une exploration libre. | Définitions, populations, fenêtres, résultats, limites et confiance documentés. |
| 8. Passage à l’échelle | Fixer une limite mémoire Java, dépasser cette limite en volume, relever compteurs et journaux Hadoop. | Volume, limite, durée et indicateurs consignés ; aucune performance sans mesure. |
| 9. Livrables | Finaliser README, scripts, configuration, résultats vérifiés, rapport et présentation. | Une autre personne peut reproduire le parcours et comprendre les recommandations. |

Les préparations indépendantes des données sont faites : Maven Wrapper, configuration Hadoop, outil Python d’inventaire et de gestion du cluster. Voir [PREPARATION.md](PREPARATION.md).

## Analyses à cadrer

### Axe 1 — Activité et rétention

À définir après lecture du catalogue et échantillonnage : événements actifs, population, période, cohortes, fenêtres de rétention et données incomplètes. Indicateurs visés : activité quotidienne, DAU, WAU, rétention et éventuelle rupture récente.

### Axe 2 — Problématique métier au choix

Le choix entre gameplay, technique, marketing/valeur ou fraude dépend des champs et clés réellement disponibles. Documenter la question, la décision à éclairer, les acteurs, les sources, le résultat attendu, l’action possible et les risques d’interprétation.

### Axe 3 — Exploration libre

Explorer un phénomène non demandé explicitement, vérifier les données et pistes alternatives, puis distinguer constat descriptif, corrélation, hypothèse et causalité démontrée.

## Décisions en attente

- L’environnement de préparation est fixé à Hadoop 3.5.0 sur `turing-server`, en mode pseudo-distribué mono-nœud ; ce n’est pas une capacité de production.
- Chemin physique et stratégie d’ingestion à décider après confirmation du téléchargement.
- Les zones HDFS `/datalake/raw`, `/datalake/processed` et `/datalake/analytics` existent ; le partitionnement reste à définir selon les sources.
- Formats réellement livrés et variantes de schéma.
- Axe métier et clés de jointure disponibles.
- Mémoire nécessaire et volume retenu pour l’expérience d’échelle.

Le smoke test synthétique valide HDFS → YARN → MapReduce, mais n’utilise pas les données du projet. La VM a environ 848 Mio de RAM ; les traitements réels peuvent nécessiter une autre machine ou davantage de ressources.

## Précautions de reproductibilité

- Ne jamais enregistrer de mot de passe ou jeton dans le dépôt, les journaux ou les commandes versionnées.
- Garder les gros fichiers et sorties générées hors de Git.
- Rendre les traitements relançables sans écraser `RAW`.
- Conserver commandes, paramètres, versions, volumes et résultats observés.
- Ne pas présenter de résultat synthétique ou miniature comme un résultat du dataset réel.
