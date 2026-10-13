# Project Nexus — Rapport de projet

> Trame à compléter après inventaire, implémentation et exécution. Ne pas transformer les intitulés ou exemples synthétiques en résultats réels.

## Résumé exécutif

- Problématique : `À compléter`
- Sources et période réellement analysées : `À compléter`
- Résultats vérifiés : `À compléter après exécution`
- Recommandation et niveau de confiance : `À compléter`

## 1. Contexte et question métier

- Contexte du studio et décision à éclairer.
- Questions d’analyse et populations concernées.
- Définitions opérationnelles retenues (joueur actif, abandon, incident, etc.).

## 2. Données et provenance

- Références au [catalogue des données](DATA_CATALOG_TEMPLATE.md) complété.
- Inventaire : chemins logiques, formats, tailles, période et partitions.
- Provenance, limites d’accès, données sensibles et règles de conservation.
- Écarts au manifeste ou au catalogue.

## 3. Architecture et flux

- Diagramme du parcours source → `RAW` → `PROCESSED` → `ANALYTICS`.
- Environnement Hadoop, versions, topologie et contraintes de ressources.
- Stratégie d’ingestion, traçabilité des lots et procédure de relance.

## 4. Qualité et préparation

| Contrôle | Règle | Enregistrements concernés | Action | Compteur observé |
|---|---|---:|---|---:|
| À compléter | À compléter | À mesurer | À documenter | À mesurer |

Documenter les valeurs absentes, invalides, doublons, corrections et rejets. Conserver les sources `RAW` inchangées.

## 5. Traitements Java MapReduce

Pour chaque job, indiquer : question, entrées/sorties, clés, Mapper, Reducer, partitionnement, relance et contrôles de résultat.

### Job 1 — Activité et rétention

`À définir après inspection des événements et horodatages.`

### Job 2 — Analyse métier et croisement de sources

`À choisir après confirmation des clés et champs disponibles.`

## 6. Analyses et résultats

### Activité, DAU, WAU et rétention

Définition, population, période, cohortes, fenêtres, résultats mesurés, données incomplètes et limites.

### Problématique métier

Question, sources croisées, méthode, résultat observé, décision possible et risques d’interprétation.

### Exploration libre

Phénomène observé, contrôles de pistes alternatives, constat descriptif, corrélation, hypothèse et éventuelle preuve causale.

Ne présenter aucun résultat avant de l’avoir recalculé et vérifié.

## 7. Expérience de passage à l’échelle

| Élément | Valeur mesurée |
|---|---|
| Job et version du code | À mesurer |
| Volume et nombre de fichiers | À mesurer |
| Limite de heap Java | À mesurer |
| Durée et ressources | À mesurer |
| Tâches Map/Reduce et compteurs Hadoop | À mesurer |
| Résultat de contrôle | À vérifier |

Expliquer comment le volume dépasse la mémoire d’un processus Java et quelles tâches Hadoop sont réparties. Distinguer l’expérience synthétique du traitement réel.

## 8. Limites, risques et recommandations

Séparer les limites de couverture, qualité, méthode et infrastructure. Distinguer association statistique et causalité. Relier chaque recommandation à une observation vérifiée.

## 9. Reproduction

- Versions Java, Hadoop, Maven et Python.
- Commandes d’inventaire, ingestion, compilation et exécution.
- Paramètres, chemins logiques et emplacements des sorties.
- Contrôles attendus et procédure de relance.

## Annexes

Catalogue complet, schémas, dictionnaire, configurations non secrètes, commandes, compteurs, exemples de sorties et décisions prises.
