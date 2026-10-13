# Déroulé de soutenance — 10 minutes par étudiant

> Plan préparatoire. Remplacer les champs à compléter par des résultats vérifiés après traitement des données réelles. Ajuster la durée si le groupe compte plusieurs étudiants.

| Temps indicatif | Partie | Contenu attendu |
|---:|---|---|
| 0:00–1:00 | Contexte et enjeu | Présenter Nexus Interactive, la décision à éclairer et la question principale. |
| 1:00–2:00 | Sources | Montrer les sources réellement reçues, leurs formats, périodes, tailles et limites. |
| 2:00–3:15 | Architecture | Expliquer ingestion et zones `RAW`, `PROCESSED`, `ANALYTICS`, avec un diagramme lisible. |
| 3:15–4:15 | Qualité | Illustrer les contrôles réalisés et les volumes conservés, corrigés ou rejetés. |
| 4:15–5:45 | Jobs Hadoop | Décrire les jobs Java MapReduce, leur croisement de sources et leur relance. |
| 5:45–7:30 | Résultats | Présenter DAU/WAU/rétention, axe métier et exploration libre, avec définitions et limites. |
| 7:30–8:45 | Passage à l’échelle | Donner volume, limite de heap, durée, tâches et compteurs Hadoop réellement mesurés. |
| 8:45–9:30 | Recommandations et limites | Relier les recommandations aux constats vérifiés ; distinguer corrélation et causalité. |
| 9:30–10:00 | Conclusion | Résumer la réponse à la question métier et ouvrir sur la suite. |

## Supports visuels à préparer

1. Carte des sources et des volumes observés.
2. Diagramme du data lake et du parcours d’une donnée.
3. Flux Mapper → Shuffle/Sort → Reducer pour chaque job.
4. Graphique des indicateurs avec période, population et dénominateur affichés.
5. Tableau des contrôles de qualité et compteurs.
6. Schéma/tableau des mesures de passage à l’échelle.

## Contrôle avant soutenance

- Chaque chiffre peut être reproduit à partir d’une commande, d’un job et d’une sortie conservée.
- Les données synthétiques sont clairement étiquetées et séparées des résultats réels.
- Les règles d’activité, rétention et jointure sont définies explicitement.
- Les limites de données, méthode et infrastructure sont visibles.
- Le temps de parole est répété avec la durée prévue pour chaque étudiant.
