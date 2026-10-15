## Avec Docker (ou azure, ou orale, une VM Linux avec 128GO de stockage = on y stocke hadoop ou hdfs; le dataset (40 GO))
Besoin métier -> identifier métriques -> collecte -> traitements -> Analyse -> Data Analyse -> Prise de décision - outil décisionnel

Oracle : Always Free

wget -r -np -c \
  --user student \
  --ask-password \
  https://gdau8rrtb2czlzgbh4uocjqb.51.75.28.54.sslip.io/

Mot de passe : le saisir uniquement à l’invite de `wget --ask-password`. Ne pas l’enregistrer dans le dépôt.

# Projet Big Data — Project Nexus

## But

Creer le dataset via un stockage cloud (Oracle Always Free)

## Contexte

Vous rejoignez l’équipe Data de Nexus Interactive, un studio qui exploite un jeu multijoueur comptant plusieurs centaines de milliers de joueurs.

Depuis plusieurs semaines, les équipes produit, marketing et techniques observent différents phénomènes qu’elles ne parviennent pas à expliquer clairement :

- évolution de l’activité et de la rétention ;
- abandons au cours de certaines parties ;
- retours concernant l’équilibrage ;
- incidents techniques ;
- différences entre populations de joueurs ;
- comportements potentiellement frauduleux.

La direction souhaite exploiter les données disponibles pour mieux comprendre la situation et déterminer les actions à entreprendre.

## Données disponibles

Vous disposez de plusieurs sources hétérogènes :

- événements de télémétrie gameplay ;
- logs techniques ;
- données sur les joueurs et les transactions stockées dans **MongoDB** ;
- référentiels métier au format CSV.

Certaines sources possèdent un volume qui rend leur traitement difficile avec un programme classique chargé intégralement en mémoire.

Le dictionnaire des champs est décrit dans [docs/DATA_CATALOG.md](docs/DATA_CATALOG.md). L’accès aux sources est décrit dans le [README](README.md).

## Votre mission

Votre équipe doit concevoir et implémenter une architecture Big Data permettant :

- d’ingérer les différentes sources ;
- de conserver les données originales ;
- de nettoyer et transformer les données ;
- de croiser plusieurs sources ;
- de produire des indicateurs et résultats analytiques ;
- de présenter à la direction les phénomènes importants découverts.

L’architecture reposera principalement sur **Hadoop** et **Java MapReduce**.

Votre objectif ne consiste pas uniquement à produire des calculs. Vous devez transformer les données en informations susceptibles d’éclairer une décision de Nexus Interactive.

## 1. Cadrage des analyses

Avant de commencer l’implémentation, vous devrez définir les analyses que vous souhaitez réaliser.

Pour chaque analyse, précisez brièvement :

- la question étudiée ;
- la décision que cette analyse pourrait améliorer ;
- les acteurs concernés ;
- les données nécessaires ;
- le résultat attendu ;
- l’action qui pourrait suivre ;
- le principal risque d’erreur ou d’interprétation.

Ce cadrage devra apparaître dans votre rapport final.

## 2. Organisation du data lake

Les données devront être organisées dans au moins trois zones.

- **RAW**
- **PROCESSED**
- **ANALYTICS**

### Zone RAW

Elle contient les données sources dans leur format d’origine.

Les données présentes dans cette zone ne doivent pas être modifiées après leur ingestion.

### Zone PROCESSED

Elle contient les données :

- nettoyées ;
- normalisées ;
- dédupliquées si nécessaire ;
- enrichies ;
- préparées pour les analyses.

### Zone ANALYTICS

Elle contient les résultats directement exploitables :

- indicateurs ;
- agrégations ;
- tableaux de synthèse ;
- populations ou événements à examiner ;
- résultats destinés au rapport et à la soutenance.

Vous devrez pouvoir expliquer le parcours d’une donnée depuis sa source jusqu’au résultat présenté.

## 3. Travail technique attendu

Votre réalisation devra comprendre au minimum :

- un environnement Hadoop fonctionnel ;
- des scripts ou commandes d’ingestion reproductibles ;
- une organisation claire des zones RAW, PROCESSED et ANALYTICS ;
- au moins deux traitements Java MapReduce significatifs ;
- au moins un croisement entre plusieurs sources ;
- des contrôles élémentaires de qualité des données ;
- des traitements pouvant être relancés ;
- une documentation permettant de reproduire l’exécution.

L’environnement pourra être Dockerisé. Si vous n’utilisez pas Docker, la procédure d’installation et d’exécution devra être suffisamment précise pour être reproduite sur une autre machine.

Les technologies complémentaires sont autorisées lorsqu’elles sont utiles, mais les traitements principaux évalués devront reposer sur Hadoop et Java MapReduce.

## 4. Passage à l’échelle

Vous devrez démontrer qu’au moins un traitement fonctionne sur un volume de données supérieur à la mémoire disponible pour un seul processus Java.

Votre démonstration devra préciser :

- le volume de données traité ;
- la limite de mémoire utilisée ;
- le traitement exécuté ;
- la manière dont Hadoop répartit le stockage et le calcul ;
- les principaux indicateurs d’exécution observés.

Il n’est pas demandé de construire une infrastructure de production complète. L’objectif consiste à démontrer l’intérêt du traitement distribué et l’impossibilité de résoudre le cas en chargeant simplement toutes les données en mémoire.

## 5. Analyses obligatoires

Pour limiter le périmètre, vous devrez réaliser trois axes d’analyse.

### Axe 1 — Activité et rétention

Vous devrez étudier :

- l’évolution du nombre de joueurs actifs ;
- le DAU et le WAU ;
- la rétention des joueurs ;
- une éventuelle évolution anormale récente.

Vous devrez définir précisément vos métriques :

- événement permettant de considérer un joueur comme actif ;
- population étudiée ;
- période d’observation ;
- définition d’une cohorte ;
- fenêtre de rétention ;
- traitement des données incomplètes.

### Axe 2 — Analyse au choix

Choisissez une problématique parmi les suivantes :

**Gameplay**

- Certains niveaux présentent-ils des taux de mort ou d’abandon anormaux ?
- Certaines armes semblent-elles déséquilibrées ?
- Certaines situations de jeu produisent-elles une expérience inhabituelle ?

**Technique**

- Existe-t-il une relation entre les problèmes techniques et l’abandon des joueurs ?
- Certaines versions du jeu semblent-elles particulièrement problématiques ?
- Certaines configurations matérielles rencontrent-elles davantage d’erreurs ?

**Marketing et valeur des joueurs**

- Quels canaux d’acquisition attirent les joueurs les plus actifs ?
- Quels canaux attirent les joueurs présentant la meilleure rétention ?
- Certains canaux semblent-ils associés à une valeur plus élevée ?

**Fraude et comportements atypiques**

- Certains comportements pourraient-ils correspondre à des bots ?
- Certaines transactions ou séquences de jeu semblent-elles anormales ?
- Certaines populations présentent-elles une activité difficilement compatible avec un comportement humain ?

### Axe 3 — Exploration libre

Vous devrez rechercher et présenter au moins un phénomène que la direction n’a pas explicitement demandé d’étudier.

Les données peuvent contenir des anomalies, des ruptures, des biais ou des relations inattendues. Leur nature et leur nombre ne vous seront pas communiqués.

Vous ne serez pas évalués sur le nombre de phénomènes annoncés, mais sur :

- la pertinence de votre méthode d’exploration ;
- la solidité des éléments présentés ;
- votre capacité à écarter les fausses pistes ;
- la qualité de votre interprétation.

Vous pouvez conclure qu’une piste n’est pas suffisamment démontrée, à condition de l’expliquer.

## 6. Qualité et limites des données

Vous devrez documenter les principaux problèmes rencontrés, par exemple :

- données manquantes ;
- doublons ;
- formats incohérents ;
- identifiants impossibles à relier ;
- dates ou fuseaux horaires incompatibles ;
- événements envoyés plusieurs fois ;
- versions différentes d’un même schéma ;
- populations insuffisamment représentées.

Une donnée n’a pas besoin d’être parfaite pour être utilisée. Vous devez cependant expliquer si sa qualité est suffisante pour répondre à la question étudiée.

## 7. Corrélation et causalité

Vous devez distinguer clairement :

- un constat descriptif ;
- une corrélation ;
- une hypothèse explicative ;
- une causalité démontrée.

Une corrélation ne devra jamais être présentée automatiquement comme une causalité.

Par exemple, si les joueurs ayant acheté un objet cosmétique présentent une meilleure rétention, vous ne pouvez pas conclure que l’achat provoque la rétention.

Vous devrez rechercher d’autres explications possibles :

- les acheteurs étaient déjà plus engagés avant l’achat ;
- une promotion a ciblé des joueurs déjà fidèles ;
- le canal d’acquisition ou la cohorte d’installation ;
- le temps de jeu, qui augmente à la fois l’exposition à la boutique et la rétention mesurée ;
- un autre facteur commun non observé.

Pour chaque phénomène important, indiquez :

- ce que les données permettent d’affirmer ;
- ce qu’elles ne permettent pas d’affirmer ;
- les explications alternatives ;
- l’analyse ou l’expérimentation complémentaire nécessaire.

## 8. Présentation des phénomènes découverts

Pour chaque phénomène retenu dans votre rapport, présentez :

1. la question étudiée ;
2. les données utilisées ;
3. le résultat observé ;
4. l’ampleur du phénomène ;
5. votre niveau de confiance ;
6. les limites de l’analyse ;
7. les conséquences possibles pour Nexus Interactive ;
8. une action ou une investigation complémentaire recommandée.

Les résultats doivent être compréhensibles par une direction qui ne connaît pas le fonctionnement détaillé de MapReduce.

## Livrables

### 1. Dépôt du projet

Le dépôt devra contenir :

- le code Java/MapReduce ;
- les scripts d’ingestion et d’exécution ;
- les fichiers de configuration ;
- l’organisation du data lake ;
- un fichier README expliquant l’installation et l’exécution ;
- des exemples ou extraits des résultats produits.

Les données volumineuses ne devront pas nécessairement être intégrées directement au dépôt. Précisez comment les générer, les récupérer ou les positionner.

### 2. Présentation technique & rapport d'analyse

Présentation format Powerpoint qui devra présenter :

- un schéma de l’architecture ;
- les sources utilisées ;
- l’organisation RAW, PROCESSED et ANALYTICS ;
- les principaux traitements ;
- les règles de nettoyage ;
- les croisements effectués ;
- la démonstration du passage à l’échelle.

Pour la partie analyse, il devra présenter :

- le cadrage des analyses ;
- les définitions des indicateurs ;
- les résultats principaux ;
- le phénomène découvert lors de l’exploration libre ;
- les limites des données ;
- la distinction entre corrélation et causalité ;
- les recommandations adressées à Nexus Interactive.

### 3. Soutenance

La soutenance dure 10 minutes par étudiant.

Pour un groupe de trois étudiants, la durée totale sera donc de 30 minutes.

Chaque étudiant devra prendre la parole et être capable d’expliquer :

- une partie de l’architecture ou des traitements ;
- au moins un résultat analytique ;
- les limites de l’interprétation ;
- les conséquences possibles pour Nexus Interactive.

La soutenance devra être présentée comme une restitution à la direction du studio, et non comme une simple démonstration de code.

### Conseils pour la soutenance

Vous pouvez structurer les 10 minutes de chaque étudiant de cette manière :

- 2 minutes : question et décision à éclairer ;
- 3 minutes : données et traitement réalisé ;
- 3 minutes : résultats et interprétation ;
- 2 minutes : limites et recommandation.

La démonstration technique détaillée pourra être préparée en complément pour la phase de questions.

## Temps de travail indicatif

Le projet représente environ 15 à 20 heures de travail par étudiant.

Une répartition possible :

- 2 h : exploration et cadrage ;
- 3 à 4 h : environnement et ingestion ;
- 5 à 6 h : traitements Hadoop/MapReduce ;
- 3 à 4 h : analyses et vérifications ;
- 2 h : documentation ;
- 1 à 2 h : préparation de la soutenance.

La qualité d’un nombre limité d’analyses sera davantage valorisée que l’accumulation de résultats superficiels.

## Critères d’évaluation

- Architecture et organisation des données + Code : 80 %
- Interprétation, corrélation et causalité : 10 %
- Clarté de la soutenance et des recommandations : 10 %

## Bonus raisonnables

Peuvent valoriser le projet sans être obligatoires :

- environnement Dockerisé ;
- traitement de questions supplémentaires ;
- tests automatisés ;
- visualisations pertinentes ;
- analyse des performances ;
- proposition d’une expérimentation ;
- mécanisme simple de détection d’anomalies réutilisable.
