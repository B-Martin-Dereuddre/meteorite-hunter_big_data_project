# METEORITE HUNTER — État d'avancement du projet

**Dernière mise à jour : 23 novembre 2026**

## 1. Présentation du projet

**Thématique :** Sciences spatiales / Analyse géospatiale

**Problématique :** Les météorites observées pendant leur chute (`Fell`) présentent-elles des caractéristiques géographiques, massiques et temporelles différentes de celles découvertes ultérieurement (`Found`) ?

Trois axes d'analyse sont envisagés :

- Répartition géographique et identification de biais potentiels dans les découvertes.
- Comparaison de la distribution des masses entre les catégories `Fell` et `Found`.
- Évolution temporelle des observations enregistrées.

**Limite importante :** le jeu de données recense les observations et découvertes de météorites. Il ne permet pas de mesurer directement la fréquence réelle des impacts de météorites sur Terre.

## 2. Jeu de données

**NASA Meteorite Landings :**

https://data.nasa.gov/docs/legacy/meteorite_landings/Meteorite_Landings.csv

- Date de téléchargement : 23 novembre 2026.
- Taille du fichier : 3 952 161 octets (environ 3,95 Mo).
- Nombre total d'enregistrements : 45 716.
- Champs : name, id, nametype, recclass, mass (g), fall, year, reclat, reclong, GeoLocation.
- Licence : à vérifier.

### Première évaluation de la qualité des données

| Contrôle | Résultat |
|---|---:|
| Nombre total d'enregistrements | 45 716 |
| Catégorie Fell | 1 107 |
| Catégorie Found | 44 609 |
| Masses manquantes | 131 |
| Masses égales à zéro | 19 |
| Années manquantes | 291 |
| Latitudes manquantes | 7 315 |
| Longitudes manquantes | 7 315 |
| Identifiants en doublon | 0 |
| Latitudes invalides | 0 |
| Longitudes au format 0–360 | 1 |
| Masses négatives | 0 |
| Années invalides détectées | 0 |

### Anomalies identifiées

- 19 enregistrements présentent une masse égale à zéro. Ces valeurs seront considérées comme manquantes pour les analyses de masse, sans supprimer les enregistrements.
- Un enregistrement, Meridiani Planum (ID 32789), possède une longitude de 354.473330. Elle devra être convertie en -5.526670.
- Les coordonnées géographiques et les années manquantes ne doivent pas entraîner la suppression globale des enregistrements. Ceux-ci seront exclus uniquement des analyses nécessitant les champs concernés.

## 3. Architecture actuelle

```text
meteorite-hunter_big_data_project/
├── README.md
├── PROJECT_STATUS.md
├── .gitignore
├── evidence/
├── k8s/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── src/
    ├── analytics/
    ├── config/
    ├── dbt/
    ├── ingestion/
    ├── processing/
    └── validation/
        └── quality_check.py
```

Le fichier CSV original est actuellement stocké localement dans :

`data/bronze/meteorite_landings.csv`

Le fichier brut est, pour le moment, exclu de Git. Il devra être téléversé sans modification dans le stockage objet S3 d'Onyxia. Son éventuelle publication dans GitHub reste conditionnée à la vérification de sa licence de redistribution.

## 4. Travail réalisé

- Création du dépôt GitHub public et connexion avec le projet local.
- Création et premier enregistrement de la structure des dossiers.
- Téléchargement et inspection du fichier CSV de la NASA.
- Vérification de la taille et du schéma du jeu de données.
- Première analyse de la qualité des données avec PowerShell.
- Création du script Python de validation : `src/validation/quality_check.py`.
- Mise à jour du script pour lire le fichier CSV depuis le dossier Bronze.
- Exécution réussie du script Python et concordance des résultats avec les contrôles PowerShell.
- Création des dossiers locaux Bronze, Silver et Gold.

## 5. Choix techniques

- Développer et tester localement avec Windows PowerShell avant le passage sur Onyxia.
- Conserver le jeu de données original sans modification dans la couche Bronze.
- Effectuer le nettoyage et la conversion des types dans la couche Silver.
- Produire des jeux de données analytiques agrégés dans la couche Gold.
- Versionner le code source et la documentation sur GitHub ; stocker les données brutes et transformées dans S3.
- Garantir la reproductibilité des règles de qualité grâce à des scripts Python.
- Utiliser DuckDB pour les analyses SQL et la validation indépendante des résultats.
- Utiliser Spark pour expérimenter le traitement distribué et le comparer aux outils locaux.
- Évaluer la pertinence de dbt et d'Iceberg en fonction de l'architecture finale.

## 6. Travaux restants — ordre de priorité

### Priorité 1 — Développement local

1. Finaliser les commits Git et mettre à jour le README.
2. Développer `clean_meteorites.py`.
3. Générer les jeux de données nettoyés Silver aux formats CSV et Parquet.
4. Ajouter des tests de qualité des données et un résumé du traitement.
5. Développer les jeux de données analytiques Gold et les requêtes SQL.
6. Valider indépendamment au moins un résultat avec DuckDB.
7. Mesurer les temps de traitement et documenter les résultats.

### Priorité 2 — Environnement Onyxia

1. Se connecter à l'environnement Onyxia.
2. Configurer les identifiants et le point d'accès S3 sans versionner de secrets.
3. Créer ou identifier le bucket du projet et ses préfixes.
4. Téléverser le fichier CSV original de la NASA dans la couche Bronze.
5. Exécuter le pipeline de traitement.
6. Stocker les résultats Silver et Gold dans S3.
7. Capturer les preuves d'exécution : espace de noms Onyxia, date et heure, contenu S3 et traces d'exécution.

### Priorité 3 — Rendu final

1. Comparer les résultats des exécutions locales et sur Onyxia.
2. Discuter des limites du projet et du passage à un volume de données 100 fois supérieur.
3. Finaliser le README (une page maximum).
4. Préparer le rapport PDF (quatre pages maximum).
5. Atteindre au moins 10 commits significatifs répartis sur plusieurs jours.
6. Garantir des contributions visibles des deux membres de l'équipe.
7. Envoyer le rapport par e-mail en respectant le format d'objet demandé.

## 7. Dépôt GitHub

https://github.com/B-Martin-Dereuddre/meteorite-hunter_big_data_project

**Branche actuelle :** `main`

**Premier commit :** `a204519`

Le dépôt a été synchronisé avec `origin` après son initialisation. Un nouveau commit intégrant le travail de validation locale reste à effectuer au 23 novembre 2026.