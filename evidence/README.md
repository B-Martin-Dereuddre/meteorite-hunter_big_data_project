# Preuves d’exécution Onyxia

Exécution `20261127T110923Z-a0805f3d`, le 27 novembre 2026 de 11:09 à 11:10 UTC (12:09 à 12:10 à Paris). Namespace `user-v-mazloum-ece`, service `vscode-pyspark-172994`.

1. [Trace du pipeline](01_execution_onyxia.svg). Lecture du CSV Bronze avec contrôle SHA-256, transformations et agrégats avec Spark 4.1.1, contrôle indépendant DuckDB 1.3.2 sur le brut, publication du manifeste.
2. [Inventaire S3](02_stockage_s3_onyxia.svg). Liste des objets Bronze, Silver et Gold relue via l’API S3, avec la taille et l’heure de modification de chaque objet.

Le manifeste est disponible dans `s3://user-v-mazloum-ece/meteorite-hunter/gold/20261127T110923Z-a0805f3d/manifest.json`.

| Indicateur contrôlé | Fell | Found |
|---|---:|---:|
| Enregistrements | 1 107 | 44 609 |
| Masses positives | 1 075 | 44 491 |
| Masse médiane (g) | 2 800 | 30,54 |
| Masse moyenne (g) | 47 070,72 | 12 467,24 |
| Années valides | 1 107 | 44 317 |
| Positions valides | 1 096 | 31 091 |
| Positions à 60° S ou plus au sud | 0 | 22 099 |

La requête [compare_raw.sql](../src/analytics/compare_raw.sql) obtient ces valeurs en relisant Bronze sans utiliser Silver ni Gold. Enchaînement local des mêmes étapes validé avec Spark 3.5.8. Les tailles publiées sur S3 sont de 3 952 161 octets pour Bronze, 5 354 526 octets pour le CSV Silver et 894 388 octets pour le Parquet Silver.
