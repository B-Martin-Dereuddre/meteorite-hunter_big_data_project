# Meteorite Hunter

## Auteurs

MAZLOUM Victor · MARTIN-DEREUDDRE Bastian

## Prérequis

Git, Python ≥ 3.11 et un service Onyxia **VS Code PySpark** avec les identifiants S3 injectés. Pour une exécution locale, utiliser Java 17/21 et installer `pyspark==3.5.8`.

## Jeu de données

**Meteorite Landings**, The Meteoritical Society, diffusé par la NASA. [Fiche et licence](https://data.nasa.gov/dataset/meteorite-landings) (accès public, licence non spécifiée) · [CSV officiel](https://data.nasa.gov/docs/legacy/meteorite_landings/Meteorite_Landings.csv). Instantané du 23/11/2026, **3 952 161 octets**, **45 716 enregistrements**. L’ingestion vérifie le SHA-256.

## Problématique

Comment les météorites observées pendant leur chute (`Fell`) diffèrent-elles des découvertes (`Found`) par leur masse, leur position et leur année de catalogage ? Les résultats décrivent les entrées du catalogue.

## Stockage S3 Onyxia

Bucket `user-v-mazloum-ece`, racine `meteorite-hunter/`. Brut `bronze/meteorite_landings.csv`. Données nettoyées `silver/<run>/`. Indicateurs, contrôles et mesures `gold/<run>/`.

## Reproduction

1. Ouvrir un terminal d’un service VS Code PySpark Onyxia, avec accès en écriture à son bucket personnel.
2. Exécuter les commandes ci-dessous. Remplacer le bucket par le sien si nécessaire.

   ```bash
   git clone https://github.com/B-Martin-Dereuddre/meteorite-hunter_big_data_project.git
   cd meteorite-hunter_big_data_project
   python -m venv --system-site-packages .venv
   source .venv/bin/activate
   python -m pip install -r src/config/requirements.txt
   set -o pipefail
   python -m src.run_pipeline --bucket user-v-mazloum-ece 2>&1 | tee onyxia-run.log
   ```

3. Lire `fell_vs_found.csv`, `by_decade.csv` et `by_grid_10deg.csv` dans Gold. La présence de `manifest.json` confirme la publication et la concordance des indicateurs Spark/DuckDB.
4. En cas d’échec, relancer la même commande. Chaque tentative a son propre identifiant. Si le jeton S3 a expiré, renouveler les identifiants Onyxia avant de relancer. Pour tester localement, omettre `--bucket`.

[Preuves d’exécution et contrôle indépendant](evidence/)
