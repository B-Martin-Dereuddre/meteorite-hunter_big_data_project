# METEORITE HUNTER — Big Data Project

## Authors
- MAZLOUM Victor
- MARTIN-DEREUDDRE Bastian


## Prerequisites
- Python 3
- Git
- Access to the ECE Onyxia environment (S3-compatible object storage)

## Dataset
- **Name:** NASA Meteorite Landings
- **Source:** https://data.nasa.gov/docs/legacy/meteorite_landings/Meteorite_Landings.csv
- **License:** To be verified from the original NASA dataset documentation
- **Download date:** 2026-11-23
- **Size:** 3,952,161 bytes (approximately 3.95 MB)
- **Records:** 45,716

## Problem statement
This project investigates differences between meteorites observed during their fall (`Fell`) and those discovered afterwards (`Found`). It explores their recorded geographical distribution, mass and temporal characteristics. The results describe catalogued observations and must not be interpreted as actual meteorite impact rates.

## S3 paths
- **Bucket:** TO BE COMPLETED
- **Bronze:** `s3://<bucket>/meteorite-hunter/bronze/`
- **Silver:** `s3://<bucket>/meteorite-hunter/silver/`
- **Gold:** `s3://<bucket>/meteorite-hunter/gold/`

## Reproduction steps
1. Download the NASA CSV from the source URL and place it in `data/bronze/meteorite_landings.csv`.
2. Run the local data quality check:
   ```powershell
   python .\src\validation\quality_check.py
   ```
3. Connect to Onyxia and configure access to the S3 bucket.
4. Upload the original CSV to the Bronze S3 path.
5. Run the processing pipeline and store the Silver and Gold outputs in S3.

The processing and analytics steps will be documented as they are implemented.
