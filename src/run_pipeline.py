"""Run the Meteorite Hunter pipeline locally or inside an Onyxia service."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from uuid import uuid4

from pyspark.sql import SparkSession

from src.analytics.gold import by_decade, by_grid, comparison
from src.ingestion.storage import BYTES, RAW_KEY, SHA256, SOURCE, ObjectStore, fetch_source, validate_source
from src.processing.clean_meteorites import audit, clean
from src.validation.verify_results import open_reference, raw_comparison, verify

ROOT = Path(__file__).resolve().parents[1]


def log(component, message):
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{now} {component} | {message}", flush=True)


def export(dataset, destination, file_format):
    staging = destination.parent / (destination.stem + ".parts")
    writer = dataset.coalesce(1).write
    if file_format == "csv":
        writer = writer.option("header", True)
    writer.format(file_format).save(str(staging))
    part, = staging.glob(f"part-*.{file_format}")
    part.replace(destination)
    shutil.rmtree(staging)


def benchmark(spark, connection, source, checks, repeats):
    samples = {"spark": [], "duckdb": []}
    for run_num in range(repeats + 1):
        queries = (
            ("spark", lambda: [r.asDict() for r in comparison(clean(spark, source))
                              .orderBy("status").collect()]),
            ("duckdb", lambda: raw_comparison(connection, source)),
        )
        for engine, query in (queries if run_num % 2 == 0 else reversed(queries)):
            started = time.perf_counter()
            result = query()
            duration = time.perf_counter() - started
            verify(result, checks)
            if run_num:
                samples[engine].append(round(duration, 4))
    return {
        "repetitions": repeats,
        "sample_seconds": samples,
        "median_seconds": {engine: statistics.median(values) for engine, values in samples.items()},
    }


def process(source, output, repeats):
    started = time.perf_counter()
    spark = (
        SparkSession.builder.appName("Meteorite Hunter")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        startup = round(time.perf_counter() - started, 3)
        log("PYSPARK", f"{spark.version} | {spark.sparkContext.master} | démarrage {startup} s")
        started = time.perf_counter()
        silver = clean(spark, source).cache()
        quality = audit(silver)
        log("QUALITÉ", json.dumps(quality, ensure_ascii=False))
        export(silver, output / "silver" / "meteorites.parquet", "parquet")
        export(silver, output / "silver" / "meteorites.csv", "csv")

        headline = comparison(silver).orderBy("status").cache()
        spark_rows = [row.asDict() for row in headline.collect()]
        export(headline, output / "gold" / "fell_vs_found.csv", "csv")
        headline.unpersist()
        export(by_decade(silver).orderBy("decade", "status"),
               output / "gold" / "by_decade.csv", "csv")
        export(by_grid(silver).orderBy("lat_band", "lon_band", "status"),
               output / "gold" / "by_grid_10deg.csv", "csv")
        silver.unpersist()
        processing = round(time.perf_counter() - started, 3)
        log("GOLD", json.dumps(spark_rows, ensure_ascii=False))
        with open_reference() as reference:
            duckdb_rows = raw_comparison(reference, source)
            verify(spark_rows, duckdb_rows)
            log("DUCKDB", "vérification directe du CSV Bronze : 7 indicateurs concordants par catégorie")
            timings = benchmark(spark, reference, source, duckdb_rows, repeats)
        log("BENCHMARK", json.dumps(timings))
        return {
            "spark_version": spark.version, "spark_startup_seconds": startup,
            "spark_processing_seconds": processing, "quality": quality,
            "comparison": spark_rows, "benchmark": timings,
        }
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, default=ROOT / "data/bronze/meteorite_landings.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "data/runs")
    parser.add_argument("--bucket", help="Bucket S3 Onyxia. Sans ce paramètre, exécution locale.")
    parser.add_argument("--benchmark-repeats", type=int, default=5, choices=range(1, 11))
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    run_id = f"{now:%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    store = ObjectStore(args.bucket) if args.bucket else None
    namespace_file = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    namespace = os.getenv("KUBERNETES_NAMESPACE") or os.getenv("ONYXIA_NAMESPACE")
    if namespace_file.exists():
        namespace = namespace_file.read_text().strip()
    log("RUN", f"{run_id} | namespace={namespace or 'local'}")
    with tempfile.TemporaryDirectory(dir=output, prefix=".staging-") as temporary:
        work = Path(temporary)
        source = args.raw.resolve()
        if source.exists():
            validate_source(source)
        else:
            source = work / "download.csv"
            fetch_source(source)
        if store:
            staged = work / "bronze.csv"
            store.stage_bronze(source, staged)
            source = staged
        else:
            log("BRONZE", f"{source.name} | {BYTES} octets | SHA-256 confirmé")

        run_dir = work / run_id
        for layer in ("silver", "gold"):
            (run_dir / layer).mkdir(parents=True)
        manifest = process(source, run_dir, args.benchmark_repeats)
        manifest.update({
            "run_id": run_id, "utc": now.isoformat(), "namespace": namespace,
            "source": {"url": SOURCE, "bytes": BYTES, "sha256": SHA256},
            "bronze_s3": store.uri(RAW_KEY) if store else None,
            "independent_verification": "passed",
        })
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if store:
            store.publish(run_dir)
        run_dir.rename(output / run_id)
    log("FIN", str(output / run_id))


if __name__ == "__main__":
    main()
