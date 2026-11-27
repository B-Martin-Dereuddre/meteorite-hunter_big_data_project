"""Bronze to Silver transformation and row-level data quality flags."""
from pyspark.sql import functions as F

SNAPSHOT_YEAR = 2013


def clean(spark, source):
    raw = (
        spark.read.option("header", True).option("mode", "FAILFAST")
        .option("multiLine", True).csv(str(source))
    )
    mass = F.expr("try_cast(`mass (g)` as double)")
    year = F.expr("try_cast(year as double)")
    latitude = F.expr("try_cast(reclat as double)")
    longitude = F.expr("try_cast(reclong as double)")
    valid_mass = (mass > 0) & (mass < float("inf"))
    valid_year = year.between(1, SNAPSHOT_YEAR) & (year == F.floor(year))
    missing_gps = F.col("reclat").isNull() | F.col("reclong").isNull()
    zero_gps = (latitude == 0) & (longitude == 0)
    valid_gps = latitude.between(-90, 90) & longitude.between(-180, 360) & ~zero_gps
    flag = lambda expr: F.coalesce(expr, F.lit(False))

    return raw.select(
        F.expr("try_cast(id as long)").alias("id"),
        F.col("name"),
        F.col("nametype").alias("name_type"),
        F.col("recclass").alias("classification"),
        F.col("fall").alias("status"),
        F.when(valid_mass, mass).alias("mass_g"),
        F.when(valid_year, year.cast("int")).alias("event_year"),
        F.when(valid_gps, latitude).alias("latitude"),
        F.when(
            valid_gps, F.when(longitude > 180, F.round(longitude - 360, 6)).otherwise(longitude)
        ).alias("longitude"),
        F.col("mass (g)").isNull().alias("mass_missing"),
        flag(mass == 0).alias("mass_zero"),
        flag(F.col("mass (g)").isNotNull() & ~flag(valid_mass) & ~flag(mass == 0)).alias("mass_invalid"),
        F.col("year").isNull().alias("year_missing"),
        flag(F.col("year").isNotNull() & ~flag(valid_year)).alias("year_invalid"),
        missing_gps.alias("gps_missing"),
        flag(zero_gps).alias("gps_zero"),
        flag(~missing_gps & ~flag(valid_gps) & ~flag(zero_gps)).alias("gps_invalid"),
        flag(valid_gps & (longitude > 180)).alias("longitude_normalized"),
    )


def audit(silver):
    flags = [name for name in silver.columns if name.endswith(
        ("_missing", "_invalid", "_zero", "_normalized")
    )]
    stats = silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("id").alias("unique_ids"),
        F.count(F.when(~F.col("status").isin("Found", "Fell") | F.col("status").isNull(), 1))
        .alias("invalid_status"),
        *(F.count(F.when(F.col(name), 1)).alias(name) for name in flags),
    ).first().asDict()
    if stats["rows"] == 0 or stats["unique_ids"] != stats["rows"] or stats["invalid_status"]:
        raise ValueError(f"Échec des contrôles d'intégrité : {stats}")
    return stats
