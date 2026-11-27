"""Spark analytical tables, keyed by observation status."""
from pyspark.sql import functions as F


def comparison(silver):
    return silver.groupBy("status").agg(
        F.count("*").alias("records"),
        F.count("mass_g").alias("valid_mass"),
        F.median("mass_g").alias("median_mass_g"),
        F.avg("mass_g").alias("mean_mass_g"),
        F.count("event_year").alias("valid_year"),
        F.count("latitude").alias("valid_gps"),
        F.count(F.when(F.col("latitude") <= -60, 1)).alias("south_60"),
    )


def by_decade(silver):
    return (
        silver.where(F.col("event_year").isNotNull())
        .withColumn("decade", (F.floor(F.col("event_year") / 10) * 10).cast("int"))
        .groupBy("decade", "status").count()
        .withColumnRenamed("count", "records")
    )


def by_grid(silver):
    # Half-open 10-degree cells except for +90/+180, which belong to the last cells.
    return (
        silver.where(F.col("latitude").isNotNull())
        .withColumn("lat_band", (F.floor(F.least("latitude", F.lit(89.999999)) / 10) * 10).cast("int"))
        .withColumn("lon_band", (F.floor(F.least("longitude", F.lit(179.999999)) / 10) * 10).cast("int"))
        .groupBy("lat_band", "lon_band", "status").count()
        .withColumnRenamed("count", "records")
    )
