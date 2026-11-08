"""Camada SILVER: dados limpos, tipados e validados.

Regras da camada:
- Expectations descartam linhas inválidas (expect_or_drop) ou apenas registram (expect).
- Colunas derivadas simples e padronização de nomes.
- Uma linha por corrida; sem agregações.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_trips",
    comment="Corridas de táxi limpas e validadas, uma linha por corrida.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_fare", "fare_amount > 0")
@dp.expect_or_drop("valid_distance", "trip_distance > 0")
@dp.expect_or_drop("valid_time_order", "dropoff_at > pickup_at")
@dp.expect("has_pickup_zip", "pickup_zip IS NOT NULL")
@dp.expect_or_drop("reasonable_distance", "trip_distance < 100")
def silver_trips():
    bronze = spark.read.table("bronze_trips")  # noqa: F821
    return (
        bronze.select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("pickup_zip").cast("string").alias("pickup_zip"),
            F.col("dropoff_zip").cast("string").alias("dropoff_zip"),
        )
        .withColumn("pickup_date", F.to_date("pickup_at"))
        .withColumn("pickup_hour", F.hour("pickup_at"))
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60.0,
        )
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
    )
