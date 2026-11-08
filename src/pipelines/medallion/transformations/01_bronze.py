"""Camada BRONZE: cópia fiel da fonte, sem transformação de negócio.

Regras da camada:
- Não filtra nem corrige dados; apenas acrescenta metadados de ingestão.
- Preserva o schema original para permitir reprocessamento das camadas seguintes.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_TABLE = spark.conf.get("source_table", "samples.nyctaxi.trips")  # noqa: F821


@dp.materialized_view(
    name="bronze_trips",
    comment="Corridas de táxi brutas, copiadas da fonte com metadados de ingestão.",
    table_properties={"quality": "bronze"},
)
def bronze_trips():
    return (
        spark.read.table(SOURCE_TABLE)  # noqa: F821
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(SOURCE_TABLE))
    )


# Quando tiver arquivos chegando em um Volume (CSV/JSON/Parquet), troque a
# materialized view acima por uma streaming table com Auto Loader:
#
# @dp.table(name="bronze_trips", table_properties={"quality": "bronze"})
# def bronze_trips():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("cloudFiles.schemaLocation", "/Volumes/<catalog>/<schema>/<volume>/_schemas/trips")
#         .load("/Volumes/<catalog>/<schema>/<volume>/landing/trips/")
#         .withColumn("_ingested_at", F.current_timestamp())
#         .withColumn("_source_file", F.col("_metadata.file_path"))
#     )
