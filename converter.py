import pandas as pd
from pyspark.sql import SparkSession

df = pd.read_csv('pii_file.csv')
df.to_parquet('pii_file.parquet')


spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
    .getOrCreate()
)
df = spark.read.format("csv").option("header", True).load("pii_file.csv")
df.write.format("delta").save("delta")