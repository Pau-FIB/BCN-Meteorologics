# #wget -O "duckdb.jar" "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.1/duckdb_jdbc-0.10.1.jar"
import os

import pyspark
from pyspark.sql import SparkSession
import duckdb
import unicodedata


# ── Configuración inicial ──────────────────────────────────────────────────

conn = duckdb.connect("formatted_zone.db")
conn.close()

spark = SparkSession.builder \
    .config("spark.jars", "duckdb.jar") \
    .getOrCreate()

JDBC_URL = "jdbc:duckdb:formatted_zone.db"
DRIVER   = "org.duckdb.DuckDBDriver"

# ── Procesamiento de archivos ──────────────────────────────────────────────

archivos = [
    "../bcn_meteo_csv/2025_accidents_gu_bcn.csv",
    "../bcn_meteo_csv/2025_accidents_persones_gu_bcn.csv",
    "../bcn_meteo_csv/2025_MeteoCat_Detall_Estacions.csv"
]

for ruta in archivos:
    # Leer
    df = spark.read.option("delimiter", ",").option("header", True).option("inferSchema", True).csv(ruta)
    table_name = os.path.basename(ruta).replace(".csv", "")
    
    # Escribir en DuckDB
    print(f"Guardando tabla: {table_name}...")
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", f'"{table_name}"') \
        .option("driver", DRIVER) \
        .mode("overwrite") \
        .save()

