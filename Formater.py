
#wget -O "duckdb.jar" "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.1/duckdb_jdbc-0.10.1.jar"
from pyspark.sql import SparkSession
import os
import unicodedata

spark = SparkSession.builder \
    .config("spark.jars", "duckdb.jar") \
    .appName("FormattedZonePipeline") \
    .getOrCreate()

raw_path = "./bcn_meteo_csv"

def clean_column(col):
    col = col.replace('\ufeff', '')
    col = col.replace('"', '')
    col = col.strip().lower().replace(" ", "_")
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    return col

def clean_table_name(name):
    return name.replace(".csv", "").lower().replace(" ", "_")

for file in os.listdir(raw_path):

    if file.endswith(".csv"):

        file_path = os.path.join(raw_path, file)

        df = spark.read.option("header", True) \
                       .option("inferSchema", True) \
                       .csv(file_path)

        new_cols = [clean_column(c) for c in df.columns]
        df = df.toDF(*new_cols)

        table_name = clean_table_name(file)

        #  escribir en DuckDB usando JDBC (como en clase)
        df.write \
          .format("jdbc") \
          .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
          .option("dbtable", table_name) \
          .option("driver", "org.duckdb.DuckDBDriver") \
          .mode("overwrite") \
          .save()