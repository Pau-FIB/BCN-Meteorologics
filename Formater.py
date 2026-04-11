# # #wget -O "duckdb.jar" "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.1/duckdb_jdbc-0.10.1.jar"
# # from pyspark.sql import SparkSession
# # import os
# # import unicodedata

# # spark = SparkSession.builder \
# #     .config("spark.jars", "duckdb.jar") \
# #     .appName("FormattedZonePipeline") \
# #     .getOrCreate()

# # raw_path = "./bcn_meteo_csv"

# # def clean_column(col):
# #     col = col.replace('\ufeff', '')
# #     col = col.replace('"', '')
# #     col = col.strip().lower().replace(" ", "_")
# #     col = ''.join(
# #         c for c in unicodedata.normalize('NFD', col)
# #         if unicodedata.category(c) != 'Mn'
# #     )
# #     return col

# # def clean_table_name(name):
# #     return name.replace(".csv", "").lower().replace(" ", "_")

# # for file in os.listdir(raw_path):

# #     if file.endswith(".csv"):

# #         file_path = os.path.join(raw_path, file)

# #         df = spark.read.option("header", True) \
# #                        .option("inferSchema", True) \
# #                        .csv(file_path)

# #         new_cols = [clean_column(c) for c in df.columns]
# #         df = df.toDF(*new_cols)

# #         table_name = clean_table_name(file)

# #         df.write \
# #           .format("jdbc") \
# #           .option("url", "jdbc:duckdb:formatted_zone.duckdb") \
# #           .option("dbtable", table_name) \
# #           .option("driver", "org.duckdb.DuckDBDriver") \
# #           .mode("overwrite") \
# #           .save()

# from pyspark.sql import SparkSession
# import duckdb
# import os
# import unicodedata

# conn = duckdb.connect("formatted_zone.duckdb")
# conn.close()

# spark = SparkSession.builder \
#     .config("spark.jars", "duckdb.jar") \
#     .appName("FormattedZonePipeline") \
#     .getOrCreate()

# raw_path = "./bcn_meteo_csv"
# print("Files:", os.listdir(raw_path))

# def clean_column(col):
#     col = col.replace('\ufeff', '')
#     col = col.replace('"', '')
#     col = col.strip().lower().replace(" ", "_")
#     col = ''.join(
#         c for c in unicodedata.normalize('NFD', col)
#         if unicodedata.category(c) != 'Mn'
#     )
#     return col

# def clean_table_name(name):
#     name = name.replace(".csv", "").lower().replace(" ", "_")
#     if name[0].isdigit():
#         name = "t_" + name
#     return name

# for file in os.listdir(raw_path):
#     if file.endswith(".csv"):
#         print("Found:", file)
#         print("Processing:", file)

#         file_path = os.path.join(raw_path, file)
#         df = spark.read.option("header", True) \
#                        .option("inferSchema", True) \
#                        .csv(file_path)

#         new_columns = [clean_column(c) for c in df.columns]
#         df = df.toDF(*new_columns)

#         print("Rows:", df.count())
#         table_name = clean_table_name(file)
#         print("Writing table:", table_name)

#         # Spark -> Pandas -> DuckDB (una tabla por CSV)
#         pdf = df.toPandas()
#         conn = duckdb.connect("formatted_zone.duckdb")
#         conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM pdf")
#         conn.close()

#         print(f"✓ {table_name} done")

# print("All tables written.")

import pyspark
from pyspark.sql import SparkSession
import duckdb
import unicodedata

# ── Funciones de limpieza ──────────────────────────────────────────────────

def clean_table_name(name):
    # Limpia el nombre del archivo para usarlo como tabla
    name = name.split("/")[-1] # Nos quedamos solo con el nombre del archivo, no la ruta
    name = name.replace(".csv", "").lower().replace(" ", "").replace("-", "_")
    if name[0].isdigit():
        name = "t" + name
    return name

def clean_column(col):
    # Limpia los nombres de las columnas (acentos, espacios, etc)
    col = col.replace('\ufeff', '')
    col = col.replace('"', '')
    col = col.strip().lower().replace(" ", "_")
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    return col

# ── Configuración inicial ──────────────────────────────────────────────────

conn = duckdb.connect("bcn_meteo.db")
conn.close()

spark = SparkSession.builder \
    .config("spark.jars", "duckdb.jar") \
    .getOrCreate()

JDBC_URL = "jdbc:duckdb:bcn_meteo.db"
DRIVER   = "org.duckdb.DuckDBDriver"

# ── Procesamiento de archivos ──────────────────────────────────────────────

archivos = [
    "./bcn_meteo_csv/2025_accidents_gu_bcn.csv",
    "./bcn_meteo_csv/2025_accidents_persones_gu_bcn.csv",
    "./bcn_meteo_csv/2025_MeteoCat_Detall_Estacions.csv"
]

for ruta in archivos:
    # 1. Leer
    df = spark.read.option("delimiter", ",").option("header", True).csv(ruta)
    
    # 2. Limpiar columnas: Aplicamos clean_column a cada nombre
    new_columns = [clean_column(c) for c in df.columns]
    df = df.toDF(*new_columns)
    
    # 3. Determinar nombre de tabla limpio
    table_name = clean_table_name(ruta)
    
    # 4. Escribir en DuckDB
    print(f"Guardando tabla: {table_name}...")
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("driver", DRIVER) \
        .mode("overwrite") \
        .save()

