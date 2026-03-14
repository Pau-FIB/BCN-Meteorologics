from pyspark.sql import SparkSession
import os
import unicodedata
import duckdb

# iniciamos la sesión de Spark
spark = SparkSession.builder \
   .appName("FormattedZonePipeline") \
   .getOrCreate()

raw_path = "./bcn_meteo_csv" # ruta de los archivos csv originales generados por el colector de datos. Se asume que cada csv corresponde a una tabla diferente.
formatted_path = "./formatted" # ruta donde se guardaran las tablas relacionales. 

os.makedirs(formatted_path, exist_ok=True) # se crea esta carpeta de salida si no existe.

def clean_column(col):
    # Funcion para homogeneizar los nombres de las columnas. Elimina caracteres especiales, acentos, convierte a minúsculas y reemplaza espacios por guiones bajos.
    col = col.replace('\ufeff', '')
    col = col.replace('"', '')
    col = col.strip()
    col = col.lower()
    col = col.replace(" ", "_")
    col = ''.join(
        c for c in unicodedata.normalize('NFD', col)
        if unicodedata.category(c) != 'Mn'
    )
    return col


def clean_table_name(name):
    #Funcion que preprocesa el nombre de la tabla. Elimina la extensión .csv, convierte a minúsculas y reemplaza espacios por guiones bajos. 
    #necesario para hacer consultas SQL sin problemas de sintaxis con duckdb.
    name = name.replace(".csv", "")
    name = name.lower()
    name = name.replace(" ", "_")
    return name


# -------------------------
# FASE 1
# Spark limpia y convierte los csv a parquet, con nombres de columnas y tablas preprocesados.
#El uso de parquet es mucho mas eficiente para el almacenamiento y consulta de datos, permite conservación de tipos de datos por lo que DuckDB no tendra que volver a inferirlos.
# -------------------------

for file in os.listdir(raw_path):

    if file.endswith(".csv"):

        file_path = os.path.join(raw_path, file)

        df = spark.read.option("header", True) \
                       .option("inferSchema", True) \
                       .csv(file_path) # hace tipado automático de las columnas para la DB final usando duckdb

        new_cols = [clean_column(c) for c in df.columns]
        df = df.toDF(*new_cols)

        table_name = clean_table_name(file)

        output_path = os.path.join(formatted_path, table_name)

        df.write.mode("overwrite").parquet(output_path, compression="snappy")# comprime los archivos parquet con snappy para ahorrar espacio sin perder rendimiento en las consultas.


# -------------------------
# FASE 2
# DuckDB crea tablas relacionales a partir de los archivos parquet generados por Spark. Cada carpeta dentro de formatted corresponde a una tabla diferente.
# -------------------------

con = duckdb.connect("formatted_zone.duckdb")# se crea la base de datos duckdb donde se guardaran las tablas relacionales. 

for folder in os.listdir(formatted_path):

    table_name = folder.lower()

    con.execute(f"""
        CREATE OR REPLACE TABLE "{table_name}" AS
        SELECT * FROM '{formatted_path}/{folder}/*.parquet'
    """)

con.close()# cerramos la conexión a la base de datos. 