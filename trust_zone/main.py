############################### SEETUP ######################################
# 1. Hadoop configuration for Spark to access Hadoop binaries
import os
import duckdb

# Set HADOOP_HOME to the parent folder of 'bin'
os.environ['HADOOP_HOME'] = "C:/hadoop"
# Add the bin directory to your PATH
os.environ['PATH'] += os.pathsep + "C:/hadoop/bin"

# 2. Create a SparkSession with the DuckDB connector
from pyspark.sql import SparkSession
# Now run your session creation
duckdb_path = "C:\\Users\\jiaha\\Documents\\Universidad\\BDA\\BCN-Meteorologics\\trust_zone\\duckdb.jar"
spark = SparkSession.builder \
    .config("spark.jars", duckdb_path) \
    .getOrCreate()

# 3. Configuración de archivos
formatted_db_path = "C:\\Users\\jiaha\\Documents\\Universidad\\BDA\\BCN-Meteorologics\\formatted_zone.db"
trusted_db_path = "C:\\Users\\jiaha\\Documents\\Universidad\\BDA\\BCN-Meteorologics\\trust_zone\\trusted_zone.db"

tablas = duckdb.connect(formatted_db_path).execute("SHOW TABLES").fetchall()
print("Tablas en formatted_zone.db:")
for tabla in tablas:
    print(tabla[0])
################################# ANALYSIS FOR FORMATTED ZONE ######################################
import sys
sys.path.insert(0, "..")

from trust_zone.Analysis import analyze_formatted_zone, get_table_summary

# Ejecutar análisis completo de la formatted zone
analysis_report = analyze_formatted_zone(formatted_db_path)
###################### DENIAL CONSTRAINTS ######################
from pyspark.sql import SparkSession, functions as F
# Load a table from DuckDB into a Spark DataFrame
def read_duckdb(table_name, path):
    formatted_path = path.replace("\\", "/")
    
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:duckdb:{formatted_path}") \
        .option("dbtable", table_name) \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .load()
def accident_denial_constraint(df_accidents, df_persones):
    # Rule 1: Duplicados
    df_accidents = df_accidents.dropDuplicates()
    df_persones = df_persones.dropDuplicates()
    
    # Rule 2: Nombres de columnas según tu análisis (nk__any vs nk_any)
    desc_cols = [
        "descripcio_causa_vianant", "descripcio_sexe", "descripcio_lloc_atropellament_vianant", 
        "descripcio_motiu_desplacament_vianant", "descripcio_motiu_desplacament_conductor", 
        "descripcio_victimitzacio", "num_postal"
    ]
    df_accidents = df_accidents.fillna("Desconegut", subset=[c for c in desc_cols if c in df_accidents.columns])
    df_persones = df_persones.fillna("Desconegut", subset=[c for c in desc_cols if c in df_persones.columns])
    
    # Rule 3: Ceros en métricas
    cols_to_zero = ["numero_morts", "numero_lesionats_lleus", "numero_lesionats_greus", "numero_victimes"]
    df_accidents = df_accidents.fillna(0, subset=cols_to_zero)

    # Aplicar Denial Constraints
    acc_valid = df_accidents.withColumn("is_invalid", (
        (F.col("numero_victimes") < (F.col("numero_morts") + F.col("numero_lesionats_lleus") + F.col("numero_lesionats_greus")))
    )).filter("is_invalid == False").drop("is_invalid")

    per_valid = df_persones.withColumn("is_invalid", (
        ((F.col("descripcio_tipus_persona") == "Conductor") & (F.col("edat") < 14)) |
        (F.col("edat") < 0) | (F.col("edat") > 110)
    )).filter("is_invalid == False").drop("is_invalid")

    return acc_valid, per_valid

def meteo_denial_constraint(df_meteo):
    # Rule 0: Duplicados
    df_meteo = df_meteo.dropDuplicates()
    
    # COMBINAR REGLAS (Si cualquiera se cumple, es inválido)
    df_meteo_valid = df_meteo.withColumn("is_invalid", (
        # Regla Temperatura
        ((F.col("acronim").startswith("T")) & ((F.col("valor") < -10) | (F.col("valor") > 42))) |
        # Regla Humedad
        ((F.col("acronim").startswith("H")) & ((F.col("valor") < 0) | (F.col("valor") > 100))) |
        # Regla Precipitacion
        ((F.col("acronim").startswith("PP")) & (F.col("valor") < 0)) |
        # Regla Dirección Viento
        ((F.col("acronim").startswith("D")) & ((F.col("valor") < 0) | (F.col("valor") > 360)))
    )).filter("is_invalid == False").drop("is_invalid")
    
    return df_meteo_valid
def save_to_trusted(df, table_name, db_path):
    """
    Función genérica para pre-crear la tabla en DuckDB y guardar desde Spark
    """
    path_fixed = db_path.replace("\\", "/")
    print(f"🔧 Preparando tabla '{table_name}' en la Trusted Zone {path_fixed}")
    
    # 1. Pre-creación dinámica con DuckDB nativo
    con = duckdb.connect(db_path)
    # Mapeo simple de tipos Spark a SQL
    schema_parts = []
    for field in df.schema:
        dtype = "DOUBLE" if "Double" in field.dataType.simpleString() else \
                "INTEGER" if "Integer" in field.dataType.simpleString() else "VARCHAR"
        schema_parts.append(f'"{field.name}" {dtype}')
    
    columnas_sql = ", ".join(schema_parts)
    con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    con.execute(f'CREATE TABLE "{table_name}" ({columnas_sql})')
    con.close()

    # 2. Escritura JDBC desde Spark
    print(f"💾 Insertando datos en '{table_name}'...")
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:duckdb:{path_fixed}") \
        .option("driver", "org.duckdb.DuckDBDriver") \
        .option("dbtable", f'"{table_name}"') \
        .mode("overwrite") \
        .save()
####################### EJECUCIÓN DEL FLUJO PRINCIPAL #######################
# 1. Carga (Formatted Zone)
df_acc_raw = read_duckdb("t2025_ACCIDENTS_GU_BCN", formatted_db_path)
df_per_raw = read_duckdb("t2025_ACCIDENTS_PERSONES_GU_BCN", formatted_db_path)
df_met_raw = read_duckdb("t2025_METEOCAT_DETALL_ESTACIONS", formatted_db_path)

# 2. Procesamiento (Aplicando tus funciones de Denial Constraints)
# Asumiendo que accident_denial_constraints devuelve (acc_v, per_v, acc_q, per_q)
accidents_valid, persones_valid = accident_denial_constraint(df_acc_raw, df_per_raw)
meteo_valid = meteo_denial_constraint(df_met_raw)

# 3. Persistencia en Trusted Zone
tablas_a_guardar = {
    "T_ACCIDENTS": accidents_valid,
    "T_PERSONES": persones_valid,
    "T_METEO": meteo_valid
}

for nombre, df_spark in tablas_a_guardar.items():
    save_to_trusted(df_spark, nombre, trusted_db_path)

print("✨ Proceso de Trusted Zone finalizado correctamente.")
####################### ANÁLISIS DE LA TRUSTED ZONE #######################
trusted_report = analyze_formatted_zone(db_path=trusted_db_path)