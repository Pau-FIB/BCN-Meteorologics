#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Módulo de análisis de datos para la formatted zone.
Proporciona funciones para analizar y explorar todas las tablas en formatted_zone.duckdb
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def analyze_formatted_zone(db_path: str = "formatted_zone.duckdb") -> dict:
    """
    Realiza un análisis completo de todas las tablas en la formatted zone.
    
    Args:
        db_path (str): Ruta a la base de datos DuckDB (por defecto: formatted_zone.duckdb)
    
    Returns:
        dict: Diccionario con análisis de todas las tablas
    """
    
    con = duckdb.connect(db_path)
    analysis_report = {}
    
    try:
        # Obtener todas las tablas
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"\n{'='*80}")
        print(f"📊 ANÁLISIS DE FORMATTED ZONE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        print(f"Total de tablas encontradas: {len(tables)}\n")
        
        for (table_name,) in tables:
            print(f"\n{'─'*80}")
            print(f"📋 TABLE: {table_name.upper()}")
            print(f"{'─'*80}")
            
            table_analysis = {}
            
            # 1. INFORMACIÓN BÁSICA
            print(f"\n🔹 Información Básica:")
            row_count = con.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
            col_count = len(con.execute(f"DESCRIBE \"{table_name}\"").fetchall())
            table_analysis['row_count'] = row_count
            table_analysis['column_count'] = col_count
            print(f"   ├─ Filas: {row_count:,}")
            print(f"   └─ Columnas: {col_count}")
            
            # 2. ESQUEMA Y TIPOS DE DATOS
            print(f"\n🔹 Esquema de Columnas:")
            columns_info = con.execute(f"DESCRIBE \"{table_name}\"").fetchall()
            table_analysis['columns'] = {}
            
            for col_name, col_type, *_ in columns_info:
                table_analysis['columns'][col_name] = col_type
                print(f"   ├─ {col_name:<30} ({col_type})")
            
            # 3. ESTADÍSTICAS DE VALORES NULOS
            print(f"\n🔹 Valores Nulos:")
            null_stats = {}
            has_nulls = False
            for col_name, *_ in columns_info:
                null_count = con.execute(f"SELECT COUNT(*) FROM \"{table_name}\" WHERE \"{col_name}\" IS NULL").fetchone()[0]
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0
                null_stats[col_name] = {'count': null_count, 'percentage': null_pct}
                
                if null_count > 0:
                    has_nulls = True
                    print(f"   ├─ {col_name:<30} {null_count:>6} ({null_pct:>5.2f}%)")
            
            if not has_nulls:
                print(f"   └─ ✅ Sin valores nulos")
            
            table_analysis['null_analysis'] = null_stats
            
            # 4. ESTADÍSTICAS NUMÉRICAS
            print(f"\n🔹 Estadísticas Numéricas:")
            numeric_stats = {}
            numeric_cols = [col for col, tipo in [(c, t) for c, t, *_ in columns_info] 
                          if any(x in tipo.upper() for x in ['INT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BIGINT', 'SMALLINT'])]
            
            if numeric_cols:
                for col in numeric_cols:
                    try:
                        stats = con.execute(f"""
                            SELECT 
                                MIN(\"{col}\") as min_val,
                                MAX(\"{col}\") as max_val,
                                AVG(\"{col}\") as avg_val,
                                STDDEV(\"{col}\") as std_val
                            FROM \"{table_name}\"
                            WHERE \"{col}\" IS NOT NULL
                        """).fetchone()
                        
                        if stats[0] is not None:
                            numeric_stats[col] = {
                                'min': stats[0],
                                'max': stats[1],
                                'avg': stats[2],
                                'std': stats[3]
                            }
                            print(f"   ├─ {col:<28}")
                            print(f"   │  ├─ Min: {stats[0]}")
                            print(f"   │  ├─ Max: {stats[1]}")
                            print(f"   │  ├─ Promedio: {stats[2]:.4f}" if isinstance(stats[2], float) else f"   │  ├─ Promedio: {stats[2]}")
                            print(f"   │  └─ Desv. Est.: {stats[3]:.4f}" if stats[3] else f"   │  └─ Desv. Est.: N/A")
                    except:
                        pass
                
                if not numeric_stats:
                    print(f"   └─ Sin columnas numéricas analizables")
            else:
                print(f"   └─ Sin columnas numéricas")
            
            table_analysis['numeric_stats'] = numeric_stats
            
            # 5. VALORES ÚNICOS (SOLO PARA COLUMNAS CON POCOS VALORES)
            print(f"\n🔹 Valores Únicos (Columnas con < 50 valores distintos):")
            unique_stats = {}
            distinct_found = False
            
            for col_name, *_ in columns_info:
                distinct_count = con.execute(f"SELECT COUNT(DISTINCT \"{col_name}\") FROM \"{table_name}\"").fetchone()[0]
                unique_stats[col_name] = distinct_count
                
                if distinct_count < 50:
                    distinct_found = True
                    print(f"   ├─ {col_name:<30} {distinct_count} valores distintos")
                    
                    # Mostrar los valores si hay pocos
                    if distinct_count <= 10:
                        values = con.execute(f"SELECT DISTINCT \"{col_name}\" FROM \"{table_name}\" ORDER BY 1").fetchall()
                        print(f"   │  └─ Valores: {', '.join(str(v[0]) for v in values)}")
            
            if not distinct_found:
                print(f"   └─ Todas las columnas tienen > 50 valores distintos")
            
            table_analysis['unique_stats'] = unique_stats
            
            # 6. VISTA PREVIA DE DATOS
            print(f"\n🔹 Vista Previa (5 primeras filas):")
            df_preview = con.execute(f"SELECT * FROM \"{table_name}\" LIMIT 5").df()
            print(f"\n{df_preview.to_string()}\n")
            
            analysis_report[table_name] = table_analysis
        
        print(f"\n{'='*80}")
        print(f"✅ ANÁLISIS COMPLETADO")
        print(f"{'='*80}\n")
        
    finally:
        con.close()
    
    return analysis_report


def get_table_summary(table_name: str, db_path: str = "formatted_zone.duckdb") -> pd.DataFrame:
    """
    Obtiene un resumen rápido de una tabla específica.
    
    Args:
        table_name (str): Nombre de la tabla
        db_path (str): Ruta a la base de datos DuckDB
    
    Returns:
        pd.DataFrame: Resumen de la tabla
    """
    con = duckdb.connect(db_path)
    try:
        df = con.execute(f"SELECT * FROM \"{table_name}\"").df()
        
        print(f"\n📊 RESUMEN: {table_name}")
        print(f"{'─'*60}")
        print(f"Filas: {len(df):,} | Columnas: {len(df.columns)}")
        print(f"\nTipos de datos:\n{df.dtypes}")
        print(f"\nInformación de nulos:\n{df.isnull().sum()}")
        print(f"\nEstadísticas descriptivas:\n{df.describe()}")
        
        return df
    finally:
        con.close()

