from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, sha2, current_timestamp
import sys

# --- CONFIGURACIÓN ---
# Recibimos argumentos desde Airflow (para no quemar rutas en el código)
# Arg 1: Ruta de entrada (ej: gs://mi-bucket/raw/ventas.csv)
# Arg 2: Ruta de salida (ej: gs://mi-bucket/processed_spark/)

if len(sys.argv) < 3:
    print("Error: Se requieren argumentos de entrada y salida")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# 1. Iniciar la Sesión de Spark (El "Cerebro")
spark = SparkSession.builder \
    .appName("ProcesamientoVentasRetail") \
    .getOrCreate()

print(f"--- Iniciando procesamiento Spark ---")
print(f"Leyendo desde: {input_path}")

# 2. Leer CSV (Spark infiere el esquema automáticamente)
# header=True significa que la primera fila tiene los títulos
df = spark.read.option("header", "True").option("inferSchema", "True").csv(input_path)

# 3. TRANSFORMACIONES
# A) Anonimizar: Encriptar el ID de transacción usando SHA-256
df_anonimo = df.withColumn("id_transaccion_hash", sha2(col("id_transaccion").cast("string"), 256))

# B) Calcular métricas de negocio
# Agrupamos por producto y sumamos cantidad y monto
df_reporte = df_anonimo.groupBy("producto") \
    .agg(
        sum("cantidad").alias("total_unidades"),
        sum("monto_total").alias("total_ventas"),
        avg("monto_total").alias("ticket_promedio")
    ) \
    .withColumn("fecha_proceso", current_timestamp())

# 4. ESCRIBIR RESULTADO
# Guardamos en formato PARQUET (comprimido y columnar)
print(f"Escribiendo resultado en: {output_path}")
df_reporte.write.mode("overwrite").parquet(output_path)

print("--- ¡Procesamiento completado con éxito! ---")
spark.stop()