from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
# DAG actualizado automaticamente desde GitHub por Daniel
# --- CONFIGURACIÓN (EDITA ESTO) ---
PROJECT_ID = 'dataengineerp'  # Tu ID de proyecto
BUCKET_NAME = 'datalake-retail-danielmarcel' # Tu bucket real
CLUSTER_NAME = 'cluster-ventas-spark'
REGION = 'us-central1'

# Configuración del Clúster
CLUSTER_CONFIG = {
    # --- AGREGA ESTO AL PRINCIPIO ---
    "gce_cluster_config": {
        "zone_uri": "us-central1-a"  # Forzamos el Barrio A (que suele estar vacío)
    },
    # -------------------------------
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 0, # Modo Single Node (Ahorro máximo)
    },
    "software_config": {
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true"
        }
    }
}

# Configuración del Job de PySpark
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET_NAME}/scripts/procesar_ventas_spark.py", # El script que acabas de subir
        "args": [
            f"gs://{BUCKET_NAME}/raw/ventas_spark.csv",      # Entrada
            f"gs://{BUCKET_NAME}/processed_spark/"           # Salida
        ],
    },
}

with DAG(
    'pipeline_bigdata_spark',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['spark', 'bigdata']
) as dag:

    # 1. Esperar archivo
    esperar_archivo = GCSObjectExistenceSensor(
        task_id='esperar_archivo',
        bucket=BUCKET_NAME,
        object='raw/ventas_spark.csv', # Ojo: buscaremos este nombre específico
        mode='poke',
        timeout=300
    )

    # 2. Crear Clúster (Solo si hay archivo)
    crear_cluster = DataprocCreateClusterOperator(
        task_id="crear_cluster_dataproc",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # 3. Enviar el trabajo pesado
    correr_spark = DataprocSubmitJobOperator(
        task_id="procesar_con_spark",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # 4. DESTRUIR Clúster (Muy importante para el dinero)
    borrar_cluster = DataprocDeleteClusterOperator(
        task_id="borrar_cluster_dataproc",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done" # Se ejecuta aunque el paso anterior falle (limpieza segura)
    )

    # Definir el orden
    esperar_archivo >> crear_cluster >> correr_spark >> borrar_cluster