import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuration
BUCKET_NAME = "jo-data-lake"
S3_CONN_ID = "minio_conn"
LOCAL_DATA_DIR = "/opt/airflow/dags/data/"

def ingest_csv_to_bronze():
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    
    # Liste tous les fichiers CSV dans le dossier local
    files = [f for f in os.listdir(LOCAL_DATA_DIR) if f.endswith('.csv')]
    
    if not files:
        print("Aucun fichier CSV trouvé pour l'ingestion.")
        return

    for filename in files:
        local_path = os.path.join(LOCAL_DATA_DIR, filename)
        # On définit le chemin dans MinIO (Zone Bronze)
        s3_key = f"bronze/batch/{filename}"
        
        # Upload vers MinIO
        s3.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"✅ Fichier {filename} ingéré avec succès dans s3://{BUCKET_NAME}/{s3_key}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,                            # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),     # Temps d'attente entre deux tentatives
}

with DAG(
    'ingestion_batch_bronze',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_csv_to_bronze_task',
        python_callable=ingest_csv_to_bronze
    )