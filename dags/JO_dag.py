import os
import pandas as pd
from datetime import datetime
from airflow import DAG
from sqlalchemy import create_engine, text
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

INPUT_CSV = "/opt/airflow/dags/data/fact_resultats_epreuves.csv"
TEMP_PARQUET = "/opt/airflow/data/transformed_data.parquet"

# Fonction de création de la connexion SQLAlchemy
def get_engine():
    conn = BaseHook.get_connection("jo_data")
    uri = conn.get_uri()
    return create_engine(uri)

# Fonction de vérification de la présence d'un fichier
def check_new_file():
    folder = "/opt/airflow/dags/data/"
    return any(f.endswith(".csv") for f in os.listdir(folder))

# Fonction d'extraction
def extract_data():
    df = pd.read_csv(INPUT_CSV, sep=',')
    engine = get_engine()
    df.to_sql('staging_extract', engine, if_exists='replace', index=False)
    print(f"{len(df)} lignes chargées dans staging_extract")
    return INPUT_CSV

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')
    df = pd.read_csv(data, sep=',')

    columns_to_drop = [
            'id_resultat_source','id_athlete_base_resultats','id_personne','id_equipe',
            'id_pays','id_evenement','evenement_en','id_edition','id_competition_sport',
            'competition_en','id_type_competition','id_ville_edition','edition_ville_en',
            'id_nation_edition_base_resultats','id_sport','sport_en','id_discipline_administrative',
            'id_specialite','id_epreuve','id_federation','federation_nom_court', 'pays_en_base_resultats',
            'performance_finale_texte', 'evenement', 'discipline_administrative', 'specialite', 
            'est_epreuve_individuelle', 'est_epreuve_olympique','est_epreuve_ete', 'federation']
    df_transformed = df.drop(columns=columns_to_drop, errors='ignore').drop_duplicates()
    
    date_columns = ['date_debut_edition', 'date_fin_edition', 'dt_creation', 'dt_modification']
    for col in date_columns:
        if col in df_transformed.columns:
            # Convertir en datetime avec le format français DD/MM/YYYY
            df_transformed[col] = pd.to_datetime(
                df_transformed[col], 
                format='%d/%m/%Y',
                errors='coerce'
            )
            df_transformed[col] = df_transformed[col].dt.date
    df_transformed.to_parquet(TEMP_PARQUET, index=False)
    return TEMP_PARQUET

# Fonction de chargement des données en base
def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_task')
    df = pd.read_parquet(data)

    engine = get_engine()

    with engine.begin() as conn:
        # Création de la table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS resultats (
                id_resultat INT PRIMARY KEY,
                source TEXT,
                athlete_nom TEXT,
                athlete_prenom TEXT,
                equipe_en TEXT,
                classement_epreuve FLOAT,
                performance_finale FLOAT,
                categorie_age TEXT,
                type_competition TEXT,
                edition_saison INT,
                date_debut_edition DATE,
                date_fin_edition DATE,
                edition_nation_en TEXT,
                sport TEXT,
                epreuve TEXT,
                epreuve_genre TEXT,
                epreuve_type TEXT,
                est_epreuve_handi INT,
                epreuve_sens_resultat INT,
                dt_creation TIMESTAMP,
                dt_modification TIMESTAMP
            );
        """))
        print("Table resultats créée ou déjà existante")
        
        # Charger les ID déjà en base
        try:
            existing_ids = pd.read_sql("SELECT id_resultat FROM resultats", conn)
            # Filtrer les nouvelles lignes
            df_new = df[~df['id_resultat'].isin(existing_ids['id_resultat'])]
            
        except Exception as e:
            df_new = df.copy()
        
        if df_new.empty:
            print("Aucune nouvelle ligne à insérer.")
        else:
            df_new.to_sql('resultats', conn, if_exists='append', index=False)
            print(f"{len(df_new)} nouvelles lignes insérées.")

# Définition du DAG
dag = DAG(
    'etl_pipeline_JO',
    description             = 'Pipeline ETL & Data Control avec Soda',
    schedule_interval       = '@daily',
    start_date              = datetime(2025, 6, 2),
    catchup                 = False,
    is_paused_upon_creation = False 
)

extract_task    = PythonOperator(task_id='extract_task', python_callable=extract_data, dag=dag)
transform_task = PythonOperator(task_id='transform_task', python_callable=transform_data, dag=dag)
load_task       = PythonOperator(task_id='load_task', python_callable=load_data, dag=dag)

# --- Contrôles qualité Soda ---
run_soda_checks_extract = BashOperator(
    task_id='run_soda_checks_extract',
    bash_command='soda scan -d my_data_source -c /opt/airflow/soda/configuration.yaml /opt/airflow/soda/checks_extract.yaml',
    dag=dag
)

run_soda_checks_load = BashOperator(
    task_id='run_soda_checks_load',
    bash_command='soda scan -d my_data_source -c /opt/airflow/soda/configuration.yaml /opt/airflow/soda/checks_load.yaml',
    dag=dag
)

# --- Dépendances ---
extract_task >> run_soda_checks_extract >> transform_task >> load_task >> run_soda_checks_load  