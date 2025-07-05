import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Ajout du dossier contenant les scripts au path Python
sys.path.append('/home/nonie/exam_IA/scripts')

# Import des fonctions définies dans ton fichier Python de traitement météo
from score_etl import extract_weather, transform_weather, load_weather

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logging.info("Chargement du DAG score_etl...")

default_args = {
    'start_date': datetime(2024, 1, 1),
}

# Définition du DAG
with DAG(
    dag_id='weather_score_etl',
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    logging.info("Initialisation des tâches du DAG weather_score_etl...")

    # Définition des tâches
    t1 = PythonOperator(
        task_id='extract_weather_task',
        python_callable=extract_weather
    )
    logging.info("Tâche extract_weather_task créée")

    t2 = PythonOperator(
        task_id='transform_weather_task',
        python_callable=transform_weather
    )
    logging.info("Tâche transform_weather_task créée")

    t3 = PythonOperator(
        task_id='load_weather_task',
        python_callable=load_weather
    )
    logging.info("Tâche load_weather_task créée")

    # Définition de l’ordre d’exécution
    t1 >> t2 >> t3
    logging.info("Ordre d'exécution défini : extract >> transform >> load")
