import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/home/nonie/exam_IA')

from weather_etl import extract, transform, load



# Configuration les logs si le DAG est exécuté indépendamment pour tester
logging.basicConfig(level=logging.INFO)
logging.info("Chargement du DAG weather_etl...")

# Définition d'une date de debut par défaut pour l'execution des tâches
default_args = {
    'start_date': datetime(2024, 1, 1),
}

# Création du DAG 
with DAG(
    dag_id='weather_etl',
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    logging.info("Initialisation des tâches du DAG weather_etl...")

    # Définition des tâches Python executant les fonctions dans le script weather_etl
    t1 = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )
    logging.info("Tâche extract_task créée")

    t2 = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )
    logging.info("Tâche transform_task créée")

    t3 = PythonOperator(
        task_id='load_task',
        python_callable=load
    )
    logging.info("Tâche load_task créée")

    # Ordre d’exécution des tâches
    t1 >> t2 >> t3
    logging.info("Ordre d'exécution défini : extract_task >> transform_task >> load_task")
