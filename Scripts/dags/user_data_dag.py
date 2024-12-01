from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_processing import stream_data
from kafka import KafkaProducer


# Configuration du logging
import logging
logging.basicConfig(level=logging.INFO)

## Paramètres à appliquer à l'ensemble des DAG

default_arguments = {
    'owner': 'Georges',
    'start_date': datetime(2024, 7, 23, 23, 12)  # datetime(année, mois, jours, heures, minutes)
}

# Définition du DAG
with DAG(
    'user_data_extrat',
    default_args=default_arguments,
    schedule='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='Données_utilisateurs',
        python_callable=stream_data
    )
