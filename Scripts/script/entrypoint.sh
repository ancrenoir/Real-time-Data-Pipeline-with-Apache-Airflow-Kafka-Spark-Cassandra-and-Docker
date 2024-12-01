#!/bin/bash

set -e

# Vérification si le fichier requirements.txt existe
if [ -f "/opt/airflow/requirements.txt" ]; then
   # Installation des dépendances listées dans requirements.txt
   $(command -v pip) install --user -r /opt/airflow/requirements.txt
fi

# Initialisation de la base de données Airflow et création d'un utilisateur admin
airflow db init && \
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin

# Mise à niveau de la base de données Airflow
$(command -v airflow) db upgrade

# Lancement du serveur web Airflow
exec airflow webserver
