from datetime import datetime
import requests
import json
from kafka import KafkaProducer

## Paramètres à appliquer à l'ensemble des DAG

default_arguments = {
    'owner': 'Georges',
    'start_date': datetime(2024, 9, 30, 23, 12)  # datetime(année, mois, jours, heures, minutes)
}

## Collection de données

def get_data():
    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    return response

## Standardisation du format de données

def formatage_data(response):
    location = response['location']
    login = response['login']
    dict_data = {}
    dict_data['Nom'] = response['name']['first']
    dict_data['Prenom'] = response['name']['last']
    dict_data['genre'] = response['gender']
    dict_data['Adresse'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    dict_data['codePostale'] = location['postcode']
    dict_data['email'] = response['email']
    dict_data['username'] = login['username']
    dict_data['id_login'] = login['uuid']
    dict_data['password'] = login['password']
    dict_data['numero'] = response['phone']
    dict_data['dob'] = response['dob']['date']
    dict_data['registred_data'] = response['registered']['date']
    dict_data['image_user'] = response['picture']['large']
    return dict_data

def stream_data():
    response = get_data()
    formatted_data = formatage_data(response)
    return print(formatted_data)

stream_data()