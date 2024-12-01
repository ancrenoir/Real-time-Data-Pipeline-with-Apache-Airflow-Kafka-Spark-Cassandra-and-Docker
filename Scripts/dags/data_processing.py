import requests
import json
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)

## Collection de données
def get_data():
    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    return response

## Standardisation du format de données qui retourne un dictionnaire dict_data contenant les données standardisées
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

    from kafka import KafkaProducer
    import time

    ## Création d'un producteur qui va se connecter à l'host 9092 de Kafka
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],  # Utilisez 'broker:29092' pour se connecter au broker Kafka dans le réseau Docker
        max_block_ms=30000,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    ## Post de la donnée via ce producteur

    temps_actuel = time.time()

        
    while True:
        
        if time.time() > temps_actuel + 60 : # dépasse une minute 
            break
            
        try:
             
             response = get_data()
             formatted_data = formatage_data(response)

             future = producer.send('topic_54', formatted_data)
             record_metadata = future.get(timeout=10)
             logging.info(f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
        except Exception as e:
            logging.error(f"Erreur d'envoie de message: {e}")
    
    time.sleep(1)

    producer.flush()
    producer.close()
    
