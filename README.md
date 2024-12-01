# Real-time Data Pipeline with Apache Airflow, Kafka, Spark, Cassandra, and Docker

## 📖 Introduction

Ce projet met en œuvre un pipeline de données en temps réel, intégrant des technologies modernes comme **Apache Kafka**, **Apache Spark**, **Cassandra**, et **Apache Airflow**. Il utilise **Docker compose** pour l'orchestration des services. L'objectif est de démontrer comment collecter, traiter et stocker des données en temps réel pour une architecture ETL robuste et scalable, pour des fin de monter en competence avec ces outils.

---

## 🏗️ Architecture

Le pipeline suit ces étapes :
1. **Ingestion des données** via des APIs (Cas d'utilisation : Random API).
2. **Streaming des données** avec Kafka et son écosystème (Zookeeper, Schema Registry, Control center etc.).
3. **Traitement des données** en temps réel avec Apache Spark.
4. **Stockage final** dans Cassandra pour une consultation rapide.
5. **Orchestration** et planification avec Apache Airflow.

### 📊 Schéma d'architecture
![Pipeline Architecture](https://github.com/ancrenoir/Real-time-Data-Pipeline-with-Apache-Airflow-Kafka-Spark-Cassandra-and-Docker/blob/main/diagram.png) ## L'image de l'architecture sera uploadé bientot 

---

## 🛠️ Technologies utilisées

- **API Random** : Pour simuler des données.
- **Apache Kafka** : Streaming des données.
- **Apache Spark** : Traitement des données en temps réel.
- **Apache Airflow** : Orchestration du pipeline.
- **Cassandra** : Stockage des données structurées.
- **Docker** : Conteneurisation des services.
- **Zookeeper** : Coordination des clusters Kafka.
--**Control Center** : Monitoring du culster Kafka.
--**Schema registry pour la scerialisation de données**

---

## 🔧 Prérequis

Avant de commencer, assurez-vous d’avoir :
1. **Docker et Docker Compose** installés de préférence sur la distrubution Ubuntu du noyau Linux
3. **Python 3.8 ou plus** installé pour certains scripts.

---

## 🚀 Installation et exécution

### 1. Clonez le projet
```bash
git clone https://github.com/ancrenoir/Real-time-Data-Pipeline-with-Apache-Airflow-Kafka-Spark-Cassandra-and-Docker.git
cd Real-time-Data-Pipeline-with-Apache-Airflow-Kafka-Spark-Cassandra-and-Docker
