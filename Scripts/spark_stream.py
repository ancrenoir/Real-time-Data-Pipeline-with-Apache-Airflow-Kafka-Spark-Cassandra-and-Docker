import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from cassandra.auth import PlainTextAuthProvider

# Configurer la journalisation
logging.basicConfig(level=logging.INFO)

def clé_connection_spark(session):
    spark_stream = "spark_streams"
    session.execute(f"""
       CREATE KEYSPACE IF NOT EXISTS {spark_stream}
       WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}""")
    print(f"La clé {spark_stream} a été crée avec succès")

def creation_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users(
                id_user UUID PRIMARY KEY,
                Nom TEXT,
                Prenom TEXT,
                genre TEXT,
                Adresse TEXT,
                codePostale TEXT,
                email TEXT,
                username TEXT,
                id_login TEXT,
                password TEXT,
                numero TEXT,
                dob TEXT,
                registred_data TEXT,
                image_user TEXT
                )""")

def insertion_data(session, **kwargs):
    print("Insertion des données")
    id_user = kwargs.get('id_user')
    Nom = kwargs.get('Nom')
    Prenom = kwargs.get('Prenom')
    genre = kwargs.get('genre')
    Adresse = kwargs.get('Adresse')
    codePostale = kwargs.get('codePostale')
    email = kwargs.get('email')
    username = kwargs.get('username')
    id_login = kwargs.get('id_login')
    password = kwargs.get('password')
    numero = kwargs.get('numero')
    dob = kwargs.get('dob')
    registred_data = kwargs.get('registred_data')
    image_user = kwargs.get('image_user')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id_user, Nom, Prenom, genre, Adresse, codePostale,
                        email, username, id_login, password, numero, dob, registred_data, image_user) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (id_user, Nom, Prenom, genre, Adresse, codePostale,
                        email, username, id_login, password, numero, dob, registred_data, image_user))
        logging.info(f"Insertion de la donnée {Nom}, {Prenom}")
    except Exception as e:
        logging.error(f"L'insertion de donnée a échoué: {e}")

def creation_spark_connection():
    Spark_con = None
    try:
        Spark_con = SparkSession.builder\
            .appName('SparkDataStreaming')\
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.51",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        Spark_con.sparkContext.setLogLevel("ERROR")
        logging.info("Connection avec Spark crée avec succès!")
    except Exception as e:
        logging.error(f"La connection n'a pas pu etre établie: {e}")

    return Spark_con

def connection_kafka(Spark_con):
    spark_df = None
    try:
        spark_df = Spark_con.readStream \
           .format('kafka') \
           .option('kafka.bootstrap.servers', 'localhost:9092') \
           .option('subscribe', 'users_created') \
           .option('startingOffsets', 'earliest') \
           .load()
        logging.info("Initialisation de la DataFrame fait!")
    except Exception as e:
        logging.warning(f"La creation de la DataFrame kafka a échoué à cause: {e}")

    return spark_df

from cassandra.cluster import Cluster

def creation_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connect()
        return cassandra_session
    except Exception as e:
        logging.error(f"Impossible d'établir une connection avec Cassandra: {e}")
        return None

def creation_donnéeStructuré_from_kafka(spark_df):
    schema = StructType([
        StructField("id_user", StringType(), False),
        StructField("Nom", StringType(), False),
        StructField("Prenom", StringType(), False),
        StructField("genre", StringType(), False),
        StructField("Adresse", StringType(), False),
        StructField("codePostale", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("id_login", StringType(), False),
        StructField("password", StringType(), False),
        StructField("numero", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registred_data", StringType(), False),
        StructField("image_user", StringType(), False),
    ])

    selection = spark_df.selectExpr('CAST(value AS STRING)') \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(selection)
    return selection

if __name__ == "__main__":
    spark_connect = creation_spark_connection()
    if spark_connect is not None:
        df = connection_kafka(spark_connect)
        cass_session = creation_cassandra_connection()
        if cass_session is not None:
            clé_connection_spark(cass_session)
            creation_table(cass_session)
            selection_df = creation_donnéeStructuré_from_kafka(df)
            streaming_requette = (selection_df.writeStream.format('org.apache.spark.sql.cassandra') \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .option('keyspace', 'spark_streams') \
                .option('table', 'created_users') \
                .start())
            streaming_requette.awaitTermination()
