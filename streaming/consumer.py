import os
import boto3
from confluent_kafka import Consumer, KafkaException
import time

# Récupération des variables d'environnement (ou valeurs par défaut pour test local)
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MINIO_URL = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')

# Configuration S3 (MinIO)
s3 = boto3.client('s3',
    endpoint_url=MINIO_URL,
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'admin'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'password123')
)

# Configuration Kafka (Redpanda)
conf = {
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': "jo-monitoring-group",
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
# Stratégie de reconnexion au démarrage
connected = False
while not connected:
    try:
        # On essaie de récupérer les métadonnées pour vérifier la connexion
        consumer.list_topics(timeout=5)
        print("✅ Connecté avec succès à Redpanda !")
        connected = True
    except KafkaException:
        print("⏳ Redpanda n'est pas encore prêt... nouvel essai dans 5s")
        time.sleep(5)
consumer.subscribe(['jo-stream-topic'])

print("En attente de messages...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        # Récupération de la donnée
        data = msg.value().decode('utf-8')
        filename = f"bronze/resultat_{msg.offset()}.json"
        
        # Écriture dans le Data Lake (MinIO)
        s3.put_object(Bucket='jo-data-lake', Key=filename, Body=data)
        print(f"Fichier {filename} sauvegardé dans le Data Lake !")
finally:
    consumer.close()