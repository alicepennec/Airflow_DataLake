import os
import json
import time
import shutil
from confluent_kafka import Producer

# Configuration pour se connecter à Redpanda
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

INPUT_DIR = "streaming/inputs"
ARCHIVE_DIR = "streaming/archive"

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

print(f"🚀 Surveillant activé sur : {INPUT_DIR}")

# Simulation de l'arrivée de fichiers JSON dans le dossier d'entrée
try:
    while True:
        files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.json')]
        
        for filename in files:
            filepath = os.path.join(INPUT_DIR, filename)
            
            data = None
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                             
                if data:
                    # Envoi vers Redpanda
                    producer.produce('jo-stream-topic', json.dumps(data).encode('utf-8'))
                    producer.flush()
                    print(f"✅ Fichier {filename} envoyé vers Redpanda !")
                    
                    # Déplacement vers archive (maintenant possible)
                    shutil.move(filepath, os.path.join(ARCHIVE_DIR, filename))

            except json.JSONDecodeError:
                print(f"❌ Erreur de lecture sur {filename}")
            except Exception as e:
                print(f"❌ Erreur lors du traitement de {filename} : {e}")
        
        time.sleep(2) # Pause de 2 secondes entre chaque scan
except KeyboardInterrupt:
    print("Arrêt du producer.")