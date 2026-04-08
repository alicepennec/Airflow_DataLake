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
                
                # ICI : Le fichier est maintenant FERMÉ car on est sorti du "with"
                
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

# Simulation avec des données brutes
""" def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur d'envoi : {err}")
    else:
        print(f"Message envoyé à {msg.topic()}")

# Simulation d'un flux de résultats
data = {
    "id_resultat": 9999,
    "athlete_nom": "DUPONT",
    "sport": "Judo",
    "performance_finale": 10.0,
    "ts": time.time()
}

producer.produce('jo-stream-topic', json.dumps(data).encode('utf-8'), callback=delivery_report)
producer.flush() """