# Utilisation d'une image Python légère
FROM python:3.11-slim

# Répertoire de travail dans le conteneur
WORKDIR /app

# Installation des dépendances système nécessaires pour confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copie des fichiers de dépendances et installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source du dossier streaming vers le conteneur
COPY . .

# Commande pour lancer le consumer (il restera en écoute constante)
CMD ["python", "consumer.py"]