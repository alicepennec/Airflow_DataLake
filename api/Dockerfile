# On part de l'image de base
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11

# On copie le fichier requirements.txt dans le conteneur
COPY requirements.txt /app/

# On lance l'installation des dépendances
RUN pip install --no-cache-dir -r /app/requirements.txt

# On copie le reste du code (votre main.py)
COPY . /app