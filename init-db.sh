#!/bin/bash
set -e

# Fonction pour créer une base de données si elle n'existe pas
create_database() {
    local database=$1
    echo "Création de la base de données : $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $database'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL
}

# Créer la base pour les données JO
create_database "jo_data"

echo "Base de données 'jo_data' créée avec succès !"