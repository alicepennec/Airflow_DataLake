from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os
import pandas as pd
from pydantic import BaseModel

app = FastAPI(title="JO Data API")

# Configuration de la connexion
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Modèle pour recevoir la requête
class SQLQuery(BaseModel):
    query: str

@app.post("/query")
def execute_custom_query(sql: SQLQuery):
    try:
        with engine.connect() as conn:
            # On exécute la requête envoyée par Streamlit
            result = conn.execute(text(sql.query))
            
            # Récupération des données et des colonnes
            data = [dict(row._mapping) for row in result]
            return {"status": "success", "data": data}
    except Exception as e:
        # On renvoie l'erreur SQL pour aider l'utilisateur
        raise HTTPException(status_code=400, detail=str(e))
    
# Modèle pour l'ajout/modification
class Resultat(BaseModel):
    id_resultat: int
    athlete_nom: str
    athlete_prenom: str
    sport: str
    classement_epreuve: float

@app.get("/")
def read_root():
    return {"message": "Bienvenue sur l'API des données JO"}

@app.get("/resultats")
def get_resultats(limit: int = 10):
    """Récupère les résultats sportifs avec une limite"""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT * FROM resultats LIMIT :limit")
            result = conn.execute(query, {"limit": limit})
            # Conversion du résultat en liste de dictionnaires
            data = [dict(row._mapping) for row in result]
            return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/resultats/athlete/{nom}")
def get_athlete(nom: str):
    """Recherche les résultats pour un athlète spécifique"""
    try:
        with engine.connect() as conn:
            query = text("SELECT * FROM resultats WHERE athlete_nom ILIKE :nom")
            result = conn.execute(query, {"nom": f"%{nom}%"})
            data = [dict(row._mapping) for row in result]
            return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/resultats")
def add_resultat(res: Resultat):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO resultats (id_resultat, athlete_nom, athlete_prenom, sport, classement_epreuve)
                VALUES (:id, :nom, :prenom, :sport, :rank)
            """), {"id": res.id_resultat, "nom": res.athlete_nom, "prenom": res.athlete_prenom, "sport": res.sport, "rank": res.classement_epreuve})
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/resultats/{id_resultat}")
def delete_resultat(id_resultat: int):
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM resultats WHERE id_resultat = :id"), {"id": id_resultat})
        return {"status": "success", "message": f"Ligne {id_resultat} supprimée"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.put("/resultats/{id_resultat}")
def update_resultat(id_resultat: int, res: Resultat):
    try:
        with engine.begin() as conn:
            # On vérifie d'abord si la ligne existe
            check_query = text("SELECT id_resultat FROM resultats WHERE id_resultat = :id")
            existing = conn.execute(check_query, {"id": id_resultat}).fetchone()
            
            if not existing:
                raise HTTPException(status_code=404, detail="Résultat non trouvé")

            # Mise à jour des champs
            update_query = text("""
                UPDATE resultats 
                SET athlete_nom = :nom, 
                    athlete_prenom = :prenom, 
                    sport = :sport, 
                    classement_epreuve = :rank
                WHERE id_resultat = :id
            """)
            
            conn.execute(update_query, {
                "id": id_resultat,
                "nom": res.athlete_nom,
                "prenom": res.athlete_prenom,
                "sport": res.sport,
                "rank": res.classement_epreuve
            })
            
        return {"status": "success", "message": f"Ligne {id_resultat} mise à jour"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))