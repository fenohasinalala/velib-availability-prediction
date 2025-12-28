import requests
import json
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession

# ==========================================
# CONFIGURATION
# ==========================================
API_URL = (
    "https://opendata.paris.fr/api/records/1.0/search/"
    "?dataset=velib-disponibilite-en-temps-reel"
    "&timezone=UTC"
    "&rows=2000"
)

# Dossier racine demandé
HDFS_ROOT_DIR = "hdfs://localhost:9000/user/hadoop/data/json"


def fetch_and_save_json():
    # 1. Initialisation Spark
    spark = SparkSession.builder \
        .appName("Velib_ETL_JSON") \
        .getOrCreate()

    print(f"[{datetime.now()}] Démarrage de l'extraction Velib vers JSON...")

    try:
        # 2. Appel API
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        raw_json = response.json()

        records = raw_json.get("records", [])
        if not records:
            print("Aucune donnée reçue.")
            return

        # 3. Génération du nom de dossier unique (Basé sur ton format)
        # Format: velib_YYYY_MM_DDTHH_MM_SS
        now = datetime.now(timezone.utc)
        folder_name = now.strftime("velib_%Y_%m_%dT%H_%M_%S")

        # Chemin complet : /user/hadoop/data/json/velib_2025_12_28T14_00_00
        full_output_path = f"{HDFS_ROOT_DIR}/{folder_name}"

        # 4. Création DataFrame
        # On convertit la liste de dictionnaires en JSON string pour Spark
        rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])
        df = spark.read.json(rdd)

        # 5. Écriture en JSON sur HDFS
        print(f"Sauvegarde vers : {full_output_path}")

        # .coalesce(1) : Force Spark à écrire UN SEUL fichier JSON par dossier
        # au lieu de plusieurs petits fichiers.
        df.coalesce(1).write.mode("overwrite").json(full_output_path)

        print("✔ Succès.")

    except Exception as e:
        print(f"✖ Erreur : {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    fetch_and_save_json()
