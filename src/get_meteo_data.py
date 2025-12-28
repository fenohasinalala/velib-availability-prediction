import argparse
import sys
from datetime import datetime
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from pyspark.sql import SparkSession

# ==========================================
# CONFIGURATION PAR DÉFAUT
# ==========================================
DEFAULT_HDFS_BASE = 'hdfs://localhost:9000/user/hadoop/data/meteo'
DEFAULT_LAT = 48.8534  # Paris
DEFAULT_LON = 2.3488  # Paris


def parse_arguments():
    parser = argparse.ArgumentParser(description='Extraction Météo vers HDFS')

    today_str = datetime.now().strftime('%Y-%m-%d')

    parser.add_argument('--start-date', type=str, default=today_str, help='YYYY-MM-DD')
    parser.add_argument('--end-date', type=str, default=today_str, help='YYYY-MM-DD')
    parser.add_argument('--latitude', type=float, default=DEFAULT_LAT)
    parser.add_argument('--longitude', type=float, default=DEFAULT_LON)
    parser.add_argument('--hdfs-path', type=str, default=DEFAULT_HDFS_BASE)

    return parser.parse_args()


def fetch_weather_data(lat, lon, start_date, end_date):
    """Récupère les données via l'API Open-Meteo et retourne un DataFrame Pandas"""

    print(f"--- Appel API Open-Meteo ({start_date} à {end_date}) ---")

    # Setup API avec cache et retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "relative_humidity_2m"],
        "timezone": "Europe/London"  # Ou auto
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Traitement des données horaires
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "precipitation": hourly.Variables(1).ValuesAsNumpy(),
            "rain": hourly.Variables(2).ValuesAsNumpy(),
            "snowfall": hourly.Variables(3).ValuesAsNumpy(),
            "humidity": hourly.Variables(4).ValuesAsNumpy()
        }

        # Conversion en Pandas DataFrame
        pdf = pd.DataFrame(data=hourly_data)
        # Conversion des types pour éviter les soucis avec Spark (float32 -> float64 souvent mieux géré)
        pdf['temperature_2m'] = pdf['temperature_2m'].astype(float)
        pdf['precipitation'] = pdf['precipitation'].astype(float)
        pdf['rain'] = pdf['rain'].astype(float)
        pdf['snowfall'] = pdf['snowfall'].astype(float)
        pdf['humidity'] = pdf['humidity'].astype(float)

        # Convertir la date en string pour l'écriture CSV propre
        pdf['date'] = pdf['date'].astype(str)

        return pdf

    except Exception as e:
        print(f"Erreur API : {e}")
        raise


def save_to_hdfs(pandas_df, start_date, end_date, base_path):
    """Convertit en Spark DataFrame et sauvegarde sur HDFS"""

    # Initialisation Spark Session
    spark = SparkSession.builder \
        .appName("Meteo_Ingestion") \
        .getOrCreate()

    try:
        # Création du Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)

        # Construction du nom de dossier de sortie
        # Ex: /user/hadoop/data/meteo/meteo_2024-01-01_to_2024-01-31
        folder_name = f"meteo_{start_date}_to_{end_date}"
        full_output_path = f"{base_path}/{folder_name}"

        print(f"--- Sauvegarde HDFS : {full_output_path} ---")

        # Écriture
        # coalesce(1) pour avoir un seul fichier CSV dans le dossier
        spark_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(full_output_path)

        print("✔ Succès : Données sauvegardées.")

    except Exception as e:
        print(f"✖ Erreur Spark/HDFS : {e}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    args = parse_arguments()

    try:
        # 1. Récupération (Pandas)
        df = fetch_weather_data(args.latitude, args.longitude, args.start_date, args.end_date)

        if df.empty:
            print("Aucune donnée récupérée.")
            sys.exit(0)

        # 2. Sauvegarde (Spark)
        save_to_hdfs(df, args.start_date, args.end_date, args.hdfs_path)

    except Exception as e:
        print(f"Arrêt du script : {e}")
        sys.exit(1)