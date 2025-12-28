import argparse
from datetime import datetime
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from pyspark.sql import SparkSession
import os


def parse_arguments():
    parser = argparse.ArgumentParser(description='Téléchargement des données météo Open-Meteo')

    today = datetime.now().date()

    parser.add_argument(
        '--start-date',
        type=str,
        default=today.strftime('%Y-%m-%d'),
        help='Date de début (format: YYYY-MM-DD). Par défaut: aujourd\'hui'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        default=today.strftime('%Y-%m-%d'),
        help='Date de fin (format: YYYY-MM-DD). Par défaut: aujourd\'hui'
    )

    parser.add_argument(
        '--latitude',
        type=float,
        default=48.8534,
        help='Latitude (par défaut: Paris 48.8534)'
    )

    parser.add_argument(
        '--longitude',
        type=float,
        default=2.3488,
        help='Longitude (par défaut: Paris 2.3488)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Nom du fichier de sortie CSV (par défaut: auto-généré)'
    )

    parser.add_argument(
        '--hdfs-path',
        type=str,
        default='hdfs://localhost:9000/user/hadoop/data/meteo',
        help='Chemin de base HDFS (par défaut: hdfs://localhost:9000/user/hadoop/data/meteo)'
    )

    args = parser.parse_args()

    # Validation des dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Format de date invalide. Utilisez YYYY-MM-DD. Erreur: {e}")

    if start_date > end_date:
        raise ValueError("La date de début doit être antérieure ou égale à la date de fin")

    return args, start_date, end_date


def fetch_weather_data(latitude, longitude, start_date, end_date):
    """
    Récupère les données météo depuis Open-Meteo API
    """
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Configuration de l'API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "daily": ["precipitation_sum", "rain_sum", "snowfall_sum", "sunrise", "sunset"],
        "hourly": ["temperature_2m", "precipitation", "rain", "snowfall", "relative_humidity_2m"],
        "timezone": "Europe/London",
    }

    print(f"Fetching weather data from {start_date} to {end_date}")
    print(f"Location: {latitude}°N {longitude}°E")

    responses = openmeteo.weather_api(url, params=params)

    # Process first location
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(4).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "temperature_2m": hourly_temperature_2m,
        "precipitation": hourly_precipitation,
        "rain": hourly_rain,
        "snowfall": hourly_snowfall,
        "relative_humidity_2m": hourly_relative_humidity_2m
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return hourly_dataframe


def save_data(df, output_filename, start_date, end_date, hdfs_base_path):
    """
    Sauvegarde le DataFrame dans HDFS
    """
    if output_filename is None:
        # Génération automatique du nom de fichier
        start_str = start_date.strftime('%Y_%m_%d')
        end_str = end_date.strftime('%Y_%m_%d')
        output_filename = f"daily_weather_{start_str}_to_{end_str}.csv"

    # Construire le chemin complet HDFS
    hdfs_full_path = os.path.join(hdfs_base_path, output_filename)

    save_to_hdfs(df, hdfs_full_path)
    print(f"Total records: {len(df)}")


def save_to_hdfs(df, hdfs_path):
    """
    Sauvegarde le DataFrame dans HDFS via PySpark
    """
    print(f"Saving to HDFS: {hdfs_path}")

    # Initialiser SparkSession
    spark = SparkSession.builder \
        .appName("Meteo_to_HDFS") \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .getOrCreate()

    try:
        # Vérifier si le chemin existe déjà
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark.sparkContext._jvm.java.net.URI(hdfs_path.split('/')[0] + '//' + hdfs_path.split('/')[2]),
            hadoop_conf
        )
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)

        if fs.exists(path):
            # Générer un nouveau nom avec timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_path = hdfs_path.rstrip('/')
            hdfs_path = f"{base_path}_{timestamp}"
            print(f"Path already exists. Using new path: {hdfs_path}")

        # Convertir le pandas DataFrame en Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Sauvegarder en CSV dans HDFS
        spark_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(hdfs_path)

        print(f"✓ Data successfully saved to HDFS: {hdfs_path}")

    except Exception as e:
        print(f"✗ Error saving to HDFS: {e}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    try:
        # Parse arguments
        args, start_date, end_date = parse_arguments()

        # Fetch weather data
        df = fetch_weather_data(args.latitude, args.longitude, start_date, end_date)

        # Save data to HDFS
        save_data(df, args.output, start_date, end_date, args.hdfs_path)

        print("\n✓ Weather data fetched and saved to HDFS successfully!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise