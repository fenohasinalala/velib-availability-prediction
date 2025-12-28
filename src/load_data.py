import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Velib_ETL") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.sql.files.ignoreMissingFiles", "true") \
    .getOrCreate()

HDFS_ROOT = "hdfs://localhost:9000/user/hadoop/data"
PARQUET_PATH = f"{HDFS_ROOT}/parquet/"
JSON_PATH = f"{HDFS_ROOT}/json/"
METEO_PATH = f"{HDFS_ROOT}/meteo/"
OUTPUT_PATH = f"{HDFS_ROOT}/data_with_meteo_final"
URL_HOLIDAYS = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/exports/csv?use_labels=true"


def load_data_from_hdfs():
    print(f">>> Reading data from: {HDFS_ROOT}")

    df_parquet = None
    df_json = None

    try:
        df_parquet = spark.read.parquet(PARQUET_PATH)
        count = df_parquet.count()
        print(f"Loaded parquet: {count} records")
    except Exception as e:
        print(f"Parquet files not found or corrupted: {e}")

    try:
        df_json_raw = spark.read.option("multiline", "true").json(JSON_PATH)
        df_json = df_json_raw.select(F.explode("records").alias("rec")).select("rec.fields.*")

        if "coordonnees_geo" in df_json.columns:
            df_json = df_json.withColumn(
                "coordonnees_geo",
                F.when(
                    F.col("coordonnees_geo").isNotNull(),
                    F.struct(
                        F.col("coordonnees_geo")[0].alias("lat"),
                        F.col("coordonnees_geo")[1].alias("lon")
                    )
                )
            )
        count = df_json.count()
        print(f"Loaded JSON: {count} records")
    except Exception as e:
        print(f"JSON files not found: {e}")

    if df_parquet is not None and df_json is not None:
        df_stations = df_parquet.unionByName(df_json, allowMissingColumns=True)
    elif df_parquet is not None:
        df_stations = df_parquet
    elif df_json is not None:
        df_stations = df_json
    else:
        raise ValueError("No data sources available")

    return df_stations.withColumn("duedate", F.to_timestamp("duedate"))


def load_meteo_hdfs():
    print(">>> Loading weather data from HDFS...")

    df_meteo = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(METEO_PATH)

    return df_meteo \
        .withColumnRenamed("date", "duedate_meteo") \
        .withColumn("duedate_meteo", F.to_timestamp("duedate_meteo"))


def get_holidays_driver():
    print(">>> Fetching holiday data from API...")

    try:
        pdf = pd.read_csv(URL_HOLIDAYS, sep=";")
        pdf = pdf[pdf['Académies'] == 'Paris']
        pdf = pdf.dropna(subset=['Date de début', 'Date de fin'])

        holiday_dates = set()
        for _, row in pdf.iterrows():
            dates = pd.date_range(start=row['Date de début'], end=row['Date de fin'])
            holiday_dates.update(dates.date)

        dates_list = [(d,) for d in holiday_dates]
        df_holidays = spark.createDataFrame(dates_list, schema=["holiday_date"])
        return df_holidays.withColumn("is_holiday", F.lit(1))

    except Exception as e:
        print(f"Failed to fetch holidays: {e}")
        return spark.createDataFrame([], schema="holiday_date date, is_holiday int")


def feature_engineering(df):
    print(">>> Applying feature engineering...")

    df_fe = df

    df_fe = df_fe.withColumn("hour", F.hour("duedate"))
    df_fe = df_fe.withColumn("month", F.month("duedate"))
    df_fe = df_fe.withColumn("dayofweek", F.dayofweek("duedate") - 1)
    df_fe = df_fe.withColumn("is_weekend", F.when(F.col("dayofweek") >= 5, 1).otherwise(0))

    df_fe = df_fe.withColumn("hour_sin", F.sin(2 * np.pi * F.col("hour") / 24))
    df_fe = df_fe.withColumn("hour_cos", F.cos(2 * np.pi * F.col("hour") / 24))
    df_fe = df_fe.withColumn("dow_sin", F.sin(2 * np.pi * F.col("dayofweek") / 7))
    df_fe = df_fe.withColumn("dow_cos", F.cos(2 * np.pi * F.col("dayofweek") / 7))

    df_fe = df_fe.withColumn("capacity",
                             F.when(F.col("capacity") == 0, 1).otherwise(F.col("capacity"))
                             )
    df_fe = df_fe.withColumn("bikes_ratio", F.col("numbikesavailable") / F.col("capacity"))
    df_fe = df_fe.withColumn("docks_ratio", F.col("numdocksavailable") / F.col("capacity"))

    if "rain" in df.columns:
        df_fe = df_fe.withColumn("is_raining",
                                 F.when(F.col("rain") > 0, 1).otherwise(0)
                                 )

    return df_fe


def main():
    print("=" * 60)
    print("Starting Velib ETL Pipeline")
    print("=" * 60)

    df_stations = load_data_from_hdfs()
    print(f"Total stations records: {df_stations.count()}")

    df_meteo = load_meteo_hdfs()
    print(f"Total weather records: {df_meteo.count()}")

    df_stations = df_stations.withColumn("join_hour", F.date_trunc("hour", "duedate"))
    df_meteo = df_meteo.withColumn("join_hour_meteo", F.date_trunc("hour", "duedate_meteo"))

    print(">>> Joining stations with weather data...")
    df_merged = df_stations.join(
        df_meteo,
        df_stations.join_hour == df_meteo.join_hour_meteo,
        how="left"
    ).drop("join_hour", "join_hour_meteo", "duedate_meteo")

    df_holidays = get_holidays_driver()
    print(f"Total holiday dates: {df_holidays.count()}")

    print(">>> Adding holiday information...")
    df_with_holidays = df_merged \
        .withColumn("date_only", F.to_date("duedate")) \
        .join(df_holidays, F.col("date_only") == df_holidays.holiday_date, how="left") \
        .fillna({"is_holiday": 0}) \
        .drop("holiday_date", "date_only")

    df_final = feature_engineering(df_with_holidays)

    print(f">>> Writing final dataset to: {OUTPUT_PATH}")
    df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

    final_count = df_final.count()
    print("=" * 60)
    print(f"Pipeline completed successfully!")
    print(f"Final dataset contains {final_count} records")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
