# 🚲 Prédiction de Disponibilité Vélib - Pipeline ETL & Machine Learning

Ce projet implémente un pipeline de données complet (End-to-End) pour analyser et prédire la disponibilité des vélos (mécaniques et électriques) et des places dans les stations Vélib' Métropole. Il intègre des données météorologiques et calendaires pour affiner les prédictions grâce à des modèles de Machine Learning distribués via Spark.


## 📖 Description du Projet

L'objectif est de fournir une brique technologique capable de :

1. **Collecter** les données temps réel de disponibilité des stations Vélib.
2. **Enrichir** ces données avec l'historique météo (Open-Meteo) et les vacances scolaires.
3. **Traiter** de gros volumes de données via **PySpark** sur un cluster Hadoop/HDFS.
4. **Entraîner** des modèles prédictifs performants (XGBoost, CatBoost, LightGBM) pour anticiper la demande.

## 🏗 Architecture & Données

Le projet s'appuie sur trois sources de données principales :

1. **Vélib' Métropole (Open Data Paris)** :
* *Source* : [API Vélib' Métropole](https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/information/)
* *Volume* : ~15 millions d'enregistrements (fichiers JSON/CSV).
* *Variables* : `numbikesavailable`, `numdocksavailable`, `mechanical`, `ebike`, `is_installed`, `is_renting`, `coordonnes_geo`, `capacity`, `duedate`, `stationcode`, `name`, `nom_arrondissement_communes`, `code_insee_commune`

2. **Historique Vélib' Métropole (Github)** :
* *Source* : [Historique API Vélib' Métropole](https://github.com/BWallyn/velib-prediction/tree/main/data/01_raw)
* *Volume* : ~1,000 enregistrements (fichiers Parquet).
* *Variables* : `numbikesavailable`, `numdocksavailable`, `mechanical`, `ebike`, `is_installed`, `is_renting`, `coordonnes_geo`, `capacity`, `duedate`, `stationcode`, `name`, `nom_arrondissement_communes`, `code_insee_commune`


3. **Données Météorologiques** :
* *Source* : [Open-Meteo Historical Weather API](https://open-meteo.com/)
* *Variables* : `temperature_2m`, `precipitation`, `rain`, `snowfall`, `relative_humidity_2m`


4. **Vacances Scolaires** :
* *Source* : [Data Éducation Gouv](https://data.education.gouv.fr/)
* *Usage* : Enrichissement des features temporelles (`is_holiday`).

**Stack Technique :**

* **Langage** : Python 3.x
* **Big Data** : Apache Spark (PySpark), HDFS (Hadoop Distributed File System)
* **Machine Learning** : Scikit-Learn, XGBoost, LightGBM, CatBoost, Optuna
* **Data Science** : Pandas, NumPy, Seaborn, Matplotlib

## 📂 Structure du Projet

```bash
.
├── docs/                       # Rapport technique
├── script_infra/               # Scripts pour les VMs, HDFS, déploiement du cluster
├── notebooks/                  # Jupyter Notebooks pour l'analyse et le ML
│   ├── data_exploration.ipynb  # Analyse exploratoire (EDA), corrélations
│   ├── model_comparison.ipynb  # Benchmark des modèles (XGBoost, LGBM, etc.)
│   ├── model_training.ipynb    # Entraînement final et tuning (Optuna)
│   └── pipeline_latest.pkl     # Pipeline de modèle sérialisé
├── src/                        # Scripts Python pour l'ETL et l'ingestion
│   ├── download.py             # Script d'ingestion API Vélib vers HDFS (JSON)
│   ├── get_meteo_data.py       # Script d'ingestion API Météo vers HDFS
│   └── load_data.py            # Pipeline ETL Spark (Nettoyage, Jointure, Feature Eng.)
└── requirements.txt            # Liste des dépendances Python

```

## ⚙️ Prérequis & Installation

### Prérequis Système

* Un environnement Python 3.8+
* Un cluster Hadoop/Spark fonctionnel (local ou distribué) avec HDFS configuré.
* Java (pour Spark).

### Installation des dépendances

Installez les bibliothèques nécessaires via `pip` :

```bash
pip install -r requirements.txt

```

*Note : Assurez-vous que les variables d'environnement `HADOOP_HOME` et `SPARK_HOME` sont correctement définies.*

## 🚀 Utilisation

Le pipeline s'exécute en trois étapes principales.

### 1. Ingestion des Données

Lancez les scripts pour récupérer les données brutes et les stocker sur HDFS.

* **Récupération des données Vélib (Temps Réel) :**
```bash
spark-submit --master local  --driver-memory 512m --executor-memory 512m --executor-cores 1 src/download.py >> src/download.log 2>&1

```


*Ce script interroge l'API OpenData Paris et sauvegarde les réponses en JSON dans `hdfs:///user/hadoop/data/json`.*
* **Récupération des données Météo :**
```bash
spark-submit --master local  --driver-memory 512m --executor-memory 512m --executor-cores 1 src/get_meteo_data.py --start-date 2024-01-01 --end-date 2024-12-31 >> src/get_meteo_data.log 2>&1

```


*Ce script télécharge l'historique météo pour Paris et sauvegarde en CSV dans `hdfs:///user/hadoop/data/meteo`.*

### 2. Traitement ETL

Exécutez le script de chargement pour fusionner les sources, nettoyer les données et créer les features.

```bash
spark-submit --master local  --driver-memory 512m --executor-memory 512m --executor-cores 1 src/load_data.py >> src/load_data.log 2>&1


```

* Lit les fichiers JSON (Vélib) et CSV (Météo) depuis HDFS.
* Ajoute les données de vacances scolaires.
* Effectue le Feature Engineering (ratio vélos/bornes, sin/cos heure, etc.).
* Sauvegarde le dataset final nettoyé au format **Parquet** dans `hdfs:///user/hadoop/data/data_with_meteo_final`.

### 3. Analyse et Modélisation

Ouvrez les notebooks Jupyter pour explorer les données et entraîner les modèles.

1. **Exploration (`data_exploration.ipynb`)** : Visualisation de la distribution des vélos, matrices de corrélation, impact de la météo.
2. **Comparaison (`model_comparison.ipynb`)** : Testez plusieurs algorithmes (RandomForest, XGBoost, CatBoost) pour identifier le plus performant.
3. **Entraînement (`model_training.ipynb`)** : Entraînement final, optimisation des hyperparamètres avec **Optuna** et export du modèle (`pipeline_XX_XX_XX.pkl`).

## 📊 Résultats & Modèles

Le projet compare plusieurs approches de régression multi-sorties (MultiOutputRegressor) pour prédire simultanément :

* Le nombre de vélos mécaniques disponibles.
* Le nombre de vélos électriques disponibles.

Les variables les plus importantes identifiées par le modèle incluent la capacité de la station, le statut "mécanique" et les ratios d'occupation historiques.
