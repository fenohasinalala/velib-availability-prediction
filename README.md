# 🚲 Prédiction de Disponibilité Vélib' - Paris

Ce projet implémente une solution de **Machine Learning** pour prédire l'offre et la demande des stations Vélib' à Paris. Il utilise une approche de régression multi-cibles (`MultiOutputRegressor`) pour anticiper simultanément le nombre de vélos et de bornettes disponibles, en s'appuyant sur l'historique d'utilisation, la météo et les vacances scolaires.

## 🎯 Objectifs

L'objectif est d'optimiser la régulation des flottes de vélos en libre-service en fournissant des prédictions précises à court terme.
Le modèle prend en compte :

* **Contextuel** : Heure, jour, jour férié, vacances scolaires (API Éducation Nationale).
* **Météorologique** : Température, précipitations, humidité (API Open-Meteo).
* **Station** : Capacité, coordonnées, arrondissement.

## 💾 Données et Sources

Le dataset agrège trois sources principales pour la période **Octobre 2024 - Décembre 2025** :

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



## 📂 Structure du Projet

```bash
.
├── data/                  # Dossier de stockage (non versionné)
│   ├── parquet/           # Données brutes Vélib'
│   ├── json/              # Données brutes Vélib'
│   └── meteo/             # Données météo récupérées
├── src/                   
│   ├── get_meteo_data.py  # Script d'extraction météo (Open-Meteo)       
│   
├── notebooks/                   
│   ├── load_data.ipynb          # Pipeline ETL : Fusion Vélib' + Météo + Vacances
│   ├── data_exploration.ipynb   # EDA : Clustering stations & Analyse temporelle  
│   └── model_training.ipynb   # Entraînement, Validation Croisée & Optimisation
├── docs/  
└── requirements.txt       # Dépendances du projet

```

## 🛠️ Installation et Configuration

### Prérequis

* **Python 3.9** ou supérieur.
* `pip` pour la gestion des paquets.

### Installation

1. Clonez le dépôt :
```bash
git clone https://git.inge.re/hasina.vagno/challenge-apprentissage-automatique.git
cd challenge-apprentissage-automatique

```


2. Installez les dépendances :
```bash
pip install -r requirements.txt

```

## 🚀 Utilisation

Pour reproduire l'entraînement complet, suivez le pipeline :

1. **Récupération des données météo** :
Ce script récupère l'historique horaire correspondant à votre période d'analyse.
```bash
python src/get_meteo_data.py

```


2. **Préparation du Dataset (`load_data.ipynb`)** :
Exécutez ce notebook pour fusionner les JSON Vélib' avec les données météo et générer le fichier `data_with_meteo.parquet`.
* *Input* : Données brutes JSON/Parquet + CSV Météo.
* *Output* : Dataset nettoyé et enrichi.


3. **Entraînement des Modèles (`model_training.ipynb`)** :
Ce notebook lance la comparaison des modèles (CatBoost, XGBoost, LightGBM, Random Forest) et l'optimisation des hyperparamètres.
* *Optimisation* : **Optuna** est utilisé pour tuner les hyperparamètres.
* *Métrique* : Multi-RMSE (Root Mean Squared Error).



## 📊 Résultats et Performance

Le modèle **CatBoostRegressor** (en mode MultiOutput) a démontré les meilleures performances globales sur le jeu de test.

### Métriques Clés

* **RMSE (Erreur Moyenne)** : ~0.28 (sur données normalisées)
* **R² Score (Vélos)** : **0.87** (±0.02)
* **R² Score (Bornettes)** : **0.82** (±0.03)

### Benchmark Rapide

| Modèle | R² Global | Temps Entraînement | Usage |
| --- | --- | --- | --- |
| **CatBoost** | **0.87** | ~45 min | Production |
| XGBoost | 0.85 | ~38 min | Comparaison |
| LightGBM | 0.83 | ~20 min | Rapide |

## 👤 Auteurs

- Ny Hasina VAGNO
- Lova RAFANOMEZANTSOA