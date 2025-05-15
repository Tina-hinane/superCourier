# SuperCourier - Mini ETL Pipeline

## Description
Ce projet implémente un pipeline ETL (Extract, Transform, Load) pour simuler et traiter des données de livraison. Le pipeline génère des données fictives, 
les enrichit avec des informations météorologiques, calcule des temps de livraison exprimés en minutes et secondes, et sauvegarde les résultats dans un fichier CSV.

## Fonctionnement du pipeline
1. **Extraction** :
    - Les données de livraison sont générées et stockées dans une base de données SQLite.
    - Les données météorologiques sont générées et sauvegardées dans un fichier JSON.

2. **Transformation** :
    - Les données de livraison sont enrichies avec les conditions météorologiques basées sur la date et l'heure de collecte.
    - Les distances, temps de base, et facteurs d'ajustement (type de colis, zone de livraison, météo, heures de pointe, etc.) sont calculés.
    - Le temps de livraison est exprimé en minutes et secondes pour plus de précision.
    - Les livraisons sont marquées comme "On Time" ou "Delayed" selon un seuil de retard.

3. **Chargement** :
    - Les données transformées sont sauvegardées dans un fichier CSV (`deliveries.csv`).

## Structure du code
- **`create_sqlite_database`** : Génère une base de données SQLite avec des données fictives de livraison.
- **`generate_weather_data`** : Crée des données météorologiques fictives pour les 3 derniers mois.
- **`extract_sqlite_data`** : Extrait les données de livraison depuis la base SQLite.
- **`load_weather_data`** : Charge les données météorologiques depuis un fichier JSON.
- **`enrich_with_weather`** : Ajoute les conditions météorologiques aux données de livraison.
- **`transform_data`** : Transforme les données en calculant les temps de livraison et en ajoutant des colonnes enrichies.
- **`save_results`** : Sauvegarde les résultats transformés dans un fichier CSV.
- **`run_pipeline`** : Orchestration du pipeline ETL.

## Fichiers générés
- **`supercourier_mini.db`** : Base de données SQLite contenant les données de livraison.
- **`weather_data.json`** : Fichier JSON contenant les données météorologiques.
- **`deliveries.csv`** : Fichier CSV contenant les données finales transformées.

## Exécution
Lancer le pipeline avec la commande suivante :
```bash
python de-code-snippet.py
```
