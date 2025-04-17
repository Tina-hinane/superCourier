# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        weekday TEXT,
        hour INTEGER,
        package_type TEXT,
        distance REAL,
        delivery_zone TEXT,
        weather_condition TEXT,
        actual_delivery_time INTEGER,
        status TEXT
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )

        # Calculate weekday and hour
        weekday = timestamp.strftime('%A')  # Day of the week
        hour = timestamp.hour  # Hour of the day

        # Random selection of package type and zone
        package_type = random.choices(
            package_types,
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]

        delivery_zone = random.choice(delivery_zones)

        # Simulate distance and delivery time
        distance = round(random.uniform(1, 50), 2)  # Distance in km
        actual_delivery_time = random.randint(10, 120)  # Delivery time in minutes

        # Determine status
        status = 'none'

        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            weekday,  # Weekday
            hour,  # Hour
            package_type,
            distance,
            delivery_zone,
            None,  # Placeholder for weather_condition
            actual_delivery_time,
            status
        ))

    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS (to be completed)
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS (to be completed by participants)

def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions
    """
    logger.info("Enriching with weather data...")
    
    # Convert date column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # Function to get weather for a given timestamp
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        
        try:
            return weather_data[date_str][hour_str]
        except KeyError:
            return None
    
    # Apply function to each row
    df['weather_condition'] = df['pickup_datetime'].apply(get_weather)
    
    return df

def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    To be completed by participants
    """
    logger.info("Transforming data...")
    
    # TODO: Add your transformation code here
    # 1. Enrich with weather data
    df_deliveries = enrich_with_weather(df_deliveries, weather_data)
    # 2. Calculate delivery times
    threshold = calculate_delay_threshold(df_deliveries)
    # 3. Determine status (on time/delayed)
    df_deliveries['status'] = np.where(
        df_deliveries['actual_delivery_time'] <= threshold,
        'On-time',
        'Delayed'
    )
    # 4. Handle missing values
    df_deliveries = handle_missing_values(df_deliveries)
    
    return df_deliveries  # Return transformed DataFrame

# 2. Calculate delivery times
def calculate_delay_threshold(df_deliveries):
    """
    Calcule le seuil de retard pour chaque livraison dans un DataFrame.
    """
    # Temps théorique de base
    base_time = 30 + df_deliveries['distance'] * 0.8

    # Facteurs d'ajustement
    package_factors = {
        'Small': 1, 'Medium': 1.2, 'Large': 1.5, 'X-Large': 2, 'Special': 2.5
    }
    zone_factors = {
        'Urban': 1.2, 'Suburban': 1, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4
    }
    weather_factors = {
        'Sunny': 1, 'Cloudy': 1.05, 'Rainy': 1.2, 'Snowy': 1.8, 'Windy': 1.1, 'Foggy': 1.15
    }
    peak_hour_factors = {
        'Morning': 1.3, 'Evening': 1.4, 'Other': 1
    }
    day_factors = {
        'Monday': 1.2, 'Friday': 1.2, 'Weekend': 0.9, 'Other': 1
    }

    # Calcul des facteurs
    package_factor = df_deliveries['package_type'].map(package_factors).fillna(1)
    zone_factor = df_deliveries['delivery_zone'].map(zone_factors).fillna(1)
    weather_factor = df_deliveries['weather_condition'].map(weather_factors).fillna(1)

    peak_hour_factor = np.where(
        (df_deliveries['hour'] >= 7) & (df_deliveries['hour'] <= 9),
        peak_hour_factors['Morning'],
        np.where(
            (df_deliveries['hour'] >= 17) & (df_deliveries['hour'] <= 19),
            peak_hour_factors['Evening'],
            peak_hour_factors['Other']
        )
    )

    day_factor = np.where(
        df_deliveries['weekday'].isin(['Monday', 'Friday']),
        day_factors['Monday'],
        np.where(
            df_deliveries['weekday'].isin(['Saturday', 'Sunday']),
            day_factors['Weekend'],
            day_factors['Other']
        )
    )

    # Temps théorique ajusté
    adjusted_time = base_time * package_factor * zone_factor * weather_factor * peak_hour_factor * day_factor

    # Seuil de retard
    delay_threshold = adjusted_time * 1.2

    return delay_threshold

# 4. Handle missing values
def handle_missing_values(df):
    """
    Gère les valeurs manquantes dans le DataFrame.
    """
    logger.info("Handling missing values...")

    # Remplir les valeurs manquantes pour les colonnes catégoriques
    categorical_columns = ['package_type', 'delivery_zone', 'weather_condition']
    for col in categorical_columns:
        df[col] = df[col].fillna('Unknown')

    # Remplir les valeurs manquantes pour les colonnes numériques
    numerical_columns = ['distance', 'actual_delivery_time']
    for col in numerical_columns:
        df[col] = df[col].fillna(df[col].mean())

    # Supprimer les lignes avec des valeurs manquantes dans des colonnes critiques
    critical_columns = ['pickup_datetime', 'weekday', 'hour']
    df = df.dropna(subset=critical_columns)

    logger.info("Missing values handled successfully")
    return df

# 5. LOADING FUNCTION (to be completed)
def save_results(df):
    """
    Saves the final DataFrame to CSV
    """
    logger.info("Saving results...")
    
    # TODO: Add your data validation code here
    
    # Save to CSV
    df.to_csv(OUTPUT_PATH, index=False)
    
    logger.info(f"Results saved to {OUTPUT_PATH}")
    return True

# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()

        weather_data = generate_weather_data()
        
        # Step 2: Extraction
        df_deliveries = extract_sqlite_data()
        
        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)
        
        # Step 4: Loading
        save_results(df_transformed)
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()
