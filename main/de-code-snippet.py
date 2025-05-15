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
    Creates a simple SQLite database with a deliveries table.

    Steps:
        1. Check if the database file already exists and remove it if necessary.
        2. Connect to the SQLite database and create a cursor object.
        3. Create a 'deliveries' table with predefined columns.
        4. Generate 1000 random delivery records with fictional data.
        5. Insert the generated records into the 'deliveries' table.
        6. Commit the changes and close the database connection.

    Returns:
        bool: True if the database is created successfully, False otherwise.

    Exceptions:
        - Handles SQLite-specific errors and logs them.
        - Handles unexpected errors and logs them.
    """
    try:
        logger.info("Creating SQLite database...")

        # Remove database if it already exists
        logger.debug("Checking if database already exists...")
        if os.path.exists(DB_PATH):
            logger.debug("Database exists, removing...")
            os.remove(DB_PATH)

        # Connect to database
        logger.debug("Connecting to SQLite database...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create deliveries table
        logger.debug("Creating deliveries table...")
        cursor.execute('''
                       CREATE TABLE deliveries (
                                                   delivery_id INTEGER PRIMARY KEY,
                                                   pickup_datetime TEXT,
                                                   package_type TEXT,
                                                   delivery_zone TEXT,
                                                   recipient_id INTEGER
                       )
                       ''')

        # Available package types and delivery zones
        package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
        delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']

        # Generate 1000 random deliveries
        logger.debug("Start generating random deliveries...")
        deliveries = []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)  # 3 months

        for i in range(1, 1001):
            # Random date within last 3 months
            timestamp = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )

            # Random selection of package type and zone
            package_type = random.choices(
                package_types,
                weights=[25, 30, 20, 15, 10]  # Relative probabilities
            )[0]

            delivery_zone = random.choice(delivery_zones)

            # Add to list
            deliveries.append((
                i,  # delivery_id
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
                package_type,
                delivery_zone,
                random.randint(1, 100)  # fictional recipient_id
            ))

        logger.debug("Random deliveries generated.")

        # Insert data
        logger.debug("Inserting data into deliveries table...")
        cursor.executemany(
            'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
            deliveries
        )

        # Commit and close
        logger.debug("Committing changes and closing database connection...")
        conn.commit()
        logger.info(f"Database created with {len(deliveries)} deliveries")

    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months.

    Returns:
        dict: A dictionary containing weather data organized by date and hour,
              or None if an error occurs during the process.

    Steps:
        1. Define weather conditions and their probabilities.
        2. Generate weather data for each day and hour within the last 3 months.
        3. Save the generated weather data to a JSON file.
        4. Handle potential errors, such as file I/O issues or unexpected exceptions.
    """
    try:
        logger.info("Generating weather data...")

        # Define weather conditions and their probabilities
        conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
        weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities

        # Define the date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        weather_data = {}

        # Generate weather data for each day and hour
        logger.debug("Generating weather data for each day and hour...")
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            weather_data[date_str] = {}

            for hour in range(24):
                if hour > 0 and random.random() < 0.7:
                    # Use the previous hour's condition with 70% probability
                    condition = weather_data[date_str].get(str(hour-1),
                                                           random.choices(conditions, weights=weights)[0])
                else:
                    # Randomly select a condition
                    condition = random.choices(conditions, weights=weights)[0]

                weather_data[date_str][str(hour)] = condition

            current_date += timedelta(days=1)

        # Save the weather data to a JSON file
        logger.debug("Saving weather data to JSON file...")
        with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
            json.dump(weather_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Weather data generated for the period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        return weather_data

    except IOError as e:
        # Handle file I/O errors
        logger.error(f"File write error: {e}")
        return None
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error: {e}")
        return None

# 3. EXTRACTION FUNCTIONS (to be completed)
def extract_sqlite_data():
    """
    Extracts delivery data from the SQLite database.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted delivery data, or None if an error occurs.

    Steps:
        1. Connect to the SQLite database using the specified database path.
        2. Execute an SQL query to fetch all records from the 'deliveries' table.
        3. Load the query results into a pandas DataFrame.
        4. Handle potential errors, such as database connection issues or SQL execution errors.
        5. Close the database connection after the operation is complete.
    """
    conn = None
    try:
        logger.info("Extracting data from SQLite database...")

        # Connect to the SQLite database
        logger.debug("Connecting to SQLite database...")
        conn = sqlite3.connect(DB_PATH)

        # Execute the query to fetch all deliveries
        logger.debug("Executing SQL query to fetch deliveries...")
        query = "SELECT * FROM deliveries"
        df = pd.read_sql(query, conn)

        logger.info(f"Extraction complete: {len(df)} records")
        return df

    except sqlite3.Error as e:
        # Handle SQLite-specific errors
        logger.error(f"SQLite error during extraction: {e}")
        return None
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error during extraction: {e}")
        return None
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

def load_weather_data():
    """
    Loads weather data from a JSON file.

    Returns:
        dict: A dictionary containing the weather data if loading is successful.
        None: If an error occurs during the loading process.

    Steps:
        1. Open and read the JSON file containing the weather data.
        2. Decode the JSON content into a Python dictionary.
        3. Handle potential errors, such as file not found or JSON decoding issues.
        4. Return the weather data or None in case of an error.
    """
    try:
        logger.info("Loading weather data...")

        # Open and load the JSON file
        with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
            weather_data = json.load(f)

        logger.info(f"Weather data loaded for {len(weather_data)} days")
        return weather_data

    except FileNotFoundError as e:
        # Handle case where the file does not exist
        logger.error(f"Weather data file not found: {e}")
        return None

    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        logger.error(f"Error decoding JSON file: {e}")
        return None

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error while loading weather data: {e}")
        return None

# 4. TRANSFORMATION FUNCTIONS (to be completed by participants)

def enrich_with_weather(df, weather_data):
    """
    Enriches a DataFrame with weather data based on pickup dates and times.

    Args:
        df (pd.DataFrame): The DataFrame containing delivery data, including a 'pickup_datetime' column.
        weather_data (dict): A dictionary containing weather data organized by date and hour.

    Returns:
        pd.DataFrame: The DataFrame enriched with a new 'Weather_Condition' column indicating weather conditions.

    Steps:
        1. Convert the 'pickup_datetime' column to datetime type.
        2. Define a function to retrieve weather conditions based on a timestamp.
        3. Apply this function to each row of the DataFrame to add the 'Weather_Condition' column.
        4. Handle cases where weather data is missing by logging a warning.
    """
    try:
        logger.info("Enriching with weather data...")

        # Convert date column to datetime
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])

        # Function to get weather for a given timestamp
        def get_weather(timestamp):
            date_str = timestamp.strftime('%Y-%m-%d')
            hour_str = str(timestamp.hour)

            try:
                # Retrieve weather condition for the given date and hour
                return weather_data[date_str][hour_str]
            except KeyError:
                # Handle missing weather data
                logger.warning(f"Missing weather data for {date_str} at hour {hour_str}")
                return None

        # Apply function to each row
        df['Weather_Condition'] = df['pickup_datetime'].apply(get_weather)

        return df

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Error during weather enrichment: {e}")
        return df

def transform_data(df_deliveries, weather_data):
    """
    Transforms delivery data by enriching it with weather data, simulating delivery times,
    and calculating additional metrics.

    Args:
        df_deliveries (pd.DataFrame): DataFrame containing delivery data extracted from the database.
        weather_data (dict): Dictionary containing weather data for the last 3 months.

    Returns:
        pd.DataFrame: Transformed DataFrame with enriched and calculated fields, or None in case of errors.

    Steps:
        1. Enrich the DataFrame with weather data based on pickup datetime.
        2. Handle missing values by filling or dropping rows as needed.
        3. Simulate delivery distances and calculate base delivery times.
        4. Apply adjustment factors (package type, delivery zone, weather, peak hours, weekdays).
        5. Calculate actual delivery times with random noise and delay thresholds.
        6. Mark deliveries as 'Delayed' or 'On Time' based on thresholds.
        7. Select and return only the required columns.
    """
    try:
        logger.info("Transforming data...")

        # 1. Enrich with weather data
        logger.debug("Enriching data with weather conditions...")
        df = enrich_with_weather(df_deliveries, weather_data)

        # 2. Handle missing values
        if df.isnull().any().any():
            logger.warning("Missing values detected. Handling missing data...")
            # Fill missing weather conditions with 'Unknown'
            df['Weather_Condition'] = df['Weather_Condition'].fillna('Unknown')
            # Drop rows with critical missing data
            df = df.dropna(subset=['pickup_datetime', 'package_type', 'delivery_zone'])

        # 3. Calculate delivery times
        logger.debug("Calculating delivery distances and base times...")
        df['Distance'] = np.round(np.random.uniform(1, 100, size=len(df)), 2)
        df['BaseTime'] = 30 + df['Distance'] * 0.8

        package_factor = {'Small': 1.0, 'Medium': 1.2, 'Large': 1.5, 'X-Large': 2.0, 'Special': 2.5}
        zone_factor = {'Urban': 1.2, 'Suburban': 1.0, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4}
        weather_factor = {'Sunny': 1.0, 'Cloudy': 1.05, 'Rainy': 1.2, 'Snowy': 1.8}

        df['PackageFactor'] = df['package_type'].map(package_factor)
        df['ZoneFactor'] = df['delivery_zone'].map(zone_factor)
        df['WeatherFactor'] = df['Weather_Condition'].map(weather_factor).fillna(1.0)

        df['Hour'] = df['pickup_datetime'].dt.hour

        def get_peak_hour_multiplier(hour):
            if 7 <= hour <= 9:
                return 1.3
            elif 17 <= hour <= 19:
                return 1.4
            return 1.0
        df['PeakFactor'] = df['Hour'].apply(get_peak_hour_multiplier)

        df['Weekday'] = df['pickup_datetime'].dt.day_name()

        def get_day_multiplier(day):
            if day in ['Monday', 'Friday']:
                return 1.2
            elif day in ['Saturday', 'Sunday']:
                return 0.9
            return 1.0
        df['DayFactor'] = df['Weekday'].apply(get_day_multiplier)

        df['Actual_Delivery_Time'] = np.round(
            df['BaseTime']
            * df['PackageFactor']
            * df['ZoneFactor']
            * df['WeatherFactor']
            * df['PeakFactor']
            * df['DayFactor'], 2
        )

        df['DelayThreshold'] = df['Actual_Delivery_Time'] * 1.2

        noise = np.random.normal(loc=1.0, scale=1.0, size=len(df))
        noise = np.clip(noise, 0.5, 2)
        df['Actual_Delivery_Time'] = np.round(df['Actual_Delivery_Time'] * noise, 2)

        # Convert delivery time to minutes and seconds
        df['Actual_Delivery_Time'] = df['Actual_Delivery_Time'].apply(
            lambda x: f"{int(x)}m {int((x - int(x)) * 60)}s"
        )

        df['Status'] = np.where(
            df['Actual_Delivery_Time'].str.extract(r'(\d+)').astype(int)[0] > 120,
            'Delayed',
            'On Time'
        )

        required_columns = ['delivery_id', 'pickup_datetime', 'Weekday', 'Hour',
                            'package_type', 'Distance', 'delivery_zone', 'Weather_Condition',
                            'Actual_Delivery_Time', 'Status']

        df_deliveries = df[required_columns]

        logger.info("Transformation complete.")
        return df_deliveries

    except KeyError as e:
        # Handle missing keys in data
        logger.error(f"Key error during transformation: {e}")
        return None
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error during transformation: {e}")
        return None

# 5. LOADING FUNCTION (to be completed)
def save_results(df):
    """
    Saves the final DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame containing the transformed delivery data.

    Returns:
        bool: True if the results were saved successfully, False otherwise.

    Steps:
        1. Validate that all required columns are present in the DataFrame.
        2. Check for missing values and log a warning if any are found.
        3. Save the DataFrame to a CSV file at the specified output path.
        4. Handle and log any errors that occur during validation or file writing.
    """
    try:
        logger.info("Saving results...")

        # Validation code
        required_columns = ['delivery_id', 'pickup_datetime', 'Weekday', 'Hour',
                            'package_type', 'Distance', 'delivery_zone', 'Weather_Condition',
                            'Actual_Delivery_Time', 'Status']

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        if df.isnull().any().any():
            logger.warning("Data contains missing values")

        # Save to CSV
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Results saved to {OUTPUT_PATH}")
        return True

    except ValueError as e:
        # Handle missing columns or validation errors
        logger.error(f"Validation error: {e}")
        return False

    except IOError as e:
        # Handle file I/O errors
        logger.error(f"File write error: {e}")
        return False

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error during saving results: {e}")
        return False

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
