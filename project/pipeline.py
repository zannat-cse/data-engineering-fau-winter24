import os
import pandas as pd
import sqlite3
import requests
import io

# Step 1: Define the dataset URL and data directory
url = "https://data.montgomerycountymd.gov/api/views/mmzv-x632/rows.csv?accessType=DOWNLOAD"
data_directory = './data'
os.makedirs(data_directory, exist_ok=True)

# Step 2: Download the dataset with SSL verification disabled
try:
    response = requests.get(url, verify=False)
    response.raise_for_status()  # Check if the request was successful
    # Read CSV from the response using io.StringIO
    data = pd.read_csv(io.StringIO(response.text))
    print("Dataset loaded successfully.")
except Exception as e:
    print(f"Failed to load dataset: {e}")
    exit(1)

# Step 3: Display basic information and clean the data
print("\nColumns in the dataset:", data.columns)
print("\nFirst few rows:", data.head())

# Step 4: Data Cleaning (Removing rows with missing values)
data.dropna(inplace=True)
data.reset_index(drop=True, inplace=True)

# Step 5: Store the cleaned data in a SQLite database
db_file = os.path.join(data_directory, 'montgomery_county_data.db')
conn = sqlite3.connect(db_file)

table_name = 'county_data'
data.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
print(f"\nData saved to SQLite table '{table_name}' in {db_file}.")

# Step 6: Confirmation of data storage
with sqlite3.connect(db_file) as conn:
    result = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn)
    print("\nSample data from SQLite database:\n", result)
