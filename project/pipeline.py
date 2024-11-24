import os
import zipfile
import requests
import pandas as pd
from io import BytesIO
import sqlite3

# Define directory and data files
data_directory = r"..\data"
os.makedirs(data_directory, exist_ok=True)

# Define URLs for datasets
url_faostat_zip = "https://bulks-faostat.fao.org/production/Production_Crops_Livestock_E_All_Data.zip"
url_world_bank_zip = "https://api.worldbank.org/v2/en/indicator/AG.LND.PRCP.MM?downloadformat=csv"

# Latin American countries and relevant years
latin_american_countries = [
    "Argentina", "Bolivia (Plurinational State of)", "Brazil", "Chile", "Colombia", 
    "Costa Rica", "Cuba", "Dominican Republic", "Ecuador", "El Salvador", "Guatemala", 
    "Honduras", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru", 
    "Uruguay", "Venezuela (Bolivarian Republic of)"
]
year_range_faostat = ['Y2015', 'Y2016', 'Y2017', 'Y2018', 'Y2019', 'Y2020', 'Y2021']
year_range_world_bank = ['2015', '2016', '2017', '2018', '2019', '2020', '2021']

# Function to download and extract ZIP files
def download_and_extract_zip(url, destination_folder):
    response = requests.get(url)
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(destination_folder)
            print(f"Extracted {len(zip_ref.namelist())} files to {destination_folder}")
            csv_files = [file for file in zip_ref.namelist() if file.endswith('.csv') and 'Metadata' not in file]
            if csv_files:
                return os.path.join(destination_folder, csv_files[0])  # Return first CSV found
            else:
                print("No valid CSV files found in the ZIP archive.")
                return None
    else:
        print(f"Failed to download data from {url}. Status code: {response.status_code}")
        return None

# Function to clean and reshape FAOSTAT data
def clean_faostat_data(file_path):
    try:
        df = pd.read_csv(file_path)
        filtered = df[df['Area'].isin(latin_american_countries)]
        filtered = filtered[['Area', 'Item', 'Element', 'Unit'] + year_range_faostat]
        # Melt to long format
        long_format = filtered.melt(
            id_vars=['Area', 'Item', 'Element', 'Unit'], 
            var_name='Year', 
            value_name='Value'
        )
        long_format['Year'] = long_format['Year'].str.strip('Y')  # Remove 'Y' prefix from years
        return long_format
    except Exception as e:
        print(f"Error processing FAOSTAT data: {e}")
        return None

# Function to clean and reshape World Bank data
def clean_world_bank_data(file_path):
    try:
        df = pd.read_csv(file_path, skiprows=4)
        filtered = df[df['Country Name'].isin(latin_american_countries)]
        filtered = filtered[['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'] + year_range_world_bank]
        # Melt to long format
        long_format = filtered.melt(
            id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'], 
            var_name='Year', 
            value_name='Value'
        )
        return long_format
    except Exception as e:
        print(f"Error processing World Bank data: {e}")
        return None

# Function to export DataFrame to SQLite database
def export_to_sqlite(df, table_name, db_name):
    try:
        conn = sqlite3.connect(db_name)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        print(f"Data saved to SQLite table '{table_name}' in {db_name}.")
    except Exception as e:
        print(f"Error exporting to SQLite: {e}")
    finally:
        conn.close()

# Download and process FAOSTAT data
faostat_folder = os.path.join(data_directory, "faostat_data")
os.makedirs(faostat_folder, exist_ok=True)
faostat_file = download_and_extract_zip(url_faostat_zip, faostat_folder)

if faostat_file:
    faostat_cleaned = clean_faostat_data(faostat_file)
    if faostat_cleaned is not None:
        faostat_cleaned_file = os.path.join(data_directory, "faostat_cleaned_long.csv")
        faostat_cleaned.to_csv(faostat_cleaned_file, index=False)
        print(f"Cleaned FAOSTAT data in long format saved to {faostat_cleaned_file}")

# Download and process World Bank data
world_bank_folder = os.path.join(data_directory, "world_bank_data")
os.makedirs(world_bank_folder, exist_ok=True)
world_bank_file = download_and_extract_zip(url_world_bank_zip, world_bank_folder)

if world_bank_file:
    world_bank_cleaned = clean_world_bank_data(world_bank_file)
    if world_bank_cleaned is not None:
        world_bank_cleaned_file = os.path.join(data_directory, "world_bank_cleaned_long.csv")
        world_bank_cleaned.to_csv(world_bank_cleaned_file, index=False)
        print(f"Cleaned World Bank data in long format saved to {world_bank_cleaned_file}")

# Export cleaned data to SQLite
sqlite_db_path = os.path.join(data_directory, "data_cleaned.db")
if faostat_cleaned is not None:
    export_to_sqlite(faostat_cleaned, "faostat_data_long", sqlite_db_path)
if world_bank_cleaned is not None:
    export_to_sqlite(world_bank_cleaned, "world_bank_data_long", sqlite_db_path)
