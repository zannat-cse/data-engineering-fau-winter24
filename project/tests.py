import os
import sqlite3
import pandas as pd
import subprocess

# Define paths and constants
FAOSTAT_CSV = "../data/faostat_cleaned_long.csv"
WORLD_BANK_CSV = "../data/world_bank_cleaned_long.csv"
SQLITE_DB = "../data/data_cleaned.db"
PIPELINE_SCRIPT = "./project/pipeline.py"


def test_pipeline_execution():

    # Testing if the pipeline script executes successfully.
    print("Testing pipeline execution...")
    result = subprocess.run(["python", PIPELINE_SCRIPT], capture_output=True, text=True)
    assert result.returncode == 0, f"Pipeline script failed: {result.stderr}"
    print("Pipeline executed successfully.")


def test_faostat_csv_exists():

    # test if the FAOSTAT cleaned CSV file is created.
    print("Testing if FAOSTAT cleaned CSV exists.")
    assert os.path.exists(FAOSTAT_CSV), f"FAOSTAT cleaned CSV file not found: {FAOSTAT_CSV}"
    print("FAOSTAT cleaned CSV exists.")


def test_world_bank_csv_exists():

    # Test if the World Bank cleaned CSV file is created.
    print("Testing if World Bank cleaned CSV exists..")
    assert os.path.exists(WORLD_BANK_CSV), f"World Bank cleaned CSV file not found: {WORLD_BANK_CSV}"
    print("World Bank cleaned CSV exists.")


def test_faostat_csv_content():

    # Test the content of the FAOSTAT cleaned CSV file.
    print("Testing FAOSTAT cleaned CSV content...")
    df = pd.read_csv(FAOSTAT_CSV)
    assert not df.empty, "FAOSTAT cleaned CSV file is empty."
    assert "Area" in df.columns, "Expected column 'Area' not found in FAOSTAT CSV."
    assert "Year" in df.columns, "Expected column 'Year' not found in FAOSTAT CSV."
    print("FAOSTAT cleaned CSV content is valid.")


def test_world_bank_csv_content():

    # Test the content of the World Bank cleaned CSV file.
    print("Testing World Bank cleaned CSV content...")
    df = pd.read_csv(WORLD_BANK_CSV)
    assert not df.empty, "World Bank cleaned CSV file is empty."
    assert "Country Name" in df.columns, "Expected column 'Country Name' not found in World Bank CSV."
    assert "Year" in df.columns, "Expected column 'Year' not found in World Bank CSV."
    print("World Bank cleaned CSV content is valid.")


def test_sqlite_db_exists():

    # testing if the SQLite database file is created.
    print("Testing if SQLite database exists...")
    assert os.path.exists(SQLITE_DB), f"SQLite database not found: {SQLITE_DB}"
    print("SQLite database exists.")


def test_sqlite_tables():

    # test if the expected tables exist in the SQLite database.
    print("Testing SQLite database tables.")
    with sqlite3.connect(SQLITE_DB) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        assert "faostat_data_long" in tables, "Table 'faostat_data_long' not found in SQLite database."
        assert "world_bank_data_long" in tables, "Table 'world_bank_data_long' not found in SQLite database."
        print("Expected tables found in SQLite database.")


def test_sqlite_table_content():
    """
    Test the content of the tables in the SQLite database.
    """
    print("Testing SQLite database table content...")
    with sqlite3.connect(SQLITE_DB) as conn:
        faostat_df = pd.read_sql("SELECT * FROM faostat_data_long;", conn)
        world_bank_df = pd.read_sql("SELECT * FROM world_bank_data_long;", conn)
        assert not faostat_df.empty, "Table 'faostat_data_long' in SQLite database is empty."
        assert not world_bank_df.empty, "Table 'world_bank_data_long' in SQLite database is empty."
        print("SQLite database tables contain valid data.")


if __name__ == "__main__":
    # Run all tests
    test_pipeline_execution()
    test_faostat_csv_exists()
    test_world_bank_csv_exists()
    test_faostat_csv_content()
    test_world_bank_csv_content()
    test_sqlite_db_exists()
    test_sqlite_tables()
    test_sqlite_table_content()

    print("All Test is Done Successfully")
