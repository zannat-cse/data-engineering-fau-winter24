import os
import subprocess
import pandas as pd

# Paths to the expected output files
EXPECTED_OUTPUT_FILES = [
    '../data/faostat_cleaned_long.csv',
    '../data/world_bank_cleaned_long.csv',
    '../data/data_cleaned.db'
]

# Function to run the ETL pipeline and capture its output
def run_etl_pipeline():
    print("Running ETL pipeline...")

    # Use subprocess to execute the pipeline
    process = subprocess.Popen(['python', './project/pipeline.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        print(f"ETL pipeline failed with error: {stderr.decode()}")
        return False

    print(f"ETL pipeline ran successfully. Output: {stdout.decode()}")
    return True

# Function to validate the existence and content of files
def validate_output_files():
    print("Validating output files")

    # Initialize an empty list for errors
    errors = []

    # Check if the output files exist and if they are non-empty
    for file in EXPECTED_OUTPUT_FILES:
        if not os.path.exists(file):
            errors.append(f"File missing: {file}")
        elif os.path.getsize(file) == 0:
            errors.append(f"File is empty: {file}")
    
    if errors: 
        for error in errors:
            print(error)
        return False
    
    # Include data file names in success messages
    print(f"Success: All files exist.")
    for file in EXPECTED_OUTPUT_FILES:
        print(f"  - {file}")
    
    return True


# Main function to run all tests sequentially
def run_tests():
    # First, run the ETL pipeline and validate if it worked
    if not run_etl_pipeline():
        print("ETL pipeline test failed.")
        return

    # Then, validate the output files
    if not validate_output_files():
        print("File validation test failed.")
        return

    # Final success message
    print("All tests passed successfully!")

if __name__ == "__main__":
    run_tests()
