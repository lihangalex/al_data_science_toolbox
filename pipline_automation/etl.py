import pandas as pd
import os
import schedule
import time
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Database connection (use environment variable)
# or hardcode in local environment DATABASE_URL = "postgresql://username:password@localhost:5432/example_db"
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise EnvironmentError("DATABASE_URL environment variable not found.")
engine = create_engine(DATABASE_URL)


def extract_file(file_path):
    """Extract data from a CSV file."""
    print(f"Extracting data from file: {file_path}")
    return pd.read_csv(file_path)


def extract_database(table_name):
    """Extract data from a database table."""
    print(f"Extracting data from database table: {table_name}")
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)


def transform_data(df):
    """Transform the data (cleaning, validation, enrichment)."""
    print("Transforming data...")
    df = df.drop_duplicates()

    # Fill missing values (example)
    if 'email' in df.columns:
        df['email'] = df['email'].fillna("unknown@example.com")

    # Add derived columns (example)
    if 'amount' in df.columns:
        df['amount_squared'] = df['amount'] ** 2

    # Validate data (example: check for missing IDs)
    if 'id' in df.columns and df['id'].isnull().any():
        raise ValueError("Missing ID values detected!")

    return df


def load_to_file(df, file_path):
    """Load the data into a CSV file."""
    print(f"Loading data to file: {file_path}")
    df.to_csv(file_path, index=False)


def load_to_database(df, table_name):
    """Load the data into a database table."""
    print(f"Loading data to database table: {table_name}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)


def etl_pipeline(source, destination, source_type="file", destination_type="file"):
    """Run the full ETL pipeline."""
    print(f"Starting ETL pipeline at {datetime.now()}...")

    # Extract
    if source_type == "file":
        data = extract_file(source)
    elif source_type == "database":
        data = extract_database(source)
    else:
        print("Unsupported source type. Use 'file' or 'database'.")
        return

    # Transform
    try:
        data = transform_data(data)
    except Exception as e:
        print(f"Error during transformation: {e}")
        return

    # Load
    if destination_type == "file":
        load_to_file(data, destination)
    elif destination_type == "database":
        load_to_database(data, destination)
    else:
        print("Unsupported destination type. Use 'file' or 'database'.")
        return

    print(f"ETL pipeline completed successfully at {datetime.now()}.")


def schedule_pipeline():
    """Schedule the ETL pipeline to run periodically."""
    schedule.every().day.at("00:00").do(etl_pipeline,
                                        source="raw_data.csv",
                                        destination="cleaned_data.csv",
                                        source_type="file",
                                        destination_type="file")
    print("Scheduled ETL pipeline to run daily at midnight.")

    while True:
        schedule.run_pending()
        time.sleep(1)


# Example Usage
if __name__ == "__main__":
    # Run pipeline manually
    etl_pipeline(
        source="raw_data.csv",
        destination="cleaned_data.csv",
        source_type="file",
        destination_type="file"
    )

    # Uncomment to enable scheduling
    # schedule_pipeline()
