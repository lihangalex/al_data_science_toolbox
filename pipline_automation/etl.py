import pandas as pd
import os
import schedule
import time
import requests
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

class ETL:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL")
        if not self.database_url:
            raise EnvironmentError("DATABASE_URL environment variable not found.")
        self.engine = create_engine(self.database_url)

    def extract_file(self, file_path):
        """Extract data from a CSV file."""
        print(f"Extracting data from file: {file_path}")
        return pd.read_csv(file_path)

    def extract_database(self, table_name):
        """Extract data from a database table."""
        print(f"Extracting data from database table: {table_name}")
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.engine)

    def extract_api(self, api_url, headers=None):
        """Extract data from an API."""
        print(f"Extracting data from API: {api_url}")
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            return pd.DataFrame(response.json())
        except Exception as e:
            print(f"Error during API extraction: {e}")
            return pd.DataFrame()

    def transform_data(self, df):
        """Transform the data (cleaning, validation, enrichment)."""
        print("Transforming data...")
        df = df.drop_duplicates()

        # Handle missing values
        if 'email' in df.columns:
            df['email'] = df['email'].fillna("unknown@example.com")

        # Add derived columns
        if 'amount' in df.columns:
            df['amount_squared'] = df['amount'] ** 2

        # Validate for missing IDs
        if 'id' in df.columns and df['id'].isnull().any():
            raise ValueError("Missing ID values detected!")

        # Standardize date formats
        for col in df.select_dtypes(include=['object']):
            if "date" in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except Exception as e:
                    print(f"Error parsing dates in column {col}: {e}")

        # Handle outliers
        if 'amount' in df.columns:
            q1 = df['amount'].quantile(0.25)
            q3 = df['amount'].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            df = df[(df['amount'] >= lower_bound) & (df['amount'] <= upper_bound)]

        return df

    def load_to_file(self, df, file_path):
        """Load the data into a CSV file."""
        print(f"Loading data to file: {file_path}")
        df.to_csv(file_path, index=False)

    def load_to_database(self, df, table_name):
        """Load the data into a database table."""
        print(f"Loading data to database table: {table_name}")
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def run_pipeline(self, source, destination, source_type, destination_type, api_headers=None):
        """Run the full ETL pipeline."""
        print(f"Starting ETL pipeline at {datetime.now()}...")

        extractors = {
            "file": self.extract_file,
            "database": self.extract_database,
            "api": lambda source: self.extract_api(source, headers=api_headers)
        }
        extractor = extractors.get(source_type)
        if not extractor:
            print("Unsupported source type. Use 'file', 'database', or 'api'.")
            return
        data = extractor(source)

        try:
            data = self.transform_data(data)
        except Exception as e:
            print(f"Error during transformation: {e}")
            return

        loaders = {
            "file": self.load_to_file,
            "database": self.load_to_database
        }
        loader = loaders.get(destination_type)
        if not loader:
            print("Unsupported destination type. Use 'file' or 'database'.")
            return
        loader(data, destination)

        print(f"ETL pipeline completed successfully at {datetime.now()}.")

    def schedule_pipeline(self, source, destination, source_type, destination_type, api_headers=None):
        schedule.every().day.at("00:00").do(self.run_pipeline,
                                             source=source,
                                             destination=destination,
                                             source_type=source_type,
                                             destination_type=destination_type,
                                             api_headers=api_headers)
        print("Scheduled ETL pipeline to run daily at midnight.")

        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    etl = ETL()

    print("Choose source type: 1) File 2) Database 3) API")
    source_choice = input("Enter your choice (1/2/3): ").strip()
    source_type = {"1": "file", "2": "database", "3": "api"}.get(source_choice)

    if source_type == "file":
        source = input("Enter file path: ").strip()
    elif source_type == "database":
        source = input("Enter table name: ").strip()
    elif source_type == "api":
        source = input("Enter API URL: ").strip()
    else:
        print("Invalid choice.")
        exit()

    print("Choose destination type: 1) File 2) Database")
    dest_choice = input("Enter your choice (1/2): ").strip()
    destination_type = {"1": "file", "2": "database"}.get(dest_choice)

    if destination_type == "file":
        destination = input("Enter output file path: ").strip()
    elif destination_type == "database":
        destination = input("Enter destination table name: ").strip()
    else:
        print("Invalid choice.")
        exit()

    api_headers = None
    if source_type == "api":
        headers_input = input("Enter API headers as key:value pairs (comma-separated) or leave blank: ").strip()
        if headers_input:
            api_headers = {k.strip(): v.strip() for k, v in (pair.split(":") for pair in headers_input.split(","))}

    etl.run_pipeline(source, destination, source_type, destination_type, api_headers)
    # Uncomment to enable scheduling
    # etl.schedule_pipeline(source, destination, source_type, destination_type, api_headers=api_headers)
