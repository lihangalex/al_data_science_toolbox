import pandas as pd

def extract_file(file_path):
    try:
        print(f"Extracting data from file: {file_path}")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return pd.DataFrame()
    except pd.errors.ParseError as e:
        print(f"Error parsing file: {e}")
        return pd.DataFrame()


