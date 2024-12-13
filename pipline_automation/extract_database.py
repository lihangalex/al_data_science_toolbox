import pandas as pd
from sqlalchemy import create_engine

# create a database engine
engine = create_engine("sqlite:///test.db")

def extract_database(table_name, parameters):
    try:
        print(f"Extracting data from database table: {table_name}")
        query = f"Select * from {table_name} where id = :id"
        df = pd.read_sql(query, engine, parameters=parameters)
        if df.empty:
            print("Warning: Query returned no results.")
        return df
    except Exception as e:
        print(f"Error during database extraction: {e}")
        return pd.DataFrame() # this returns an empty dataframe