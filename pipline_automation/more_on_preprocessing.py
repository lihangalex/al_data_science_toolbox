import pandas as pd
import numpy as np
import re
from datetime import datetime

def remove_duplicates(df):
    """Remove duplicate rows."""
    print("Removing duplicate rows...")
    return df.drop_duplicates()

def handle_missing_values(df):
    """Handle missing values by filling or dropping depending on the column."""
    print("Handling missing values...")
    # Fill missing emails with a default
    if 'email' in df.columns:
        df['email'] = df['email'].fillna("unknown@example.com")

    # Fill missing numerical values with the column mean
    for col in df.select_dtypes(include=['float64', 'int64']).columns:
        df[col] = df[col].fillna(df[col].mean())

    # Drop rows where critical columns have missing values
    critical_columns = ['id', 'email']
    df = df.dropna(subset=[col for col in critical_columns if col in df.columns])

    return df

def detect_and_handle_outliers(df, column):
    """Detect and handle outliers using the IQR method."""
    print(f"Handling outliers in column: {column}...")
    if column in df.columns:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df

def sanitize_text_fields(df):
    """Sanitize text fields to remove unwanted characters."""
    print("Sanitizing text fields...")
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9@._\-\s]', '', str(x)) if pd.notnull(x) else x)
    return df

def standardize_date_formats(df):
    """Convert all date columns to a standard format."""
    print("Standardizing date formats...")
    for col in df.select_dtypes(include=['object']):
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def add_derived_columns(df):
    """Add derived columns for analysis."""
    print("Adding derived columns...")
    if 'amount' in df.columns:
        df['amount_squared'] = df['amount'] ** 2
    if 'transaction_date' in df.columns:
        df['transaction_year'] = df['transaction_date'].dt.year
    return df

def validate_data(df):
    """Perform data validation checks."""
    print("Validating data...")
    if 'id' in df.columns and df['id'].isnull().any():
        raise ValueError("Missing ID values detected!")
    if 'email' in df.columns and not df['email'].str.contains('@').all():
        raise ValueError("Invalid email addresses detected!")
    return df

def drop_null_threshold(df, threshold=0.5):
    """Drop columns where more than a threshold of values are missing."""
    print(f"Dropping columns with more than {threshold*100}% missing values...")
    return df.loc[:, df.isnull().mean() < threshold]

def transform_data(df):
    """Master transformation function to handle all steps."""
    print("Starting data transformation...")
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = detect_and_handle_outliers(df, 'amount')
    df = sanitize_text_fields(df)
    df = standardize_date_formats(df)
    df = add_derived_columns(df)
    df = validate_data(df)
    df = drop_null_threshold(df, threshold=0.5)
    print("Data transformation complete.")
    return df

# Example usage
if __name__ == "__main__":
    data = {
        "id": [1, 2, None, 4],
        "email": ["john@example.com", None, "invalid@#email.com", "jane.doe@example.com"],
        "amount": [100, 200, 3000, -50],
        "name": ["john", "jane", "doe", "alice"],
        "transaction_date": ["2023-01-01", "invalid_date", None, "2024-02-15"],
        "extra_col": [None, None, None, None]
    }

    df = pd.DataFrame(data)
    print("Raw Data:")
    print(df)

    transformed_df = transform_data(df)
    print("Transformed Data:")
    print(transformed_df)
