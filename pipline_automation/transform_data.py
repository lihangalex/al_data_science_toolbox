import pandas as pd
def transform_data(df):
    print("Transforming data...")

    # Remove duplicates
    df = df.drop_duplicates()

    # Handle missing values
    if 'email' in df.columns:
        df['email'] = df['email'].fillna("unknown@example.com")

    # Add derived columns
    if 'amount' in df.columns:
        df['amount_squared'] = df['amount'] ** 2

    # Remove rows with missing criticalfields
    critical_columns = ['id', 'email']
    df = df.dropna(subset=[col for col in critical_columns if col in df.columns])

    # Standardize date formats
    for col in df.selectdtypes(include=['object']):
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Handle outliers in numerical columns
    if 'amount' in df.columns:
        q1 = df['amount'].quantile(0.25)
        q3 = df['amount'].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        df = df[(df['amount'] >= lower_bound) & (df['amount'] <= upper_bound)]

    # Sanitize text fields
    for col in df.select_dtypes(include=['object']):
        if 'email' in col.lower():
            df[col] = df[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9@._-]', '', x) if pd.notnull(x) else x)

    # Capitalize names
    if 'name' in df.columns:
        df['name'] = df['name'].str.title()

    return df