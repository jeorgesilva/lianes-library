import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
# ==============================
# 1. creating the path to CSV
# ==============================
csv_path = "/Users/jeorgecassiodesousasilva/Documents/lianes-library/lianes-library/data/books_clean_debug2.csv"

# ==============================
# 2. read it trating errors
# ==============================
# engine="python" + on_bad_lines="skip" avoids the errors:

books_df = pd.read_csv(
    csv_path,
    engine="python",
    on_bad_lines="skip"   # ou "warn" se quiser ver avisos
)

# =====================================
# 3. send DataFrame to MySQL table
# =====================================
# create the connection engine
schema = "lianes_library"
host = "127.0.0.1"
user = "root"
raw_password = "Frut@!1469"
password = urllib.parse.quote_plus(raw_password)
port = 3306

connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{schema}"
engine = create_engine(connection_string)

# map the columns from CSV to DB table
column_mapping = {
    "title": "title",          # make the correspondence CSV -> DB
    "isbn": "ISBN",            
    "authors": "author",       #the ones not listed here will be ignored
}

# filter only the columns that exist in the DataFrame
existing_cols = {csv_col: db_col for csv_col, db_col in column_mapping.items() if csv_col in books_df.columns}

# create a new DataFrame with only the existing columns and renamed
books_for_db = books_df[list(existing_cols.keys())].rename(columns=existing_cols)

# send to SQL table 'books'
books_for_db.to_sql(
    'books',
    if_exists='append',
    con=engine,
    index=False
)
