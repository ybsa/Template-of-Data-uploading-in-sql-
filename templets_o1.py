import pandas as pd
import os
import logging
import time
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_db_engine(db_type="sqlite", db_name="inventory.db", user=None, password=None, host="localhost", port=None):
    """
    Create a SQLAlchemy engine for SQLite, MySQL, or PostgreSQL.
    
    db_type: choose "sqlite", "mysql", or "postgresql"
    db_name: database name (or file name if SQLite)
    user, password: your database username & password
    host: database server address (e.g., "localhost" or IP)
    port: database server port (MySQL default=3306, PostgreSQL default=5432)
    """
    if db_type == "sqlite":
        # SQLite → only needs a file name
        return create_engine(f"sqlite:///{db_name}")
    elif db_type == "mysql":
        # MySQL → you must install "pymysql" library: pip install pymysql
        return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")
    elif db_type == "postgresql":
        # PostgreSQL → you must install "psycopg2": pip install psycopg2
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    else:
        raise ValueError("Unsupported database type. Choose sqlite, mysql, or postgresql.")

def ingest_db(df, table_name, engine, if_exists="append"):
    """
    Load a DataFrame into the database.
    
    if_exists options:
      - "replace" → drop old table and create new one
      - "append" → add new rows to existing table
      - "fail" → error if table exists
    """
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    logging.info(f"Ingested {len(df)} rows into table: {table_name}")

def load_raw_data(data_dir, engine, chunksize=50000):
    """
    Load all CSV files in a folder into the database.
    - data_dir: folder path where CSV files are stored
    - engine: database connection
    - chunksize: number of rows to read at once (good for large files)
    """
    start = time.time()

    if not os.path.exists(data_dir):
        logging.error(f"Data directory not found: {data_dir}")
        return

    for file_name in os.listdir(data_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(data_dir, file_name)
            table_name = file_name.replace(".csv", "").replace(" ", "_").lower()

            logging.info(f"Processing file: {file_name} → table: {table_name}")

            try:
                for chunk in pd.read_csv(file_path, chunksize=chunksize):
                    ingest_db(chunk, table_name, engine, if_exists="append")
                logging.info(f"✅ Finished ingesting {file_name}")
            except Exception as e:
                logging.exception(f"❌ Error processing {file_name}: {e}")

    total_time = (time.time() - start) / 60
    logging.info("---------- Ingestion Complete ----------")
    logging.info(f"Total time taken: {total_time:.2f} minutes")

if __name__ == "__main__":
    # ----------------------------------------
    # STEP 1: Choose which database you want
    # ----------------------------------------

    # 🔹 1) SQLite (easiest, no setup needed)
    engine = create_db_engine(db_type="sqlite", db_name="inventory.db")

    # 🔹 2) MySQL (requires user, password, host, port)
    # engine = create_db_engine(
    #     db_type="mysql", 
    #     db_name="inventory", 
    #     user="root", 
    #     password="yourpassword", 
    #     host="localhost", 
    #     port=3306
    # )

    # 🔹 3) PostgreSQL (requires user, password, host, port)
    # engine = create_db_engine(
    #     db_type="postgresql", 
    #     db_name="inventory", 
    #     user="postgres", 
    #     password="yourpassword", 
    #     host="localhost", 
    #     port=5432
    # )

    # ----------------------------------------
    # STEP 2: Set your CSV folder location
    # ----------------------------------------
    data_dir = r"C:\Users\wind xebec\Downloads\data\data"  # <-- CHANGE this to your folder path

    # ----------------------------------------
    # STEP 3: Run the loader
    # ----------------------------------------
    load_raw_data(data_dir, engine, chunksize=50000)
