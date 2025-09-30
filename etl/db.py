import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv("key.env")

DB_NAME = os.getenv("POSTGRES_DB", "spotify")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def get_connection():
    """Return a psycopg2 connection to Postgres."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT,
    )
    return conn
