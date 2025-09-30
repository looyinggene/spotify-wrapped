import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import fetch_saved_tracks, fetch_saved_genres
from etl.load import load_saved_tracks_from_csv, load_genres_from_csv
from etl.db_utils import run_sql_scripts
import pandas as pd

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spotify_saved_tracks_etl_pipeline",
    default_args=default_args,
    description="Spotify ETL pipeline for saved tracks to bronze/silver/gold",
    schedule_interval="0 3 */14 * *",  # Run every 14 days at 3AM UTC
    start_date=datetime(2025, 9, 11),
    catchup=False,
    max_active_runs=1,
) as dag:

    # --- Create Bronze Tables ---
    create_bronze_tables = PythonOperator(
        task_id="create_saved_bronze_tables",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/bronze/create_saved_songs.sql",
            "sql/bronze/create_saved_genres.sql"
        ]}
    )

    # --- Extract Saved Tracks ---
    extract_saved_tracks = PythonOperator(
        task_id="extract_saved_tracks",
        python_callable=fetch_saved_tracks
    )

    # --- Extract Saved Genres ---
    def extract_saved_genres_task():
        df = pd.read_csv("data/saved_songs.csv")
        fetch_saved_genres(df["artist_id"].tolist())

    extract_saved_genres = PythonOperator(
        task_id="extract_saved_genres",
        python_callable=extract_saved_genres_task
    )

    # --- Load Saved Tracks ---
    load_saved_tracks = PythonOperator(
        task_id="load_saved_tracks",
        python_callable=load_saved_tracks_from_csv
    )

    # --- Load Genres ---
    load_genres = PythonOperator(
        task_id="load_saved_genres",
        python_callable=load_genres_from_csv
    )

    # --- Silver Transform ---
    silver_transform = PythonOperator(
        task_id="silver_saved_transform",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/silver/spotify_saved_songs_load.sql",
            "sql/silver/spotify_saved_genres_load.sql"
        ]}
    )

    # --- Gold Transform ---
    gold_transform = PythonOperator(
        task_id="gold_saved_transform",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/gold/saved_tracks/fact_saved_songs.sql",
            "sql/gold/saved_tracks/dim_saved_tracks.sql",
            "sql/gold/saved_tracks/dim_saved_artists.sql",
            "sql/gold/saved_tracks/dim_saved_albums.sql"
        ]}
    )

    # --- Build Gold Tables ---
    gold_view = PythonOperator(
        task_id="gold_view",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/views/saved_tracks/view_album_diversity_top_artists.sql",
            "sql/views/saved_tracks/view_saved_genre_summary.sql",
            "sql/views/saved_tracks/view_top_saved_artists.sql",
        ]}
    )

    # Task order
    create_bronze_tables >> extract_saved_tracks >> extract_saved_genres >> [load_saved_tracks, load_genres] >> silver_transform >> gold_transform >> gold_view
