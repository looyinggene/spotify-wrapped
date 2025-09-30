import sys
sys.path.append("/opt/airflow")
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from etl.extract import fetch_recent_played, fetch_artist_genres, fetch_top_song_audio_previews,fetch_top_artist_song_audio_previews
from etl.load import load_songs_from_csv, load_genres_from_csv, load_artist_images_from_csv, load_top_song_previews_from_csv, load_top_artist_previews_from_csv
from etl.db_utils import run_sql_scripts


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="spotify_etl_pipeline",
    default_args=default_args,
    description="Spotify ETL pipeline to bronze/silver/gold",
    schedule_interval= "0 2-15 * * *",
    start_date=datetime(2025, 9, 10),
    catchup=False,
    max_active_runs=5,
) as dag:
    # --- Create Bronze Tables (safe to run every time) --- 
    create_bronze_tables = PythonOperator(
    task_id="create_bronze_tables",
    python_callable=run_sql_scripts,
    op_kwargs={"file_paths": [
        "sql/bronze/create_spotify_songs.sql",
        "sql/bronze/create_spotify_genres.sql",
        "sql/bronze/create_spotify_artist_images.sql"
    ]}
)

    # --- Extract Songs ---
    extract_songs = PythonOperator(
        task_id="extract_songs",
        python_callable=fetch_recent_played
    )

    # --- Extract Genres ---
    def extract_genres_task():
        import pandas as pd
        df = pd.read_csv("data/recent_songs.csv")
        fetch_artist_genres(df["artist_id"].tolist())

    extract_genres = PythonOperator(
        task_id="extract_genres",
        python_callable=extract_genres_task
    )

    # --- Load Songs ---
    load_songs = PythonOperator(
        task_id="load_songs",
        python_callable=load_songs_from_csv
    )

    # --- Load Genres ---
    load_genres = PythonOperator(
        task_id="load_genres",
        python_callable=load_genres_from_csv
    )

    # --- Load Artist Images ---
    load_artist_images = PythonOperator(
        task_id="load_artist_images",
        python_callable=load_artist_images_from_csv
    )

    # --- Transform to Silver ---
    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/silver/spotify_songs_load.sql",
            "sql/silver/spotify_genres_load.sql",
            "sql/silver/spotify_artist_images_load.sql"
        ]}
    )

    # --- Build Gold Tables ---
    gold_transform = PythonOperator(
        task_id="gold_transform",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/gold/listening_history/fact_listening_history.sql",
            "sql/gold/listening_history/dim_tracks.sql",
            "sql/gold/listening_history/dim_artists.sql",
            "sql/gold/listening_history/dim_albums.sql",
            "sql/gold/listening_history/top_song_previews.sql",
            "sql/gold/listening_history/top_artist_previews.sql"
        ]}
    )

    # --- Build Gold Tables ---
    gold_view = PythonOperator(
        task_id="gold_view",
        python_callable=run_sql_scripts,
        op_kwargs={"file_paths": [
            "sql/views/listening_history/view_monthly_genre_distribution.sql",
            "sql/views/listening_history/view_monthly_genre_top_artists.sql",
            "sql/views/listening_history/view_top_artists.sql",
            "sql/views/listening_history/view_top_songs.sql",
            "sql/views/listening_history/view_total_listening_by_day.sql",
            "sql/views/listening_history/view_total_listening_overall.sql",
            "sql/views/listening_history/view_top_songs_with_preview.sql",
            "sql/views/listening_history/view_top_artists_with_preview.sql"
        ]}
    )

    fetch_top_song_previews = PythonOperator(
        task_id="fetch_top_song_previews",
        python_callable=fetch_top_song_audio_previews
    )

    load_top_song_previews = PythonOperator(
        task_id="load_top_song_previews",
        python_callable=load_top_song_previews_from_csv
    )

    fetch_top_artist_previews = PythonOperator(
        task_id="fetch_top_artist_previews",
        python_callable=fetch_top_artist_song_audio_previews,
        op_kwargs={'num_top_artists': 3}  # Pass the argument for N=3
    )

    # Load the combined CSV for top N artists into the gold table
    load_top_artist_previews = PythonOperator(
        task_id="load_top_artist_previews",
        python_callable=load_top_artist_previews_from_csv
    )


# DAG Task Order
create_bronze_tables >> extract_songs >> extract_genres
[load_songs, load_genres, load_artist_images] >> silver_transform >> gold_transform >> gold_view
    
# Branching for Audio Previews (These should run *after* gold views are created)
gold_view >> [fetch_top_song_previews, fetch_top_artist_previews]
    
# Sequential loading after fetching
fetch_top_song_previews >> load_top_song_previews
fetch_top_artist_previews >> load_top_artist_previews
