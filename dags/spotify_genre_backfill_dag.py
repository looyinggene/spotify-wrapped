import sys
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.backfill import backfill_missing_listening_genres, backfill_missing_saved_genres, backfill_missing_artist_images

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spotify_genre_backfill_biweekly",
    default_args=default_args,
    description="Biweekly backfill of missing artist genres (listening + saved) and images",
    schedule_interval="0 3 */14 * 0",  # every 2 weeks on Sunday at 3AM UTC
    start_date=datetime(2025, 9, 14),
    catchup=False,
    max_active_runs=1,
) as dag:

    backfill_listening_genres = PythonOperator(
        task_id="backfill_listening_genres",
        python_callable=backfill_missing_listening_genres
    )

    backfill_saved_genres = PythonOperator(
        task_id="backfill_saved_genres",
        python_callable=backfill_missing_saved_genres
    )
    
    backfill_artist_images = PythonOperator(
        task_id="backfill_artist_images",
        python_callable=backfill_missing_artist_images
    )

    backfill_listening_genres >> backfill_saved_genres >> backfill_artist_images
