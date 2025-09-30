import pandas as pd
import psycopg2
import psycopg2.extras as extras
from psycopg2.extras import execute_values
from etl.db import get_connection

def load_songs_from_csv(csv_path="/opt/airflow/data/recent_songs.csv"):
    """Load song data from CSV into Postgres."""
    df = pd.read_csv(csv_path)
    load_songs(df)

def load_songs(df):
    """Insert song data into bronze.spotify_songs with deduplication."""
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO bronze.spotify_songs (
        played_at_utc, played_date_utc, song_name, artist_name, 
        song_duration_ms, song_link, album_art_link, album_name, 
        album_id, artist_id, track_id, last_updated_datetime_utc
    ) VALUES %s
    ON CONFLICT (played_at_utc) DO NOTHING;
    """

    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(df)} song records")

def load_genres_from_csv(csv_path="/opt/airflow/data/recent_genres.csv"):
    """Load genre data from CSV into Postgres."""
    df = pd.read_csv(csv_path)
    load_genres(df)

def load_genres(df):
    """Insert or update genre data into bronze.spotify_genres."""
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO bronze.spotify_genres (
        artist_id, artist_name, artist_genre, last_updated_datetime_utc
    ) VALUES %s
    ON CONFLICT (artist_id) DO UPDATE SET
        artist_name = EXCLUDED.artist_name,
        artist_genre = COALESCE(EXCLUDED.artist_genre, bronze.spotify_genres.artist_genre),
        last_updated_datetime_utc = EXCLUDED.last_updated_datetime_utc;
    """

    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Upserted {len(df)} genre records")

def load_saved_tracks_from_csv(csv_path="/opt/airflow/data/saved_songs.csv"):
    """Load saved track data into bronze.spotify_saved_songs."""
    df = pd.read_csv(csv_path)
    load_saved_tracks(df)

def load_saved_tracks(df):
    """Insert saved tracks into bronze.spotify_saved_songs (deduplicated)."""
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO bronze.spotify_saved_songs (
        added_at, track_id,track_name, artist_id, artist_name,
        album_id, album_name, album_art_link,
        duration_ms, popularity,last_updated_datetime_utc
    ) VALUES %s
    ON CONFLICT (track_id) DO NOTHING;
    """

    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(df)} saved song records")

def load_saved_genres_from_csv(csv_path="/opt/airflow/data/saved_genres.csv"):
    """Load saved artist genres data from CSV into Postgres."""
    df = pd.read_csv(csv_path)
    load_saved_genres(df)

def load_saved_genres(df):
    """Insert or update artist genres for saved songs."""
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO bronze.spotify_saved_genres (
        artist_id, artist_name, artist_genre, last_updated_datetime_utc
    ) VALUES %s
    ON CONFLICT (artist_id) DO UPDATE SET
        artist_name = EXCLUDED.artist_name,
        artist_genre = COALESCE(EXCLUDED.artist_genre, bronze.spotify_saved_genres.artist_genre),
        last_updated_datetime_utc = EXCLUDED.last_updated_datetime_utc;
    """

    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Upserted {len(df)} saved genre records")

def load_artist_images_from_csv(csv_path="/opt/airflow/data/artist_images.csv"):
    """Load artist image data from CSV into a new Postgres table."""
    df = pd.read_csv(csv_path)
    load_artist_images(df)

def load_artist_images(df):
    """Insert or update artist images into a new table."""
    conn = get_connection()
    cur = conn.cursor()

    insert_query = """
    INSERT INTO bronze.spotify_artist_images (
        artist_id, artist_image_link, last_updated_datetime_utc
    ) VALUES %s
    ON CONFLICT (artist_id) DO UPDATE SET
        artist_image_link = EXCLUDED.artist_image_link,
        last_updated_datetime_utc = EXCLUDED.last_updated_datetime_utc;
    """

    values = [tuple(x) for x in df.to_numpy()]
    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Upserted {len(df)} artist image records")

def load_top_song_previews_from_csv():
    """
    Loads top song audio previews from a CSV file into the gold.top_song_previews table.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        df = pd.read_csv("data/top_song_previews.csv")
        
        if df.empty:
            print("No new top song previews to load. Exiting.")
            return
            
        print(f"Loading {len(df)} top song previews into gold.spotify_top_song_previews.")
        
        # Select only the columns that exist in the database table
        df = df[['track_id', 'audio_preview_url']]

        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        
        query = "INSERT INTO gold.spotify_top_song_previews({}) VALUES %s ON CONFLICT (track_id) DO UPDATE SET audio_preview_url=EXCLUDED.audio_preview_url".format(cols)
        extras.execute_values(cur, query, tuples)
        
        conn.commit()
        print("✅ Successfully loaded top song previews.")
    except (Exception, psycopg2.Error) as error:
        print("❌ Failed to load top song previews:", error)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

def load_top_artist_previews_from_csv():
    """
    Loads top song audio previews for top N artists from a single CSV file 
    into the gold.spotify_top_artist_previews table.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        df = pd.read_csv("data/top_n_artist_song_audio_previews.csv")
        
        if df.empty:
            print("No top artist song previews to load. Exiting.")
            return
            
        print(f"Loading {len(df)} top artist song previews into gold.spotify_top_artist_previews.")
        
        df = df[['artist_rank', 'track_id', 'audio_preview_url']]

        # 1. Truncate the table first (as per the SQL script design)
        cur.execute("TRUNCATE TABLE gold.spotify_top_artist_previews;")
        
        # 2. Prepare data for bulk insert
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        
        # 3. Use execute_values for efficient loading
        # The ON CONFLICT clause is not strictly needed after TRUNCATE, 
        # but the TRUNCATE ensures we only have the latest run.
        query = "INSERT INTO gold.spotify_top_artist_previews({}) VALUES %s".format(cols)
        extras.execute_values(cur, query, tuples)
        
        conn.commit()
        print("✅ Successfully loaded top artist song previews.")
    except (Exception, psycopg2.Error) as error:
        print("❌ Failed to load top artist song previews:", error)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


