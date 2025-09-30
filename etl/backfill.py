import requests
import pandas as pd
from etl.refresh import get_access_token
from datetime import datetime
from etl.db import get_connection
from etl.extract import fetch_artist_genres
from etl.load import load_genres, load_artist_images

def backfill_missing_listening_genres():
    """Find missing artist_ids from listening history and load their genres."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT DISTINCT artist_id
        FROM bronze.spotify_songs
        WHERE artist_id IS NOT NULL
          AND artist_id NOT IN (SELECT artist_id FROM bronze.spotify_genres);
    """
    cur.execute(query)
    missing = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not missing:
        print("🎉 No missing artists from listening history to backfill.")
        return

    print(f"🔍 Found {len(missing)} missing artists from listening history.")
    df = fetch_artist_genres(missing)
    load_genres(df)
    print(f"✅ Backfilled {len(df)} genres from listening history.")


def backfill_missing_saved_genres():
    """Find missing artist_ids from saved songs and load their genres."""
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT DISTINCT artist_id
        FROM bronze.spotify_saved_songs
        WHERE artist_id IS NOT NULL
          AND artist_id NOT IN (SELECT artist_id FROM bronze.spotify_genres);
    """
    cur.execute(query)
    missing = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    if not missing:
        print("🎉 No missing artists from saved songs to backfill.")
        return

    print(f"🔍 Found {len(missing)} missing artists from saved songs.")
    df = fetch_artist_genres(missing)
    load_genres(df)
    print(f"✅ Backfilled {len(df)} genres from saved songs.")

def backfill_missing_artist_images():
    """
    Backfills missing artist images for artists that exist in the
    bronze.spotify_songs table but not in bronze.spotify_artist_images.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Query for all unique artist IDs that are missing an image link
        cur.execute("""
            SELECT DISTINCT t1.artist_id
            FROM bronze.spotify_songs t1
            LEFT JOIN bronze.spotify_artist_images t2
            ON t1.artist_id = t2.artist_id
            WHERE t2.artist_id IS NULL;
        """)
        
        missing_artists = [row[0] for row in cur.fetchall()]
        print(f"Found {len(missing_artists)} artists with missing image links.")
        
        if not missing_artists:
            print("No artists to backfill. Exiting.")
            return

        # Use the reusable function to fetch images for the missing artists
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        records = []

        for artist_id in missing_artists:
            try:
                url = f"https://api.spotify.com/v1/artists/{artist_id}"
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                image_url = data["images"][0]["url"] if data.get("images") else None
                if image_url:
                    records.append({
                        "artist_id": data.get("id"),
                        "artist_image_link": image_url,
                        "last_updated_datetime_utc": datetime.utcnow().isoformat()
                    })
            except requests.RequestException as e:
                print(f"Skipping artist {artist_id} due to API error: {e}")
            
        if records:
            df = pd.DataFrame(records)
            load_artist_images(df)
        else:
            print("No new artist image records to load.")

    except Exception as e:
        print(f"An error occurred during artist image backfill: {e}")
    finally:
        if conn:
            conn.close()

