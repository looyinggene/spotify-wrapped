import requests
import pandas as pd
import datetime as dt
from datetime import datetime
from dotenv import load_dotenv
import os
from etl.refresh import get_access_token
from etl.db import get_connection

load_dotenv("key.env")

def fetch_artist_images(artist_ids):
    """
    Fetch image URL for each artist ID from the Spotify API.
    Saves results to a CSV file.
    """
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    records = []

    for artist_id in set(artist_ids):
        if not artist_id:
            continue

        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        records.append({
            "artist_id": data.get("id"),
            "artist_image_link": data["images"][0]["url"] if data.get("images") else None,
            "last_updated_datetime_utc": dt.datetime.utcnow().isoformat()
        })
    
    df = pd.DataFrame(records)
    df.to_csv("data/artist_images.csv", index=False)
    print(f"✅ Saved {len(df)} artist images to data/artist_images.csv")
    return df

def fetch_recent_played(limit=50, after_timestamp=None):
    token = get_access_token()
    headers = {"Authorization":f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/me/player/recently-played?limit={limit}"

    if after_timestamp:
        url += f"&after={after_timestamp}"
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json().get("items", [])

    records = []
    artist_ids = set()
    for item in data:
        track = item["track"]
        artist_id = track["artists"][0]["id"]
        artist_ids.add(artist_id)
        records.append({
            "played_at_utc": item["played_at"],
            "played_date_utc": item["played_at"][:10],
            "song_name": track["name"],
            "artist_name": track["artists"][0]["name"],
            "song_duration_ms": track["duration_ms"],
            "song_link": track["external_urls"]["spotify"],
            "album_art_link": track["album"]["images"][1]["url"] if track["album"]["images"] else None,
            "album_name": track["album"]["name"],
            "album_id": track["album"]["id"],
            "artist_id": artist_id,
            "track_id": track["id"],
            "last_updated_datetime_utc": dt.datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(records)
    df.to_csv("data/recent_songs.csv", index=False)
    
    # After fetching songs, fetch artist images.
    artist_image_df = fetch_artist_images(list(artist_ids))
    return df, artist_image_df

def fetch_artist_genres(artist_ids):
    """
    Fetch primary genre for each artist ID in the list.
    """
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    records = []
    for artist_id in set(artist_ids):
        if not artist_id:
            continue

        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        records.append({
            "artist_id": data.get("id"),
            "artist_name": data.get("name"),
            "artist_genre": data["genres"][0] if data.get("genres") else None,
            "last_updated_datetime_utc": dt.datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(records)
    df.to_csv("data/recent_genres.csv", index=False)
    return df

def fetch_saved_tracks(limit=50):
    """
    Fetch ALL saved tracks (liked songs) from your Spotify account.
    """
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.spotify.com/v1/me/tracks"

    all_records = []
    artist_ids = set() 
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            track = item["track"]
            artist_id = track["artists"][0]["id"] if track["artists"] else None
            if artist_id:
                artist_ids.add(artist_id) 
            all_records.append({
                "added_at": item["added_at"],
                "track_id": track["id"],
                "track_name": track["name"],
                "artist_id": artist_id,
                "artist_name": track["artists"][0]["name"] if track["artists"] else None,
                "album_id": track["album"]["id"],
                "album_name": track["album"]["name"],
                "album_art_link": track["album"]["images"][0]["url"] if track["album"]["images"] else None,
                "duration_ms": track["duration_ms"],
                "popularity": track.get("popularity"),
                "last_updated_datetime_utc": dt.datetime.utcnow().isoformat()
            })

        offset += limit
        print(f"Fetched {len(all_records)} saved songs so far...")

    df = pd.DataFrame(all_records)
    df.to_csv("data/saved_songs.csv", index=False)
    print(f"✅ Saved {len(df)} tracks to data/saved_songs.csv")

    artist_image_df = fetch_artist_images(list(artist_ids))
    return df, artist_image_df

def fetch_saved_genres(artist_ids):
    """
    Fetch genres for artists in saved tracks.
    """
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    records = []
    for artist_id in set(artist_ids):
        if not artist_id:
            continue

        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        records.append({
            "artist_id": data.get("id"),
            "artist_name": data.get("name"),
            "artist_genre": data["genres"][0] if data.get("genres") else None,
            "last_updated_datetime_utc": dt.datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(records)
    df.to_csv("data/saved_genres.csv", index=False)
    print(f"✅ Saved {len(df)} artist genres to data/saved_genres.csv")
    return df

def fetch_top_song_audio_previews():
    """
    Fetches the 30-second audio preview URL for the top 5 most listened-to songs
    from the gold layer view by querying the Node.js microservice.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. We now fetch the unique track_id, song_name, and artist_name
        query = """
            SELECT track_id, song_name, artist_name
            FROM gold.view_top_songs
            ORDER BY play_count DESC
            LIMIT 5;
        """
        cur.execute(query)
        top_songs = cur.fetchall()
        
        if not top_songs:
            print("No top songs found to fetch audio for. Exiting.")
            return pd.DataFrame()

        print(f"Found {len(top_songs)} top songs. Fetching audio preview links from microservice.")
        
        # 2. Prepare the data for the POST request to the Node.js service
        # This is where we sanitize the data with .strip()
        songs_to_fetch = [
            {"songName": song_name.strip(), "artistName": artist_name.strip()}
            for _, song_name, artist_name in top_songs
        ]

        # 3. Call the Node.js service to get the preview URLs
        api_url = "http://nodejs-app:3000/fetch-previews"
        response = requests.post(api_url, json={"songs": songs_to_fetch})
        response.raise_for_status()
        
        results = response.json().get('results', [])
        
        # 4. Create a dictionary for quick lookup using the reliable trackId
        # The key has been changed from 'previewUrls' to the correct 'previewUrl'
        preview_map = {
            item.get('trackId'): item.get('previewUrl')
            for item in results
        }
        
        # 5. Create a DataFrame with the track_id and the found preview URL
        records = []
        for track_id, _, _ in top_songs:
            records.append({
                "track_id": track_id,
                "audio_preview_url": preview_map.get(track_id, None),
                "last_updated_datetime_utc": datetime.utcnow().isoformat()
            })
        
        df = pd.DataFrame(records)
        df.to_csv("data/top_song_previews.csv", index=False)
        print(f"✅ Saved {len(df)} top song audio links to data/top_song_previews.csv")
        return df

    except Exception as e:
        print(f"An error occurred during top song audio fetch: {e}")
    finally:
        if conn:
            conn.close()

def fetch_top_artist_song_audio_previews(num_top_artists=5):
    """
    Fetches the audio preview URL for the single top song for each of the 
    top 'N' artists and saves all results to a single CSV file.
    
    Args:
        num_top_artists (int): The number of top artists (N) to process (e.g., 2 for top 1 and top 2).
    """
    
    # Accumulator for all records across all artists
    all_artist_song_records = []
    
    # The song limit is fixed at 1 for the top song of each artist
    song_limit_per_artist = 1 
    
    for artist_rank in range(1, num_top_artists + 1):
        conn = None
        current_artist_records = []

        try:
            conn = get_connection()
            cur = conn.cursor()
            
            # 1. Identify the Top Artist at the current rank
            artist_query = f"""
                SELECT artist_name
                FROM gold.view_top_artists
                ORDER BY total_minutes_listened DESC
                LIMIT 1 OFFSET {artist_rank - 1};
            """
            cur.execute(artist_query)
            top_artist_result = cur.fetchone()
            
            if not top_artist_result:
                print(f"No artist found at rank {artist_rank}. Stopping loop.")
                break
                
            target_artist_name = top_artist_result[0]
            
            # 2. Find the single top song (LIMIT 1) by that specific artist
            song_query = f"""
                SELECT track_id, song_name, artist_name
                FROM gold.view_top_songs
                WHERE artist_name = %s
                ORDER BY play_count DESC
                LIMIT {song_limit_per_artist};
            """
            cur.execute(song_query, (target_artist_name,))
            top_songs_by_artist = cur.fetchall()
            
            if not top_songs_by_artist:
                print(f"No top song found for artist: {target_artist_name} (Rank {artist_rank}). Skipping.")
                continue

            print(f"Found top song for {target_artist_name} (Rank {artist_rank}). Fetching audio preview.")

            # 3. Prepare the data for the POST request
            songs_to_fetch = [
                {"songName": song_name.strip(), "artistName": artist_name.strip(), "trackId": track_id}
                for track_id, song_name, artist_name in top_songs_by_artist
            ]

            # 4. Call the Node.js service to get the preview URLs
            api_url = "http://nodejs-app:3000/fetch-previews"
            response = requests.post(api_url, json={"songs": songs_to_fetch})
            response.raise_for_status()
            
            results = response.json().get('results', [])
            
            # 5. Create a dictionary for quick lookup
            preview_map = {
                item.get('trackId'): item.get('previewUrl')
                for item in results
            }
            
            # 6. Accumulate records
            for track_id, song_name, artist_name in top_songs_by_artist:
                current_artist_records.append({
                    "artist_rank": artist_rank, # NEW: Include rank for easy identification
                    "track_id": track_id,
                    "song_name": song_name,
                    "artist_name": artist_name,
                    "audio_preview_url": preview_map.get(track_id, None),
                    "last_updated_datetime_utc": datetime.utcnow().isoformat()
                })
            
            # Add the current artist's song(s) to the master list
            all_artist_song_records.extend(current_artist_records)

        except Exception as e:
            print(f"An error occurred while processing Rank {artist_rank}: {e}")
        finally:
            if conn:
                conn.close()

    # 7. Final Step: Write ALL accumulated records to a single CSV file
    if all_artist_song_records:
        df = pd.DataFrame(all_artist_song_records)
        filename = f"data/top_n_artist_song_audio_previews.csv"
        df.to_csv(filename, index=False)
        print(f"✅ Saved audio links for {len(all_artist_song_records)} top artist songs to {filename}")
        return df
    else:
        print("No top artist songs found to save.")
        return pd.DataFrame()

