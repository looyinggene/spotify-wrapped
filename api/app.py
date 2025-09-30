from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import os
import math

app = Flask(__name__)
CORS(app)
# --- Database Connection Configuration ---
# The host 'postgres' is the name of the service in docker-compose.yml
# This allows communication between the containers.
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/spotify"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

@app.route('/api/total-unique-songs')
def get_top_songs():
    """
    API endpoint to retrieve the top songs, with an optional limit parameter.
    """

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Query 'gold' layer view using the provided limit
        cur.execute(f"""
            SELECT COUNT(*)
            FROM gold.view_top_songs;
        """)
        total_unique_songs = cur.fetchone()[0]

        return jsonify({"total_unique_songs": total_unique_songs})
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve data"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/top-songs-with-previews')
def get_top_songs_with_previews():
    """
    API endpoint to retrieve top songs with their audio preview URLs.
    Can be limited to the top N songs using a 'limit' parameter.
    """
    limit = request.args.get('limit', 5) # Default to 5 if no limit is specified

    conn = None
    try:
        limit = int(limit)
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(f"""
            SELECT
                song_name,
                artist_name,
                play_count,
                audio_preview_url,
                album_art_link
            FROM gold.view_top_songs_with_previews
            ORDER BY play_count DESC
            LIMIT {limit};
        """)
        results = cur.fetchall()

        top_songs = []
        for row in results:
            top_songs.append({
                "song_name": row[0],
                "artist_name": row[1],
                "play_count": row[2],
                "audio_preview_url": row[3],
                "album_art_link": row[4]
            })

        return jsonify(top_songs)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Failed to retrieve data"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/top-artists')
def get_top_artists():
    """
    API endpoint to retrieve top artists, with an optional limit.
    """
    # Get the 'limit' parameter from the URL. Default to 5 if not provided.
    limit = request.args.get('limit', 5)

    try:
        # Validate that the limit is a positive integer
        limit = int(limit)
        if limit <= 0:
            return jsonify({"error": "Limit must be a positive integer"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid limit parameter"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Query the view with the limit
        cur.execute(f"""
            SELECT artist_name, total_minutes_listened,artist_image_link,audio_preview_url
            FROM gold.view_top_artists_with_previews
            ORDER BY total_minutes_listened DESC
            LIMIT {limit};
        """)
        results = cur.fetchall()

        top_artists = []
        for row in results:
            top_artists.append({
                "artist_name": row[0],
                "total_minutes_listened": row[1],
                "artist_image_link":row[2],
                "audio_preview_url":row[3]
            })

        return jsonify(top_artists)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve data"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/total-listening-by-day')
def get_total_listening_by_day():
    """
    API endpoint to retrieve total listening time by day.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # The view is already ordered, so the query is straightforward.
        cur.execute("SELECT played_date_myt, total_plays, total_minutes_listened FROM gold.view_total_listening_by_day;")
        results = cur.fetchall()

        total_listening_by_day = []
        for row in results:
            total_listening_by_day.append({
                "played_date": row[0].strftime("%Y-%m-%d"),
                "total_plays": row[1],
                "total_minutes_listened": float(row[2])
            })
        
        return jsonify(total_listening_by_day)
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve data"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/most-listened-day')
def get_most_listened_day():
    """
    API endpoint to retrieve the single day with the longest listening time.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Query the view and get only the top record
        cur.execute("""
            SELECT
                played_date_myt,
                total_minutes_listened
            FROM
                gold.view_total_listening_by_day
            ORDER BY
                total_minutes_listened DESC
            LIMIT 1;
        """)
        
        result = cur.fetchone() # Use fetchone() for a single record

        if result:
            top_day = {
                "day": result[0].strftime("%Y-%m-%d"),
                "total_minutes_listened": float(result[1])
            }
            return jsonify(top_day)
        else:
            return jsonify({"message": "No data found"}), 404
            
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve data"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/total-listened-minutes')
def get_total_listened_minutes():
    """
    API endpoint to retrieve the total minutes listened from the overall view.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT total_minutes_listened FROM gold.view_total_listening_overall;")
        result = cur.fetchone()

        if result:
            total_minutes = {
                "total_minutes_listened": math.ceil(result[0])
            }
            return jsonify(total_minutes)
        else:
            return jsonify({"message": "No data found"}), 404
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve total minutes listened"}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)