CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.spotify_saved_genres (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT NOT NULL,
    artist_genre TEXT,
    last_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
