CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.spotify_saved_songs;

CREATE TABLE IF NOT EXISTS bronze.spotify_saved_songs (
    added_at TIMESTAMP,
    track_id TEXT PRIMARY KEY,
    track_name TEXT NOT NULL,
    artist_id TEXT,
    artist_name TEXT NOT NULL,
    album_id TEXT,
    album_name TEXT,
    album_art_link TEXT,
    duration_ms INT,
    popularity INT,
    last_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
