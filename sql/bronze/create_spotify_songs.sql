CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.spotify_songs (
    played_at_utc TIMESTAMP PRIMARY KEY,
    played_date_utc DATE NOT NULL,
    song_name TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    song_duration_ms INT,
    song_link TEXT,
    album_art_link TEXT,
    album_name TEXT,
    album_id TEXT,
    artist_id TEXT,
    track_id TEXT,
    last_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);