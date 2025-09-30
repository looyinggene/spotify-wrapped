-- Drop table if it exists (optional, for development)
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.spotify_genres (
    artist_id                TEXT PRIMARY KEY,     -- Unique ID for artist
    artist_name              TEXT NOT NULL,        -- Artist name
    artist_genre             TEXT,                -- Primary genre
    last_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
