CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_saved_artists (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT,
    artist_genre TEXT
);

TRUNCATE TABLE gold.dim_saved_artists;
INSERT INTO gold.dim_saved_artists 
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_genre
FROM silver.spotify_saved_genres
WHERE artist_id IS NOT NULL
ON CONFLICT (artist_id) DO NOTHING;