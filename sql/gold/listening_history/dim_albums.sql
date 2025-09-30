CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_albums (
    album_id TEXT PRIMARY KEY,
    album_name TEXT,
    album_art_link TEXT
);

TRUNCATE TABLE gold.dim_albums;

INSERT INTO gold.dim_albums 
SELECT DISTINCT
    album_id,
    album_name,
    album_art_link
FROM silver.spotify_songs
WHERE album_id IS NOT NULL
ON CONFLICT (album_id) DO NOTHING;
