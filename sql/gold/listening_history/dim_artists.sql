CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_artists (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT,
    artist_genre TEXT,
    artist_image_link TEXT
);

TRUNCATE TABLE gold.dim_artists;

INSERT INTO gold.dim_artists 
SELECT DISTINCT
    t1.artist_id,
    t1.artist_name,
    t1.artist_genre,
    t2.artist_image_link
FROM silver.spotify_genres AS t1
LEFT JOIN silver.spotify_artist_images AS t2
    ON t1.artist_id = t2.artist_id
WHERE t1.artist_id IS NOT NULL
ON CONFLICT (artist_id) DO NOTHING;