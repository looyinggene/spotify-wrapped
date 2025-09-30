DROP TABLE IF EXISTS silver.spotify_genres;

CREATE TABLE silver.spotify_genres (
    artist_id TEXT PRIMARY KEY,  -- Unique per artist
    artist_name TEXT,
    artist_genre TEXT,
    dwh_create_date TIMESTAMP
); 

INSERT INTO silver.spotify_genres (
    artist_id, artist_name, artist_genre, dwh_create_date
)
SELECT
    TRIM(artist_id),
    TRIM(artist_name),
    COALESCE(
    NULLIF(NULLIF(TRIM(artist_genre), 'NaN'), ''), 
    'Unknown') AS artist_genre,
    (last_updated_datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur')
FROM bronze.spotify_genres
ON CONFLICT (artist_id) DO NOTHING;
