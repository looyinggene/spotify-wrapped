DROP TABLE IF EXISTS silver.spotify_artist_images;

CREATE TABLE silver.spotify_artist_images (
    artist_id TEXT PRIMARY KEY,  -- Unique per artist
    artist_image_link TEXT,
    dwh_create_date TIMESTAMP
); 

INSERT INTO silver.spotify_artist_images (
    artist_id, artist_image_link, dwh_create_date
)
SELECT
    TRIM(artist_id),
    TRIM(artist_image_link),
    (last_updated_datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur')
FROM bronze.spotify_artist_images
ON CONFLICT (artist_id) DO NOTHING;