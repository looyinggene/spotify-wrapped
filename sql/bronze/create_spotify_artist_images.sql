CREATE TABLE IF NOT EXISTS bronze.spotify_artist_images (
    artist_id TEXT PRIMARY KEY,
    artist_image_link TEXT,
    last_updated_datetime_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);