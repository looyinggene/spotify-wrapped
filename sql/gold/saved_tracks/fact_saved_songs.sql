CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_saved_songs (
    track_id TEXT,
    artist_id TEXT,
    album_id TEXT,
    added_at_myt TIMESTAMP PRIMARY KEY,
    added_date_myt DATE,
    added_time_myt TEXT,
    dwh_create_date TIMESTAMP
);

TRUNCATE TABLE gold.fact_saved_songs;
INSERT INTO gold.fact_saved_songs (
    track_id, artist_id, album_id, added_at_myt,
    added_date_myt, added_time_myt, dwh_create_date
)
SELECT
    track_id, artist_id, album_id, added_at_myt,
    added_date_myt, added_time_myt, dwh_create_date
FROM silver.spotify_saved_songs
ON CONFLICT (added_at_myt) DO NOTHING;