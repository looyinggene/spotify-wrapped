CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_listening_history (
    track_id TEXT,
    artist_id TEXT,
    album_id TEXT,
    played_at_myt TIMESTAMP PRIMARY KEY,
    played_date_myt DATE,
    played_time_myt TEXT,
    dwh_create_date TIMESTAMP
);

TRUNCATE TABLE gold.fact_listening_history;

INSERT INTO gold.fact_listening_history (
    track_id, artist_id, album_id, played_at_myt,
    played_date_myt, played_time_myt, dwh_create_date
)
SELECT
    track_id, artist_id, album_id, played_at_myt,
    played_date_myt, played_time_myt, dwh_create_date
FROM silver.spotify_songs;