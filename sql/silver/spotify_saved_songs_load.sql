CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.spotify_saved_songs;

CREATE TABLE silver.spotify_saved_songs (
    added_at_myt TIMESTAMP PRIMARY KEY,
    added_date_myt DATE,
    added_time_myt TEXT,
    track_id TEXT,
    song_name TEXT,
    song_duration_sec NUMERIC,
    artist_id TEXT,
    artist_name TEXT,
    popularity NUMERIC,
    album_id TEXT,
    album_name TEXT,
    album_art_link TEXT,
    dwh_create_date TIMESTAMP
);

INSERT INTO silver.spotify_saved_songs (
    added_at_myt, added_date_myt, added_time_myt, track_id,
    song_name, song_duration_sec, artist_id,
    artist_name, popularity, album_id, album_name, album_art_link, dwh_create_date
)
SELECT
    (added_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur'),
    DATE(added_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur'),
    TO_CHAR(added_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur', 'HH24:MI:SS'),
    TRIM(track_id),
    TRIM(track_name),
    ROUND(duration_ms / 1000.0, 2),
    TRIM(artist_id),
    TRIM(artist_name),
    popularity,
    TRIM(album_id),
    TRIM(album_name),
    album_art_link,
    (last_updated_datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur')
FROM bronze.spotify_saved_tracks
ORDER BY added_at ASC  -- ORDER BY clause must be here
ON CONFLICT (added_at_myt) DO NOTHING;