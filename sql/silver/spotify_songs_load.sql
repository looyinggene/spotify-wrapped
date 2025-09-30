CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.spotify_songs;

CREATE TABLE silver.spotify_songs (
    played_at_myt TIMESTAMP PRIMARY KEY, 
    played_date_myt DATE,
    played_time_myt TEXT,
    track_id TEXT,
    song_name TEXT,
    song_duration_sec NUMERIC,
    song_link TEXT,
    artist_id TEXT,
    artist_name TEXT,
    album_id TEXT,
    album_name TEXT,
    album_art_link TEXT,
    dwh_create_date TIMESTAMP
);

INSERT INTO silver.spotify_songs (
    played_at_myt, played_date_myt, played_time_myt, track_id,
    song_name, song_duration_sec, song_link, artist_id,
    artist_name, album_id, album_name, album_art_link, dwh_create_date
)
SELECT
    (played_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur'),
    DATE(played_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur'),
    TO_CHAR(played_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur', 'HH24:MI:SS'),
    TRIM(track_id),
    TRIM(song_name),
    ROUND(song_duration_ms / 1000.0, 2),
    song_link,
    TRIM(artist_id),
    TRIM(artist_name),
    TRIM(album_id),
    TRIM(album_name),
    album_art_link,
    (last_updated_datetime_utc AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Kuala_Lumpur')
FROM bronze.spotify_songs
ORDER BY played_at_utc ASC
ON CONFLICT (played_at_myt) DO NOTHING;


