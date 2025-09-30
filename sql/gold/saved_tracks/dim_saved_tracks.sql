CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_saved_tracks (
    track_id TEXT PRIMARY KEY,
    song_name TEXT,
    artist_id TEXT,
    album_id TEXT,
    song_duration_sec NUMERIC
);

TRUNCATE TABLE gold.dim_saved_tracks;
INSERT INTO gold.dim_saved_tracks
SELECT DISTINCT
    track_id,
    song_name,
    artist_id,
    album_id,
    song_duration_sec
FROM silver.spotify_saved_songs
WHERE track_id IS NOT NULL
ON CONFLICT (track_id) DO NOTHING;