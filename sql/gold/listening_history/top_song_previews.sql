-- This table stores the audio preview URLs for top songs.
-- It is populated by a dedicated ETL task that runs after the gold views are created.
CREATE TABLE IF NOT EXISTS gold.spotify_top_song_previews (
    track_id TEXT PRIMARY KEY,
    audio_preview_url TEXT
);

-- Truncate the table to ensure we only have the latest top 2 songs.
TRUNCATE TABLE gold.spotify_top_song_previews;