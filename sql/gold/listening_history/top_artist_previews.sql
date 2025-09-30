-- This table stores the audio preview URLs for the top song of the top N artists.
-- It is populated by the dedicated ETL task that reads the combined CSV output.
CREATE TABLE IF NOT EXISTS gold.spotify_top_artist_previews (
    -- artist_rank is the rank of the artist (1, 2, 3, etc.)
    artist_rank INTEGER PRIMARY KEY, 
    artist_name TEXT,
    -- track_id is the unique identifier for the song 
    track_id TEXT UNIQUE NOT NULL, 
    audio_preview_url TEXT
);

-- Truncate the table before loading new data to ensure we only have the latest 
-- top N artist songs (e.g., top 1 and top 2 artists).
TRUNCATE TABLE gold.spotify_top_artist_previews;