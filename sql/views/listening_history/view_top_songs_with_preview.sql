CREATE OR REPLACE VIEW gold.view_top_songs_with_previews AS
SELECT
    t.*,
    p.audio_preview_url
FROM gold.view_top_songs t
INNER JOIN gold.spotify_top_song_previews p
    ON t.track_id = p.track_id;