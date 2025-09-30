CREATE OR REPLACE VIEW gold.view_top_artists_with_previews AS
WITH RankedArtists AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_minutes_listened DESC) AS artist_rank
    FROM gold.view_top_artists
    LIMIT 5
)
SELECT
    r.*,
    p.audio_preview_url
FROM RankedArtists r
LEFT JOIN gold.spotify_top_artist_previews p
    ON r.artist_rank = p.artist_rank;