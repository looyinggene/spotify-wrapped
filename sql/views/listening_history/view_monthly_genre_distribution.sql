CREATE OR REPLACE VIEW gold.view_monthly_genre_distribution AS
SELECT
    DATE_TRUNC('month', f.played_at_myt)::DATE AS month,
    a.artist_genre,
    COUNT(*) AS plays,
    ROUND(SUM(t.song_duration_sec) / 60.0,2) AS total_minutes_listened
FROM gold.fact_listening_history f
LEFT JOIN gold.dim_tracks t ON f.track_id = t.track_id
LEFT JOIN gold.dim_artists a ON f.artist_id = a.artist_id
WHERE a.artist_genre IS NOT NULL
GROUP BY month, a.artist_genre
ORDER BY month, total_minutes_listened DESC;