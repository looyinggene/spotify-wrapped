CREATE OR REPLACE VIEW gold.view_total_listening_by_day AS
SELECT 
    played_date_myt,
    COUNT(*) AS total_plays,
    ROUND(SUM(t.song_duration_sec) / 60.0, 2) AS total_minutes_listened
FROM gold.fact_listening_history f
LEFT JOIN gold.dim_tracks t ON f.track_id = t.track_id
GROUP BY played_date_myt
ORDER BY total_minutes_listened DESC;
