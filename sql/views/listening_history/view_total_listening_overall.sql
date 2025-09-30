CREATE OR REPLACE VIEW gold.view_total_listening_overall AS
SELECT 
    ROUND(SUM(t.song_duration_sec) / 60.0, 2) AS total_minutes_listened
FROM gold.fact_listening_history f
JOIN gold.dim_tracks t ON f.track_id = t.track_id;

