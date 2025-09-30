CREATE OR REPLACE VIEW gold.view_top_artists AS
SELECT
  a.artist_id,
  a.artist_name,
  ROUND(SUM(t.song_duration_sec) / 60.0,2) AS total_minutes_listened,
  a.artist_image_link
FROM gold.fact_listening_history f
JOIN gold.dim_tracks t ON f.track_id = t.track_id
JOIN gold.dim_artists a ON f.artist_id = a.artist_id
GROUP BY a.artist_id, a.artist_name, a.artist_image_link
ORDER BY total_minutes_listened DESC;
