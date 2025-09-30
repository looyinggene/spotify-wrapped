CREATE OR REPLACE VIEW gold.view_top_songs AS
SELECT 
    t.track_id,
    t.song_name,
    a.artist_name,
    t.album_id,
    b.album_art_link,
    COUNT(*) AS play_count,
    ROUND(SUM(t.song_duration_sec) / 60, 2) AS total_minutes_listened,
    a.artist_image_link
FROM gold.fact_listening_history f
LEFT JOIN gold.dim_tracks t ON f.track_id = t.track_id
LEFT JOIN gold.dim_artists a ON t.artist_id = a.artist_id
LEFT JOIN gold.dim_albums b ON t.album_id = b.album_id
GROUP BY t.track_id,t.song_name, a.artist_name, t.album_id, b.album_art_link,a.artist_image_link
ORDER BY play_count DESC;
