CREATE OR REPLACE VIEW gold.view_album_diversity_top_artists AS
SELECT 
	s.artist_id,
	a.artist_name,
	COUNT(s.track_id) as total_saved_songs,
	COUNT(DISTINCT s.album_id) as total_saved_albums
FROM gold.fact_saved_songs s
LEFT JOIN gold.dim_saved_artists a
ON s.artist_id = a.artist_id
GROUP BY s.artist_id, a.artist_name
ORDER BY total_saved_songs DESC;