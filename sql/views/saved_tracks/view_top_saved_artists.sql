CREATE OR REPLACE VIEW gold.view_top_saved_artists AS
SELECT 
	s.artist_id,
	a.artist_name,
	COUNT(s.track_id) as total_saved_songs
FROM gold.fact_saved_songs s
LEFT JOIN gold.dim_saved_artists a
ON s.artist_id = a.artist_id
GROUP BY s.artist_id,a.artist_name
ORDER BY total_saved_songs DESC;