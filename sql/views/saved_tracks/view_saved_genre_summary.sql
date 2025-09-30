CREATE OR REPLACE VIEW gold.view_saved_genre_summary AS
WITH genre_counts AS (
    SELECT 
        COALESCE(a.artist_genre, 'Unknown') AS genre,
        COUNT(*) AS song_count
    FROM gold.fact_saved_songs f
    LEFT JOIN gold.dim_saved_artists a
        ON f.artist_id = a.artist_id
    GROUP BY COALESCE(a.artist_genre, 'Unknown')
),
total AS (
    SELECT COUNT(DISTINCT COALESCE(artist_genre, 'Unknown')) AS total_genres
    FROM gold.dim_saved_artists
)
SELECT 
    g.genre,
    g.song_count,
    t.total_genres
FROM genre_counts g
CROSS JOIN total t
ORDER BY g.song_count DESC;
