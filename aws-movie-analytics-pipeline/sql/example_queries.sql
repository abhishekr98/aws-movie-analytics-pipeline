-- Example Athena Queries

-- Get top 10 actors by frequency in cast list
SELECT name, COUNT(*) as appearances
FROM movie_analysis.cast_data
GROUP BY name
ORDER BY appearances DESC
LIMIT 10;

-- Find all movies where a given actor appeared
SELECT id, name, character
FROM movie_analysis.cast_data
WHERE name = 'Tom Hanks';
