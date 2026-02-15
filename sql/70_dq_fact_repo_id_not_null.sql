-- Falla si hay commits en FACT sin repo_id
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS ok
FROM dw.fact_commits
WHERE repo_id IS NULL;
