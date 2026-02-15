-- Falla si existe algún repo_id en FACT que no exista en DIM
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS ok
FROM dw.fact_commits f
LEFT JOIN dw.dim_repository d ON d.repo_id = f.repo_id
WHERE d.repo_id IS NULL;
