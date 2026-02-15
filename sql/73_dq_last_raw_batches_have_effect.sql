-- Falla si en los últimos 30 minutos hubo batches en raw.github_commits con payload > 0
-- pero NO se refleja nada en stg.commits (mala señal)
WITH recent_raw AS (
  SELECT COUNT(*) AS raw_batches
  FROM raw.github_commits
  WHERE ingested_at >= NOW() - INTERVAL '30 minutes'
    AND jsonb_array_length(payload) > 0
),
recent_stg AS (
  SELECT COUNT(*) AS stg_rows
  FROM stg.commits
  WHERE raw_ingested_at >= NOW() - INTERVAL '30 minutes'
)
SELECT CASE
  WHEN (SELECT raw_batches FROM recent_raw) = 0 THEN 1          -- no hubo commits nuevos: OK
  WHEN (SELECT stg_rows FROM recent_stg) > 0 THEN 1             -- sí se reflejó en STG: OK
  ELSE 0
END AS ok;
