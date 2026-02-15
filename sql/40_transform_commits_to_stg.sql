WITH exploded AS (
  SELECT
    (c->>'sha') AS sha,
    r.repo_full_name,
    (c->'commit'->'author'->>'name') AS author_name,
    (c->'commit'->'author'->>'email') AS author_email,
    (c->'author'->>'login') AS author_login,
    (c->'commit'->>'message') AS commit_message,
    (c->'commit'->'author'->>'date')::timestamptz AS committed_at,
    r.ingested_at AS raw_ingested_at
  FROM raw.github_commits r
  CROSS JOIN LATERAL jsonb_array_elements(r.payload) c
  WHERE (c->>'sha') IS NOT NULL
),
dedup AS (
  -- Si un sha aparece en varios batches, nos quedamos con el más reciente
  SELECT DISTINCT ON (sha)
    sha, repo_full_name, author_name, author_email, author_login,
    commit_message, committed_at, raw_ingested_at
  FROM exploded
  ORDER BY sha, raw_ingested_at DESC
)
INSERT INTO stg.commits (
  sha, repo_full_name, author_name, author_email, author_login,
  commit_message, committed_at, raw_ingested_at, last_seen_at
)
SELECT
  sha, repo_full_name, author_name, author_email, author_login,
  commit_message, committed_at, raw_ingested_at, NOW()
FROM dedup
ON CONFLICT (sha) DO UPDATE SET
  repo_full_name = EXCLUDED.repo_full_name,
  author_name = EXCLUDED.author_name,
  author_email = EXCLUDED.author_email,
  author_login = EXCLUDED.author_login,
  commit_message = EXCLUDED.commit_message,
  committed_at = EXCLUDED.committed_at,
  raw_ingested_at = EXCLUDED.raw_ingested_at,
  last_seen_at = NOW();
