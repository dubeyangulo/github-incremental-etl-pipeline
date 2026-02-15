INSERT INTO dw.fact_commits (
  sha, repo_id, committed_at,
  author_login, author_name, author_email,
  commit_message
)
SELECT
  c.sha,
  r.repo_id,
  c.committed_at,
  c.author_login,
  c.author_name,
  c.author_email,
  c.commit_message
FROM stg.commits c
JOIN stg.repos r ON r.full_name = c.repo_full_name
ON CONFLICT (sha) DO UPDATE SET
  repo_id = EXCLUDED.repo_id,
  committed_at = EXCLUDED.committed_at,
  author_login = EXCLUDED.author_login,
  author_name = EXCLUDED.author_name,
  author_email = EXCLUDED.author_email,
  commit_message = EXCLUDED.commit_message,
  load_dts = NOW();
