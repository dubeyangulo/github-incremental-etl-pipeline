-- Upsert de dim_repository desde stg.repos
INSERT INTO dw.dim_repository (
  repo_id, full_name, name, owner_login, is_private, html_url, description,
  language, default_branch, created_at, updated_at, pushed_at,
  stargazers_count, forks_count, open_issues_count
)
SELECT
  r.repo_id, r.full_name, r.name, r.owner_login, r.is_private, r.html_url, r.description,
  r.language, r.default_branch, r.created_at, r.updated_at, r.pushed_at,
  r.stargazers_count, r.forks_count, r.open_issues_count
FROM stg.repos r
ON CONFLICT (repo_id) DO UPDATE SET
  full_name = EXCLUDED.full_name,
  name = EXCLUDED.name,
  owner_login = EXCLUDED.owner_login,
  is_private = EXCLUDED.is_private,
  html_url = EXCLUDED.html_url,
  description = EXCLUDED.description,
  language = EXCLUDED.language,
  default_branch = EXCLUDED.default_branch,
  created_at = EXCLUDED.created_at,
  updated_at = EXCLUDED.updated_at,
  pushed_at = EXCLUDED.pushed_at,
  stargazers_count = EXCLUDED.stargazers_count,
  forks_count = EXCLUDED.forks_count,
  open_issues_count = EXCLUDED.open_issues_count;

-- Upsert de fact_commits desde stg.commits
INSERT INTO dw.fact_commits (
  sha, repo_id, committed_at, author_login, author_name, author_email,
  commit_message, additions, deletions, changed_files
)
SELECT
  c.sha,
  COALESCE(c.repo_id, dr.repo_id) AS repo_id,
  c.committed_at,
  c.author_login,
  c.author_name,
  c.author_email,
  c.commit_message,
  c.additions,
  c.deletions,
  c.changed_files
FROM stg.commits c
LEFT JOIN stg.repos r ON r.full_name = c.repo_full_name
LEFT JOIN dw.dim_repository dr ON dr.repo_id = r.repo_id
ON CONFLICT (sha) DO UPDATE SET
  repo_id = EXCLUDED.repo_id,
  committed_at = EXCLUDED.committed_at,
  author_login = EXCLUDED.author_login,
  author_name = EXCLUDED.author_name,
  author_email = EXCLUDED.author_email,
  commit_message = EXCLUDED.commit_message,
  additions = EXCLUDED.additions,
  deletions = EXCLUDED.deletions,
  changed_files = EXCLUDED.changed_files,
  load_dts = NOW();
