-- Carga/actualiza la dimensión de repositorios desde stg.repos
-- (SCD tipo 1: sobrescribe valores)

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
