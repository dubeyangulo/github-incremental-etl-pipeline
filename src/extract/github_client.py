from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple
import requests


class GitHubClient:
    def __init__(self, token: Optional[str] = None, user_agent: str = "etl-github-pipeline"):
        self.base_url = "https://api.github.com"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _request(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        resp = self.session.get(url, params=params, timeout=30)

        # Manejo simple de rate limit (si toca)
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                sleep_s = max(1, int(reset) - int(time.time()) + 2)
                time.sleep(sleep_s)
                resp = self.session.get(url, params=params, timeout=30)

        resp.raise_for_status()
        return resp

    def list_org_repos(self, org: str, per_page: int = 100, max_pages: int = 10) -> List[Dict[str, Any]]:
        """
        Trae repos públicos de una org. Pagina por `page`.
        max_pages limita para evitar traer miles (puedes subirlo luego).
        """
        repos: List[Dict[str, Any]] = []
        url = f"{self.base_url}/orgs/{org}/repos"

        for page in range(1, max_pages + 1):
            resp = self._request(url, params={"per_page": per_page, "page": page, "type": "public", "sort": "pushed"})
            batch = resp.json()
            if not batch:
                break
            repos.extend(batch)

            # Si viene menos del per_page, ya no hay más
            if len(batch) < per_page:
                break

        return repos
