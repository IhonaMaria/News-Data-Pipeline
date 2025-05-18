"""
This module defines the NewsFetcher class, which retrieves recent news articles
from the NewsAPI based on specified topics. It fetches articles concurrently
using ThreadPoolExecutor, filters for uniqueness by URL, and tags each article
with its corresponding topic. It is the ingestion step in a modular
content curation pipeline.
"""

from __future__ import annotations
import logging, requests
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

log, MAX_WORKERS = logging.getLogger(__name__), 6

class NewsFetcher:
    BASE_URL = "https://newsapi.org/v2/everything"

    def __init__(self, api_key: str, topics: list[str],
                 page_size: int = 30, session: requests.Session | None = None):
        self.api_key, self.topics, self.page_size = api_key, topics, page_size
        self.session = session or requests.Session()

    def _fetch_topic(self, topic: str, s_iso: str, e_iso: str) -> List[Dict[str, Any]]:
        params = {"q": topic, "from": s_iso, "to": e_iso, "language": "en",
                  "sortBy": "popularity", "pageSize": self.page_size,
                  "apiKey": self.api_key}
        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("NewsAPI error for topic %s → %s", topic, exc)
            return []
        arts = resp.json().get("articles", [])
        for art in arts: art["topic"] = topic
        return arts

    def fetch_last_days(self, days_back: int = 7) -> List[Dict]:
        today, start = date.today(), (date.today() - timedelta(days=days_back))
        seen, merged = set(), []
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(self.topics))) as p:
            futs = [p.submit(self._fetch_topic, t, start.isoformat(), today.isoformat())
                    for t in self.topics]
            for fut in as_completed(futs):
                for art in fut.result():
                    if (url := art.get("url")) and url not in seen:
                        seen.add(url); merged.append({
                            "topic": art["topic"], "title": art.get("title", ""),
                            "description": art.get("description", ""), "url": url,
                            "publishedAt": art.get("publishedAt", ""),
                        })
        log.info("Fetched %s unique articles", len(merged))
        return merged