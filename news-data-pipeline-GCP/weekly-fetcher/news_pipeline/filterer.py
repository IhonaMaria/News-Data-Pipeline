"""
Responsible for evaluating the relevance of the fetched articles using OpenAI API.
Each article is scored between 0 and 1 based on a domain-specific system prompt, and only
those meeting the threshold are retained. In production, it reads raw articles from a GCS
bucket and writes filtered results back; in local mode, it filters in-memory.
"""  

from __future__ import annotations
import json, logging, itertools, os
from datetime import datetime
from typing import Any, Dict, List
from openai import OpenAI
try:
    from google.cloud import storage             # used only if bucket supplied
except ImportError:
    storage = None

log, BATCH = logging.getLogger(__name__), 20


SYSTEM_MSG = """
You are a professional content curator for LinkedIn.
Audience: data‑scientists, data‑engineers / AI builders, engineers.
An article is HIGHLY relevant if it covers:\n"
    • real‑world data pipelines, lakehouse, MLOps, orchestration\n"
    • deploying or productizing GenAI or LLMs\n"
    • career advice or mindset shifts between DS ↔ DE ↔ AI\n"
    NOT relevant: generic business news, advertisement, consumer gadgets, crypto, politics.\n"

TASK
────
For each article you receive, output an object with:
  • "url"   – exactly the same URL you got
  • "score" – a floating‑point relevance from 0 (low) to 1 (high)

ONLY respond with JSON in this form:
{
  "items": [
    {"url": "...", "score": 0.83},
    …
  ]
}
""".strip()


class GptRelevanceFilter:
    def __init__(self, client: OpenAI, bucket_name: str | None = None,
                 raw_prefix="raw/", filtered_prefix="filtered/",
                 model="gpt-4o-mini", threshold=0.0): # Default threshold
        self.client, self.model, self.th = client, model, threshold
        self.raw_prefix, self.filtered_prefix = raw_prefix, filtered_prefix
        self.bucket = (storage.Client().bucket(bucket_name)
                       if bucket_name and storage else None)

    # — PUBLIC —
    def filter(self, arts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [a | {"score": s}
                for a, s in self._score_articles(arts) if s >= self.th]

    # — RUN end‑to‑end (download + upload) —
    def run(self) -> List[Dict[str, Any]]:
        if not self.bucket:               # LOCAL MODE
            raise RuntimeError("run() needs a bucket; use filter() locally.")
        raw = json.loads(self._latest_blob(self.raw_prefix).download_as_text())
        kept = self.filter(raw)
        ts_run = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        blob = self.bucket.blob(f"{self.filtered_prefix}filtered_{ts_run}.json")
        blob.upload_from_string(json.dumps(kept, ensure_ascii=False),
                                content_type="application/json")
        log.info("Wrote %s kept articles to gs://%s/%s",
                 len(kept), self.bucket.name, blob.name)
        return kept

    # — helpers —
    def _score_articles(self, arts: List[Dict[str, Any]]):
        """Return list of (article_dict, score)."""
        results = []
        for batch in _batched(arts, BATCH):
            prompt = "\n\n".join(
                f"- TITLE: {a['title']}\nDESC: {a['description']}" for a in batch
            )
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": SYSTEM_MSG},
                        {"role": "user",   "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )

            array = json.loads(resp.choices[0].message.content).get("items", [])
            url2s = {d.get("url"): d.get("score", 0) for d in array}

            for art in batch:
                results.append((art, url2s.get(art["url"], 0)))
        return results


    def _latest_blob(self, prefix):        # prod‑only helper
        blobs = sorted(self.bucket.list_blobs(prefix=prefix),
                       key=lambda b: b.time_created, reverse=True)
        return blobs[0] if blobs else None

def _batched(it, n):
    it = iter(it)
    while batch := list(itertools.islice(it, n)):
        yield batch
