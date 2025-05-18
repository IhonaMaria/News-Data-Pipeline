"""
Generates LinkedIn-style content ideas from filtered articles using the OpenAI API.
Groups articles by topic and prompts GPT to return markdown-formatted post suggestions.
Can run locally or save results to GCS in production.
"""

from __future__ import annotations
import logging, json, os, textwrap
from typing import Any, Dict, List
from openai import OpenAI
try:
    from google.cloud import storage
except ImportError:
    storage = None

log = logging.getLogger(__name__)

HEAD = textwrap.dedent("""
You are a LinkedIn content strategist.
Persona: data‑scientist pivoting into data‑engineering / AI.

For EACH topic, craft THREE post ideas:
 • start with a spicy HOOK
 • add 1‑2 TAKEAWAYS
 • end with a CTA
Write as Markdown bullet points.
Here are the articles:
""")

class GptSummarizer:
    def __init__(self, client: OpenAI, bucket_name: str | None = None,
                 filtered_prefix="filtered/", ideas_prefix="ideas/",
                 model="gpt-4o-mini"):
        self.client, self.model = client, model
        self.bucket = (storage.Client().bucket(bucket_name)
                       if bucket_name and storage else None)
        self.filtered_prefix, self.ideas_prefix = filtered_prefix, ideas_prefix

    def summarize(self, arts: List[Dict[str, Any]]) -> str:
        prompt = self._build_prompt(arts)
        resp = self.client.chat.completions.create(
            model=self.model, messages=[{"role":"user","content":prompt}], temperature=0.7
        )
        return resp.choices[0].message.content.strip()

    def run(self) -> str:                   
        if not self.bucket:
            raise RuntimeError("run() needs a bucket; use summarize() locally.")
        flt = json.loads(self._latest_blob(self.filtered_prefix).download_as_text())
        ideas = self.summarize(flt)
        ts = os.path.basename(self._latest_blob(self.filtered_prefix).name)\
        .replace("filtered_", "").replace(".json", "")

        self.bucket.blob(f"{self.ideas_prefix}ideas_{ts}.md")\
            .upload_from_string(ideas, content_type="text/markdown")
        log.info("Uploaded ideas md to bucket"); return ideas

    def _latest_blob(self, prefix):
        blobs = sorted(self.bucket.list_blobs(prefix=prefix),
                       key=lambda b: b.time_created, reverse=True)
        return blobs[0] if blobs else None

    def _build_prompt(self, arts):
        by_topic: Dict[str, List] = {}
        for a in arts: by_topic.setdefault(a["topic"], []).append(a)
        parts = [HEAD]
        for t, items in by_topic.items():
            parts.append(f"\n## {t}\n")
            parts += [f"- {a['title']}: {a['description']}" for a in items]
        return "\n".join(parts)
