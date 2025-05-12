"""
Command-line utility for local smoke testing of the news curation pipeline.
It fetches articles from NewsAPI, optionally filters them for relevance using GPT,
and summarizes them into markdown content ideas. Results are saved locally to a file.

Usage:
    python -m adapters.cli [--days N] [--no-filter] [--outfile path]
    
For example, to fetch articles from the past 3 days, filter them using GPT, generate content with GPT and save the result with the default name: 

python -m adapters.cli --days 3   
"""

from __future__ import annotations
import argparse, logging
from pathlib import Path
from openai import OpenAI

from news_pipeline.settings   import Settings
from news_pipeline.fetcher    import NewsFetcher
from news_pipeline.filterer   import GptRelevanceFilter
from news_pipeline.summarizer import GptSummarizer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("cli")

def _args():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=None)
    p.add_argument("--no-filter", action="store_true")
    p.add_argument("--outfile", type=Path, default=Path("ideas_output.md"))
    return p.parse_args()

def main() -> None:
    args, cfg = _args(), Settings()
    client    = OpenAI(api_key=cfg.openai_key)

    # 1 fetch
    arts = NewsFetcher(cfg.news_api_key, cfg.topics, cfg.page_size)\
           .fetch_last_days(args.days or cfg.days_back)

    # 2 filter (optional)
    if not args.no_filter:
        arts = GptRelevanceFilter(client, bucket_name=None).filter(arts)

    # 3 summarize
    ideas = GptSummarizer(client, bucket_name=None).summarize(arts)

    args.outfile.write_text(ideas, encoding="utf‑8")
    print(f"\nSaved ideas to {args.outfile.resolve()}\n")
    print(ideas[:1500])

if __name__ == "__main__":
    main()
