"""
Implements the core logic of the weekly news data pipeline for deployment on GCP as a Gen-2 Cloud Function.
It fetches recent articles by topic, filters them using GPT for relevance, summarizes them into content ideas,
and stores each stage (raw, filtered, summarized) in a GCS bucket. The `weekly_pipeline` function serves as the
HTTP entry point for triggering the pipeline.
"""

import json, datetime, logging, functions_framework
from google.cloud import storage
from openai import OpenAI

from news_pipeline.settings   import Settings
from news_pipeline.fetcher    import NewsFetcher
from news_pipeline.filterer   import GptRelevanceFilter
from news_pipeline.summarizer import GptSummarizer

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("weekly_pipeline")

# Cold-start singletons
cfg       = Settings()                         # env-vars: NEWS_API_KEY, OPENAI_API_KEY, BUCKET_NAME
client    = OpenAI(api_key=cfg.openai_key)
gcs_bucket = storage.Client().bucket(cfg.bucket_name)

fetcher   = NewsFetcher(cfg.news_api_key, cfg.topics, cfg.page_size)
filterer  = GptRelevanceFilter(client, bucket_name=cfg.bucket_name, threshold=0.0) # Modify as needed
summarizr = GptSummarizer(client, bucket_name=cfg.bucket_name)

def _run_pipeline():
    # 1) fetch raw articles
    raw = fetcher.fetch_last_days(cfg.days_back)
    ts  = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    gcs_bucket.blob(f"raw/raw_{ts}.json").upload_from_string(
        json.dumps(raw, ensure_ascii=False),
        content_type="application/json"
    )

    # 2) filter relevance
    filtered = filterer.run()  # downloads raw, scores + uploads filtered_<ts>.json

    # 3) generate content ideas
    ideas_md = summarizr.run()  # downloads filtered, uploads ideas_<ts>.md

    log.info("Pipeline complete: raw=%d, filtered=%d", len(raw), len(filtered))

@functions_framework.http
def weekly_pipeline(request):
    """Cloud Function Gen-2 HTTP entry point."""
    try:
        _run_pipeline()
        return ("OK", 200)
    except Exception as e:
        log.exception("Pipeline error")
        return (str(e), 500)
