"""
Loads variables from a local .env file if available (for local dev).
Defaults for optional parameters (e.g., topics, page size).
"""

from __future__ import annotations
from dataclasses import dataclass, field
import os, sys

try:                      # local .env convenience
    from dotenv import load_dotenv; load_dotenv()
except ModuleNotFoundError:
    pass

@dataclass
class Settings:         
    news_api_key: str = field(default_factory=lambda: os.environ["NEWS_API_KEY"])
    bucket_name : str = field(default_factory=lambda: os.getenv("BUCKET_NAME", ""))
    openai_key  : str = field(default_factory=lambda: os.environ["OPENAI_API_KEY"])
    topics      : list[str] = field(
        default_factory=lambda: os.getenv(
            "TOPICS", "AI,technology,data,health,productivity,self-improvement"
        ).split(",")
    )
    page_size   : int = int(os.getenv("PAGE_SIZE", 30))
    days_back   : int = int(os.getenv("DAYS_BACK", 7))

if __name__ == "__main__":  
    from pprint import pprint; pprint(Settings())


