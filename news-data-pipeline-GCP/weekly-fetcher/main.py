"""
Entry point for deploying the weekly news pipeline as a Google Cloud Function.
This file is required by GCP to locate the function handler when deploying via `gcloud functions deploy`.
"""

from adapters.gcf_function import weekly_pipeline