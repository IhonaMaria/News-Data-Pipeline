import os
from flask import jsonify
from google.cloud import tasks_v2
import functions_framework

@functions_framework.http
def trigger_job(request):
    client = tasks_v2.CloudTasksClient()

    project = os.environ["GCP_PROJECT"]
    region = "europe-west1"
    queue = "summarization-queue"
    target_url = os.environ["SUMMARIZER_URL"]

    parent = client.queue_path(project, region, queue)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": target_url,
            "oidc_token": {
                "service_account_email": f"{project}@appspot.gserviceaccount.com"
            }
        }
    }

    client.create_task(parent=parent, task=task)
    return jsonify({"status": "Task enqueued"})
