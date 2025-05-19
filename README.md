# NEWS DATA PIPELINE

### Project overview
This schema provides an overview of the workflow of the project, its architecture and how it was implemented:
In few words, this project extracts and filters specific news, uses GPT API to generate content ideas and formats and sends those ideas periodically to my personal email.
It has all been deployed and scaled through GCP.


# Documentation index

---

## Project overview

### Why I built this
My main motivation behind this project was to become familiar with GCP services and cloud computing best practices by building a solution that tackled a real necessity and made my life a little bit easier. I used to spend a lot of time trying to stay updated in the data and AI field, which is an ever evolving field. Moreover, I like to be active on Linkedin, and that also requires a lot of time too. I thought it would be helpful to create a pipeline especially designed to search for tech news, brainstorms content ideas based on those news, and sends them to my email periodically. This way, I can read them and get inspiration from everywhere and with few effort. 

### Why it matters
This project is more than just a technical exercise—it’s a way to solve a problem I’ve encountered personally. 
It is my way of combining curiosity, experimentation, and problem-solving to build something meaningful. It’s not just about the result but the process of understanding and refining every step along the way.

### Architecture summary
This project follows a clean, modular architecture that contains two adapters. The core logic — fetching, filtering, and summarizing news — is completely isolated from how and where it's run. The same code can be triggered locally (via CLI), or in GCP. This architecture brings many benefits: it’s easy to test, scales automatically, separates concerns, and keeps the pipeline flexible for future changes. 

The pipeline is deployed on **Google Cloud Functions** (Gen 2) and triggered via **Cloud Scheduler** using a decoupled workflow. Scheduler calls a lightweight trigger function, which enqueues a **Cloud Task**. This task invokes the main pipeline function that fetches, filters, and summarizes news, saving results to **Cloud Storage**. Separately, **App Engine** runs a Flask app that sends the final content via email every week.

## Directory & File Structure
### ## `weekly-fetcher/`

Core logic for fetching, filtering, and summarizing technical news using the OpenAI API.

- **`news_pipeline/`**  
  - `fetcher.py` – Fetches recent articles from NewsAPI by topic (parallelized).  
  - `filterer.py` – Scores and filters articles for technical relevance using GPT.  
  - `summarizer.py` – Generates LinkedIn-style post ideas from curated content.  
  - `settings.py` – Loads configuration from environment variables or `.env` files.  
  - **Purpose**: Houses the reusable business logic, independent of cloud/runtime context.

- **`adapters/`**  
  - `cli.py` – CLI adapter for local testing and development.  
  - `gcf_function.py` – Cloud Function adapter to expose the pipeline via HTTP (GCP).  
  - **Purpose**: Acts as the interface layer to run the same logic locally or in the cloud.

- `main.py` – Entry point to run the pipeline.  
- `requirements.txt` – Lists dependencies for running the pipeline.

---

### ## `trigger_job/`

A lightweight Cloud Function that enqueues a Cloud Task to trigger the main pipeline asynchronously.

- `main.py` – Defines the function that will be triggered by Cloud Scheduler.  
- `requirements.txt` – Required packages for this standalone trigger function.

---

### ## `content-emailer/`

Flask web service deployed on Google App Engine to send the weekly digest via email.

- `main.py` – Defines `/tasks/send-ideas`, the route triggered weekly by `cron.yaml`.  
- `cron.yaml` – App Engine scheduler configuration to run the email job every Monday at 08:00 (Madrid time).  
- `requirements.txt` – Flask + SMTP mail libraries to send styled HTML and plain text emails.

---
## Smoke Testing Strategy

Before deploying to GCP, the pipeline is tested locally using the `cli.py` adapter.

Example:

```bash
python -m adapters.cli --days 5 --outfile ideas_output.md





---


