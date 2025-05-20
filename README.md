# NEWS DATA PIPELINE

A fully automated, cloud-native system that fetches technical news, uses GPT to curate and brainstorm content ideas, and delivers a weekly digest to your gmail inbox. Deployed and scaled on Google Cloud Platform (GCP).

![image](https://github.com/user-attachments/assets/362369c0-a462-4653-be08-f13f049432a7)

## Project overview

### Why I built this
I built this project to make my life easier and learn GCP by automating two time-consuming tasks:

- Keeping up with fast-moving AI/data news
- Brainstorming content ideas for Linkedin or Medium

Now, every week I get an email with content ideas related to my fields of interest. This way, I can be informed and get inspired from anywhere.

### Why it matters
This project is more than just a technical exercise—it’s a way to solve a problem I’ve encountered personally. 
It is my way of combining curiosity, experimentation, and problem-solving to build something meaningful. It’s not just about the result but the process of understanding and refining every step along the way.

### Architecture
#### Core Pipeline
- **`news_pipeline/`**: Pure business logic for fetching, filtering, and summarizing articles.  
- Decoupled from I/O, HTTP, and cloud concerns. Ensures high testability and reuse.  

#### Adapters
- **CLI Adapter** (`adapters/cli.py`):  
  Run end-to-end locally for development and smoke tests.
  
Example:

```bash
python -m adapters.cli --days 5 --outfile ideas_output.md
```
This would generate a file named ideas_output.md with content ideas based on the last 5 days of news. 




- **Cloud Function Adapter** (`adapters/gcf_function.py`):  
  HTTP-trigger entrypoint for GCP deployment  

#### Event-Driven Orchestration
1. **Cloud Scheduler**  
   - Fires a cron job every week (e.g. Monday at 08:00 CET).  
   - Sends a quick HTTP request to the trigger function.  
2. **Trigger Function** (`trigger_job`)  
   - Enqueues a task into a **Cloud Tasks** queue.  
3. **Cloud Tasks**  
   - Holds the job until ready, then issues an authenticated HTTP POST to `news_pipeline`.  
4. **Main Pipeline** (`news_pipeline`)  
   - Executes fetch → filter → summarize 
   - After each stage, writes output artifacts (`raw_*.json`, `filtered_*.json`, `ideas_*.md`) to **Cloud Storage** for traceability.  

#### Email Delivery
- A separate **Flask app** on **App Engine** reads the latest summaries from the bucket  
- Formats and sends the weekly digest via SMTP

## Directory & File Structure
### a) `weekly-fetcher/`

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

### b) `trigger_job/`

A lightweight Cloud Function that enqueues a Cloud Task to trigger the main pipeline asynchronously.

- `main.py` – Defines the function that will be triggered by Cloud Scheduler.  
- `requirements.txt` – Required packages for this standalone trigger function.

---

### c) `content-emailer/`

Flask web service deployed on Google App Engine to send the weekly digest via email.

- `main.py` – Defines `/tasks/send-ideas`, the route triggered weekly by `cron.yaml`.  
- `cron.yaml` – App Engine scheduler configuration to run the email job every Monday at 08:00 (Madrid time).  
- `requirements.txt` – Flask + SMTP mail libraries to send styled HTML and plain text emails.
