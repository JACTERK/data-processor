# Data Processor

A robust Go service designed to process data for RAG (Retrieval-Augmented Generation) systems. This service ingests data from a queue, processes it using OpenAI's LLM for embeddings/summarization, and stores the results back into a PostgreSQL database.

## Features

- **Queue-based Processing**: Polls a `ingest_queue` table for new jobs.
- **LLM Integration**: Uses OpenAI API for generating embeddings and processing text.
- **Webhook Support**: Optional webhook server for external integrations.
- **Configurable**: Fully configurable via environment variables.

## Prerequisites

- [Go](https://go.dev/dl/) 1.25 or higher
- PostgreSQL database (Supabase recommended)
- OpenAI API Key

## Setup

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/JACTERK/data-processor.git
    cd data-processor
    ```

2.  **Configure Environment Variables:**

    Copy the example environment file to `.env`:

    ```bash
    cp .env.example .env
    ```

    Edit `.env` and fill in your details.

    ### Environment Variables

    | Variable               | Description                                                                 | Default |
    | ---------------------- | --------------------------------------------------------------------------- | ------- |
    | `DB_CONNECTION_STRING` | The connection URL for your PostgreSQL database.                            | -       |
    | `OPENAI_API_KEY`       | Your OpenAI API Key (starts with `sk-...`).                                 | -       |
    | `CHUNK_SIZE`           | The maximum character size for text chunks.                                 | `1000`  |
    | `CHUNK_OVERLAP`        | The number of characters to overlap between chunks.                         | `200`   |
    | `PORT`                 | The port for the webhook server.                                            | `9992`  |
    | `WEBHOOK_SECRET`       | A secret key for securing incoming webhooks.                                | `secret`|

    ### Supabase Connection String

    If you are using **Supabase**, your `DB_CONNECTION_STRING` should look like this:

    ```text
    postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
    ```

    You can find this in your Supabase Dashboard under **Settings > Database > Connection pooler** or **Connection string**.

    > **Note:** For a long-running service like this, using the **Session** connection (Port 5432) is generally recommended over the Transaction Pooler (Port 6543), as the application manages its own connection pool via `pgx`.

3.  **Database Setup:**

    Initialize your database schema using the provided SQL file. You can run this in the Supabase SQL Editor:
    
    - [db.sql](db.sql)

## Usage

Run the service:

```bash
go run main.go
```

The service will start, connect to the database, and begin polling for new jobs in the `ingest_queue`.
