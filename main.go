package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JACTERK/data-processor/config"
	"github.com/JACTERK/data-processor/db"
	"github.com/JACTERK/data-processor/ingest"
	"github.com/JACTERK/data-processor/llm"
	"github.com/JACTERK/data-processor/webhook"
)

func main() {
	log.Println("Starting Data Processor Service...")

	// 1. Load Config
	cfg := config.LoadConfig()

	// 2. Initialize DB
	database, err := db.NewDB(cfg.DBConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer database.Close()
	log.Println("Connected to database")

	// 3. Initialize LLM Client
	// If key is missing, we might panic or run in a dry-run mode. For now panic.
	if cfg.OpenAIKey == "" {
		log.Fatal("OPENAI_API_KEY is required")
	}
	llmClient := llm.NewClient(cfg.OpenAIKey)

	// 4. Initialize Processor
	processor := ingest.NewProcessor(database, llmClient, cfg.ChunkSize, cfg.ChunkOverlap, cfg.ChildMinTokens, cfg.ChildMaxTokens)

	// 5. Start Webhook Server (Optional if secret/port set?)
	// We'll start it always, as it handles webhooks if configured.
	if cfg.Port != "" {
		webhookServer := webhook.NewServer(cfg.Port, database, processor)
		go func() {
			if err := webhookServer.Start(); err != nil {
				log.Printf("Webhook server error: %v", err)
			}
		}()
	}

	// 6. Start Polling Loop
	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	processLoop(ctx, database, processor)
}

func processLoop(ctx context.Context, database *db.DB, processor *ingest.Processor) {
	ticker := time.NewTicker(10 * time.Second) // Poll every 10 seconds? Or tighter loop?
	defer ticker.Stop()

	// Initial check
	checkAndProcess(ctx, database, processor)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkAndProcess(ctx, database, processor)
		}
	}
}

func checkAndProcess(ctx context.Context, database *db.DB, processor *ingest.Processor) {
	// Requirements: "The app will wait until a new entry is made"
	// Polling is acceptable. "In order to be fast and efficient" implies we can poll frequently or use listen/notify (Postgres).
	// Given standard patterns, polling every few seconds is fine.

	// We consume ONE job at a time per tick or drain the queue?
	// Let's drain the queue until empty.

	for {
		if ctx.Err() != nil {
			return
		}

		job, err := database.GetNextJob(ctx)
		if err != nil {
			log.Printf("Error processing queue: %v", err)
			return // Wait for next tick
		}
		if job == nil {
			return // No more jobs
		}

		// Process Job
		if err := processor.ProcessJob(ctx, job); err != nil {
			log.Printf("Failed to process job %s: %v", job.ID, err)
			if updateErr := database.UpdateJobStatus(ctx, job.ID, "failed", err.Error()); updateErr != nil {
				log.Printf("Failed to update job status to failed: %v", updateErr)
			}
		} else {
			log.Printf("Successfully processed job %s", job.ID)
			if updateErr := database.UpdateJobStatus(ctx, job.ID, "completed", ""); updateErr != nil {
				log.Printf("Failed to update job status to completed: %v", updateErr)
			}
		}
	}
}
