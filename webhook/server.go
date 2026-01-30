package webhook

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/JACTERK/data-processor/db"
	"github.com/JACTERK/data-processor/ingest"
	"github.com/jomei/notionapi"
)

type Server struct {
	DB        *db.DB
	Processor *ingest.Processor
	Port      string
}

func NewServer(port string, database *db.DB, processor *ingest.Processor) *Server {
	return &Server{
		Port:      port,
		DB:        database,
		Processor: processor,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", s.handleWebhook)

	log.Printf("Starting webhook server on port %s", s.Port)
	return http.ListenAndServe(":"+s.Port, mux)
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// 1. Check for verification request
	var verificationReq struct {
		VerificationToken string `json:"verification_token"`
	}
	if err := json.Unmarshal(body, &verificationReq); err == nil && verificationReq.VerificationToken != "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"verification_token": verificationReq.VerificationToken,
		})
		return
	}

	// 2. Parse Event
	// We handle standard generic events to extract type and data
	var generic map[string]interface{}
	if err := json.Unmarshal(body, &generic); err != nil {
		log.Printf("Failed to unmarshal webhook generic: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	eventType, _ := generic["type"].(string)
	log.Printf("Received webhook event: %s", eventType)

	// Extract data object
	dataBytes, _ := json.Marshal(generic["data"])
	var page notionapi.Page
	if err := json.Unmarshal(dataBytes, &page); err != nil {
		// Not a page event or different structure
		return
	}

	pageID := page.ID.String()
	var parentDBID string
	if page.Parent.Type == "database_id" {
		parentDBID = page.Parent.DatabaseID.String()
	}

	// If parentDBID is empty (e.g. page in workspace), we can't easily map to a "Notion DB" config
	// unless we blindly search or have a default. For now, we require DB parent.
	if parentDBID == "" {
		log.Printf("Page %s has no database parent, skipping webhook", pageID)
		w.WriteHeader(http.StatusOK)
		return
	}

	handleAsync(s, pageID, parentDBID, eventType)
	w.WriteHeader(http.StatusOK)
}

func handleAsync(s *Server, pageID, parentDBID, eventType string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Map ParentDBID to Config
		// We need a method on DB to find config by EXTERNAL Notion DB ID (not uuid)
		// Assuming we added 'notion_database_id' or similar to stored config,
		// OR we rely on `ingest_queue.notion_db_id` which was UUID.
		// Implementation Plan Assumption: We need to enable lookup by external ID.
		config, err := s.DB.GetNotionConfigByExternalID(ctx, parentDBID)
		if err != nil {
			log.Printf("Could not find config for DB %s: %v", parentDBID, err)
			return
		}

		client := notionapi.NewClient(notionapi.Token(config.NotionAPIKey))

		if eventType == "page.deleted" || eventType == "page.archived" { // Check Notion docs for exact delete event
			// 'page.deleted' isn't standard in all versions, 'archived' property is usually set.
			// But assuming webhook sends 'page.deleted' or we check archived status.
			// Currently assuming generic handling.
			s.DB.DeleteDocument(ctx, config.TenantID, pageID)
			return
		}

		// Fetch fresh page
		page, err := client.Page.Get(ctx, notionapi.PageID(pageID))
		if err != nil {
			log.Printf("Failed to fetch page %s: %v", pageID, err)
			return
		}

		if page.Archived {
			s.DB.DeleteDocument(ctx, config.TenantID, pageID)
			return
		}

		if err := s.Processor.ProcessSinglePage(ctx, client, page, config.TenantID, config.ID); err != nil {
			log.Printf("Error processing webhook page: %v", err)
		}
	}()
}
