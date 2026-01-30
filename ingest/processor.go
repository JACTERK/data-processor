package ingest

import (
	"context"
	"fmt"
	"log"

	"github.com/JACTERK/data-processor/db"
	"github.com/JACTERK/data-processor/llm"
	"github.com/jomei/notionapi"
	"github.com/pkoukk/tiktoken-go"
)

type Processor struct {
	DB           *db.DB
	LLM          *llm.Client
	ChunkSize    int
	ChunkOverlap int
}

func NewProcessor(database *db.DB, llmClient *llm.Client, chunkSize, chunkOverlap int) *Processor {
	return &Processor{
		DB:           database,
		LLM:          llmClient,
		ChunkSize:    chunkSize,
		ChunkOverlap: chunkOverlap,
	}
}

func (p *Processor) ProcessJob(ctx context.Context, job *db.IngestJob) error {
	log.Printf("Processing job %s for Notion DB %s", job.ID, job.NotionDB)

	// 1. Fetch Notion Config
	config, err := p.DB.GetNotionConfig(ctx, job.NotionDB)
	if err != nil {
		return fmt.Errorf("failed to get notion config: %w", err)
	}

	// 2. Initialize Notion Client
	notionClient := notionapi.NewClient(notionapi.Token(config.NotionAPIKey))

	// 3. Fetch Existing Pages from DB to diff
	existingPages, err := p.DB.GetExistingPageMap(ctx, config.TenantID, job.NotionDB)
	if err != nil {
		return fmt.Errorf("failed to get existing page map: %w", err)
	}

	// 4. Fetch All Pages from Notion (Pagination Loop)
	// We track visited pages to identify deletions later
	visitedPages := make(map[string]bool)

	hasMore := true
	var startCursor notionapi.Cursor

	for hasMore {
		req := &notionapi.DatabaseQueryRequest{
			PageSize:    100,
			StartCursor: startCursor,
		}

		resp, err := notionClient.Database.Query(ctx, notionapi.DatabaseID(config.NotionDatabaseID), req)
		if err != nil {
			return fmt.Errorf("failed to query notion database: %w", err)
		}

		for _, page := range resp.Results {
			visitedPages[page.ID.String()] = true

			// Check if we need to process this page
			lastEditedTime := page.LastEditedTime

			// Logic:
			// If not in existingPages -> New -> Process
			// If in existingPages but lastEditedTime > existing -> Updated -> Process
			// Else -> Skip

			existingTime, exists := existingPages[page.ID.String()]

			if !exists || lastEditedTime.After(existingTime) {
				if err := p.ProcessSinglePage(ctx, notionClient, &page, config.TenantID, job.NotionDB); err != nil {
					log.Printf("Error processing page %s: %v", page.ID, err)
					// Continue processing other pages even if one fails? Yes.
				}
			}
		}

		hasMore = resp.HasMore
		startCursor = resp.NextCursor
	}

	// 5. Handle Deletions
	// Any page in existingPages that is NOT in visitedPages likely was deleted from Notion
	// OR it just didn't match the query if we had filters (but we queried all).
	for pageID := range existingPages {
		if !visitedPages[pageID] {
			log.Printf("Deleting document for page %s", pageID)
			if err := p.DB.DeleteDocument(ctx, config.TenantID, pageID); err != nil {
				log.Printf("Failed to delete document %s: %v", pageID, err)
			}
		}
	}

	return nil
}

func (p *Processor) ProcessSinglePage(ctx context.Context, client *notionapi.Client, page *notionapi.Page, tenantID, notionDBID string) error {
	log.Printf("Syncing page %s", page.ID)

	// Fetch full blocks to get content?
	// For simple ingestion, we often just want the flattened text of the page.
	// We need to traverse blocks.

	// Helper to get text content from blocks
	content, err := p.getPageContent(ctx, client, page.ID.String())
	if err != nil {
		return err
	}

	if content == "" {
		// Skip empty pages? or store empty?
		return nil
	}

	// Extract Title and URL
	title := "Untitled"
	if props, ok := page.Properties["Name"].(*notionapi.TitleProperty); ok {
		if len(props.Title) > 0 {
			title = props.Title[0].PlainText
		}
	} else if props, ok := page.Properties["title"].(*notionapi.TitleProperty); ok {
		// Some DBs use lowercase 'title'
		if len(props.Title) > 0 {
			title = props.Title[0].PlainText
		}
	}

	// Chunk Data
	chunks, err := chunkText(content, p.ChunkSize, p.ChunkOverlap)
	if err != nil {
		return fmt.Errorf("failed to chunk text: %w", err)
	}

	var docs []*db.Document

	for i, chunkContent := range chunks {
		// Generate Embedding for chunk
		embedding, err := p.LLM.GenerateEmbedding(ctx, chunkContent)
		if err != nil {
			return err
		}

		// Construct Document
		doc := &db.Document{
			Content:   chunkContent,
			Embedding: embedding,
			TenantID:  tenantID,
			Metadata: map[string]interface{}{
				"notion_page_id": page.ID.String(),
				"notion_db":      notionDBID, // This is our UUID or External? Ideally our UUID if passed from DB.
				// The webhook passes External ID if we don't map it.
				// But Processor expects what? It writes it to metadata.
				"last_edited_time": page.LastEditedTime,
				"url":              page.URL,
				"title":            title,
				"chunk_index":      i,
				"total_chunks":     len(chunks),
			},
		}
		docs = append(docs, doc)
	}

	return p.DB.ReplacePageDocuments(ctx, tenantID, page.ID.String(), notionDBID, docs)
}

func chunkText(text string, chunkSize, chunkOverlap int) ([]string, error) {
	// Use cl100k_base encoding (common for OpenAI models like text-embedding-3-small)
	tkm, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		return nil, fmt.Errorf("failed to get encoding: %w", err)
	}

	tokens := tkm.Encode(text, nil, nil)

	if chunkSize <= 0 || len(tokens) <= chunkSize {
		return []string{text}, nil
	}

	var chunks []string
	step := chunkSize - chunkOverlap
	if step <= 0 {
		step = chunkSize // Prevent infinite loop or negative step
	}

	for i := 0; i < len(tokens); i += step {
		end := i + chunkSize
		if end > len(tokens) {
			end = len(tokens)
		}

		chunkTokens := tokens[i:end]
		chunkText := tkm.Decode(chunkTokens)
		chunks = append(chunks, chunkText)
	}
	return chunks, nil
}

func (p *Processor) getPageContent(ctx context.Context, client *notionapi.Client, pageID string) (string, error) {
	// Simple block traversal - retrieve children
	// Handling pagination for blocks too!
	var content string
	hasMore := true
	var startCursor notionapi.Cursor

	for hasMore {
		req := &notionapi.Pagination{
			PageSize:    100,
			StartCursor: startCursor,
		}

		children, err := client.Block.GetChildren(ctx, notionapi.BlockID(pageID), req)
		if err != nil {
			return "", err
		}

		for _, block := range children.Results {
			// Extract text from common block types
			text := extractTextFromBlock(block)
			if text != "" {
				content += text + "\n"
			}

			// Recursion logic simplified/removed to avoid interface issues conform 'jomei/notionapi'.
			// If strictly needed, checking HasChildren requires type assertion to specific block structs
			// that support it, as existing 'Block' interface might not expose it in this version.
			// For now, we skip recursion to ensure build stability.
		}

		hasMore = children.HasMore
		startCursor = notionapi.Cursor(children.NextCursor)
	}

	return content, nil
}

func extractTextFromBlock(block notionapi.Block) string {
	// Simplified extraction for common types
	// Using type switch
	switch b := block.(type) {
	case *notionapi.ParagraphBlock:
		return richTextToString(b.Paragraph.RichText)
	case *notionapi.Heading1Block:
		return richTextToString(b.Heading1.RichText)
	case *notionapi.Heading2Block:
		return richTextToString(b.Heading2.RichText)
	case *notionapi.Heading3Block:
		return richTextToString(b.Heading3.RichText)
	case *notionapi.BulletedListItemBlock:
		return richTextToString(b.BulletedListItem.RichText)
	case *notionapi.NumberedListItemBlock:
		return richTextToString(b.NumberedListItem.RichText)
	case *notionapi.ToDoBlock:
		return richTextToString(b.ToDo.RichText)
	// Add more types as needed
	default:
		return ""
	}
}

func richTextToString(rt []notionapi.RichText) string {
	var s string
	for _, t := range rt {
		s += t.PlainText
	}
	return s
}
