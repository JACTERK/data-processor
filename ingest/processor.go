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

	// Helper to get text content from blocks
	snippets, err := p.getPageContent(ctx, client, page.ID.String())
	if err != nil {
		return err
	}

	if len(snippets) == 0 {
		// Skip empty pages
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
	chunks, err := chunkSnippets(snippets, p.ChunkSize)
	if err != nil {
		return fmt.Errorf("failed to chunk text: %w", err)
	}

	var docs []*db.Document

	for i, chunk := range chunks {
		// Generate Embedding for chunk
		embedding, err := p.LLM.GenerateEmbedding(ctx, chunk.Content)
		if err != nil {
			return err
		}

		// Construct Document
		doc := &db.Document{
			Content:   chunk.Content,
			Embedding: embedding,
			TenantID:  tenantID,
			Metadata: map[string]interface{}{
				"notion_page_id":   page.ID.String(),
				"notion_db":        notionDBID,
				"last_edited_time": page.LastEditedTime,
				"url":              page.URL,
				"title":            title,
				"chunk_index":      i,
				"total_chunks":     len(chunks),
				"anchor_block_id":  chunk.AnchorBlockID,
			},
		}
		docs = append(docs, doc)
	}

	return p.DB.ReplacePageDocuments(ctx, tenantID, page.ID.String(), notionDBID, docs)
}

type Chunk struct {
	Content       string
	AnchorBlockID string
}

type Snippet struct {
	Text    string
	BlockID string
}

func chunkSnippets(snippets []Snippet, chunkSize int) ([]Chunk, error) {
	tkm, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		return nil, fmt.Errorf("failed to get encoding: %w", err)
	}

	var chunks []Chunk
	var currentChunkSnippets []Snippet
	currentTokens := 0

	for _, snippet := range snippets {
		snippetTokens := len(tkm.Encode(snippet.Text, nil, nil))

		// If single snippet is larger than chunk size, we must split it
		if snippetTokens > chunkSize {
			// First, flush any accumulated snippets
			if len(currentChunkSnippets) > 0 {
				chunks = append(chunks, createChunkFromSnippets(currentChunkSnippets))
				currentChunkSnippets = []Snippet{}
				currentTokens = 0
			}

			// Now split the large snippet
			// We split the distinct text but keep the SAME BlockID for all parts
			// This ensures deep linking works (landing on the big block)
			text := snippet.Text
			for len(tkm.Encode(text, nil, nil)) > chunkSize {
				fullTokens := tkm.Encode(text, nil, nil)

				// Take first chunkSize tokens
				chunkTokens := fullTokens[:chunkSize]
				chunkText := tkm.Decode(chunkTokens)

				chunks = append(chunks, Chunk{
					Content:       chunkText,
					AnchorBlockID: snippet.BlockID,
				})

				// Remaining
				remainingTokens := fullTokens[chunkSize:]
				text = tkm.Decode(remainingTokens)
			}
			// Add remainder
			if text != "" {
				currentChunkSnippets = append(currentChunkSnippets, Snippet{Text: text, BlockID: snippet.BlockID})
				currentTokens = len(tkm.Encode(text, nil, nil))
			}
			continue
		}

		// Normal processing
		if currentTokens+snippetTokens > chunkSize {
			chunks = append(chunks, createChunkFromSnippets(currentChunkSnippets))
			currentChunkSnippets = []Snippet{}
			currentTokens = 0
		}

		currentChunkSnippets = append(currentChunkSnippets, snippet)
		currentTokens += snippetTokens
	}

	// Flush remaining
	if len(currentChunkSnippets) > 0 {
		chunks = append(chunks, createChunkFromSnippets(currentChunkSnippets))
	}

	return chunks, nil
}

func createChunkFromSnippets(snippets []Snippet) Chunk {
	if len(snippets) == 0 {
		return Chunk{}
	}
	var content string
	for _, s := range snippets {
		content += s.Text + "\n"
	}
	return Chunk{
		Content:       content,
		AnchorBlockID: snippets[0].BlockID,
	}
}

func (p *Processor) getPageContent(ctx context.Context, client *notionapi.Client, pageID string) ([]Snippet, error) {
	// Simple block traversal - retrieve children
	// Handling pagination for blocks too!
	var snippets []Snippet
	hasMore := true
	var startCursor notionapi.Cursor

	for hasMore {
		req := &notionapi.Pagination{
			PageSize:    100,
			StartCursor: startCursor,
		}

		children, err := client.Block.GetChildren(ctx, notionapi.BlockID(pageID), req)
		if err != nil {
			return nil, err
		}

		for _, block := range children.Results {
			// Extract text from common block types
			text := extractTextFromBlock(block)
			if text != "" {
				snippets = append(snippets, Snippet{
					Text:    text,
					BlockID: block.GetID().String(),
				})
			}
		}

		hasMore = children.HasMore
		startCursor = notionapi.Cursor(children.NextCursor)
	}

	return snippets, nil
}

func extractTextFromBlock(block notionapi.Block) string {
	// Simplified extraction for common types
	// Add more types as needed
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
	case *notionapi.CalloutBlock:
		return richTextToString(b.Callout.RichText)
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
