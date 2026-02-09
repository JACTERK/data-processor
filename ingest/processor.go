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
	DB             *db.DB
	LLM            *llm.Client
	ChunkSize      int
	ChunkOverlap   int
	ChildMinTokens int
	ChildMaxTokens int
}

func NewProcessor(database *db.DB, llmClient *llm.Client, chunkSize, chunkOverlap, childMinTokens, childMaxTokens int) *Processor {
	return &Processor{
		DB:             database,
		LLM:            llmClient,
		ChunkSize:      chunkSize,
		ChunkOverlap:   chunkOverlap,
		ChildMinTokens: childMinTokens,
		ChildMaxTokens: childMaxTokens,
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

	snippets, err := p.getPageContent(ctx, client, page.ID.String())
	if err != nil {
		return err
	}

	if len(snippets) == 0 {
		return nil
	}

	// Extract Title
	title := "Untitled"
	if props, ok := page.Properties["Name"].(*notionapi.TitleProperty); ok {
		if len(props.Title) > 0 {
			title = props.Title[0].PlainText
		}
	} else if props, ok := page.Properties["title"].(*notionapi.TitleProperty); ok {
		if len(props.Title) > 0 {
			title = props.Title[0].PlainText
		}
	}

	// Chunk into parents with children
	parents, err := chunkSnippets(snippets, p.ChunkSize, p.ChildMinTokens, p.ChildMaxTokens)
	if err != nil {
		return fmt.Errorf("failed to chunk text: %w", err)
	}

	// Collect all child texts for batch embedding
	var allChildTexts []string
	for _, parent := range parents {
		for _, child := range parent.Children {
			allChildTexts = append(allChildTexts, child.Content)
		}
	}

	// Generate embeddings in one batch
	embeddings, err := p.LLM.GenerateEmbeddings(ctx, allChildTexts)
	if err != nil {
		return fmt.Errorf("failed to generate embeddings: %w", err)
	}

	// Build parent-child document groups
	var groups []db.ParentWithChildren
	embIdx := 0

	for i, parent := range parents {
		parentDoc := &db.Document{
			Content:  parent.Content,
			TenantID: tenantID,
			DocType:  "parent",
			Metadata: map[string]interface{}{
				"notion_page_id":   page.ID.String(),
				"notion_db":        notionDBID,
				"last_edited_time": page.LastEditedTime,
				"url":              page.URL,
				"title":            title,
				"chunk_index":      i,
				"total_chunks":     len(parents),
			},
		}

		var childDocs []*db.Document
		for j, child := range parent.Children {
			childDoc := &db.Document{
				Content:   child.Content,
				Embedding: embeddings[embIdx],
				TenantID:  tenantID,
				DocType:   "child",
				Metadata: map[string]interface{}{
					"notion_page_id":     page.ID.String(),
					"notion_db":          notionDBID,
					"last_edited_time":   page.LastEditedTime,
					"url":                page.URL,
					"title":              title,
					"parent_chunk_index": i,
					"child_chunk_index":  j,
					"anchor_block_id":    child.AnchorBlockID,
				},
			}
			childDocs = append(childDocs, childDoc)
			embIdx++
		}

		groups = append(groups, db.ParentWithChildren{Parent: parentDoc, Children: childDocs})
	}

	return p.DB.ReplacePageDocuments(ctx, tenantID, page.ID.String(), notionDBID, groups)
}

type ParentChunk struct {
	Content       string
	AnchorBlockID string
	Snippets      []Snippet
	Children      []ChildChunk
}

type ChildChunk struct {
	Content       string
	AnchorBlockID string
}

type Snippet struct {
	Text    string
	BlockID string
}

// chunkSnippets produces parent chunks (for LLM context) each containing
// child chunks (for vector search + accurate deep linking).
func chunkSnippets(snippets []Snippet, parentSize, minChildTokens, maxChildTokens int) ([]ParentChunk, error) {
	parents, err := chunkSnippetsIntoParents(snippets, parentSize)
	if err != nil {
		return nil, err
	}
	for i := range parents {
		if err := splitParentIntoChildren(&parents[i], minChildTokens, maxChildTokens); err != nil {
			return nil, err
		}
	}
	return parents, nil
}

// chunkSnippetsIntoParents groups snippets into parent-sized chunks (~300 tokens),
// retaining the source snippets on each parent for later child generation.
func chunkSnippetsIntoParents(snippets []Snippet, parentSize int) ([]ParentChunk, error) {
	tkm, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		return nil, fmt.Errorf("failed to get encoding: %w", err)
	}

	var parents []ParentChunk
	var currentSnippets []Snippet
	currentTokens := 0

	for _, snippet := range snippets {
		snippetTokens := len(tkm.Encode(snippet.Text, nil, nil))

		// If single snippet is larger than parent size, split it
		if snippetTokens > parentSize {
			// Flush accumulated snippets
			if len(currentSnippets) > 0 {
				parents = append(parents, createParentChunkFromSnippets(currentSnippets))
				currentSnippets = nil
				currentTokens = 0
			}

			// Split the large snippet, keeping the same BlockID
			text := snippet.Text
			for len(tkm.Encode(text, nil, nil)) > parentSize {
				fullTokens := tkm.Encode(text, nil, nil)
				chunkTokens := fullTokens[:parentSize]
				chunkText := tkm.Decode(chunkTokens)

				splitSnippet := Snippet{Text: chunkText, BlockID: snippet.BlockID}
				parents = append(parents, createParentChunkFromSnippets([]Snippet{splitSnippet}))

				remainingTokens := fullTokens[parentSize:]
				text = tkm.Decode(remainingTokens)
			}
			// Add remainder to accumulator
			if text != "" {
				currentSnippets = append(currentSnippets, Snippet{Text: text, BlockID: snippet.BlockID})
				currentTokens = len(tkm.Encode(text, nil, nil))
			}
			continue
		}

		// Normal: flush if adding this snippet would exceed parent size
		if currentTokens+snippetTokens > parentSize {
			parents = append(parents, createParentChunkFromSnippets(currentSnippets))
			currentSnippets = nil
			currentTokens = 0
		}

		currentSnippets = append(currentSnippets, snippet)
		currentTokens += snippetTokens
	}

	if len(currentSnippets) > 0 {
		parents = append(parents, createParentChunkFromSnippets(currentSnippets))
	}

	return parents, nil
}

func createParentChunkFromSnippets(snippets []Snippet) ParentChunk {
	if len(snippets) == 0 {
		return ParentChunk{}
	}
	var content string
	for _, s := range snippets {
		content += s.Text + "\n"
	}
	return ParentChunk{
		Content:       content,
		AnchorBlockID: snippets[0].BlockID,
		Snippets:      snippets,
	}
}

// splitParentIntoChildren produces child chunks from a parent's snippets using
// a hybrid block-level strategy:
//   - Each block (snippet) becomes its own child by default
//   - Tiny blocks (< minTokens) are merged with the next block
//   - Large blocks (> maxTokens) are split at token boundaries
func splitParentIntoChildren(parent *ParentChunk, minTokens, maxTokens int) error {
	tkm, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		return fmt.Errorf("failed to get encoding: %w", err)
	}

	var children []ChildChunk
	var accSnippets []Snippet
	accTokens := 0

	flushAcc := func() {
		if len(accSnippets) == 0 {
			return
		}
		var content string
		for _, s := range accSnippets {
			content += s.Text + "\n"
		}
		children = append(children, ChildChunk{
			Content:       content,
			AnchorBlockID: accSnippets[0].BlockID,
		})
		accSnippets = nil
		accTokens = 0
	}

	for _, snippet := range parent.Snippets {
		snippetTokens := len(tkm.Encode(snippet.Text, nil, nil))

		// Large block: flush accumulator, then split the block
		if snippetTokens > maxTokens {
			flushAcc()

			text := snippet.Text
			for len(tkm.Encode(text, nil, nil)) > maxTokens {
				fullTokens := tkm.Encode(text, nil, nil)
				chunkText := tkm.Decode(fullTokens[:maxTokens])
				children = append(children, ChildChunk{
					Content:       chunkText,
					AnchorBlockID: snippet.BlockID,
				})
				text = tkm.Decode(fullTokens[maxTokens:])
			}
			if text != "" {
				accSnippets = append(accSnippets, Snippet{Text: text, BlockID: snippet.BlockID})
				accTokens = len(tkm.Encode(text, nil, nil))
			}
			continue
		}

		// Tiny block: accumulate with neighbors
		if snippetTokens < minTokens {
			accSnippets = append(accSnippets, snippet)
			accTokens += snippetTokens
			// If accumulated enough, flush
			if accTokens >= minTokens {
				flushAcc()
			}
			continue
		}

		// Normal-sized block: flush any accumulated tiny blocks, then emit this block as its own child
		flushAcc()
		children = append(children, ChildChunk{
			Content:       snippet.Text + "\n",
			AnchorBlockID: snippet.BlockID,
		})
	}

	// Flush remaining accumulated snippets
	flushAcc()

	parent.Children = children
	return nil
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
