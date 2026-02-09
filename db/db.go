package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

type IngestJob struct {
	ID           string
	NotionDB     string
	TenantID     string
	Status       string
	ErrorMessage string
	UpdatedAt    time.Time
}

type NotionDBConfig struct {
	ID               string
	NotionDatabaseID string
	NotionAPIKey     string
	WebhookSecret    string
	TenantID         string
}

type Document struct {
	ID        string
	Content   string
	Metadata  map[string]interface{}
	Embedding []float32
	TenantID  string
	CreatedAt time.Time
	DocType   string // "parent" or "child"
	ParentID  string // UUID of parent doc (children only)
}

type ParentWithChildren struct {
	Parent   *Document
	Children []*Document
}

func NewDB(connString string) (*DB, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	db.Pool.Close()
}

func (db *DB) GetNextJob(ctx context.Context) (*IngestJob, error) {
	// Atomic update: Find oldest 'queued' job, lock it, update to 'processing', return it.
	var job IngestJob

	err := db.Pool.QueryRow(ctx, `
		UPDATE ingest_queue
		SET status = 'processing', updated_at = NOW()
		WHERE id = (
			SELECT id
			FROM ingest_queue
			WHERE status = 'queued'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, notion_db, tenant_id, status, updated_at
	`).Scan(&job.ID, &job.NotionDB, &job.TenantID, &job.Status, &job.UpdatedAt)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No jobs available
		}
		return nil, fmt.Errorf("failed to get next job: %w", err)
	}

	return &job, nil
}

func (db *DB) UpdateJobStatus(ctx context.Context, jobID, status, errorMessage string) error {
	_, err := db.Pool.Exec(ctx, `
		UPDATE ingest_queue
		SET status = $2, error_message = $3, updated_at = NOW()
		WHERE id = $1
	`, jobID, status, errorMessage)
	return err
}

func (db *DB) GetNotionConfig(ctx context.Context, notionDBID string) (*NotionDBConfig, error) {
	var config NotionDBConfig
	err := db.Pool.QueryRow(ctx, `
		SELECT id, notion_api_key, COALESCE(webhook_secret, ''), tenant_id, notion_database_id
		FROM notion_dbs 
		WHERE id = $1
	`, notionDBID).Scan(&config.ID, &config.NotionAPIKey, &config.WebhookSecret, &config.TenantID, &config.NotionDatabaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get notion config: %w", err)
	}
	return &config, nil
}

func (db *DB) GetNotionConfigByExternalID(ctx context.Context, externalDBID string) (*NotionDBConfig, error) {
	var config NotionDBConfig
	// We need ot added notion_database_id to schema?
	// The user prompt said: "This webhook will be stored in the notion_dbs table".
	// But `notion_dbs` keys off an internal UUID.
	// We are adding `notion_database_id` to schema now.
	err := db.Pool.QueryRow(ctx, `
		SELECT id, notion_api_key, COALESCE(webhook_secret, ''), tenant_id, notion_database_id
		FROM notion_dbs 
		WHERE notion_database_id = $1
	`, externalDBID).Scan(&config.ID, &config.NotionAPIKey, &config.WebhookSecret, &config.TenantID, &config.NotionDatabaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get notion config by external id: %w", err)
	}
	return &config, nil
}

func (db *DB) GetExistingPageMap(ctx context.Context, tenantID, notionDBID string) (map[string]time.Time, error) {
	rows, err := db.Pool.Query(ctx, `
		SELECT metadata->>'notion_page_id', (metadata->>'last_edited_time')::timestamp
		FROM documents
		WHERE tenant_id = $1 AND metadata->>'notion_db' = $2 AND doc_type = 'parent'
	`, tenantID, notionDBID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pageMap := make(map[string]time.Time)
	for rows.Next() {
		var pageID string
		var lastEdited time.Time
		if err := rows.Scan(&pageID, &lastEdited); err != nil {
			// If scan fails (e.g. metadata missing), we might skip or log.
			// For now, continue.
			continue
		}
		if pageID != "" {
			pageMap[pageID] = lastEdited
		}
	}
	return pageMap, nil
}

func (db *DB) ReplacePageDocuments(ctx context.Context, tenantID, notionPageID, notionDBID string, groups []ParentWithChildren) error {
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Delete existing documents for this page (covers both parents and children)
	_, err = tx.Exec(ctx, `
		DELETE FROM documents
		WHERE tenant_id = $1 AND metadata->>'notion_page_id' = $2
	`, tenantID, notionPageID)
	if err != nil {
		return err
	}

	// 2. Insert parent-child groups
	for _, group := range groups {
		// Insert parent (no embedding)
		parentMetaJSON, err := json.Marshal(group.Parent.Metadata)
		if err != nil {
			return err
		}

		var parentID string
		err = tx.QueryRow(ctx, `
			INSERT INTO documents (content, metadata, embedding, tenant_id, doc_type)
			VALUES ($1, $2, NULL, $3, 'parent')
			RETURNING id
		`, group.Parent.Content, parentMetaJSON, tenantID).Scan(&parentID)
		if err != nil {
			return fmt.Errorf("failed to insert parent document: %w", err)
		}

		// Insert children referencing the parent
		for _, child := range group.Children {
			childMetaJSON, err := json.Marshal(child.Metadata)
			if err != nil {
				return err
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO documents (content, metadata, embedding, tenant_id, doc_type, parent_id)
				VALUES ($1, $2, $3, $4, 'child', $5)
			`, child.Content, childMetaJSON, pgvectorFormat(child.Embedding), tenantID, parentID)
			if err != nil {
				return fmt.Errorf("failed to insert child document: %w", err)
			}
		}
	}

	return tx.Commit(ctx)
}

func (db *DB) DeleteDocument(ctx context.Context, tenantID, notionPageID string) error {
	_, err := db.Pool.Exec(ctx, `
		DELETE FROM documents 
		WHERE tenant_id = $1 AND metadata->>'notion_page_id' = $2
	`, tenantID, notionPageID)
	return err
}

func pgvectorFormat(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}
	// Manual formatting to ensure comma separation which fmt.Sprint doesn't always guarantee for slices (it uses spaces)
	var b []byte
	b = append(b, '[')
	for i, v := range vec {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, []byte(fmt.Sprintf("%f", v))...)
	}
	b = append(b, ']')
	return string(b)
}
