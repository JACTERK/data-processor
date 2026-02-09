package llm

import (
	"context"
	"fmt"

	"github.com/sashabaranov/go-openai"
)

type Client struct {
	api *openai.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		api: openai.NewClient(apiKey),
	}
}

func (c *Client) GenerateEmbedding(ctx context.Context, text string) ([]float32, error) {
	resp, err := c.api.CreateEmbeddings(ctx, openai.EmbeddingRequestStrings{
		Input: []string{text},
		Model: openai.SmallEmbedding3, // text-embedding-3-small
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("no embedding data returned")
	}

	return resp.Data[0].Embedding, nil
}

func (c *Client) GenerateEmbeddings(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	resp, err := c.api.CreateEmbeddings(ctx, openai.EmbeddingRequestStrings{
		Input: texts,
		Model: openai.SmallEmbedding3,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate embeddings: %w", err)
	}

	if len(resp.Data) != len(texts) {
		return nil, fmt.Errorf("expected %d embeddings, got %d", len(texts), len(resp.Data))
	}

	embeddings := make([][]float32, len(texts))
	for _, d := range resp.Data {
		embeddings[d.Index] = d.Embedding
	}
	return embeddings, nil
}
