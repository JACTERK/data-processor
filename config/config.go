package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DBConnectionString string
	OpenAIKey          string
	ChunkSize          int
	ChunkOverlap       int
	Port               string
	WebhookSecret      string
}

func LoadConfig() *Config {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	return &Config{
		DBConnectionString: getEnv("DB_CONNECTION_STRING", ""),
		OpenAIKey:          getEnv("OPENAI_API_KEY", ""),
		ChunkSize:          getEnvInt("CHUNK_SIZE", 300),
		ChunkOverlap:       getEnvInt("CHUNK_OVERLAP", 0),
		Port:               getEnv("PORT", "8080"),
		WebhookSecret:      getEnv("WEBHOOK_SECRET", ""),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	if fallback == "" {
		// TODO: Log error or panic
		log.Printf("Error: %s is not set", key)
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return fallback
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Printf("Invalid integer for %s: %v. Using default %d", key, err, fallback)
		return fallback
	}
	return val
}
