package main

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"kzen-go/minioserver"
)

func main() {
	_ = godotenv.Load()

	cfg := minioserver.Config{
		Endpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		Bucket:    getEnv("MINIO_BUCKET", "mybucket"),
		UseSSL:    getEnv("MINIO_USE_SSL", "false") == "true",
		Listen:    getEnv("LISTEN_ADDR", ":8080"),
		APIKey:    getEnv("API_KEY", ""),
	}

	if err := minioserver.Run(cfg); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}
