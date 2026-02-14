package main

import (
	"log"

	"github.com/joho/godotenv"

	"kzen-go/minioserver"
	"kzen-go/golib"
)

func main() {
	_ = godotenv.Load()

	cfg := minioserver.Config{
		Endpoint:  golib.GetEnv("MINIO_ENDPOINT", "localhost:9000"),
		AccessKey: golib.GetEnv("MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey: golib.GetEnv("MINIO_SECRET_KEY", "minioadmin"),
		Bucket:    golib.GetEnv("MINIO_BUCKET", "mybucket"),
		UseSSL:    golib.GetEnv("MINIO_USE_SSL", "false") == "true",
		Listen:    golib.GetEnv("LISTEN_ADDR", ":8080"),
		APIKey:    golib.GetEnv("API_KEY", ""),
	}

	if err := minioserver.Run(cfg); err != nil {
		log.Fatalf("server: %v", err)
	}
}
