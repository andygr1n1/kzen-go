package minioserver

import (
	"log"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config holds the MinIO proxy server configuration.
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Listen    string
	APIKey    string
}

// Run starts the MinIO proxy server.
func Run(cfg Config) error {
	cfg.Endpoint = strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "https://"), "http://")
	if i := strings.Index(cfg.Endpoint, "/"); i != -1 {
		cfg.Endpoint = cfg.Endpoint[:i]
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/objects/", objectsHandler(client, cfg.Bucket))
	mux.HandleFunc("/batch", batchHandler(client, cfg.Bucket))
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/health/", healthHandler)
	mux.HandleFunc("/debug/list", debugList(client, cfg.Bucket))

	handler := Chain(corsMiddleware, logMiddleware)(mux)
	if cfg.APIKey != "" {
		handler = apiKeyMiddleware(cfg.APIKey)(handler)
		log.Printf("API key auth enabled")
	}

	log.Printf("MinIO proxy listening on %s (bucket: %s)", cfg.Listen, cfg.Bucket)
	return http.ListenAndServe(cfg.Listen, handler)
}
