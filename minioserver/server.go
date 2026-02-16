package minioserver

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	Listen    string
	APIKey    string
}

const (
	KZEN_STORAGE = "kzen-storage"
)

func Run(cfg Config) error {
	cfg.Endpoint = strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "https://"), "http://")
	if i := strings.Index(cfg.Endpoint, "/"); i != -1 {
		cfg.Endpoint = cfg.Endpoint[:i]
	}

	// Higher connection pool limits avoid intermittent 500s when many images load concurrently.
	// Default transport only keeps 2 idle conns per host, causing connection churn under load.
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    cfg.UseSSL,
		Transport: transport,
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
	/* kzen */
	mux.HandleFunc(fmt.Sprintf("/%s-objects/", KZEN_STORAGE), objectsHandler(client, KZEN_STORAGE))
	mux.HandleFunc(fmt.Sprintf("/%s-upload-images", KZEN_STORAGE), uploadImagesToMinioServer(client, KZEN_STORAGE, "/kzen"))
	mux.HandleFunc(fmt.Sprintf("/%s-debug-list", KZEN_STORAGE), debugList(client, KZEN_STORAGE))

	handler := Chain(corsMiddleware, logMiddleware)(mux)
	if cfg.APIKey != "" {
		handler = apiKeyMiddleware(cfg.APIKey)(handler)
		log.Printf("API key auth enabled")
	}

	log.Printf("MinIO proxy listening on %s (bucket: %s)", cfg.Listen, cfg.Bucket)
	return http.ListenAndServe(cfg.Listen, handler)
}
