package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	envEndpoint        = "MINIO_ENDPOINT"         // e.g. "kvm.local:9000"
	envAccessKey       = "MINIO_ACCESS_KEY"
	envSecretKey       = "MINIO_SECRET_KEY"
	envBucket          = "MINIO_BUCKET"
	envUseSSL          = "MINIO_USE_SSL"          // "true" or "false"
	envListen          = "LISTEN_ADDR"            // default ":8080"
)

func main() {
	endpoint := getEnv(envEndpoint, "localhost:9000")
	accessKey := getEnv(envAccessKey, "minioadmin")
	secretKey := getEnv(envSecretKey, "minioadmin")
	bucket := getEnv(envBucket, "mybucket")
	useSSL := getEnv(envUseSSL, "false") == "true"
	listen := getEnv(envListen, ":8080")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalf("minio client: %v", err)
	}

	mux := http.NewServeMux()

	// GET /objects/*  - download object from MinIO
	mux.HandleFunc("GET /objects/", proxyGet(client, bucket))

	// POST /objects/* - upload object to MinIO
	mux.HandleFunc("POST /objects/", proxyPost(client, bucket))

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	log.Printf("MinIO proxy listening on %s (bucket: %s)", listen, bucket)
	if err := http.ListenAndServe(listen, corsMiddleware(logMiddleware(mux))); err != nil {
		log.Fatalf("server: %v", err)
	}
}

func proxyGet(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, "/objects/")
		if objectKey == "" {
			http.Error(w, "object key required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		obj, err := client.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("get object %q: %v", objectKey, err)
			http.Error(w, "object not found", http.StatusNotFound)
			return
		}
		defer obj.Close()

		info, err := obj.Stat()
		if err != nil {
			log.Printf("stat object %q: %v", objectKey, err)
			http.Error(w, "failed to get object info", http.StatusInternalServerError)
			return
		}

		if info.ContentType != "" {
			w.Header().Set("Content-Type", info.ContentType)
		}
		w.Header().Set("Content-Length", fmtSize(info.Size))

		if _, err := io.Copy(w, obj); err != nil {
			log.Printf("stream object %q: %v", objectKey, err)
		}
	}
}

func proxyPost(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, "/objects/")
		if objectKey == "" {
			http.Error(w, "object key required", http.StatusBadRequest)
			return
		}

		var body io.Reader
		contentType := "application/octet-stream"

		if strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			file, hdr, err := r.FormFile("file")
			if err != nil {
				http.Error(w, "multipart form requires 'file' field", http.StatusBadRequest)
				return
			}
			defer file.Close()
			body = file
			if hdr.Header.Get("Content-Type") != "" {
				contentType = hdr.Header.Get("Content-Type")
			}
		} else {
			body = r.Body
			if ct := r.Header.Get("Content-Type"); ct != "" {
				contentType = ct
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()

		_, err := client.PutObject(ctx, bucket, objectKey, body, -1, minio.PutObjectOptions{
			ContentType: contentType,
		})
		if err != nil {
			log.Printf("put object %q: %v", objectKey, err)
			http.Error(w, "upload failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"ok":true,"key":"` + objectKey + `"}`))
	}
}

func fmtSize(n int64) string {
	return fmt.Sprintf("%d", n)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %v", r.Method, r.URL.Path, time.Since(start))
	})
}
