package minioserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func objectsHandler(client *minio.Client, bucket string) http.HandlerFunc {
	get := proxyGet(client, bucket)
	post := proxyPost(client, bucket)
	put := proxyPut(client, bucket)
	del := proxyDelete(client, bucket)
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			get(w, r)
		case http.MethodHead:
			get(w, r)
		case http.MethodPost:
			post(w, r)
		case http.MethodPut:
			put(w, r)
		case http.MethodDelete:
			del(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func batchHandler(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			batchGet(client, bucket, w, r)
		case http.MethodPost:
			batchPost(client, bucket, w, r)
		case http.MethodDelete:
			batchDelete(client, bucket, w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func batchGet(client *minio.Client, bucket string, w http.ResponseWriter, r *http.Request) {
	keysParam := r.URL.Query().Get("keys")
	if keysParam == "" {
		http.Error(w, "keys query required (e.g. ?keys=a.jpg,b.jpg)", http.StatusBadRequest)
		return
	}
	keys := strings.Split(keysParam, ",")
	for i, k := range keys {
		keys[i] = strings.TrimSpace(k)
	}
	if len(keys) == 0 {
		http.Error(w, "at least one key required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	type result struct {
		key  string
		data []byte
		ct   string
		err  error
	}
	results := make([]result, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		if key == "" {
			continue
		}
		wg.Add(1)
		go func(idx int, objKey string) {
			defer wg.Done()
			obj, err := client.GetObject(ctx, bucket, objKey, minio.GetObjectOptions{})
			if err != nil {
				results[idx] = result{key: objKey, err: err}
				return
			}
			defer obj.Close()
			info, err := obj.Stat()
			if err != nil {
				results[idx] = result{key: objKey, err: err}
				return
			}
			data, err := io.ReadAll(obj)
			if err != nil {
				results[idx] = result{key: objKey, err: err}
				return
			}
			ct := info.ContentType
			if ct == "" {
				ct = "application/octet-stream"
			}
			results[idx] = result{key: objKey, data: data, ct: ct}
		}(i, key)
	}
	wg.Wait()

	mpw := multipart.NewWriter(w)
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+mpw.Boundary())
	w.WriteHeader(http.StatusOK)

	for _, res := range results {
		if res.err != nil {
			log.Printf("batch GET %q: %v", res.key, res.err)
			continue
		}
		part, _ := mpw.CreatePart(map[string][]string{
			"Content-Type":        {res.ct},
			"Content-Disposition": {`form-data; name="` + res.key + `"; filename="` + res.key + `"`},
		})
		part.Write(res.data)
	}
	mpw.Close()
}

func batchPost(client *minio.Client, bucket string, w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if !strings.Contains(ct, "multipart/form-data") {
		http.Error(w, "multipart form required", http.StatusBadRequest)
		return
	}
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		http.Error(w, "invalid multipart form", http.StatusBadRequest)
		return
	}

	keysParam := r.FormValue("keys")
	if keysParam == "" {
		http.Error(w, "keys form field required (comma-separated object keys)", http.StatusBadRequest)
		return
	}
	keyList := strings.Split(keysParam, ",")
	for i, k := range keyList {
		keyList[i] = strings.TrimSpace(k)
	}

	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		files = r.MultipartForm.File["file"]
	}
	if len(files) != len(keyList) {
		http.Error(w, fmt.Sprintf("keys count (%d) must match files count (%d)", len(keyList), len(files)), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
	defer cancel()

	type uploadResult struct {
		Key string `json:"key"`
		OK  bool   `json:"ok"`
		Err string `json:"error,omitempty"`
	}
	results := make([]uploadResult, len(keyList))
	var wg sync.WaitGroup
	for i := range keyList {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			objKey := keyList[idx]
			file := files[idx]
			f, err := file.Open()
			if err != nil {
				results[idx] = uploadResult{Key: objKey, Err: err.Error()}
				return
			}
			defer f.Close()
			contentType := file.Header.Get("Content-Type")
			if contentType == "" {
				contentType = "application/octet-stream"
			}
			_, err = client.PutObject(ctx, bucket, objKey, f, -1, minio.PutObjectOptions{ContentType: contentType})
			if err != nil {
				results[idx] = uploadResult{Key: objKey, Err: err.Error()}
				return
			}
			results[idx] = uploadResult{Key: objKey, OK: true}
		}(i)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"uploaded": results})
}

func batchDelete(client *minio.Client, bucket string, w http.ResponseWriter, r *http.Request) {
	keysParam := r.URL.Query().Get("keys")
	if keysParam == "" {
		http.Error(w, "keys query required (e.g. ?keys=a.jpg,b.jpg)", http.StatusBadRequest)
		return
	}
	keys := strings.Split(keysParam, ",")
	for i, k := range keys {
		keys[i] = strings.TrimSpace(k)
	}
	if len(keys) == 0 {
		http.Error(w, "at least one key required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	type delResult struct {
		Key string `json:"key"`
		OK  bool   `json:"ok"`
		Err string `json:"error,omitempty"`
	}
	results := make([]delResult, len(keys))
	var wg sync.WaitGroup
	for i, key := range keys {
		if key == "" {
			continue
		}
		wg.Add(1)
		go func(idx int, objKey string) {
			defer wg.Done()
			err := client.RemoveObject(ctx, bucket, objKey, minio.RemoveObjectOptions{})
			if err != nil {
				results[idx] = delResult{Key: objKey, Err: err.Error()}
				return
			}
			results[idx] = delResult{Key: objKey, OK: true}
		}(i, key)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"deleted": results})
}

func debugList(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		ch := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
		var keys []string
		for obj := range ch {
			if obj.Err != nil {
				log.Printf("list objects: %v", obj.Err)
				http.Error(w, obj.Err.Error(), http.StatusInternalServerError)
				return
			}
			keys = append(keys, obj.Key)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"bucket": bucket, "objects": keys})
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
			log.Printf("GET %q bucket=%q err: %v", objectKey, bucket, err)
			w.Header().Set("X-MinIO-Error", err.Error())
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

func proxyPut(client *minio.Client, bucket string) http.HandlerFunc {
	return proxyPost(client, bucket)
}

func proxyDelete(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, "/objects/")
		if objectKey == "" {
			http.Error(w, "object key required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		err := client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{})
		if err != nil {
			log.Printf("DELETE %q: %v", objectKey, err)
			http.Error(w, "delete failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true,"deleted":"` + objectKey + `"}`))
	}
}

func fmtSize(n int64) string {
	return fmt.Sprintf("%d", n)
}
