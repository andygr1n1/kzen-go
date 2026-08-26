package mediahandlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

const maxGoalFileBytes = 10 << 20 // 10MB per file

// resolveGoalFileContentType returns the stored Content-Type for allowlisted PDF/FB2, or "" if rejected.
func resolveGoalFileContentType(filename, headerCT string) string {
	lowerName := strings.ToLower(filename)
	ct := strings.ToLower(strings.TrimSpace(headerCT))
	if strings.HasSuffix(lowerName, ".pdf") || ct == "application/pdf" {
		return "application/pdf"
	}
	if strings.HasSuffix(lowerName, ".fb2") ||
		ct == "application/x-fictionbook+xml" ||
		ct == "application/fictionbook+xml" {
		return "application/x-fictionbook+xml"
	}
	return ""
}

// UploadFilesToMinioServerV1 accepts multipart path+binary pairs (and optional deletedSources).
// Pass-through PutObject for PDF/FB2 only; each file must be <= 10MB.
func UploadFilesToMinioServerV1(client *minio.Client, bucket string, folderPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Enough headroom for several 10MB files in one batch.
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadFilesToMinioServerV1:parse form error"})
			return
		}

		deletedSourcesStr := strings.TrimSpace(r.FormValue("deletedSources"))
		var deletedSources []string
		if deletedSourcesStr != "" {
			for _, p := range strings.Split(deletedSourcesStr, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					deletedSources = append(deletedSources, p)
				}
			}
		}

		var paths []string
		if r.MultipartForm != nil && r.MultipartForm.Value != nil {
			for _, p := range r.MultipartForm.Value["path"] {
				p = strings.TrimSpace(p)
				if p != "" {
					paths = append(paths, p)
				}
			}
		}
		if len(paths) == 0 {
			if pathStr := strings.TrimSpace(r.FormValue("path")); pathStr != "" {
				paths = []string{pathStr}
			}
		}

		var ids []string
		if r.MultipartForm != nil && r.MultipartForm.Value != nil {
			for _, id := range r.MultipartForm.Value["id"] {
				ids = append(ids, strings.TrimSpace(id))
			}
		} else if idStr := strings.TrimSpace(r.FormValue("id")); idStr != "" {
			ids = []string{idStr}
		}

		var fileHeaders []*multipart.FileHeader
		if r.MultipartForm != nil && r.MultipartForm.File != nil {
			fileHeaders = r.MultipartForm.File["binary"]
			if len(fileHeaders) == 0 {
				fileHeaders = r.MultipartForm.File["files"]
			}
			if len(fileHeaders) == 0 {
				fileHeaders = r.MultipartForm.File["file"]
			}
		}

		if len(fileHeaders) == 0 && len(deletedSources) == 0 {
			respondJSON(w, http.StatusOK, map[string]any{
				"msg":      "No files to upload or delete",
				"inserted": []map[string]string{},
				"deleted":  []string{},
			})
			return
		}

		if len(fileHeaders) > len(paths) {
			respondJSON(w, http.StatusBadRequest, map[string]any{"msg": "kZenUploadFilesToMinioServerV1:missing path for uploaded file"})
			return
		}
		for i := range fileHeaders {
			if strings.TrimSpace(paths[i]) == "" {
				respondJSON(w, http.StatusBadRequest, map[string]any{"msg": "kZenUploadFilesToMinioServerV1:missing path for uploaded file"})
				return
			}
		}

		for _, fh := range fileHeaders {
			ct := resolveGoalFileContentType(fh.Filename, fh.Header.Get("Content-Type"))
			if ct == "" {
				respondJSON(w, http.StatusBadRequest, map[string]any{
					"msg": fmt.Sprintf("kZenUploadFilesToMinioServerV1:unsupported type %q", fh.Filename),
				})
				return
			}
			if fh.Size > 0 && fh.Size > maxGoalFileBytes {
				respondJSON(w, http.StatusBadRequest, map[string]any{
					"msg": fmt.Sprintf("kZenUploadFilesToMinioServerV1:file exceeds 10MB %q", fh.Filename),
				})
				return
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
		defer cancel()

		type uploadResult struct {
			path string
			id   string
			err  error
		}
		results := make([]uploadResult, len(fileHeaders))
		deleteErrors := make([]error, len(deletedSources))
		deletedPaths := make([]string, len(deletedSources))
		var wg sync.WaitGroup

		prefix := strings.TrimPrefix(folderPrefix, "/")

		for i, fh := range fileHeaders {
			wg.Add(1)
			filePath := strings.TrimSpace(paths[i])
			id := ""
			if i < len(ids) {
				id = ids[i]
			}
			go func(idx int, fh *multipart.FileHeader, filePath, id string) {
				defer wg.Done()

				contentType := resolveGoalFileContentType(fh.Filename, fh.Header.Get("Content-Type"))
				if contentType == "" {
					results[idx] = uploadResult{err: fmt.Errorf("unsupported type %q", fh.Filename)}
					return
				}

				f, err := fh.Open()
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("open %q: %w", fh.Filename, err)}
					return
				}
				defer f.Close()

				limited := io.LimitReader(f, maxGoalFileBytes+1)
				objectData, err := io.ReadAll(limited)
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("read %q: %w", fh.Filename, err)}
					return
				}
				if len(objectData) > maxGoalFileBytes {
					results[idx] = uploadResult{err: fmt.Errorf("file exceeds 10MB %q", fh.Filename)}
					return
				}

				objectKey := path.Join(prefix, filePath)
				_, err = client.PutObject(ctx, bucket, objectKey,
					bytes.NewReader(objectData), int64(len(objectData)),
					minio.PutObjectOptions{ContentType: contentType})
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("put %q: %w", objectKey, err)}
					return
				}
				results[idx] = uploadResult{path: filePath, id: id}
			}(i, fh, filePath, id)
		}

		for i, raw := range deletedSources {
			wg.Add(1)
			delKey := objectKeyFromDeleteInput(raw, folderPrefix)
			orig := raw
			go func(idx int, objectKey string, original string) {
				defer wg.Done()
				if objectKey == "" {
					return
				}
				if err := client.RemoveObject(ctx, bucket, objectKey, minio.RemoveObjectOptions{}); err != nil {
					errStr := err.Error()
					if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "NoSuchKey") {
						log.Printf("uploadFilesV1: path to delete not found (skipping): %q", objectKey)
						return
					}
					deleteErrors[idx] = fmt.Errorf("delete %q: %w", objectKey, err)
					return
				}
				deletedPaths[idx] = original
			}(i, delKey, orig)
		}

		wg.Wait()

		for _, res := range results {
			if res.err != nil {
				log.Printf("uploadFilesV1: %v", res.err)
				msg := "kZenUploadFilesToMinioServerV1:upload error"
				if strings.Contains(res.err.Error(), "exceeds 10MB") {
					msg = "kZenUploadFilesToMinioServerV1:file exceeds 10MB"
					respondJSON(w, http.StatusBadRequest, map[string]any{"msg": msg})
					return
				}
				if strings.Contains(res.err.Error(), "unsupported type") {
					respondJSON(w, http.StatusBadRequest, map[string]any{"msg": "kZenUploadFilesToMinioServerV1:unsupported type"})
					return
				}
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": msg})
				return
			}
		}
		for _, err := range deleteErrors {
			if err != nil {
				log.Printf("uploadFilesV1: %v", err)
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadFilesToMinioServerV1:delete error"})
				return
			}
		}

		inserted := make([]map[string]string, 0, len(results))
		for _, res := range results {
			inserted = append(inserted, map[string]string{"id": res.id, "path": res.path})
		}
		deleted := make([]string, 0, len(deletedPaths))
		for _, p := range deletedPaths {
			if p != "" {
				deleted = append(deleted, p)
			}
		}
		respondJSON(w, http.StatusOK, map[string]any{"inserted": inserted, "deleted": deleted})
	}
}
