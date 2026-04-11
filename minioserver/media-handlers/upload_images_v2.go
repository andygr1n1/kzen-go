package mediahandlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

const kzenStorageObjectsPrefix = "kzen-storage-objects/"

// isKnownFormFieldV2 marks reserved multipart value keys for V2 (includes deletedSources).
func isKnownFormFieldV2(key string) bool {
	knownFields := map[string]bool{
		"userId":           true,
		"folder":           true,
		"imgPathsToDelete": true,
		"deletedSources":   true,
		"imgPaths":         true,
		"paths":            true,
		"path":             true,
		"imgPath":          true,
		"ids":              true,
		"id":               true,
		"fileIds":          true,
		"fileId":           true,
		"newSources":       true,
		"attachedFiles":    true,
		"files":            true,
		"file":             true,
		"binary":           true,
	}
	return knownFields[key]
}

// objectKeyFromDeleteInput maps a client-provided delete token (bare path or full URL) to a MinIO object key.
func objectKeyFromDeleteInput(raw string, folderPrefix string) string {
	p := strings.TrimSpace(raw)
	if p == "" {
		return ""
	}
	if i := strings.Index(p, "?"); i >= 0 {
		p = p[:i]
	}
	if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
		if u, err := url.Parse(p); err == nil && u.Path != "" {
			p = strings.TrimPrefix(u.Path, "/")
		}
	} else {
		p = strings.TrimPrefix(p, "/")
	}
	// Strip optional public proxy segment: .../kzen-storage-objects/<objectKey>
	if after, ok := strings.CutPrefix(p, kzenStorageObjectsPrefix); ok {
		p = after
	}
	if strings.HasPrefix(p, "kzen/") {
		return p
	}
	pref := strings.TrimPrefix(folderPrefix, "/")
	if pref == "" {
		return p
	}
	return path.Join(pref, p)
}

// UploadImagesToMinioServerV2 accepts multipart like the legacy handler, but:
// - Does not require userId/folder; each file's target path is the full segment after folderPrefix (e.g. userId/media/.../file.jpeg).
// - Form field deletedSources (comma-separated) replaces imgPathsToDelete; values may be full URLs or bare paths (see objectKeyFromDeleteInput).
// - Missing path for an uploaded file returns 400 (no UUID fallback).
func UploadImagesToMinioServerV2(client *minio.Client, bucket string, folderPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := r.ParseMultipartForm(50 << 20); err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServerV2:parse form error"})
			return
		}

		deletedSourcesStr := strings.TrimSpace(r.FormValue("deletedSources"))
		imgPathsStr := strings.TrimSpace(r.FormValue("imgPaths"))
		idsStr := strings.TrimSpace(r.FormValue("ids"))

		var deletedSources []string
		if deletedSourcesStr != "" {
			for _, p := range strings.Split(deletedSourcesStr, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					deletedSources = append(deletedSources, p)
				}
			}
		}

		var imgPaths []string
		var pathById map[string]string
		var pathByFilename map[string]string
		var idById map[string]string
		var orderedIds []string

		pathByFilename = make(map[string]string)
		if r.MultipartForm != nil && r.MultipartForm.Value != nil {
			for key, values := range r.MultipartForm.Value {
				if len(values) > 0 && !isKnownFormFieldV2(key) {
					lowerKey := strings.ToLower(key)
					if strings.HasSuffix(lowerKey, ".jpg") || strings.HasSuffix(lowerKey, ".jpeg") ||
						strings.HasSuffix(lowerKey, ".png") || strings.HasSuffix(lowerKey, ".gif") ||
						strings.HasSuffix(lowerKey, ".svg") || strings.HasSuffix(lowerKey, ".webp") {
						pathByFilename[key] = strings.TrimSpace(values[0])
					}
				}
			}
		}

		if newSourcesStr := strings.TrimSpace(r.FormValue("newSources")); newSourcesStr != "" {
			var payload struct {
				NewSources []struct {
					ID   string `json:"id"`
					Path string `json:"path"`
				} `json:"newSources"`
			}
			if err := json.Unmarshal([]byte(newSourcesStr), &payload); err == nil {
				pathById = make(map[string]string)
				idById = make(map[string]string)
				for _, f := range payload.NewSources {
					id := strings.TrimSpace(f.ID)
					p := strings.TrimSpace(f.Path)
					if id != "" && p != "" {
						pathById[id] = p
						idById[id] = id
						orderedIds = append(orderedIds, id)
						imgPaths = append(imgPaths, p)
					}
				}
			}
		}
		if len(pathById) == 0 {
			if attachedFilesStr := strings.TrimSpace(r.FormValue("attachedFiles")); attachedFilesStr != "" {
				var payload struct {
					AttachedFiles []struct {
						ID   string `json:"id"`
						Path string `json:"path"`
					} `json:"attachedFiles"`
				}
				if err := json.Unmarshal([]byte(attachedFilesStr), &payload); err == nil {
					pathById = make(map[string]string)
					idById = make(map[string]string)
					for _, f := range payload.AttachedFiles {
						id := strings.TrimSpace(f.ID)
						p := strings.TrimSpace(f.Path)
						if id != "" && p != "" {
							pathById[id] = p
							idById[id] = id
						} else if p != "" {
							imgPaths = append(imgPaths, p)
						}
					}
				}
			}
		}
		if len(imgPaths) == 0 && imgPathsStr != "" {
			for _, p := range strings.Split(imgPathsStr, ",") {
				p = strings.TrimSpace(p)
				imgPaths = append(imgPaths, p)
			}
		}
		if len(imgPaths) == 0 {
			if pathsStr := strings.TrimSpace(r.FormValue("paths")); pathsStr != "" {
				for _, p := range strings.Split(pathsStr, ",") {
					p = strings.TrimSpace(p)
					if p != "" {
						imgPaths = append(imgPaths, p)
					}
				}
			}
		}
		if len(imgPaths) == 0 && r.MultipartForm != nil && r.MultipartForm.Value != nil && r.MultipartForm.Value["path"] != nil {
			for _, p := range r.MultipartForm.Value["path"] {
				p = strings.TrimSpace(p)
				if p != "" {
					imgPaths = append(imgPaths, p)
				}
			}
		}
		if len(imgPaths) == 0 {
			if pathStr := strings.TrimSpace(r.FormValue("path")); pathStr != "" {
				imgPaths = []string{pathStr}
			}
		}
		if len(imgPaths) == 0 {
			if r.MultipartForm != nil && r.MultipartForm.Value != nil && r.MultipartForm.Value["imgPath"] != nil {
				imgPaths = r.MultipartForm.Value["imgPath"]
			} else if imgPathStr := strings.TrimSpace(r.FormValue("imgPath")); imgPathStr != "" {
				imgPaths = []string{imgPathStr}
			}
		}
		var ids []string
		if idsStr != "" {
			for _, id := range strings.Split(idsStr, ",") {
				ids = append(ids, strings.TrimSpace(id))
			}
		} else if r.MultipartForm != nil && r.MultipartForm.Value != nil && r.MultipartForm.Value["id"] != nil {
			ids = r.MultipartForm.Value["id"]
		} else if idStr := strings.TrimSpace(r.FormValue("id")); idStr != "" {
			ids = []string{idStr}
		}

		var fileHeaders []*multipart.FileHeader
		if r.MultipartForm != nil && r.MultipartForm.File != nil {
			fileHeaders = r.MultipartForm.File["files"]
			if len(fileHeaders) == 0 {
				fileHeaders = r.MultipartForm.File["file"]
			}
			if len(fileHeaders) == 0 {
				fileHeaders = r.MultipartForm.File["binary"]
			}
		}

		var fileIds []string
		if fileIdsStr := strings.TrimSpace(r.FormValue("fileIds")); fileIdsStr != "" {
			for _, id := range strings.Split(fileIdsStr, ",") {
				id = strings.TrimSpace(id)
				if id != "" {
					fileIds = append(fileIds, id)
				}
			}
		} else if r.MultipartForm != nil && r.MultipartForm.Value != nil && r.MultipartForm.Value["fileId"] != nil {
			fileIds = r.MultipartForm.Value["fileId"]
		}
		if len(fileIds) == 0 && len(orderedIds) > 0 {
			fileIds = orderedIds
		}

		if len(fileHeaders) == 0 && len(deletedSources) == 0 {
			respondJSON(w, http.StatusOK, map[string]any{
				"msg":      "No files to upload or delete",
				"inserted": []map[string]string{},
				"deleted":  []string{},
			})
			return
		}

		// Resolve target path per file; V2 requires a non-empty path for each upload.
		resolvedPaths := make([]string, len(fileHeaders))
		resolvedIDs := make([]string, len(fileHeaders))
		for i, fh := range fileHeaders {
			imgPath := ""
			fileID := ""
			filename := fh.Filename
			if p, ok := pathByFilename[filename]; ok {
				imgPath = p
			}
			if imgPath == "" && i < len(fileIds) && pathById != nil {
				fileID = fileIds[i]
				if p, ok := pathById[fileID]; ok {
					imgPath = p
				}
			}
			if imgPath == "" && i < len(imgPaths) {
				imgPath = imgPaths[i]
			}
			id := ""
			if fileID != "" && idById != nil {
				if mappedID, ok := idById[fileID]; ok {
					id = mappedID
				}
			}
			if id == "" && i < len(ids) {
				id = ids[i]
			}
			resolvedPaths[i] = imgPath
			resolvedIDs[i] = id
		}

		for i := range fileHeaders {
			if strings.TrimSpace(resolvedPaths[i]) == "" {
				respondJSON(w, http.StatusBadRequest, map[string]any{"msg": "kZenUploadImagesToMinioServerV2:missing path for uploaded file"})
				return
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
		defer cancel()

		type uploadResult struct {
			imgPath string
			id      string
			err     error
		}
		results := make([]uploadResult, len(fileHeaders))
		deleteErrors := make([]error, len(deletedSources))
		deletedPaths := make([]string, len(deletedSources))
		var wg sync.WaitGroup

		prefix := strings.TrimPrefix(folderPrefix, "/")

		for i, fh := range fileHeaders {
			wg.Add(1)
			imgPath := strings.TrimSpace(resolvedPaths[i])
			id := resolvedIDs[i]
			go func(idx int, fh *multipart.FileHeader, imgPath, id string) {
				defer wg.Done()

				f, err := fh.Open()
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("open %q: %w", fh.Filename, err)}
					return
				}
				defer f.Close()

				isSvg := fh.Header.Get("Content-Type") == "image/svg+xml" ||
					strings.HasSuffix(strings.ToLower(fh.Filename), ".svg")

				var objectData []byte
				var contentType string

				if isSvg {
					objectData, err = io.ReadAll(f)
					if err != nil {
						results[idx] = uploadResult{err: fmt.Errorf("read %q: %w", fh.Filename, err)}
						return
					}
					contentType = "image/svg+xml"
				} else {
					raw, err := io.ReadAll(f)
					if err != nil {
						results[idx] = uploadResult{err: fmt.Errorf("read %q: %w", fh.Filename, err)}
						return
					}
					objectData, contentType = processRasterImage(raw, fh.Filename)
				}

				objectKey := path.Join(prefix, imgPath)

				_, err = client.PutObject(ctx, bucket, objectKey,
					bytes.NewReader(objectData), int64(len(objectData)),
					minio.PutObjectOptions{ContentType: contentType})
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("put %q: %w", objectKey, err)}
					return
				}
				results[idx] = uploadResult{imgPath: imgPath, id: id}
			}(i, fh, imgPath, id)
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
						log.Printf("uploadImagesV2: path to delete not found (skipping): %q", objectKey)
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
				log.Printf("uploadImagesV2: %v", res.err)
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServerV2:upload error"})
				return
			}
		}
		for _, err := range deleteErrors {
			if err != nil {
				log.Printf("uploadImagesV2: %v", err)
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServerV2:delete error"})
				return
			}
		}

		inserted := make([]map[string]string, 0, len(results))
		for _, res := range results {
			inserted = append(inserted, map[string]string{"id": res.id, "img_path": res.imgPath})
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
