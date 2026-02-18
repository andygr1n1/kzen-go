package minioserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	xdraw "golang.org/x/image/draw"
)

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func objectsHandler(client *minio.Client, bucket string) http.HandlerFunc {
	return objectsHandlerWithPrefix(client, bucket, "/objects/")
}

func objectsHandlerWithPrefix(client *minio.Client, bucket string, pathPrefix string) http.HandlerFunc {
	get := proxyGetWithPrefix(client, bucket, pathPrefix)
	post := proxyPostWithPrefix(client, bucket, pathPrefix)
	put := proxyPutWithPrefix(client, bucket, pathPrefix)
	del := proxyDeleteWithPrefix(client, bucket, pathPrefix)
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

// objectLister abstracts MinIO ListObjects for testability.
type objectLister interface {
	ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

func debugList(client objectLister, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		/* prefix is the folder -> http://localhost:9004/debug/list?prefix=kzen/ */
		prefix := r.URL.Query().Get("prefix")

		log.Printf("debugList: %s", prefix)

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

const statRetries = 3
const statRetryDelay = 50 * time.Millisecond

func proxyGet(client *minio.Client, bucket string) http.HandlerFunc {
	return proxyGetWithPrefix(client, bucket, "/objects/")
}

func proxyGetWithPrefix(client *minio.Client, bucket string, pathPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, pathPrefix)
		if objectKey == "" {
			http.Error(w, "object key required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		// StatObject can intermittently return "Access Denied" under concurrent load.
		// Retry a few times before failing.
		var info minio.ObjectInfo
		var err error
		for attempt := 0; attempt < statRetries; attempt++ {
			info, err = client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
			if err == nil {
				break
			}
			if !strings.Contains(err.Error(), "Access Denied") {
				break
			}
			if attempt < statRetries-1 {
				time.Sleep(statRetryDelay)
			}
		}
		if err != nil {
			log.Printf("stat object %q bucket=%q: %v", objectKey, bucket, err)
			w.Header().Set("X-MinIO-Error", err.Error())
			if strings.Contains(err.Error(), "does not exist") {
				http.Error(w, "object not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to get object info", http.StatusInternalServerError)
			return
		}

		obj, err := client.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("GET %q bucket=%q err: %v", objectKey, bucket, err)
			w.Header().Set("X-MinIO-Error", err.Error())
			http.Error(w, "object not found", http.StatusNotFound)
			return
		}
		defer obj.Close()

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
	return proxyPostWithPrefix(client, bucket, "/objects/")
}

func proxyPostWithPrefix(client *minio.Client, bucket string, pathPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, pathPrefix)
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

func proxyPutWithPrefix(client *minio.Client, bucket string, pathPrefix string) http.HandlerFunc {
	return proxyPostWithPrefix(client, bucket, pathPrefix)
}

// resizeToFit scales img to fit within maxW×maxH while preserving aspect ratio.
// If the image already fits, it is returned unchanged (no enlargement).
func resizeToFit(img image.Image, maxW, maxH int) image.Image {
	bounds := img.Bounds()
	origW := bounds.Dx()
	origH := bounds.Dy()
	if origW <= maxW && origH <= maxH {
		return img
	}

	scaleW := float64(maxW) / float64(origW)
	scaleH := float64(maxH) / float64(origH)
	scale := scaleW
	if scaleH < scaleW {
		scale = scaleH
	}

	newW := int(float64(origW) * scale)
	newH := int(float64(origH) * scale)
	if newW < 1 {
		newW = 1
	}
	if newH < 1 {
		newH = 1
	}

	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	xdraw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, xdraw.Over, nil)
	return dst
}

// processRasterImage decodes a raster image, resizes it to fit within 1920×1080
// (without enlargement), and encodes it as JPEG (quality 100).
// Falls back to JPEG-only (no resize) on resize error, or raw bytes on total failure.
func processRasterImage(data []byte, filename string) ([]byte, string) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		log.Printf("uploadImages: decode %q failed: %v, uploading raw", filename, err)
		return data, "application/octet-stream"
	}

	resized := resizeToFit(img, 1920, 1080)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: 100}); err != nil {
		log.Printf("uploadImages: jpeg encode %q failed: %v, trying without resize", filename, err)
		buf.Reset()
		// Fallback: encode original without resize
		if err2 := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 100}); err2 != nil {
			log.Printf("uploadImages: fallback encode %q also failed: %v, uploading raw", filename, err2)
			return data, "application/octet-stream"
		}
	}
	return buf.Bytes(), "image/jpeg"
}

// isKnownFormField checks if a form field key is a known/reserved field name
func isKnownFormField(key string) bool {
	knownFields := map[string]bool{
		"userId":           true,
		"folder":           true,
		"imgPathsToDelete": true,
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

// Accepts multipart form: files (multiple), userId, folder, imgPathsToDelete (comma-separated, optional),
// imgPaths (comma-separated, optional), ids (comma-separated, optional), or imgPath/id (singular). When imgPaths and ids are provided
// in same order as files, they are used as object paths; otherwise a new filename is generated.
// img_path already includes the extension (e.g. userId_id_folder.jpeg).
// When folderPrefix is provided, it is prepended to all MinIO object keys (uploads and deletes).
// Old images listed in imgPathsToDelete are removed.
// All uploads and deletes run concurrently.
// Returns on 200: { inserted: [{id, img_path}], deleted: [img_path1, img_path2, ...] }
func uploadImagesToMinioServer(client *minio.Client, bucket string, folderPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := r.ParseMultipartForm(50 << 20); err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServer:parse form error"})
			return
		}

		userId := strings.TrimSpace(r.FormValue("userId"))
		folder := strings.TrimSpace(r.FormValue("folder"))
		imgPathsToDeleteStr := strings.TrimSpace(r.FormValue("imgPathsToDelete"))
		imgPathsStr := strings.TrimSpace(r.FormValue("imgPaths"))
		idsStr := strings.TrimSpace(r.FormValue("ids"))

		if userId == "" {
			respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServer:bad data"})
			return
		}
		if folder == "" {
			respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServer:folder is required"})
			return
		}

		var imgPathsToDelete []string
		if imgPathsToDeleteStr != "" {
			for _, p := range strings.Split(imgPathsToDeleteStr, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					imgPathsToDelete = append(imgPathsToDelete, p)
				}
			}
		}

		var imgPaths []string
		var pathById map[string]string      // map from file id to path
		var pathByFilename map[string]string // map from original filename to target path
		var idById map[string]string        // map from file id to id (for response)
		var orderedIds []string              // ordered list of ids from newSources for index matching
		
		// Build pathByFilename map from form values (filename -> target path mappings)
		// FormData has entries like: "498d7930dc27f5d5c6877bccb102fd65.jpg" -> "eb000d27-a5cd-4994-b8ad-bebb9cbaa281/acdcd19e-27eb-4441-bada-5ee1012e7378.jpg"
		pathByFilename = make(map[string]string)
		if r.MultipartForm != nil && r.MultipartForm.Value != nil {
			for key, values := range r.MultipartForm.Value {
				// Check if key looks like a filename (has extension) and is not a known form field
				if len(values) > 0 && !isKnownFormField(key) {
					// Check if it's a filename by looking for common image extensions
					lowerKey := strings.ToLower(key)
					if strings.HasSuffix(lowerKey, ".jpg") || strings.HasSuffix(lowerKey, ".jpeg") ||
						strings.HasSuffix(lowerKey, ".png") || strings.HasSuffix(lowerKey, ".gif") ||
						strings.HasSuffix(lowerKey, ".svg") || strings.HasSuffix(lowerKey, ".webp") {
						pathByFilename[key] = strings.TrimSpace(values[0])
					}
				}
			}
		}
		
		// Try newSources JSON first (e.g. { "newSources": [ { "id": "rc-upload-...", "path": "id/filename.jpg" }, ... ] })
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
						imgPaths = append(imgPaths, p) // Also add to imgPaths for index matching
					}
				}
			}
		}
		// Try attachedFiles JSON (e.g. { "attachedFiles": [ { "id": "...", "path": "id/filename.jpg" }, ... ] })
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
							// Fallback: if no id, use path by index
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

		// Use ordered files from "files", "file", or "binary" field.
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

		// Get file ids to match files with their paths
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
		// If we have orderedIds from newSources but no explicit fileIds, use orderedIds for matching
		if len(fileIds) == 0 && len(orderedIds) > 0 {
			fileIds = orderedIds
		}

		// If no files to upload and no files to delete, return success
		if len(fileHeaders) == 0 && len(imgPathsToDelete) == 0 {
			respondJSON(w, http.StatusOK, map[string]any{
				"msg":      "No files to upload or delete",
				"inserted": []map[string]string{},
				"deleted":  []string{},
			})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 120*time.Second)
		defer cancel()

		type uploadResult struct {
			imgPath string // final img_path (used for object key or returned to client)
			id      string
			err     error
		}
		results := make([]uploadResult, len(fileHeaders))
		deleteErrors := make([]error, len(imgPathsToDelete))
		deletedPaths := make([]string, len(imgPathsToDelete))
		var wg sync.WaitGroup

		// Upload each file concurrently (only if there are files).
		for i, fh := range fileHeaders {
			wg.Add(1)
			imgPath := ""
			fileId := ""
			
			// First priority: Match by filename (from formData filename -> path mappings)
			// FormData has entries like: "498d7930dc27f5d5c6877bccb102fd65.jpg" -> "eb000d27-a5cd-4994-b8ad-bebb9cbaa281/acdcd19e-27eb-4441-bada-5ee1012e7378.jpg"
			filename := fh.Filename
			if p, ok := pathByFilename[filename]; ok {
				imgPath = p
			}
			
			// Second priority: Try to match by file id
			if imgPath == "" && i < len(fileIds) && pathById != nil {
				fileId = fileIds[i]
				if p, ok := pathById[fileId]; ok {
					imgPath = p
				}
			}
			// Fallback to array index matching
			if imgPath == "" && i < len(imgPaths) {
				imgPath = imgPaths[i]
			}
			
			id := ""
			// Get id from idById map if available
			if fileId != "" && idById != nil {
				if mappedId, ok := idById[fileId]; ok {
					id = mappedId
				}
			}
			// Fallback to array index matching
			if id == "" && i < len(ids) {
				id = ids[i]
			}
			
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
				var ext string

				if isSvg {
					objectData, err = io.ReadAll(f)
					if err != nil {
						results[idx] = uploadResult{err: fmt.Errorf("read %q: %w", fh.Filename, err)}
						return
					}
					contentType = "image/svg+xml"
					ext = ".svg"
				} else {
					raw, err := io.ReadAll(f)
					if err != nil {
						results[idx] = uploadResult{err: fmt.Errorf("read %q: %w", fh.Filename, err)}
						return
					}
					objectData, contentType = processRasterImage(raw, fh.Filename)
					if contentType == "image/jpeg" {
						ext = ".jpeg"
					} else {
						ext = path.Ext(fh.Filename)
						if ext == "" {
							ext = ".bin"
						}
					}
				}

				var objectKey string
				var finalImgPath string
				if imgPath != "" {
					finalImgPath = imgPath
					objectKey = path.Join(folder, imgPath)
				} else {
					fileName := fmt.Sprintf("%s_%s%s", userId, uuid.New().String(), ext)
					finalImgPath = fileName
					objectKey = path.Join(folder, fileName)
				}
				if folderPrefix != "" {
					prefix := strings.TrimPrefix(folderPrefix, "/")
					objectKey = path.Join(prefix, objectKey)
				}

				_, err = client.PutObject(ctx, bucket, objectKey,
					bytes.NewReader(objectData), int64(len(objectData)),
					minio.PutObjectOptions{ContentType: contentType})
				if err != nil {
					results[idx] = uploadResult{err: fmt.Errorf("put %q: %w", objectKey, err)}
					return
				}
				results[idx] = uploadResult{imgPath: finalImgPath, id: id}
			}(i, fh, imgPath, id)
		}

		// Delete old images concurrently. imgPathsToDelete: full keys (folder/path) or filenames (path only).
		for i, p := range imgPathsToDelete {
			wg.Add(1)
			objKey := p
			if p != "" && !strings.Contains(p, "/") {
				objKey = path.Join(folder, p)
			}
			if folderPrefix != "" {
				prefix := strings.TrimPrefix(folderPrefix, "/")
				objKey = path.Join(prefix, objKey)
			}
			go func(idx int, delKey string) {
				defer wg.Done()
				if err := client.RemoveObject(ctx, bucket, delKey, minio.RemoveObjectOptions{}); err != nil {
					errStr := err.Error()
					if strings.Contains(errStr, "does not exist") || strings.Contains(errStr, "NoSuchKey") {
						log.Printf("uploadImages: path to delete not found (skipping): %q", delKey)
						return
					}
					deleteErrors[idx] = fmt.Errorf("delete %q: %w", delKey, err)
					return
				}
				deletedPaths[idx] = p // return original path as sent by client
			}(i, objKey)
		}

		wg.Wait()

		for _, res := range results {
			if res.err != nil {
				log.Printf("uploadImages: %v", res.err)
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServer:upload error"})
				return
			}
		}
		for _, err := range deleteErrors {
			if err != nil {
				log.Printf("uploadImages: %v", err)
				respondJSON(w, http.StatusInternalServerError, map[string]any{"msg": "kZenUploadImagesToMinioServer:delete error"})
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

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func proxyDelete(client *minio.Client, bucket string) http.HandlerFunc {
	return proxyDeleteWithPrefix(client, bucket, "/objects/")
}

func proxyDeleteWithPrefix(client *minio.Client, bucket string, pathPrefix string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectKey := strings.TrimPrefix(r.URL.Path, pathPrefix)
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
