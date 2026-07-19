package minioserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

var uuidInNameRe = regexp.MustCompile(`(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// parseStoryIDFromStoryImageFilename extracts story_id from names like
// `{userId}_{storyId}_stories.jpeg` (2nd UUID when 1st matches pathUserID)
// or `{storyId}_{uuid}.jpeg` (1st UUID when 1st is not the path user).
func parseStoryIDFromStoryImageFilename(filename, pathUserID string) string {
	matches := uuidInNameRe.FindAllString(filename, -1)
	if len(matches) >= 2 {
		if pathUserID != "" && strings.EqualFold(matches[0], pathUserID) {
			return matches[1]
		}
		return matches[0]
	}
	if len(matches) >= 1 {
		return matches[0]
	}
	return ""
}

type createStoryFolderResult struct {
	Moved   []string `json:"moved"`
	Skipped []string `json:"skipped"`
	Errors  []string `json:"errors"`
}

// createStoryFolderHandler lists kzen/users/*/media/stories/* files that sit directly
// under stories/ (not already in a story_id subfolder), parses story_id from the filename,
// and moves each object to kzen/users/{userId}/media/stories/{storyId}/{filename}.
func createStoryFolderHandler(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
		defer cancel()

		result := createStoryFolderResult{
			Moved:   []string{},
			Skipped: []string{},
			Errors:  []string{},
		}

		prefix := "kzen/users/"
		for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if obj.Err != nil {
				result.Errors = append(result.Errors, obj.Err.Error())
				continue
			}
			key := obj.Key
			if strings.HasSuffix(key, "/") {
				continue
			}

			// Expect: kzen/users/{userId}/media/stories/{file}  (no extra slash after stories/)
			rest := strings.TrimPrefix(key, "kzen/users/")
			parts := strings.Split(rest, "/")
			// users/{userId}/media/stories/{file} → 4 parts
			if len(parts) != 4 || parts[1] != "media" || parts[2] != "stories" {
				continue
			}
			userID := parts[0]
			filename := parts[3]
			if userID == "" || filename == "" {
				continue
			}

			storyID := parseStoryIDFromStoryImageFilename(filename, userID)
			if storyID == "" {
				result.Skipped = append(result.Skipped, fmt.Sprintf("%s: could not parse story_id", key))
				continue
			}

			destKey := path.Join("kzen", "users", userID, "media", "stories", storyID, filename)
			if destKey == key {
				result.Skipped = append(result.Skipped, key+": already at destination")
				continue
			}

			_, err := client.CopyObject(ctx,
				minio.CopyDestOptions{Bucket: bucket, Object: destKey},
				minio.CopySrcOptions{Bucket: bucket, Object: key},
			)
			if err != nil {
				msg := fmt.Sprintf("copy %s -> %s: %v", key, destKey, err)
				log.Printf("[create-story-folder] %s", msg)
				result.Errors = append(result.Errors, msg)
				continue
			}
			if err := client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
				msg := fmt.Sprintf("remove %s after copy to %s: %v", key, destKey, err)
				log.Printf("[create-story-folder] %s", msg)
				result.Errors = append(result.Errors, msg)
				continue
			}
			result.Moved = append(result.Moved, fmt.Sprintf("%s -> %s", key, destKey))
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":     len(result.Errors) == 0,
			"moved":  len(result.Moved),
			"skipped": len(result.Skipped),
			"errors": len(result.Errors),
			"details": result,
		})
	}
}
