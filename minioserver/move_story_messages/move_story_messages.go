package movestorymessages

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

type csvRow struct {
	StoryMessageID string
	StoryID        string
	UserID         string
}

type moveResult struct {
	MovedFolders  []string `json:"moved_folders"`
	MovedObjects  []string `json:"moved_objects"`
	Skipped       []string `json:"skipped"`
	Errors        []string `json:"errors"`
}

func loadCSV(path string) ([]csvRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("csv empty or header-only")
	}

	header := records[0]
	idxMsg, idxStory, idxUser := -1, -1, -1
	for i, h := range header {
		switch strings.TrimSpace(h) {
		case "story_message_id":
			idxMsg = i
		case "story_id":
			idxStory = i
		case "user_id":
			idxUser = i
		}
	}
	if idxMsg < 0 || idxStory < 0 || idxUser < 0 {
		return nil, fmt.Errorf("csv missing required columns story_message_id,story_id,user_id")
	}

	var rows []csvRow
	for _, rec := range records[1:] {
		if len(rec) <= idxUser {
			continue
		}
		rows = append(rows, csvRow{
			StoryMessageID: strings.TrimSpace(rec[idxMsg]),
			StoryID:        strings.TrimSpace(rec[idxStory]),
			UserID:         strings.TrimSpace(rec[idxUser]),
		})
	}
	return rows, nil
}

func defaultCSVPath() string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "story_messages.csv"
	}
	return filepath.Join(filepath.Dir(thisFile), "story_messages.csv")
}

// Handler moves kzen/stories/story-messages/{messageId}/* to
// kzen/users/{userId}/media/stories/{storyId}/story_messages/{messageId}/*
// using story_messages.csv in this package.
func Handler(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		csvPath := strings.TrimSpace(r.URL.Query().Get("csv"))
		if csvPath == "" {
			csvPath = defaultCSVPath()
		}

		rows, err := loadCSV(csvPath)
		if err != nil {
			http.Error(w, "csv: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
		defer cancel()

		result := moveResult{
			MovedFolders: []string{},
			MovedObjects: []string{},
			Skipped:      []string{},
			Errors:       []string{},
		}

		for _, row := range rows {
			if row.StoryMessageID == "" || row.StoryID == "" || row.UserID == "" {
				result.Skipped = append(result.Skipped, fmt.Sprintf("%s: incomplete csv row", row.StoryMessageID))
				continue
			}

			srcPrefix := path.Join("kzen", "stories", "story-messages", row.StoryMessageID) + "/"
			destPrefix := path.Join("kzen", "users", row.UserID, "media", "stories", row.StoryID, "story_messages", row.StoryMessageID) + "/"

			var keys []string
			for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: srcPrefix, Recursive: true}) {
				if obj.Err != nil {
					result.Errors = append(result.Errors, obj.Err.Error())
					continue
				}
				if strings.HasSuffix(obj.Key, "/") {
					continue
				}
				keys = append(keys, obj.Key)
			}
			if len(keys) == 0 {
				result.Skipped = append(result.Skipped, fmt.Sprintf("%s: no objects under %s", row.StoryMessageID, srcPrefix))
				continue
			}

			folderOK := true
			for _, srcKey := range keys {
				rel := strings.TrimPrefix(srcKey, srcPrefix)
				destKey := destPrefix + rel
				_, err := client.CopyObject(ctx,
					minio.CopyDestOptions{Bucket: bucket, Object: destKey},
					minio.CopySrcOptions{Bucket: bucket, Object: srcKey},
				)
				if err != nil {
					msg := fmt.Sprintf("copy %s -> %s: %v", srcKey, destKey, err)
					log.Printf("[move-story-messages] %s", msg)
					result.Errors = append(result.Errors, msg)
					folderOK = false
					continue
				}
				if err := client.RemoveObject(ctx, bucket, srcKey, minio.RemoveObjectOptions{}); err != nil {
					msg := fmt.Sprintf("remove %s after copy: %v", srcKey, err)
					log.Printf("[move-story-messages] %s", msg)
					result.Errors = append(result.Errors, msg)
					folderOK = false
					continue
				}
				result.MovedObjects = append(result.MovedObjects, fmt.Sprintf("%s -> %s", srcKey, destKey))
			}
			if folderOK {
				result.MovedFolders = append(result.MovedFolders, row.StoryMessageID)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"ok":             len(result.Errors) == 0,
			"moved_folders":  len(result.MovedFolders),
			"moved_objects":  len(result.MovedObjects),
			"skipped":        len(result.Skipped),
			"errors":         len(result.Errors),
			"details":        result,
		})
	}
}
