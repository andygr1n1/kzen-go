package minioserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

type copyGoalMediaRequest struct {
	UserID       string `json:"userId"`
	SourceGoalID string `json:"sourceGoalId"`
	TargetGoalID string `json:"targetGoalId"`
}

type copyGoalMediaResult struct {
	Copied  int      `json:"copied"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
}

// copyGoalMediaHandler copies all objects under users/{userId}/media/goals/{sourceGoalId}/
// to the matching paths with targetGoalId (folder + filename segments).
func copyGoalMediaHandler(client *minio.Client, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req copyGoalMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		userID := strings.TrimSpace(req.UserID)
		sourceID := strings.TrimSpace(req.SourceGoalID)
		targetID := strings.TrimSpace(req.TargetGoalID)
		if userID == "" || sourceID == "" || targetID == "" {
			http.Error(w, "userId, sourceGoalId, and targetGoalId are required", http.StatusBadRequest)
			return
		}
		if sourceID == targetID {
			http.Error(w, "source and target goal ids must differ", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
		defer cancel()

		prefix := fmt.Sprintf("kzen/users/%s/media/goals/%s/", userID, sourceID)
		result := copyGoalMediaResult{}

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
			destKey := strings.ReplaceAll(key, sourceID, targetID)
			if destKey == key {
				result.Skipped++
				continue
			}

			_, err := client.CopyObject(ctx,
				minio.CopyDestOptions{Bucket: bucket, Object: destKey},
				minio.CopySrcOptions{Bucket: bucket, Object: key},
			)
			if err != nil {
				msg := fmt.Sprintf("copy %s -> %s: %v", key, destKey, err)
				log.Printf("[copy-goal-media] %s", msg)
				result.Errors = append(result.Errors, msg)
				continue
			}
			result.Copied++
		}

		if len(result.Errors) > 0 {
			w.WriteHeader(http.StatusInternalServerError)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}
