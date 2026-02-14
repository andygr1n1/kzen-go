package minioserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7"
)

// mockObjectLister returns predefined objects for testing.
type mockObjectLister struct {
	objects []minio.ObjectInfo
}

func (m *mockObjectLister) ListObjects(_ context.Context, _ string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo, len(m.objects)+1)
	for _, obj := range m.objects {
		if opts.Prefix == "" || (len(obj.Key) >= len(opts.Prefix) && obj.Key[:len(opts.Prefix)] == opts.Prefix) {
			ch <- obj
		}
	}
	close(ch)
	return ch
}

func TestDebugList_Default(t *testing.T) {
	mock := &mockObjectLister{
		objects: []minio.ObjectInfo{
			{Key: "file1.txt"},
			{Key: "file2.txt"},
			{Key: "uploads/doc.pdf"},
		},
	}
	handler := debugList(mock, "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/debug/list", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("got Content-Type %q, want application/json", ct)
	}

	var resp struct {
		Bucket  string   `json:"bucket"`
		Objects []string `json:"objects"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Bucket != "test-bucket" {
		t.Errorf("got bucket %q, want test-bucket", resp.Bucket)
	}
	wantKeys := []string{"file1.txt", "file2.txt", "uploads/doc.pdf"}
	if len(resp.Objects) != len(wantKeys) {
		t.Errorf("got %d objects, want %d: %v", len(resp.Objects), len(wantKeys), resp.Objects)
	}
	for i, key := range wantKeys {
		if i < len(resp.Objects) && resp.Objects[i] != key {
			t.Errorf("objects[%d] = %q, want %q", i, resp.Objects[i], key)
		}
	}
}

func TestDebugList_WithPrefix(t *testing.T) {
	mock := &mockObjectLister{
		objects: []minio.ObjectInfo{
			{Key: "uploads/file1.pdf"},
			{Key: "uploads/file2.pdf"},
			{Key: "other/random.txt"},
		},
	}
	handler := debugList(mock, "test-bucket")

	req := httptest.NewRequest(http.MethodGet, "/debug/list?prefix=uploads/", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", rec.Code, http.StatusOK)
	}

	var resp struct {
		Bucket  string   `json:"bucket"`
		Objects []string `json:"objects"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	wantKeys := []string{"uploads/file1.pdf", "uploads/file2.pdf"}
	if len(resp.Objects) != len(wantKeys) {
		t.Errorf("got %d objects, want %d: %v (prefix filter should exclude other/)", len(resp.Objects), len(wantKeys), resp.Objects)
	}
	for i, key := range wantKeys {
		if i < len(resp.Objects) && resp.Objects[i] != key {
			t.Errorf("objects[%d] = %q, want %q", i, resp.Objects[i], key)
		}
	}
}

func TestDebugList_MethodNotAllowed(t *testing.T) {
	mock := &mockObjectLister{objects: nil}
	handler := debugList(mock, "test-bucket")

	req := httptest.NewRequest(http.MethodPost, "/debug/list", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("got status %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
