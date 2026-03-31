package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

func TestPublishEventRoutesByDynamicPathField(t *testing.T) {
	var (
		mu    sync.Mutex
		paths []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(ClientSettings{
		URL:          server.URL,
		PathPrefix:   "/litelog-new/api/log/stream",
		PathField:    "stream",
		Timeout:      3 * time.Second,
		ContentType:  "application/json",
		Format:       "json",
		Headers:      map[string]string{},
		BatchPublish: false,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	err = client.PublishEvent(publisher.Event{
		Content: beat.Event{
			Fields: common.MapStr{
				"stream":  "edge_app",
				"message": "hello",
			},
		},
	})
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/litelog-new/api/log/stream/edge_app" {
		t.Fatalf("unexpected paths: %#v", paths)
	}
}

func TestPublishEventOmitsPathFieldFromRequestBodyInDynamicMode(t *testing.T) {
	type requestPayload map[string]interface{}

	var (
		mu      sync.Mutex
		payload requestPayload
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var decoded requestPayload
		if err := json.NewDecoder(r.Body).Decode(&decoded); err != nil {
			t.Fatalf("decode payload failed: %v", err)
		}
		mu.Lock()
		payload = decoded
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(ClientSettings{
		URL:          server.URL,
		PathPrefix:   "/litelog-new/api/log/stream",
		PathField:    "stream",
		Timeout:      3 * time.Second,
		ContentType:  "application/json",
		Format:       "json",
		Headers:      map[string]string{},
		BatchPublish: false,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	err = client.PublishEvent(publisher.Event{
		Content: beat.Event{
			Fields: common.MapStr{
				"stream":  "edge_app",
				"message": "hello",
			},
		},
	})
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}
	if _, exists := payload["stream"]; exists {
		t.Fatalf("dynamic routing field should not be serialized in request body: %#v", payload)
	}
}

func TestBatchPublishEventGroupsEventsByStream(t *testing.T) {
	type requestPayload []map[string]interface{}

	var (
		mu          sync.Mutex
		requestHits = map[string]int{}
		requestSize = map[string]int{}
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var payload requestPayload
		err := json.NewDecoder(r.Body).Decode(&payload)
		if err != nil {
			t.Fatalf("decode payload failed: %v", err)
		}

		mu.Lock()
		requestHits[r.URL.Path]++
		requestSize[r.URL.Path] = len(payload)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(ClientSettings{
		URL:          server.URL,
		PathPrefix:   "/litelog-new/api/log/stream",
		PathField:    "stream",
		Timeout:      3 * time.Second,
		ContentType:  "application/json",
		Format:       "json",
		Headers:      map[string]string{},
		BatchPublish: true,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	err = client.BatchPublishEvent([]publisher.Event{
		{Content: beat.Event{Fields: common.MapStr{"stream": "edge_app", "message": "a"}}},
		{Content: beat.Event{Fields: common.MapStr{"stream": "edge_k3s", "message": "b"}}},
		{Content: beat.Event{Fields: common.MapStr{"stream": "edge_app", "message": "c"}}},
	})
	if err != nil {
		t.Fatalf("BatchPublishEvent failed: %v", err)
	}

	expectedHits := map[string]int{
		"/litelog-new/api/log/stream/edge_app": 1,
		"/litelog-new/api/log/stream/edge_k3s": 1,
	}
	if len(requestHits) != len(expectedHits) {
		t.Fatalf("unexpected request hits: %#v", requestHits)
	}
	for path, expected := range expectedHits {
		if requestHits[path] != expected {
			t.Fatalf("unexpected hit count for %s: got %d want %d", path, requestHits[path], expected)
		}
	}
	if requestSize["/litelog-new/api/log/stream/edge_app"] != 2 {
		t.Fatalf("unexpected batch size for edge_app: %d", requestSize["/litelog-new/api/log/stream/edge_app"])
	}
	if requestSize["/litelog-new/api/log/stream/edge_k3s"] != 1 {
		t.Fatalf("unexpected batch size for edge_k3s: %d", requestSize["/litelog-new/api/log/stream/edge_k3s"])
	}
}

func TestPublishEventFallsBackToStaticPathWhenDynamicFieldMissing(t *testing.T) {
	var (
		mu    sync.Mutex
		paths []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(ClientSettings{
		URL:          server.URL + "/litelog-new/api/log/stream/default",
		PathPrefix:   "/litelog-new/api/log/stream",
		PathField:    "stream",
		Timeout:      3 * time.Second,
		ContentType:  "application/json",
		Format:       "json",
		Headers:      map[string]string{},
		BatchPublish: false,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	err = client.PublishEvent(publisher.Event{
		Content: beat.Event{
			Fields: common.MapStr{
				"message": "no-stream",
			},
		},
	})
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/litelog-new/api/log/stream/default" {
		t.Fatalf("unexpected fallback path: %#v", paths)
	}
}

func TestPublishEventAppendsPathSuffix(t *testing.T) {
	var (
		mu    sync.Mutex
		paths []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(ClientSettings{
		URL:          server.URL,
		PathPrefix:   "/litelog-new/api/log/stream",
		PathField:    "stream",
		PathSuffix:   "/_json",
		Timeout:      3 * time.Second,
		ContentType:  "application/json",
		Format:       "json",
		Headers:      map[string]string{},
		BatchPublish: false,
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	err = client.PublishEvent(publisher.Event{
		Content: beat.Event{
			Fields: common.MapStr{
				"stream":  "edge_app",
				"message": "hello",
			},
		},
	})
	if err != nil {
		t.Fatalf("PublishEvent failed: %v", err)
	}
	if len(paths) != 1 || paths[0] != "/litelog-new/api/log/stream/edge_app/_json" {
		t.Fatalf("unexpected path with suffix: %#v", paths)
	}
}
