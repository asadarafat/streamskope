package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKubernetesStoreCreatesDurableSessionAndOwnedResources(t *testing.T) {
	t.Parallel()

	var mutex sync.Mutex
	requests := make([]string, 0)
	bodies := make(map[string]map[string]any)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Fatalf("authorization header was not applied")
		}
		key := request.Method + " " + request.URL.Path
		mutex.Lock()
		requests = append(requests, key)
		mutex.Unlock()
		if request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/producers/events") {
			writeJSON(t, response, http.StatusOK, map[string]any{
				"apiVersion": "kafka.eda.nokia.com/v1",
				"kind":       "Producer",
				"metadata":   map[string]any{"name": "events", "namespace": "eda"},
				"spec":       map[string]any{"brokers": "old:9092", "exports": []any{map[string]any{"topic": "alarms"}}},
			})
			return
		}
		if request.Method == http.MethodGet {
			writeJSON(t, response, http.StatusNotFound, map[string]any{"reason": "NotFound"})
			return
		}
		if request.Body != nil {
			body, readError := io.ReadAll(request.Body)
			if readError != nil {
				t.Fatalf("read request: %v", readError)
			}
			if len(body) > 0 {
				var decoded map[string]any
				if decodeError := json.Unmarshal(body, &decoded); decodeError != nil {
					t.Fatalf("decode request: %v", decodeError)
				}
				mutex.Lock()
				bodies[key] = decoded
				mutex.Unlock()
			}
		}
		writeJSON(t, response, http.StatusCreated, map[string]any{})
	}))
	t.Cleanup(server.Close)

	store := NewKubernetesStore(server.URL, "fixture-token", server.Client())
	request := validRequest()
	session := CaptureSession{
		ExpiresAt: time.Unix(1_800_000_120, 0).UTC(),
		Finalizer: finalizerName,
		ID:        request.ID,
		Phase:     "Pending",
		Request:   request,
	}
	if _, err := store.Create(context.Background(), session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := store.EnsureOwned(context.Background(), session); err != nil {
		t.Fatalf("ensure resources: %v", err)
	}

	mutex.Lock()
	defer mutex.Unlock()
	for _, expected := range []string{
		"POST /apis/capture.streamskope.io/v1alpha1/namespaces/eda/capturesessions",
		"POST /api/v1/namespaces/eda/services",
		"POST /apis/apps/v1/namespaces/eda/statefulsets",
		"POST /apis/kafka.eda.nokia.com/v1/namespaces/eda/producers",
		"PATCH /apis/capture.streamskope.io/v1alpha1/namespaces/eda/capturesessions/7ac7d717-8ef0-4a55-bd33-5b723e6b6918/status",
	} {
		if !containsString(requests, expected) {
			t.Fatalf("missing request %s in %v", expected, requests)
		}
	}
	workload := bodies["POST /apis/apps/v1/namespaces/eda/statefulsets"]
	service := bodies["POST /api/v1/namespaces/eda/services"]
	if nestedString(service, "spec", "type") != "ClusterIP" {
		t.Fatal("capture service is not private")
	}
	if nestedString(workload, "metadata", "labels", sessionLabel) != request.ID {
		t.Fatal("workload does not carry exact session ownership")
	}
}

func TestKubernetesStoreRefusesToDeleteUnownedResources(t *testing.T) {
	t.Parallel()

	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			deleteCalls++
		}
		writeJSON(t, response, http.StatusOK, map[string]any{
			"metadata": map[string]any{
				"labels": map[string]any{managedByLabel: "someone-else", sessionLabel: validRequest().ID},
			},
		})
	}))
	t.Cleanup(server.Close)
	store := NewKubernetesStore(server.URL, "fixture-token", server.Client())
	session := CaptureSession{ID: validRequest().ID, Request: validRequest()}

	if err := store.CleanupOwned(context.Background(), session); err == nil {
		t.Fatal("cleanup accepted an unowned resource")
	}
	if deleteCalls != 0 {
		t.Fatalf("unowned resource received %d delete requests", deleteCalls)
	}
}

func TestKubernetesStorePersistsStoppedLeaseBeforeCleanup(t *testing.T) {
	t.Parallel()
	var patch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPatch || !strings.HasSuffix(request.URL.Path, "/capturesessions/"+validRequest().ID) {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&patch); err != nil {
			t.Errorf("decode patch: %v", err)
		}
		writeJSON(t, response, http.StatusOK, map[string]any{})
	}))
	t.Cleanup(server.Close)
	store := NewKubernetesStore(server.URL, "fixture-token", server.Client())
	request := validRequest()
	expiry := time.Unix(1_800_000_000, 0).UTC()
	session := CaptureSession{ID: request.ID, Request: request, ExpiresAt: expiry, Phase: "Stopping"}
	if _, err := store.Update(context.Background(), session); err != nil {
		t.Fatalf("persist stop: %v", err)
	}
	if nestedString(patch, "spec", "leaseExpiresAt") != expiry.Format(time.RFC3339) {
		t.Fatalf("stopped lease was not persisted: %v", patch)
	}
}

func TestKubernetesStoreConfirmsOwnedResourceDeletion(t *testing.T) {
	t.Parallel()
	sessionID := validRequest().ID
	getsAfterDelete := 0
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodDelete {
			deleted = true
			writeJSON(t, response, http.StatusOK, map[string]any{})
			return
		}
		if deleted {
			getsAfterDelete++
			if getsAfterDelete >= 3 {
				response.WriteHeader(http.StatusNotFound)
				return
			}
		}
		writeJSON(t, response, http.StatusOK, map[string]any{
			"metadata": map[string]any{
				"labels": map[string]any{managedByLabel: managedByValue, sessionLabel: sessionID},
			},
		})
	}))
	t.Cleanup(server.Close)
	store := NewKubernetesStore(server.URL, "fixture-token", server.Client())
	if err := store.deleteOwned(context.Background(), sessionID, "/owned"); err != nil {
		t.Fatalf("delete owned resource: %v", err)
	}
	if getsAfterDelete < 3 {
		t.Fatalf("cleanup returned before confirming deletion: %d checks", getsAfterDelete)
	}
}

func TestClusterProducerUsesNamespacedKubernetesPath(t *testing.T) {
	t.Parallel()
	source := SourceReference{
		APIVersion: "kafka.eda.nokia.com/v1",
		Kind:       "ClusterProducer",
		Name:       "events",
		Namespace:  "default",
	}
	if got, want := producerPath(source, source.Name), "/apis/kafka.eda.nokia.com/v1/namespaces/default/clusterproducers/events"; got != want {
		t.Fatalf("cluster producer path = %q, want %q", got, want)
	}
}

func TestKubernetesStoreDoesNotPatchUnchangedOwnedResources(t *testing.T) {
	t.Parallel()
	request := validRequest()
	start := time.Unix(1_800_000_000, 0).UTC()
	session := CaptureSession{ID: request.ID, Request: request, ExpiresAt: start.Add(120 * time.Second)}
	source := map[string]any{
		"apiVersion": "kafka.eda.nokia.com/v1",
		"kind":       "Producer",
		"metadata":   map[string]any{"name": "events", "namespace": "eda"},
		"spec":       map[string]any{"exports": []any{map[string]any{"topic": "alarms"}}},
	}
	resources, err := BuildCaptureResources(source, request, start)
	if err != nil {
		t.Fatalf("plan resources: %v", err)
	}
	resourceName := captureResourceName(request.ID)
	existing := map[string]map[string]any{
		"/api/v1/namespaces/eda/services/" + resourceName:                       resources.Service,
		"/apis/apps/v1/namespaces/eda/statefulsets/" + resourceName:             resources.Workload,
		"/apis/kafka.eda.nokia.com/v1/namespaces/eda/producers/" + resourceName: resources.Exporter,
	}
	resourceSpec := existing["/apis/kafka.eda.nokia.com/v1/namespaces/eda/producers/"+resourceName]["spec"].(map[string]any)
	resourceSpec["timeout"] = "10s" // EDA may add defaults not owned by the capture plan.
	patches := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/producers/events") {
			writeJSON(t, response, http.StatusOK, source)
			return
		}
		if request.Method == http.MethodGet {
			if value, found := existing[request.URL.Path]; found {
				writeJSON(t, response, http.StatusOK, value)
				return
			}
		}
		if request.Method == http.MethodPatch && !strings.HasSuffix(request.URL.Path, "/status") {
			patches++
		}
		writeJSON(t, response, http.StatusOK, map[string]any{})
	}))
	t.Cleanup(server.Close)
	store := NewKubernetesStore(server.URL, "fixture-token", server.Client())
	if err := store.EnsureOwned(context.Background(), session); err != nil {
		t.Fatalf("ensure owned resources: %v", err)
	}
	if patches != 0 {
		t.Fatalf("unchanged resources received %d unnecessary updates", patches)
	}
}

func writeJSON(t *testing.T, response http.ResponseWriter, status int, value any) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func nestedString(value map[string]any, path ...string) string {
	current := any(value)
	for _, key := range path {
		record, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = record[key]
	}
	result, _ := current.(string)
	return result
}
