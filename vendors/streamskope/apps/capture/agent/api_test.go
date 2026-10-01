package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIManagesCaptureSessionLifecycle(t *testing.T) {
	t.Parallel()

	clock := time.Unix(1_800_000_000, 0).UTC()
	store := newMemorySessionStore()
	manager := NewSessionManager(store, func() time.Time { return clock })
	handler := NewAPIHandler(manager, nil)
	request := validRequest()

	created := performJSONRequest(t, handler, http.MethodPost, "/v1/sessions", request)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.Code, created.Body.String())
	}
	var session CaptureSession
	decodeJSONResponse(t, created, &session)
	if session.ID != request.ID || session.Request.Source != request.Source {
		t.Fatalf("unexpected created session: %+v", session)
	}

	inspected := performJSONRequest(t, handler, http.MethodGet, "/v1/sessions/"+request.ID, nil)
	if inspected.Code != http.StatusOK {
		t.Fatalf("inspect status = %d, body=%s", inspected.Code, inspected.Body.String())
	}

	clock = clock.Add(10 * time.Second)
	renewed := performJSONRequest(t, handler, http.MethodPatch, "/v1/sessions/"+request.ID+"/lease", map[string]any{"leaseSeconds": 240})
	if renewed.Code != http.StatusOK {
		t.Fatalf("renew status = %d, body=%s", renewed.Code, renewed.Body.String())
	}
	decodeJSONResponse(t, renewed, &session)
	if session.Request.LeaseSeconds != 240 || !session.ExpiresAt.Equal(clock.Add(240*time.Second)) {
		t.Fatalf("lease was not renewed: %+v", session)
	}

	removed := performJSONRequest(t, handler, http.MethodDelete, "/v1/sessions/"+request.ID, nil)
	if removed.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, body=%s", removed.Code, removed.Body.String())
	}
	if store.cleanupCalls != 1 || store.finalizerCalls != 1 {
		t.Fatalf("owned cleanup was incomplete: cleanup=%d finalizer=%d", store.cleanupCalls, store.finalizerCalls)
	}
}

func TestAPIBoundsAndClassifiesFailures(t *testing.T) {
	t.Parallel()

	store := newMemorySessionStore()
	manager := NewSessionManager(store, func() time.Time { return time.Unix(1_800_000_000, 0).UTC() })
	handler := NewAPIHandler(manager, nil)

	health := performJSONRequest(t, handler, http.MethodGet, "/healthz", nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"status":"ready"`) || !strings.Contains(health.Body.String(), `"version":"v26.8.2"`) {
		t.Fatalf("unexpected health response: %d %s", health.Code, health.Body.String())
	}

	missing := performJSONRequest(t, handler, http.MethodGet, "/v1/sessions/7ac7d717-8ef0-4a55-bd33-5b723e6b6918", nil)
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), `"code":"SESSION_NOT_FOUND"`) {
		t.Fatalf("unexpected missing response: %d %s", missing.Code, missing.Body.String())
	}

	invalid := performJSONRequest(t, handler, http.MethodPost, "/v1/sessions", map[string]any{"id": "invalid"})
	if invalid.Code != http.StatusBadRequest || !strings.Contains(invalid.Body.String(), `"code":"INVALID_REQUEST"`) {
		t.Fatalf("unexpected invalid response: %d %s", invalid.Code, invalid.Body.String())
	}

	largeBody := bytes.NewBuffer(make([]byte, maximumAPIRequestBytes+1))
	largeRequest := httptest.NewRequest(http.MethodPost, "/v1/sessions", largeBody)
	largeResponse := httptest.NewRecorder()
	handler.ServeHTTP(largeResponse, largeRequest)
	if largeResponse.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large request status = %d", largeResponse.Code)
	}
}

func TestAPIRemovingAbsentCaptureIsIdempotent(t *testing.T) {
	t.Parallel()

	for _, state := range []string{"never created", "already removed", "lease expired"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			clock := time.Unix(1_800_000_000, 0).UTC()
			store := newMemorySessionStore()
			manager := NewSessionManager(store, func() time.Time { return clock })
			handler := NewAPIHandler(manager, nil)
			request := validRequest()
			path := "/v1/sessions/" + request.ID
			if state != "never created" {
				if _, err := manager.Activate(context.Background(), request); err != nil {
					t.Fatalf("activate: %v", err)
				}
				if state == "already removed" {
					removed := performJSONRequest(t, handler, http.MethodDelete, path, nil)
					if removed.Code != http.StatusNoContent {
						t.Fatalf("initial removal status = %d, body=%s", removed.Code, removed.Body.String())
					}
				} else {
					clock = clock.Add(time.Duration(request.LeaseSeconds+1) * time.Second)
					if err := manager.Reconcile(context.Background()); err != nil {
						t.Fatalf("expire capture: %v", err)
					}
				}
			}

			// An old client's cleanup must not remove a newer capture or its resources.
			otherRequest := validRequest()
			otherRequest.ID = "290b9378-10a6-4739-a69a-0058e8fb936f"
			other, err := manager.Activate(context.Background(), otherRequest)
			if err != nil {
				t.Fatalf("activate unrelated capture: %v", err)
			}
			if err := manager.Reconcile(context.Background()); err != nil {
				t.Fatalf("ensure unrelated resources: %v", err)
			}
			cleanupCalls, finalizerCalls := store.cleanupCalls, store.finalizerCalls
			for attempt := 0; attempt < 2; attempt++ {
				removed := performJSONRequest(t, handler, http.MethodDelete, path, nil)
				if removed.Code != http.StatusNoContent || removed.Body.Len() != 0 {
					t.Fatalf("removal attempt %d: status=%d body=%s", attempt+1, removed.Code, removed.Body.String())
				}
			}
			if store.cleanupCalls != cleanupCalls || store.finalizerCalls != finalizerCalls {
				t.Fatal("absent capture removal attempted unrelated resource cleanup")
			}
			if len(store.sessions) != 1 || store.sessions[other.ID] != other {
				t.Fatal("absent capture removal changed the unrelated session")
			}
			for _, method := range []string{http.MethodGet, http.MethodPatch} {
				queryPath := path
				var body any
				if method == http.MethodPatch {
					queryPath += "/lease"
					body = map[string]any{"leaseSeconds": 120}
				}
				missing := performJSONRequest(t, handler, method, queryPath, body)
				if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), `"code":"SESSION_NOT_FOUND"`) {
					t.Fatalf("%s missing capture: status=%d body=%s", method, missing.Code, missing.Body.String())
				}
			}
		})
	}
}

func TestAPIRemovalDoesNotConfirmUnverifiedCleanup(t *testing.T) {
	t.Parallel()

	for _, failure := range []string{"invalid identity", "store unavailable", "cleanup denied"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			store := newMemorySessionStore()
			manager := NewSessionManager(store, time.Now)
			handler := NewAPIHandler(manager, nil)
			request := validRequest()
			if _, err := manager.Activate(context.Background(), request); err != nil {
				t.Fatalf("activate: %v", err)
			}
			id := request.ID
			switch failure {
			case "invalid identity":
				id = "not-a-uuid"
			case "store unavailable":
				store.listError = errors.New("Kubernetes API unavailable")
			case "cleanup denied":
				store.cleanupError = errors.New("resource deletion forbidden")
			}
			removed := performJSONRequest(t, handler, http.MethodDelete, "/v1/sessions/"+id, nil)
			if removed.Code != http.StatusBadRequest || !strings.Contains(removed.Body.String(), `"code":"SESSION_OPERATION_FAILED"`) {
				t.Fatalf("unverified removal: status=%d body=%s", removed.Code, removed.Body.String())
			}
			if _, exists := store.sessions[request.ID]; !exists || store.finalizerCalls != 0 {
				t.Fatal("failed removal discarded the session or released its finalizer")
			}
		})
	}
}

func performJSONRequest(t *testing.T, handler http.Handler, method string, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if value != nil {
		if err := json.NewEncoder(&body).Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequestWithContext(context.Background(), method, path, &body)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeJSONResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, response.Body.String())
	}
}
