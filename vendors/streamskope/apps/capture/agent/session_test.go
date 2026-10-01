package agent

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func validRequest() CreateSessionRequest {
	return CreateSessionRequest{
		ID:           "7ac7d717-8ef0-4a55-bd33-5b723e6b6918",
		LeaseSeconds: 120,
		LocalPort:    19092,
		Source: SourceReference{
			APIVersion: "kafka.eda.nokia.com/v1",
			Kind:       "Producer",
			Name:       "events",
			Namespace:  "eda",
		},
	}
}

func TestValidateCreateSessionRequest(t *testing.T) {
	t.Parallel()

	request := validRequest()
	if err := ValidateCreateSessionRequest(request); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*CreateSessionRequest)
	}{
		{name: "invalid session identity", mutate: func(value *CreateSessionRequest) { value.ID = "session" }},
		{name: "unsupported source version", mutate: func(value *CreateSessionRequest) { value.Source.APIVersion = "kafka.eda.nokia.com/v2" }},
		{name: "unsupported source kind", mutate: func(value *CreateSessionRequest) { value.Source.Kind = "Topic" }},
		{name: "invalid source name", mutate: func(value *CreateSessionRequest) { value.Source.Name = "EVENTS" }},
		{name: "invalid namespace", mutate: func(value *CreateSessionRequest) { value.Source.Namespace = "eda/system" }},
		{name: "short lease", mutate: func(value *CreateSessionRequest) { value.LeaseSeconds = 29 }},
		{name: "long lease", mutate: func(value *CreateSessionRequest) { value.LeaseSeconds = 901 }},
		{name: "privileged local port", mutate: func(value *CreateSessionRequest) { value.LocalPort = 443 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validRequest()
			test.mutate(&candidate)
			if err := ValidateCreateSessionRequest(candidate); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

func TestBuildCaptureResourcesDoesNotMutateSource(t *testing.T) {
	t.Parallel()

	source := map[string]any{
		"apiVersion": "kafka.eda.nokia.com/v1",
		"kind":       "Producer",
		"metadata": map[string]any{
			"name":      "events",
			"namespace": "eda",
			"labels":    map[string]any{"source": "true"},
		},
		"spec": map[string]any{
			"brokers": "external:9092",
			"exports": []any{map[string]any{"topic": "alarms"}},
			"sasl":    map[string]any{"mechanism": "plain"},
			"tls":     map[string]any{"enabled": true},
		},
	}
	original := DeepCopyObject(source)

	resources, err := BuildCaptureResources(source, validRequest(), time.Unix(1_800_000_000, 0).UTC())
	if err != nil {
		t.Fatalf("build capture resources: %v", err)
	}
	if !reflect.DeepEqual(source, original) {
		t.Fatal("source resource was mutated")
	}
	copySpec := resources.Exporter["spec"].(map[string]any)
	if copySpec["brokers"] != "streamskope-capture-7ac7d717.eda.svc:9092" {
		t.Fatalf("unexpected capture broker: %v", copySpec["brokers"])
	}
	if _, exists := copySpec["sasl"]; exists {
		t.Fatal("source SASL configuration copied to the private capture broker")
	}
	if _, exists := copySpec["tls"]; exists {
		t.Fatal("source TLS configuration copied to the private capture broker")
	}
	if resources.Service["kind"] != "Service" || resources.Workload["kind"] != "StatefulSet" {
		t.Fatal("bounded broker resources were not planned")
	}
}

func TestManagerActivationIsIdempotentAndRejectsConflict(t *testing.T) {
	t.Parallel()

	clock := time.Unix(1_800_000_000, 0).UTC()
	store := newMemorySessionStore()
	manager := NewSessionManager(store, func() time.Time { return clock })
	request := validRequest()

	first, err := manager.Activate(context.Background(), request)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	second, err := manager.Activate(context.Background(), request)
	if err != nil {
		t.Fatalf("repeat activation: %v", err)
	}
	if !reflect.DeepEqual(first, second) || store.createCalls != 1 {
		t.Fatalf("activation was not idempotent: first=%+v second=%+v creates=%d", first, second, store.createCalls)
	}

	conflict := validRequest()
	conflict.ID = "290b9378-10a6-4739-a69a-0058e8fb936f"
	_, err = manager.Activate(context.Background(), conflict)
	if !errors.Is(err, ErrSessionConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestExpiredSessionCleansResourcesBeforeFinalizer(t *testing.T) {
	t.Parallel()

	clock := time.Unix(1_800_000_000, 0).UTC()
	store := newMemorySessionStore()
	manager := NewSessionManager(store, func() time.Time { return clock })
	if _, err := manager.Activate(context.Background(), validRequest()); err != nil {
		t.Fatalf("activate: %v", err)
	}

	clock = clock.Add(121 * time.Second)
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if store.cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", store.cleanupCalls)
	}
	if store.finalizerCalls != 1 {
		t.Fatalf("finalizer releases = %d, want 1", store.finalizerCalls)
	}
	if store.cleanupSequence >= store.finalizerSequence {
		t.Fatalf("finalizer released before cleanup: cleanup=%d finalizer=%d", store.cleanupSequence, store.finalizerSequence)
	}
}

func TestStopCannotLeaveResourcesRecreatedByReconciliation(t *testing.T) {
	t.Parallel()
	store := &blockingSessionStore{
		memorySessionStore: newMemorySessionStore(),
		entered:            make(chan struct{}),
		resume:             make(chan struct{}),
	}
	manager := NewSessionManager(store, time.Now)
	request := validRequest()
	if _, err := manager.Activate(context.Background(), request); err != nil {
		t.Fatalf("activate: %v", err)
	}
	reconciled := make(chan error, 1)
	go func() { reconciled <- manager.Reconcile(context.Background()) }()
	<-store.entered
	stopped := make(chan error, 1)
	go func() { stopped <- manager.Remove(context.Background(), request.ID) }()
	var stopErr error
	stopCompleted := false
	select {
	case stopErr = <-stopped:
		stopCompleted = true
	case <-time.After(100 * time.Millisecond):
	}
	close(store.resume)
	if err := <-reconciled; err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !stopCompleted {
		stopErr = <-stopped
	}
	if stopErr != nil {
		t.Fatalf("stop: %v", stopErr)
	}
	if store.owned.Load() {
		t.Fatal("stop left a capture resource recreated by reconciliation")
	}
}

func TestFailedStopRemainsExpiredForCleanupRetry(t *testing.T) {
	t.Parallel()
	clock := time.Unix(1_800_000_000, 0).UTC()
	store := newMemorySessionStore()
	manager := NewSessionManager(store, func() time.Time { return clock })
	request := validRequest()
	if _, err := manager.Activate(context.Background(), request); err != nil {
		t.Fatalf("activate: %v", err)
	}
	store.cleanupError = errors.New("deletion still in progress")
	if err := manager.Remove(context.Background(), request.ID); err == nil {
		t.Fatal("expected an incomplete cleanup error")
	}
	remaining := store.sessions[request.ID]
	if remaining.ExpiresAt.After(clock) || remaining.Phase != "Stopping" {
		t.Fatalf("session remains active after failed stop: %+v", remaining)
	}
	store.cleanupError = nil
	if err := manager.Reconcile(context.Background()); err != nil {
		t.Fatalf("retry cleanup: %v", err)
	}
	if store.ensureCalls != 0 || store.finalizerCalls != 1 {
		t.Fatalf("cleanup retry recreated resources: ensure=%d finalizer=%d", store.ensureCalls, store.finalizerCalls)
	}
}

type blockingSessionStore struct {
	*memorySessionStore
	entered chan struct{}
	resume  chan struct{}
	owned   atomic.Bool
}

func (store *blockingSessionStore) EnsureOwned(_ context.Context, _ CaptureSession) error {
	close(store.entered)
	<-store.resume
	store.owned.Store(true)
	return nil
}

func (store *blockingSessionStore) CleanupOwned(ctx context.Context, session CaptureSession) error {
	store.owned.Store(false)
	return store.memorySessionStore.CleanupOwned(ctx, session)
}

type memorySessionStore struct {
	listError         error
	cleanupError      error
	cleanupCalls      int
	cleanupSequence   int
	createCalls       int
	ensureCalls       int
	finalizerCalls    int
	finalizerSequence int
	sequence          int
	sessions          map[string]CaptureSession
}

func newMemorySessionStore() *memorySessionStore {
	return &memorySessionStore{sessions: map[string]CaptureSession{}}
}

func (store *memorySessionStore) List(_ context.Context) ([]CaptureSession, error) {
	if store.listError != nil {
		return nil, store.listError
	}
	values := make([]CaptureSession, 0, len(store.sessions))
	for _, session := range store.sessions {
		values = append(values, session)
	}
	return values, nil
}

func (store *memorySessionStore) Create(_ context.Context, session CaptureSession) (CaptureSession, error) {
	store.createCalls++
	store.sessions[session.ID] = session
	return session, nil
}

func (store *memorySessionStore) Update(_ context.Context, session CaptureSession) (CaptureSession, error) {
	store.sessions[session.ID] = session
	return session, nil
}

func (store *memorySessionStore) EnsureOwned(_ context.Context, _ CaptureSession) error {
	store.ensureCalls++
	return nil
}

func (store *memorySessionStore) CleanupOwned(_ context.Context, session CaptureSession) error {
	store.sequence++
	store.cleanupCalls++
	store.cleanupSequence = store.sequence
	return store.cleanupError
}

func (store *memorySessionStore) ReleaseFinalizer(_ context.Context, session CaptureSession) error {
	store.sequence++
	store.finalizerCalls++
	store.finalizerSequence = store.sequence
	delete(store.sessions, session.ID)
	return nil
}
