package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "streamskope-capture-agent"
	sessionLabel   = "capture.streamskope.io/session"
	expiresAtKey   = "capture.streamskope.io/expires-at"
	finalizerName  = "capture.streamskope.io/owned-resources"
)

var (
	dnsLabelPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
	uuidPattern     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

	ErrSessionConflict = errors.New("another capture session is active")
	ErrSessionNotFound = errors.New("capture session not found")
)

type SourceReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
}

type CreateSessionRequest struct {
	ID           string          `json:"id"`
	LeaseSeconds int             `json:"leaseSeconds"`
	LocalPort    int             `json:"localPort"`
	Source       SourceReference `json:"source"`
}

type CaptureSession struct {
	ExpiresAt time.Time            `json:"expiresAt"`
	Finalizer string               `json:"finalizer"`
	ID        string               `json:"id"`
	Phase     string               `json:"phase"`
	Request   CreateSessionRequest `json:"request"`
}

type CaptureResources struct {
	Exporter map[string]any
	Service  map[string]any
	Workload map[string]any
}

type SessionStore interface {
	List(context.Context) ([]CaptureSession, error)
	Create(context.Context, CaptureSession) (CaptureSession, error)
	Update(context.Context, CaptureSession) (CaptureSession, error)
	EnsureOwned(context.Context, CaptureSession) error
	CleanupOwned(context.Context, CaptureSession) error
	ReleaseFinalizer(context.Context, CaptureSession) error
}

type SessionManager struct {
	now   func() time.Time
	store SessionStore
	mu    sync.Mutex
}

func NewSessionManager(store SessionStore, now func() time.Time) *SessionManager {
	return &SessionManager{now: now, store: store}
}

func ValidateCreateSessionRequest(request CreateSessionRequest) error {
	if !uuidPattern.MatchString(request.ID) {
		return errors.New("session id must be a lowercase UUID")
	}
	if request.Source.APIVersion != "kafka.eda.nokia.com/v1" && request.Source.APIVersion != "kafka.eda.nokia.com/v1alpha1" {
		return errors.New("unsupported Kafka exporter API version")
	}
	if request.Source.Kind != "Producer" && request.Source.Kind != "ClusterProducer" {
		return errors.New("unsupported Kafka exporter kind")
	}
	if !validDNSName(request.Source.Name) || !validDNSName(request.Source.Namespace) {
		return errors.New("source name and namespace must be DNS names")
	}
	if request.LeaseSeconds < 30 || request.LeaseSeconds > 900 {
		return errors.New("lease must be between 30 and 900 seconds")
	}
	if request.LocalPort < 1024 || request.LocalPort > 65535 {
		return errors.New("local port must be between 1024 and 65535")
	}
	return nil
}

func validDNSName(value string) bool {
	return len(value) > 0 && len(value) <= 253 && dnsLabelPattern.MatchString(value) && !strings.Contains(value, "..")
}

func (manager *SessionManager) Activate(ctx context.Context, request CreateSessionRequest) (CaptureSession, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := ValidateCreateSessionRequest(request); err != nil {
		return CaptureSession{}, err
	}
	sessions, err := manager.store.List(ctx)
	if err != nil {
		return CaptureSession{}, fmt.Errorf("list capture sessions: %w", err)
	}
	now := manager.now().UTC()
	for _, session := range sessions {
		if session.ID == request.ID && session.Phase == "Stopping" {
			return CaptureSession{}, ErrSessionConflict
		}
		if !session.ExpiresAt.After(now) {
			continue
		}
		if session.ID == request.ID && session.Request == request {
			return session, nil
		}
		return CaptureSession{}, ErrSessionConflict
	}
	session := CaptureSession{
		ExpiresAt: now.Add(time.Duration(request.LeaseSeconds) * time.Second),
		Finalizer: finalizerName,
		ID:        request.ID,
		Phase:     "Pending",
		Request:   request,
	}
	created, err := manager.store.Create(ctx, session)
	if err != nil {
		return CaptureSession{}, fmt.Errorf("create capture session: %w", err)
	}
	return created, nil
}

func (manager *SessionManager) Reconcile(ctx context.Context) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	sessions, err := manager.store.List(ctx)
	if err != nil {
		return fmt.Errorf("list capture sessions: %w", err)
	}
	now := manager.now().UTC()
	for _, session := range sessions {
		if session.ExpiresAt.After(now) {
			if err := manager.store.EnsureOwned(ctx, session); err != nil {
				return fmt.Errorf("ensure capture resources: %w", err)
			}
			continue
		}
		if err := manager.store.CleanupOwned(ctx, session); err != nil {
			return fmt.Errorf("clean expired capture resources: %w", err)
		}
		if err := manager.store.ReleaseFinalizer(ctx, session); err != nil {
			return fmt.Errorf("release capture finalizer: %w", err)
		}
	}
	return nil
}

func (manager *SessionManager) Get(ctx context.Context, sessionID string) (CaptureSession, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.get(ctx, sessionID)
}

func (manager *SessionManager) get(ctx context.Context, sessionID string) (CaptureSession, error) {
	if !uuidPattern.MatchString(sessionID) {
		return CaptureSession{}, errors.New("session id must be a lowercase UUID")
	}
	sessions, err := manager.store.List(ctx)
	if err != nil {
		return CaptureSession{}, fmt.Errorf("list capture sessions: %w", err)
	}
	for _, session := range sessions {
		if session.ID == sessionID {
			return session, nil
		}
	}
	return CaptureSession{}, ErrSessionNotFound
}

func (manager *SessionManager) Refresh(ctx context.Context, sessionID string, leaseSeconds int) (CaptureSession, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	session, err := manager.get(ctx, sessionID)
	if err != nil {
		return CaptureSession{}, err
	}
	if session.Phase == "Stopping" || !session.ExpiresAt.After(manager.now().UTC()) {
		return CaptureSession{}, errors.New("capture session is stopping or expired")
	}
	request := session.Request
	request.LeaseSeconds = leaseSeconds
	if err := ValidateCreateSessionRequest(request); err != nil {
		return CaptureSession{}, err
	}
	session.Request = request
	session.ExpiresAt = manager.now().UTC().Add(time.Duration(leaseSeconds) * time.Second)
	updated, err := manager.store.Update(ctx, session)
	if err != nil {
		return CaptureSession{}, fmt.Errorf("refresh capture lease: %w", err)
	}
	return updated, nil
}

func (manager *SessionManager) Remove(ctx context.Context, sessionID string) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	session, err := manager.get(ctx, sessionID)
	if err == ErrSessionNotFound {
		// Cleanup may already have completed or the lease may have expired.
		// Only a successful store lookup can confirm the session is absent.
		return nil
	}
	if err != nil {
		return err
	}
	if session.Phase != "Stopping" {
		session.Phase = "Stopping"
		session.ExpiresAt = manager.now().UTC()
		if _, err := manager.store.Update(ctx, session); err != nil {
			return fmt.Errorf("mark capture session for cleanup: %w", err)
		}
	}
	if err := manager.store.CleanupOwned(ctx, session); err != nil {
		return fmt.Errorf("clean capture resources: %w", err)
	}
	if err := manager.store.ReleaseFinalizer(ctx, session); err != nil {
		return fmt.Errorf("release capture finalizer: %w", err)
	}
	return nil
}

func (manager *SessionManager) BrokerAddress(ctx context.Context, sessionID string) (string, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	session, err := manager.get(ctx, sessionID)
	if err != nil {
		return "", err
	}
	if !session.ExpiresAt.After(manager.now().UTC()) {
		return "", errors.New("capture session lease expired")
	}
	return captureResourceName(session.ID) + "." + session.Request.Source.Namespace + ".svc:9093", nil
}

func DeepCopyObject(source map[string]any) map[string]any {
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil
	}
	var copied map[string]any
	if err := json.Unmarshal(encoded, &copied); err != nil {
		return nil
	}
	return copied
}

func BuildCaptureResources(source map[string]any, request CreateSessionRequest, now time.Time) (CaptureResources, error) {
	if err := ValidateCreateSessionRequest(request); err != nil {
		return CaptureResources{}, err
	}
	exporter := DeepCopyObject(source)
	if exporter == nil {
		return CaptureResources{}, errors.New("source exporter cannot be copied")
	}
	spec, ok := exporter["spec"].(map[string]any)
	if !ok {
		return CaptureResources{}, errors.New("source exporter has no specification")
	}
	exports, ok := spec["exports"].([]any)
	if !ok || len(exports) == 0 {
		return CaptureResources{}, errors.New("source exporter has no exports")
	}
	shortID := request.ID[:8]
	name := "streamskope-capture-" + shortID
	labels := map[string]any{managedByLabel: managedByValue, sessionLabel: request.ID}
	annotations := map[string]any{expiresAtKey: now.Add(time.Duration(request.LeaseSeconds) * time.Second).UTC().Format(time.RFC3339)}
	exporter["apiVersion"] = request.Source.APIVersion
	exporter["kind"] = request.Source.Kind
	exporterMetadata := map[string]any{
		"annotations": annotations,
		"labels":      labels,
		"name":        name,
	}
	if request.Source.Kind != "ClusterProducer" {
		exporterMetadata["namespace"] = request.Source.Namespace
	}
	exporter["metadata"] = exporterMetadata
	spec["brokers"] = name + "." + request.Source.Namespace + ".svc:9092"
	delete(spec, "sasl")
	delete(spec, "tls")

	service := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"annotations": annotations,
			"labels":      labels,
			"name":        name,
			"namespace":   request.Source.Namespace,
		},
		"spec": map[string]any{
			"ports": []any{
				map[string]any{"name": "kafka-internal", "port": 9092, "targetPort": "kafka-internal"},
				map[string]any{"name": "kafka-tunnel", "port": 9093, "targetPort": "kafka-tunnel"},
			},
			"selector": map[string]any{"capture.streamskope.io/workload": name},
			"type":     "ClusterIP",
		},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "StatefulSet",
		"metadata": map[string]any{
			"annotations": annotations,
			"labels":      labels,
			"name":        name,
			"namespace":   request.Source.Namespace,
		},
		"spec": map[string]any{
			"replicas":    1,
			"serviceName": name,
			"selector": map[string]any{
				"matchLabels": map[string]any{"capture.streamskope.io/workload": name},
			},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"capture.streamskope.io/workload": name}},
				"spec": map[string]any{
					"containers": []any{map[string]any{
						"args": []any{
							"redpanda", "start", "--smp=1", "--memory=512M", "--reserve-memory=0M", "--overprovisioned", "--node-id=0",
							"--kafka-addr=internal://0.0.0.0:9092,tunnel://0.0.0.0:9093",
							"--advertise-kafka-addr=internal://" + name + "." + request.Source.Namespace + ".svc:9092,tunnel://127.0.0.1:" + fmt.Sprint(request.LocalPort),
							"--rpc-addr=0.0.0.0:33145",
						},
						"image": "docker.redpanda.com/redpandadata/redpanda:v24.3.5@sha256:8b2802411074676c96a81eb65483b29e63a212673023869e91fd41d728aaa278",
						"name":  "redpanda",
						"ports": []any{
							map[string]any{"containerPort": 9092, "name": "kafka-internal"},
							map[string]any{"containerPort": 9093, "name": "kafka-tunnel"},
						},
						"resources": map[string]any{
							"limits":   map[string]any{"cpu": "500m", "memory": "768Mi"},
							"requests": map[string]any{"cpu": "100m", "memory": "384Mi"},
						},
						"volumeMounts": []any{map[string]any{"mountPath": "/var/lib/redpanda/data", "name": "data"}},
					}},
					"terminationGracePeriodSeconds": 30,
					"volumes":                       []any{map[string]any{"emptyDir": map[string]any{}, "name": "data"}},
				},
			},
		},
	}
	return CaptureResources{Exporter: exporter, Service: service, Workload: workload}, nil
}
