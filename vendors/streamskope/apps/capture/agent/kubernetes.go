package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	maximumKubernetesResponseBytes = 4 * 1024 * 1024
	serviceAccountDirectory        = "/var/run/secrets/kubernetes.io/serviceaccount"
)

type KubernetesStore struct {
	baseURL string
	client  *http.Client
	probe   func(context.Context, string) error
	token   string
}

type kubernetesError struct {
	status int
	text   string
}

func (failure kubernetesError) Error() string {
	return failure.text
}

func NewKubernetesStore(baseURL string, token string, client *http.Client) *KubernetesStore {
	return &KubernetesStore{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
		probe:   func(context.Context, string) error { return nil },
		token:   token,
	}
}

func NewInClusterKubernetesStore() (*KubernetesStore, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if host == "" || port == "" {
		return nil, errors.New("in-cluster Kubernetes address is unavailable")
	}
	token, err := os.ReadFile(filepath.Join(serviceAccountDirectory, "token"))
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	certificate, err := os.ReadFile(filepath.Join(serviceAccountDirectory, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certificate) {
		return nil, errors.New("service account CA is invalid")
	}
	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool},
	}
	store := NewKubernetesStore(
		"https://"+netJoinHostPort(host, port),
		strings.TrimSpace(string(token)),
		&http.Client{Timeout: 15 * time.Second, Transport: transport},
	)
	store.probe = func(ctx context.Context, address string) error {
		connection, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", address)
		if err != nil {
			return err
		}
		return connection.Close()
	}
	return store, nil
}

func netJoinHostPort(host string, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

func (store *KubernetesStore) List(ctx context.Context) ([]CaptureSession, error) {
	value, err := store.request(ctx, http.MethodGet, "/apis/capture.streamskope.io/v1alpha1/capturesessions", nil, "")
	if err != nil {
		return nil, err
	}
	items, _ := value["items"].([]any)
	sessions := make([]CaptureSession, 0, len(items))
	for _, item := range items {
		record, ok := item.(map[string]any)
		if !ok {
			continue
		}
		session, parseError := captureSessionFromResource(record)
		if parseError != nil {
			continue
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (store *KubernetesStore) Create(ctx context.Context, session CaptureSession) (CaptureSession, error) {
	path := "/apis/capture.streamskope.io/v1alpha1/namespaces/" + escapePath(session.Request.Source.Namespace) + "/capturesessions"
	_, err := store.request(ctx, http.MethodPost, path, captureSessionResource(session), "application/json")
	if err != nil {
		return CaptureSession{}, err
	}
	return session, nil
}

func (store *KubernetesStore) Update(ctx context.Context, session CaptureSession) (CaptureSession, error) {
	patch := map[string]any{"spec": map[string]any{
		"leaseExpiresAt": session.ExpiresAt.Format(time.RFC3339),
		"leaseSeconds":   session.Request.LeaseSeconds,
	}}
	if _, err := store.request(ctx, http.MethodPatch, captureSessionPath(session), patch, "application/merge-patch+json"); err != nil {
		return CaptureSession{}, err
	}
	return session, nil
}

func (store *KubernetesStore) EnsureOwned(ctx context.Context, session CaptureSession) error {
	sourcePath := producerPath(session.Request.Source, session.Request.Source.Name)
	source, err := store.request(ctx, http.MethodGet, sourcePath, nil, "")
	if err != nil {
		return fmt.Errorf("read source exporter: %w", err)
	}
	resources, err := BuildCaptureResources(source, session.Request, session.ExpiresAt.Add(-time.Duration(session.Request.LeaseSeconds)*time.Second))
	if err != nil {
		return err
	}
	name := captureResourceName(session.ID)
	namespace := escapePath(session.Request.Source.Namespace)
	for _, resource := range []struct {
		collection string
		path       string
		value      map[string]any
	}{
		{
			collection: "/api/v1/namespaces/" + namespace + "/services",
			path:       "/api/v1/namespaces/" + namespace + "/services/" + name,
			value:      resources.Service,
		},
		{
			collection: "/apis/apps/v1/namespaces/" + namespace + "/statefulsets",
			path:       "/apis/apps/v1/namespaces/" + namespace + "/statefulsets/" + name,
			value:      resources.Workload,
		},
		{
			collection: producerCollection(session.Request.Source),
			path:       producerPath(session.Request.Source, name),
			value:      resources.Exporter,
		},
	} {
		if err := store.upsertOwned(ctx, session.ID, resource.collection, resource.path, resource.value); err != nil {
			return err
		}
	}
	broker := captureResourceName(session.ID) + "." + session.Request.Source.Namespace + ".svc:9093"
	if err := store.probe(ctx, broker); err != nil {
		return fmt.Errorf("capture broker is not ready: %w", err)
	}
	status := map[string]any{"status": map[string]any{"phase": "Ready"}}
	if _, err := store.request(ctx, http.MethodPatch, captureSessionPath(session)+"/status", status, "application/merge-patch+json"); err != nil {
		return fmt.Errorf("publish capture readiness: %w", err)
	}
	return nil
}

func (store *KubernetesStore) CleanupOwned(ctx context.Context, session CaptureSession) error {
	name := captureResourceName(session.ID)
	namespace := escapePath(session.Request.Source.Namespace)
	paths := []string{
		producerPath(session.Request.Source, name),
		"/apis/apps/v1/namespaces/" + namespace + "/statefulsets/" + name,
		"/api/v1/namespaces/" + namespace + "/services/" + name,
	}
	for _, path := range paths {
		if err := store.deleteOwned(ctx, session.ID, path); err != nil {
			return err
		}
	}
	return nil
}

func (store *KubernetesStore) ReleaseFinalizer(ctx context.Context, session CaptureSession) error {
	path := captureSessionPath(session)
	patch := map[string]any{"metadata": map[string]any{"finalizers": []any{}}}
	if _, err := store.request(ctx, http.MethodPatch, path, patch, "application/merge-patch+json"); err != nil && !isKubernetesStatus(err, http.StatusNotFound) {
		return err
	}
	if _, err := store.request(ctx, http.MethodDelete, path, map[string]any{"propagationPolicy": "Foreground"}, "application/json"); err != nil && !isKubernetesStatus(err, http.StatusNotFound) {
		return err
	}
	return nil
}

func (store *KubernetesStore) upsertOwned(ctx context.Context, sessionID string, collection string, path string, desired map[string]any) error {
	existing, err := store.request(ctx, http.MethodGet, path, nil, "")
	if isKubernetesStatus(err, http.StatusNotFound) {
		_, createError := store.request(ctx, http.MethodPost, collection, desired, "application/json")
		return createError
	}
	if err != nil {
		return err
	}
	if !ownedBySession(existing, sessionID) {
		return errors.New("capture resource ownership conflict")
	}
	if matchesDesiredResource(existing, desired) {
		return nil
	}
	_, err = store.request(ctx, http.MethodPatch, path, desired, "application/merge-patch+json")
	return err
}

func matchesDesiredResource(actual any, desired any) bool {
	switch expected := desired.(type) {
	case map[string]any:
		current, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range expected {
			if !matchesDesiredResource(current[key], value) {
				return false
			}
		}
		return true
	case []any:
		current, ok := actual.([]any)
		if !ok || len(current) != len(expected) {
			return false
		}
		for index, value := range expected {
			if !matchesDesiredResource(current[index], value) {
				return false
			}
		}
		return true
	default:
		currentJSON, currentError := json.Marshal(actual)
		expectedJSON, expectedError := json.Marshal(desired)
		return currentError == nil && expectedError == nil && bytes.Equal(currentJSON, expectedJSON)
	}
}

func (store *KubernetesStore) deleteOwned(ctx context.Context, sessionID string, path string) error {
	existing, err := store.request(ctx, http.MethodGet, path, nil, "")
	if isKubernetesStatus(err, http.StatusNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !ownedBySession(existing, sessionID) {
		return errors.New("refusing to delete a capture resource without exact ownership")
	}
	_, err = store.request(ctx, http.MethodDelete, path, map[string]any{"propagationPolicy": "Foreground"}, "application/json")
	if isKubernetesStatus(err, http.StatusNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 100; attempt++ {
		remaining, readErr := store.request(ctx, http.MethodGet, path, nil, "")
		if isKubernetesStatus(readErr, http.StatusNotFound) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if !ownedBySession(remaining, sessionID) {
			return errors.New("capture resource ownership changed during deletion")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("capture resource deletion was not confirmed within 10 seconds")
}

func (store *KubernetesStore) request(ctx context.Context, method string, path string, body map[string]any, contentType string) (map[string]any, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, store.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+store.token)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := store.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maximumKubernetesResponseBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maximumKubernetesResponseBytes {
		return nil, errors.New("Kubernetes response exceeded the bounded limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, kubernetesError{status: response.StatusCode, text: "Kubernetes request failed with status " + response.Status}
	}
	if len(encoded) == 0 {
		return map[string]any{}, nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, errors.New("Kubernetes returned invalid JSON")
	}
	return decoded, nil
}

func captureSessionResource(session CaptureSession) map[string]any {
	return map[string]any{
		"apiVersion": "capture.streamskope.io/v1alpha1",
		"kind":       "CaptureSession",
		"metadata": map[string]any{
			"finalizers": []any{session.Finalizer},
			"labels":     map[string]any{managedByLabel: managedByValue, sessionLabel: session.ID},
			"name":       session.ID,
			"namespace":  session.Request.Source.Namespace,
		},
		"spec": map[string]any{
			"leaseExpiresAt": session.ExpiresAt.Format(time.RFC3339),
			"leaseSeconds":   session.Request.LeaseSeconds,
			"localPort":      session.Request.LocalPort,
			"source": map[string]any{
				"apiVersion": session.Request.Source.APIVersion,
				"kind":       session.Request.Source.Kind,
				"name":       session.Request.Source.Name,
				"namespace":  session.Request.Source.Namespace,
			},
		},
	}
}

func captureSessionFromResource(resource map[string]any) (CaptureSession, error) {
	metadata, _ := resource["metadata"].(map[string]any)
	spec, _ := resource["spec"].(map[string]any)
	source, _ := spec["source"].(map[string]any)
	status, _ := resource["status"].(map[string]any)
	id, _ := metadata["name"].(string)
	expiresText, _ := spec["leaseExpiresAt"].(string)
	expiresAt, err := time.Parse(time.RFC3339, expiresText)
	if err != nil {
		return CaptureSession{}, errors.New("capture session lease is invalid")
	}
	localPort, ok := jsonNumberToInt(spec["localPort"])
	if !ok {
		return CaptureSession{}, errors.New("capture session local port is invalid")
	}
	leaseSeconds, ok := jsonNumberToInt(spec["leaseSeconds"])
	if !ok {
		return CaptureSession{}, errors.New("capture session lease duration is invalid")
	}
	phase, _ := status["phase"].(string)
	if phase == "" {
		phase = "Pending"
	}
	namespace, _ := source["namespace"].(string)
	request := CreateSessionRequest{
		ID:           id,
		LeaseSeconds: leaseSeconds,
		LocalPort:    localPort,
		Source: SourceReference{
			APIVersion: stringValue(source["apiVersion"]),
			Kind:       stringValue(source["kind"]),
			Name:       stringValue(source["name"]),
			Namespace:  namespace,
		},
	}
	return CaptureSession{ExpiresAt: expiresAt, Finalizer: finalizerName, ID: id, Phase: phase, Request: request}, nil
}

func captureSessionPath(session CaptureSession) string {
	return "/apis/capture.streamskope.io/v1alpha1/namespaces/" + escapePath(session.Request.Source.Namespace) + "/capturesessions/" + escapePath(session.ID)
}

func producerCollection(source SourceReference) string {
	version := strings.TrimPrefix(source.APIVersion, "kafka.eda.nokia.com/")
	root := "/apis/kafka.eda.nokia.com/" + escapePath(version) + "/namespaces/" + escapePath(source.Namespace)
	if source.Kind == "ClusterProducer" {
		return root + "/clusterproducers"
	}
	return root + "/producers"
}

func producerPath(source SourceReference, name string) string {
	return producerCollection(source) + "/" + escapePath(name)
}

func captureResourceName(sessionID string) string {
	return "streamskope-capture-" + sessionID[:8]
}

func ownedBySession(resource map[string]any, sessionID string) bool {
	metadata, _ := resource["metadata"].(map[string]any)
	labels, _ := metadata["labels"].(map[string]any)
	return labels[managedByLabel] == managedByValue && labels[sessionLabel] == sessionID
}

func escapePath(value string) string {
	return url.PathEscape(value)
}

func isKubernetesStatus(err error, status int) bool {
	var failure kubernetesError
	return errors.As(err, &failure) && failure.status == status
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func jsonNumberToInt(value any) (int, bool) {
	number, ok := value.(float64)
	if !ok || number != float64(int(number)) {
		return 0, false
	}
	return int(number), true
}
