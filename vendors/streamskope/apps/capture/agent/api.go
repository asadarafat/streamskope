package agent

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const maximumAPIRequestBytes = 32 * 1024
const captureAgentVersion = "v26.8.2"

type APIHandler struct {
	manager *SessionManager
	tunnel  http.Handler
}

type apiFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewAPIHandler(manager *SessionManager, tunnel http.Handler) *APIHandler {
	return &APIHandler{manager: manager, tunnel: tunnel}
}

func (handler *APIHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	if request.URL.Path == "/healthz" {
		if request.Method != http.MethodGet {
			handler.failure(response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "The requested method is not supported.")
			return
		}
		handler.json(response, http.StatusOK, map[string]string{"status": "ready", "version": captureAgentVersion})
		return
	}
	if request.URL.Path == "/v1/sessions" {
		handler.serveSessions(response, request)
		return
	}
	if strings.HasPrefix(request.URL.Path, "/v1/sessions/") {
		handler.serveSession(response, request)
		return
	}
	http.NotFound(response, request)
}

func (handler *APIHandler) serveSessions(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		handler.failure(response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "The requested method is not supported.")
		return
	}
	var value CreateSessionRequest
	if err := decodeBoundedJSON(request, &value); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, errAPIRequestTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		handler.failure(response, status, "INVALID_REQUEST", "The capture session request is invalid.")
		return
	}
	created, err := handler.manager.Activate(request.Context(), value)
	if errors.Is(err, ErrSessionConflict) {
		handler.failure(response, http.StatusConflict, "SESSION_CONFLICT", "Another capture session is active.")
		return
	}
	if err != nil {
		handler.failure(response, http.StatusBadRequest, "INVALID_REQUEST", "The capture session request is invalid.")
		return
	}
	handler.json(response, http.StatusCreated, created)
}

func (handler *APIHandler) serveSession(response http.ResponseWriter, request *http.Request) {
	if strings.HasSuffix(request.URL.Path, "/tunnel") {
		if handler.tunnel == nil {
			http.NotFound(response, request)
			return
		}
		handler.tunnel.ServeHTTP(response, request)
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/v1/sessions/")
	lease := strings.HasSuffix(path, "/lease")
	sessionID := strings.TrimSuffix(path, "/lease")
	if sessionID == "" || strings.Contains(sessionID, "/") {
		http.NotFound(response, request)
		return
	}

	var value CaptureSession
	var err error
	switch {
	case request.Method == http.MethodGet && !lease:
		value, err = handler.manager.Get(request.Context(), sessionID)
	case request.Method == http.MethodPatch && lease:
		var update struct {
			LeaseSeconds int `json:"leaseSeconds"`
		}
		if decodeError := decodeBoundedJSON(request, &update); decodeError != nil {
			status := http.StatusBadRequest
			if errors.Is(decodeError, errAPIRequestTooLarge) {
				status = http.StatusRequestEntityTooLarge
			}
			handler.failure(response, status, "INVALID_REQUEST", "The lease request is invalid.")
			return
		}
		value, err = handler.manager.Refresh(request.Context(), sessionID, update.LeaseSeconds)
	case request.Method == http.MethodDelete && !lease:
		err = handler.manager.Remove(request.Context(), sessionID)
		if err == nil {
			response.WriteHeader(http.StatusNoContent)
			return
		}
	default:
		handler.failure(response, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "The requested method is not supported.")
		return
	}
	if errors.Is(err, ErrSessionNotFound) {
		handler.failure(response, http.StatusNotFound, "SESSION_NOT_FOUND", "The capture session does not exist.")
		return
	}
	if err != nil {
		handler.failure(response, http.StatusBadRequest, "SESSION_OPERATION_FAILED", "The capture session operation failed.")
		return
	}
	handler.json(response, http.StatusOK, value)
}

var errAPIRequestTooLarge = errors.New("API request exceeds the bounded limit")

func decodeBoundedJSON(request *http.Request, target any) error {
	limited := io.LimitReader(request.Body, maximumAPIRequestBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(encoded) > maximumAPIRequestBytes {
		return errAPIRequestTooLarge
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func (handler *APIHandler) json(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func (handler *APIHandler) failure(response http.ResponseWriter, status int, code string, message string) {
	handler.json(response, status, apiFailure{Code: code, Message: message})
}
