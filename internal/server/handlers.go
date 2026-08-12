package server

import (
	"encoding/json"
	"net/http"
)

// statusBody is the JSON body returned by the liveness/readiness endpoints.
type statusBody struct {
	Status string `json:"status"`
}

// errorBody is the JSON body returned for every error response produced
// by this package. The shape is fixed: {"error":{"message":...,"type":...}}.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// writeJSON encodes v as JSON to w with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a response in the package's fixed error shape.
func writeError(w http.ResponseWriter, status int, msg, errType string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Message: msg, Type: errType}})
}

// healthzHandler is a liveness probe: it always returns 200 as long as
// the process is up and able to serve HTTP, with no external checks.
func healthzHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statusBody{Status: "ok"})
}

// readyzHandler is a readiness probe: it should only return 200 once
// the gateway can actually serve traffic (i.e. its dependencies are
// reachable), unlike healthzHandler which only reflects process liveness.
func readyzHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: check Redis and provider connectivity before reporting ready.
	writeJSON(w, http.StatusOK, statusBody{Status: "ready"})
}
