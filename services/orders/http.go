package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/example/orderflow/pkg/events"
)

//go:embed web
var webFS embed.FS

func (s *Service) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.httpCreate)
	mux.HandleFunc("GET /orders", s.httpList)
	mux.HandleFunc("GET /orders/{id}", s.httpGet)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	site, _ := fs.Sub(webFS, "web") // dashboard at /
	mux.Handle("GET /", http.FileServerFS(site))
	return mux
}

func (s *Service) httpCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CustomerID string        `json:"customer_id"`
		Items      []events.Item `json:"items"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed JSON"})
		return
	}
	id, status, created, err := s.Create(r.Context(), req.CustomerID, req.Items, r.Header.Get("Idempotency-Key"))
	if err != nil {
		writeErr(r.Context(), w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]string{"id": id, "status": status})
}

func (s *Service) httpGet(w http.ResponseWriter, r *http.Request) {
	o, err := s.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, o)
}

func (s *Service) httpList(w http.ResponseWriter, r *http.Request) {
	list, err := s.List(r.Context(), 25)
	if err != nil {
		writeErr(r.Context(), w, err)
		return
	}
	if list == nil {
		list = []Order{}
	}
	writeJSON(w, http.StatusOK, list)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	default:
		slog.ErrorContext(ctx, "request failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}
