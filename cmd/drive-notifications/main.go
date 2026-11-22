package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	maxStreams   = 1024
	writeTimeout = 10 * time.Second
	heartbeat    = 20 * time.Second
)

type streams struct {
	mu      sync.Mutex
	byToken map[string]chan struct{}
}

func validToken(token string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == token
}

func (s *streams) events(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || !validToken(token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	if _, exists := s.byToken[token]; exists {
		s.mu.Unlock()
		http.Error(w, "already subscribed", http.StatusConflict)
		return
	}
	if len(s.byToken) >= maxStreams {
		s.mu.Unlock()
		http.Error(w, "subscription limit reached", http.StatusServiceUnavailable)
		return
	}
	ch := make(chan struct{}, 1)
	s.byToken[token] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.byToken, token)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	controller := http.NewResponseController(w)
	write := func(frame string) error {
		if err := controller.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			return err
		}
		if _, err := fmt.Fprint(w, frame); err != nil {
			return err
		}
		if err := controller.Flush(); err != nil {
			return err
		}
		return controller.SetWriteDeadline(time.Time{})
	}
	if write("event: ready\ndata: {}\n\n") != nil {
		return
	}
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			if write("event: changed\ndata: {}\n\n") != nil {
				return
			}
		case <-ticker.C:
			if write(": heartbeat\n\n") != nil {
				return
			}
		}
	}
}

func (s *streams) notifications(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("X-Goog-Channel-Token")
	if !validToken(token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch r.Header.Get("X-Goog-Resource-State") {
	case "sync":
	case "change":
		s.mu.Lock()
		if ch := s.byToken[token]; ch != nil {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		s.mu.Unlock()
	default:
		http.Error(w, "invalid resource state", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	s := &streams{byToken: make(map[string]chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", s.events)
	mux.HandleFunc("POST /drive/notifications", s.notifications)
	server := &http.Server{
		Addr:              "127.0.0.1:8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      writeTimeout,
	}
	log.Fatal(server.ListenAndServe())
}
