package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type PutResponse struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}

func main() {
	mux := http.NewServeMux()

	// health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// POST /object
	mux.HandleFunc("/objects", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()
		b, err := io.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		if len(b) == 0 {
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(b)
		id := hex.EncodeToString(sum[:])
		resp := PutResponse{
			ID:   id,
			Size: int64(len(b)),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	addr := getenv("LISTEN_ADDR", ":8080")
	fmt.Printf("listening on %s\n", addr)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
