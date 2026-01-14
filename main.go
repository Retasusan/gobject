package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

type PutResponse struct {
	ID   string `json:"id"`
	Size int64  `json:"size"`
}

var idRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

var storeDir = getenv("STORE_DIR", "./store")

func main() {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		panic(err)
	}

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

		if err := putAtomic(storeDir, id, b); err != nil {
			http.Error(w, "failed to store object", http.StatusInternalServerError)
			return
		}

		resp := PutResponse{
			ID:   id,
			Size: int64(len(b)),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	// GET /objects/{id}
	mux.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := filepath.Base(r.URL.Path)
		if !idRe.MatchString(id) {
			http.Error(w, "invalid id", http.StatusBadGateway)
			return
		}

		path := filepath.Join(storeDir, id+".blob")
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to open file", http.StatusNotFound)
			return
		}
		defer f.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, f)
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

func putAtomic(storeDir, id string, b []byte) error {
	finalPath := filepath.Join(storeDir, id+".blob")

	// 冪等: return success if already exists
	if _, err := os.Stat(finalPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	//tmp directory
	tmpDir := filepath.Join(storeDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return err
	}

	//create tmp file
	f, err := os.CreateTemp(tmpDir, "put-*.tmp")
	if err != nil {
		return nil
	}
	tmpName := f.Name()

	// cleaning when failed
	defer func() {
		f.Close()
		os.Remove(tmpName)
	}()

	//write
	if _, err := f.Write(b); err != nil {
		return err
	}

	// ensure to close disc
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	//publish as atomic
	return os.Rename(tmpName, finalPath)
}
