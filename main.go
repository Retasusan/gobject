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
	ID          string `json:"id"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
}

type Meta struct {
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
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

		id, size, ct, err := putAtomicStream(storeDir, r.Body)
		if err != nil {
			http.Error(w, "failed to store object", http.StatusInternalServerError)
			return
		}

		resp := PutResponse{ID: id, Size: size, ContentType: ct}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)

	})

	// GET /objects/{id}
	mux.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := filepath.Base(r.URL.Path)
		if !idRe.MatchString(id) {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		path := filepath.Join(storeDir, id+".blob")
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to open file", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		// Content-Type は meta から（既に実装済み）
		metaPath := filepath.Join(storeDir, id+".meta.json")
		if b, err := os.ReadFile(metaPath); err == nil {
			var m Meta
			if json.Unmarshal(b, &m) == nil && m.ContentType != "" {
				w.Header().Set("Content-Type", m.ContentType)
			}
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		// modTime はキャッシュ/Range用に必要
		st, err := f.Stat()
		if err != nil {
			http.Error(w, "stat failed", http.StatusInternalServerError)
			return
		}

		// ここが核心：Range/HEAD/206 を全部やってくれる
		http.ServeContent(w, r, id, st.ModTime(), f)
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

func putAtomicStream(storeDir string, r io.Reader) (id string, size int64, ct string, err error) {
	h := sha256.New()

	tmpDir := filepath.Join(storeDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", 0, "", err
	}

	f, err := os.CreateTemp(tmpDir, "put-*.tmp")
	if err != nil {
		return "", 0, "", err
	}
	tmpName := f.Name()
	defer func() {
		f.Close()
		os.Remove(tmpName)
	}()

	// --- 先頭512bytesだけ読む（Content-Type 判定用）---
	var sniff [512]byte
	n0, err := io.ReadFull(r, sniff[:])
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", 0, "", err
	}
	ct = http.DetectContentType(sniff[:n0])

	// 先頭分はすでに読んだので、ファイル＆ハッシュにまず書く
	if n0 > 0 {
		if _, err := f.Write(sniff[:n0]); err != nil {
			return "", 0, "", err
		}
		if _, err := h.Write(sniff[:n0]); err != nil {
			return "", 0, "", err
		}
		size += int64(n0)
	}

	// 残りをストリーミング
	w := io.MultiWriter(f, h)
	n, err := io.Copy(w, r)
	if err != nil {
		return "", 0, "", err
	}
	size += n

	sum := h.Sum(nil)
	id = hex.EncodeToString(sum)

	finalPath := filepath.Join(storeDir, id+".blob")
	metaPath := filepath.Join(storeDir, id+".meta.json")

	// 冪等
	if _, err := os.Stat(finalPath); err == nil {
		// meta が無ければ作る（安全）
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			meta := Meta{ContentType: ct, Size: size}
			b, _ := json.Marshal(meta)
			_ = os.WriteFile(metaPath, b, 0o644)
		}
		return id, size, ct, nil
	} else if !os.IsNotExist(err) {
		return "", 0, "", err
	}

	if err := f.Sync(); err != nil {
		return "", 0, "", err
	}
	if err := f.Close(); err != nil {
		return "", 0, "", err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return "", 0, "", err
	}

	// meta 保存
	meta := Meta{ContentType: ct, Size: size}
	b, _ := json.Marshal(meta)
	if err := os.WriteFile(metaPath, b, 0o644); err != nil {
		return "", 0, "", err
	}

	return id, size, ct, nil
}
