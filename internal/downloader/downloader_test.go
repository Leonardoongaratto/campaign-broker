package downloader_test

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"campaign-broker/internal/downloader"
)

func TestHTTPDownloader_DownloadAndEncodeBase64(t *testing.T) {
	t.Run("successful download and encode with token", func(t *testing.T) {
		expectedContent := "mock media file binary stream"
		expectedBase64 := base64.StdEncoding.EncodeToString([]byte(expectedContent))

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer secret_file_token", r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(expectedContent))
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		encoded, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "secret_file_token")

		require.NoError(t, err)
		assert.Equal(t, expectedBase64, encoded)
	})

	t.Run("successful download with already prefixed bearer token", func(t *testing.T) {
		expectedContent := "raw image data"
		expectedBase64 := base64.StdEncoding.EncodeToString([]byte(expectedContent))

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer raw_token_value", r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(expectedContent))
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		encoded, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "Bearer raw_token_value")

		require.NoError(t, err)
		assert.Equal(t, expectedBase64, encoded)
	})

	t.Run("empty url returns error", func(t *testing.T) {
		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(context.Background(), "   ", "")

		require.ErrorIs(t, err, downloader.ErrInvalidURL)
	})

	t.Run("non-200 status returns download error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "")

		require.ErrorIs(t, err, downloader.ErrDownloadFailed)
	})

	t.Run("network error returns download error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		serverURL := server.URL
		server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(context.Background(), serverURL, "")

		require.ErrorIs(t, err, downloader.ErrDownloadFailed)
	})

	t.Run("context cancelled before download", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("some data"))
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(ctx, server.URL, "")

		require.Error(t, err)
	})

	t.Run("context timeout during slow streaming download", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			flusher, ok := w.(http.Flusher)
			if ok {
				_, _ = w.Write([]byte("chunk 1"))
				flusher.Flush()
			}
			time.Sleep(300 * time.Millisecond)
			_, _ = w.Write([]byte("chunk 2"))
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(ctx, server.URL, "")

		require.Error(t, err)
	})

	t.Run("token with mixed case and whitespace", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "Bearer clean_token", r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("media_content"))
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "   bEaReR    clean_token   ")
		require.NoError(t, err)
	})

	t.Run("token with only bearer prefix without value", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Empty(t, r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("media_content"))
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		_, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "bearer   ")
		require.NoError(t, err)
	})

	t.Run("concurrent downloads under stress", func(t *testing.T) {
		expectedContent := "concurrent stream content payload"
		expectedBase64 := base64.StdEncoding.EncodeToString([]byte(expectedContent))

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(expectedContent))
		}))
		defer server.Close()

		dl := downloader.NewHTTPDownloader(5 * time.Second)
		concurrency := 50
		errChan := make(chan error, concurrency)

		for i := 0; i < concurrency; i++ {
			go func() {
				res, err := dl.DownloadAndEncodeBase64(context.Background(), server.URL, "token_123")
				if err != nil {
					errChan <- err
					return
				}
				if res != expectedBase64 {
					errChan <- errors.New("base64 mismatch")
					return
				}
				errChan <- nil
			}()
		}

		for i := 0; i < concurrency; i++ {
			assert.NoError(t, <-errChan)
		}
	})
}

func BenchmarkHTTPDownloader_DownloadAndEncodeBase64(b *testing.B) {
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	dl := downloader.NewHTTPDownloader(10 * time.Second)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := dl.DownloadAndEncodeBase64(ctx, server.URL, "bench_token")
		if err != nil {
			b.Fatalf("unexpected benchmark error: %v", err)
		}
	}
}
