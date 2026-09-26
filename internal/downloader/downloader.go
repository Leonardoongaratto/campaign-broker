package downloader

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidURL     = errors.New("invalid media url")
	ErrDownloadFailed = errors.New("failed to download media")
)

type Downloader interface {
	DownloadAndEncodeBase64(ctx context.Context, fileURL string, fileToken string) (string, error)
}

type HTTPDownloader struct {
	client     *http.Client
	bufferPool sync.Pool
}

func NewHTTPDownloader(timeout time.Duration) *HTTPDownloader {
	transport := &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
	}

	return &HTTPDownloader{
		client: &http.Client{
			Transport: transport,
			Timeout:   timeout,
		},
		bufferPool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 32*1024))
			},
		},
	}
}

func (d *HTTPDownloader) DownloadAndEncodeBase64(ctx context.Context, fileURL string, fileToken string) (string, error) {
	if strings.TrimSpace(fileURL) == "" {
		return "", ErrInvalidURL
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return "", err
	}

	token := strings.TrimSpace(fileToken)
	if token != "" {
		lower := strings.ToLower(token)
		if strings.HasPrefix(lower, "bearer ") {
			token = strings.TrimSpace(token[7:])
		} else if lower == "bearer" {
			token = ""
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return "", ErrDownloadFailed
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", ErrDownloadFailed
	}

	encodedBuf := d.bufferPool.Get().(*bytes.Buffer)
	encodedBuf.Reset()
	defer d.bufferPool.Put(encodedBuf)

	encoder := base64.NewEncoder(base64.StdEncoding, encodedBuf)
	if _, err := io.Copy(encoder, resp.Body); err != nil {
		return "", ErrDownloadFailed
	}
	if err := encoder.Close(); err != nil {
		return "", ErrDownloadFailed
	}

	return encodedBuf.String(), nil
}
