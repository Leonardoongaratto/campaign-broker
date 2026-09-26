package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"campaign-broker/internal/api"
	"campaign-broker/internal/api/handlers"
	"campaign-broker/internal/domain"
	"campaign-broker/internal/downloader"
	"campaign-broker/internal/service"
)

func setupTestServer(t *testing.T, pub *MockPublisher, dl downloader.Downloader) *httptest.Server {
	gin.SetMode(gin.TestMode)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	if dl == nil {
		dl = downloader.NewHTTPDownloader(2 * time.Second)
	}

	brokerService := service.NewBrokerService(pub, dl)
	brokerHandler := handlers.NewBrokerHandler(brokerService, logger)

	router := api.SetupRouter(api.RouterConfig{
		AuthURL:       "",
		BrokerHandler: brokerHandler,
		Logger:        logger,
	})

	return httptest.NewServer(router)
}

func TestChaos_AMQPBackpressureAndExhaustion(t *testing.T) {
	pub := NewMockPublisher()
	pub.SetDelay(20 * time.Millisecond)
	pub.SetFailureRate(0.3)

	mediaServer := NewMockMediaServer()
	defer mediaServer.Close()

	srv := setupTestServer(t, pub, nil)
	defer srv.Close()

	client := &http.Client{Timeout: 2 * time.Second}

	var wg sync.WaitGroup
	var successCount, errCount int64
	var mu sync.Mutex

	totalRequests := 50
	for i := 0; i < totalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			spec := GeneratePayload(mediaServer.URL())
			req, err := http.NewRequest(http.MethodPost, srv.URL+spec.Endpoint, bytes.NewReader(spec.Body))
			if err != nil {
				return
			}
			for k, v := range spec.Headers {
				req.Header.Set(k, v)
			}

			resp, err := client.Do(req)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			mu.Lock()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
				successCount++
			} else if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusServiceUnavailable {
				errCount++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	assert.Greater(t, successCount+errCount, int64(0))
	assert.Equal(t, int64(totalRequests), successCount+errCount)
}

func TestChaos_MediaDownloadTimeoutAndFailure(t *testing.T) {
	mediaServer := NewMockMediaServer()
	defer mediaServer.Close()

	mediaServer.SetDelay(400 * time.Millisecond)
	mediaServer.SetFailureRate(1.0)

	pub := NewMockPublisher()
	dl := downloader.NewHTTPDownloader(1 * time.Second)
	srv := setupTestServer(t, pub, dl)
	defer srv.Close()

	mediaURL := mediaServer.URL() + "/media/test-doc.pdf"
	corrID := "chaos-corr-1"
	reqPayload := domain.OutboundRequest{
		Destinations: []domain.Destination{
			{CorrelationID: &corrID, Destination: "+5511999990001"},
		},
		Message: domain.RawMessage{
			Document: &domain.FileData{
				URL:  &mediaURL,
				Type: "pdf",
			},
		},
	}

	body, err := json.Marshal(reqPayload)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/conversations/webhook/broker/message", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-Id", "chaos-user")
	req.Header.Set("X-Scope", "api/broker:write")
	req.Header.Set("X-BV-File-Token", "Bearer chaos-token")

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var errResp domain.ErrorResponse
	err = json.NewDecoder(resp.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Equal(t, "Failed to download document", errResp.Detail.Message)
}

func TestChaos_GoroutineLeakAndResourceReclamation(t *testing.T) {
	pub := NewMockPublisher()
	srv := setupTestServer(t, pub, nil)

	tr := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 50,
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	numRequests := 100
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			corrID := fmt.Sprintf("leak-corr-%d", idx)
			phone := fmt.Sprintf("+55119999%04d", idx)
			text := "Benchmark leak detection"
			reqPayload := domain.OutboundRequest{
				Destinations: []domain.Destination{
					{CorrelationID: &corrID, Destination: phone},
				},
				Message: domain.RawMessage{
					MessageText: &text,
				},
			}
			data, _ := json.Marshal(reqPayload)
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/conversations/webhook/broker/message", bytes.NewReader(data))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-User-Id", "leak-tester")
			req.Header.Set("X-Scope", "api/broker:write")

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}(i)
	}

	wg.Wait()

	tr.CloseIdleConnections()
	srv.Close()
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()

	goroutineDelta := finalGoroutines - initialGoroutines
	if goroutineDelta < 0 {
		goroutineDelta = 0
	}
	assert.LessOrEqual(t, goroutineDelta, 15, "Goroutine delta should be negligible, indicating no goroutine leaks")
}
