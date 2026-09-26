package main

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

var ErrMockPublisherFailed = errors.New("mock publisher simulated failure")

type MockPublisher struct {
	mu                sync.RWMutex
	delay             time.Duration
	failureRate       float64
	totalPublished    atomic.Uint64
	channelsPublished atomic.Uint64
	campaignPublished atomic.Uint64
	totalErrors       atomic.Uint64
	isClosed          bool
}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}

func (m *MockPublisher) SetDelay(d time.Duration) {
	m.mu.Lock()
	m.delay = d
	m.mu.Unlock()
}

func (m *MockPublisher) SetFailureRate(rate float64) {
	m.mu.Lock()
	m.failureRate = rate
	m.mu.Unlock()
}

func (m *MockPublisher) Publish(ctx context.Context, routingKey string, message any) error {
	m.mu.RLock()
	if m.isClosed {
		m.mu.RUnlock()
		return errors.New("mock publisher closed")
	}
	delay := m.delay
	failRate := m.failureRate
	m.mu.RUnlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			m.totalErrors.Add(1)
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	if failRate > 0 && rand.Float64() < failRate {
		m.totalErrors.Add(1)
		return ErrMockPublisherFailed
	}

	m.totalPublished.Add(1)
	if routingKey == "campaign_queue" {
		m.campaignPublished.Add(1)
	} else {
		m.channelsPublished.Add(1)
	}

	return nil
}

func (m *MockPublisher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isClosed = true
	return nil
}

func (m *MockPublisher) Stats() (total, channels, campaign, errs uint64) {
	return m.totalPublished.Load(), m.channelsPublished.Load(), m.campaignPublished.Load(), m.totalErrors.Load()
}

type MockMediaServer struct {
	server       *httptest.Server
	mu           sync.RWMutex
	delay        time.Duration
	failureRate  float64
	requestCount atomic.Uint64
}

func NewMockMediaServer() *MockMediaServer {
	ms := &MockMediaServer{}

	mux := http.NewServeMux()
	mux.HandleFunc("/media/", func(w http.ResponseWriter, r *http.Request) {
		ms.requestCount.Add(1)

		ms.mu.RLock()
		delay := ms.delay
		failRate := ms.failureRate
		ms.mu.RUnlock()

		if delay > 0 {
			time.Sleep(delay)
		}

		if failRate > 0 && rand.Float64() < failRate {
			http.Error(w, "simulated media failure", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("mock-media-binary-data-for-stress-testing"))
	})

	ms.server = httptest.NewServer(mux)
	return ms
}

func (ms *MockMediaServer) URL() string {
	if ms.server == nil {
		return ""
	}
	return ms.server.URL
}

func (ms *MockMediaServer) SetDelay(d time.Duration) {
	ms.mu.Lock()
	ms.delay = d
	ms.mu.Unlock()
}

func (ms *MockMediaServer) SetFailureRate(rate float64) {
	ms.mu.Lock()
	ms.failureRate = rate
	ms.mu.Unlock()
}

func (ms *MockMediaServer) Close() {
	if ms.server != nil {
		ms.server.Close()
	}
}

func (ms *MockMediaServer) Requests() uint64 {
	return ms.requestCount.Load()
}
