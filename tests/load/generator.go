package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type LoadConfig struct {
	TargetBaseURL  string
	MediaBaseURL   string
	RPS            int
	Duration       time.Duration
	Concurrency    int
	WarmupDuration time.Duration
}

type LoadRunner struct {
	cfg       LoadConfig
	collector *MetricsCollector
	client    *http.Client
}

func NewLoadRunner(cfg LoadConfig) *LoadRunner {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 200
	}
	if cfg.RPS <= 0 {
		cfg.RPS = 1000
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 5 * time.Second
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:        3000,
		MaxIdleConnsPerHost: 3000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	expectedReqs := int(float64(cfg.RPS) * (cfg.WarmupDuration + cfg.Duration).Seconds() * 1.5)
	if expectedReqs < 10000 {
		expectedReqs = 10000
	}

	return &LoadRunner{
		cfg:       cfg,
		collector: NewMetricsCollector(expectedReqs),
		client:    client,
	}
}

func (r *LoadRunner) Run(ctx context.Context) PerformanceSummary {
	r.collector.Start()

	stopSampling := make(chan struct{})
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				r.collector.SampleRuntime()
			}
		}
	}()

	totalDuration := r.cfg.Duration
	ratePerSec := r.cfg.RPS

	jobs := make(chan struct{}, ratePerSec*2)

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				spec := GeneratePayload(r.cfg.MediaBaseURL)
				reqURL := r.cfg.TargetBaseURL + spec.Endpoint

				req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(spec.Body))
				if err != nil {
					r.collector.Record(0, 0, true)
					continue
				}

				for k, v := range spec.Headers {
					req.Header.Set(k, v)
				}

				start := time.Now()
				resp, err := r.client.Do(req)
				latency := time.Since(start)

				if err != nil {
					r.collector.Record(0, latency, true)
					continue
				}

				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				r.collector.Record(resp.StatusCode, latency, false)
			}
		}()
	}

	interval := time.Second / time.Duration(ratePerSec)
	if interval <= 0 {
		interval = time.Microsecond
	}

	limiter := time.NewTicker(interval)
	defer limiter.Stop()

	endTime := time.Now().Add(totalDuration)

	for time.Now().Before(endTime) {
		select {
		case <-ctx.Done():
			goto Done
		case <-limiter.C:
			select {
			case jobs <- struct{}{}:
			default:
			}
		}
	}

Done:
	close(jobs)
	wg.Wait()

	close(stopSampling)
	r.collector.Stop()

	return r.collector.Summary()
}
