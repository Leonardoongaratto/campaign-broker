package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockPublisher_DirectUnit(t *testing.T) {
	pub := NewMockPublisher()
	defer pub.Close()

	ctx := context.Background()
	err := pub.Publish(ctx, "channels_queue", map[string]string{"key": "value"})
	require.NoError(t, err)

	err = pub.Publish(ctx, "campaign_queue", map[string]string{"key": "value"})
	require.NoError(t, err)

	total, channels, campaign, errs := pub.Stats()
	assert.Equal(t, uint64(2), total)
	assert.Equal(t, uint64(1), channels)
	assert.Equal(t, uint64(1), campaign)
	assert.Equal(t, uint64(0), errs)

	pub.SetDelay(10 * time.Millisecond)
	start := time.Now()
	err = pub.Publish(ctx, "channels_queue", "delayed")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 10*time.Millisecond)

	cancCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = pub.Publish(cancCtx, "channels_queue", "cancelled")
	assert.Error(t, err)

	pub.SetFailureRate(1.0)
	err = pub.Publish(ctx, "channels_queue", "failing")
	assert.ErrorIs(t, err, ErrMockPublisherFailed)

	_ = pub.Close()
	err = pub.Publish(ctx, "channels_queue", "after_close")
	assert.Error(t, err)
}

func TestMockMediaServer_DirectUnit(t *testing.T) {
	ms := NewMockMediaServer()
	defer ms.Close()

	assert.NotEmpty(t, ms.URL())
	ms.SetDelay(5 * time.Millisecond)
	ms.SetFailureRate(0.0)
	assert.Equal(t, uint64(0), ms.Requests())
}

func TestMetricsCollector_Unit(t *testing.T) {
	mc := NewMetricsCollector(100)
	mc.Start()

	mc.Record(200, 2*time.Millisecond, false)
	mc.Record(202, 3*time.Millisecond, false)
	mc.Record(400, 1*time.Millisecond, false)
	mc.Record(503, 4*time.Millisecond, false)
	mc.Record(418, 5*time.Millisecond, false)
	mc.Record(0, 0, true)

	mc.SampleRuntime()
	mc.Stop()

	summary := mc.Summary()
	assert.Equal(t, 6, summary.TotalRequests)
	assert.Equal(t, uint64(1), summary.NetworkErrors)
	assert.Equal(t, uint64(1), summary.StatusCodes[200])
	assert.Equal(t, uint64(1), summary.StatusCodes[202])
	assert.Equal(t, uint64(1), summary.StatusCodes[400])
	assert.Equal(t, uint64(1), summary.StatusCodes[503])
	assert.Equal(t, uint64(1), summary.StatusCodes[418])

	rep := summary.String()
	assert.Contains(t, rep, "CAMPAIGN BROKER - PERFORMANCE TEST REPORT")
	assert.Contains(t, rep, "p50 (Median)")
	assert.Contains(t, rep, "HTTP 200:             1")
}

func TestGeneratePayload_Distribution(t *testing.T) {
	mediaBase := "http://127.0.0.1:9999"
	for i := 0; i < 50; i++ {
		spec := GeneratePayload(mediaBase)
		assert.NotEmpty(t, spec.Endpoint)
		assert.NotEmpty(t, spec.Body)
		assert.Equal(t, "application/json", spec.Headers["Content-Type"])
		assert.Equal(t, "load-test-agent", spec.Headers["X-User-Id"])
	}
}

func TestLoadRunner_IntegrationRun(t *testing.T) {
	pub := NewMockPublisher()
	srv := setupTestServer(t, pub, nil)
	defer srv.Close()

	runner := NewLoadRunner(LoadConfig{
		TargetBaseURL: srv.URL,
		MediaBaseURL:  srv.URL,
		RPS:           100,
		Duration:      500 * time.Millisecond,
		Concurrency:   10,
	})

	summary := runner.Run(context.Background())
	assert.Greater(t, summary.TotalRequests, 0)
	assert.Greater(t, summary.ThroughputRPS, 0.0)
}

func TestRunLoadApp_EmbeddedRun(t *testing.T) {
	summary, err := runLoadApp([]string{"-duration=300ms", "-rps=20", "-workers=2", "-warmup=100ms"})
	require.NoError(t, err)
	assert.Greater(t, summary.TotalRequests, 0)
}

func TestRunLoadApp_WithTarget(t *testing.T) {
	pub := NewMockPublisher()
	srv := setupTestServer(t, pub, nil)
	defer srv.Close()

	summary, err := runLoadApp([]string{"-target=" + srv.URL, "-duration=200ms", "-rps=10", "-workers=2", "-warmup=0s"})
	require.NoError(t, err)
	assert.Greater(t, summary.TotalRequests, 0)
}

func TestRunLoadApp_InvalidFlag(t *testing.T) {
	_, err := runLoadApp([]string{"-unknown-flag-error"})
	assert.Error(t, err)
}
