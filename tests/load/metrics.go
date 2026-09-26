package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RuntimeSnapshot struct {
	Timestamp      time.Time
	NumGoroutine   int
	HeapAllocBytes uint64
	HeapSysBytes   uint64
	NumGC          uint32
}

func CaptureRuntimeSnapshot() RuntimeSnapshot {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return RuntimeSnapshot{
		Timestamp:      time.Now(),
		NumGoroutine:   runtime.NumGoroutine(),
		HeapAllocBytes: m.HeapAlloc,
		HeapSysBytes:   m.HeapSys,
		NumGC:          m.NumGC,
	}
}

type MetricsCollector struct {
	mu           sync.Mutex
	latencies    []time.Duration
	statusCounts map[int]*atomic.Uint64
	networkErrs  atomic.Uint64
	startTime    time.Time
	endTime      time.Time
	initialStats RuntimeSnapshot
	peakStats    RuntimeSnapshot
	finalStats   RuntimeSnapshot
}

func NewMetricsCollector(capacity int) *MetricsCollector {
	if capacity <= 0 {
		capacity = 50000
	}
	mc := &MetricsCollector{
		latencies:    make([]time.Duration, 0, capacity),
		statusCounts: make(map[int]*atomic.Uint64),
		initialStats: CaptureRuntimeSnapshot(),
	}
	mc.statusCounts[200] = &atomic.Uint64{}
	mc.statusCounts[202] = &atomic.Uint64{}
	mc.statusCounts[400] = &atomic.Uint64{}
	mc.statusCounts[401] = &atomic.Uint64{}
	mc.statusCounts[403] = &atomic.Uint64{}
	mc.statusCounts[500] = &atomic.Uint64{}
	mc.statusCounts[503] = &atomic.Uint64{}
	return mc
}

func (mc *MetricsCollector) Start() {
	mc.startTime = time.Now()
	mc.initialStats = CaptureRuntimeSnapshot()
	mc.peakStats = mc.initialStats
}

func (mc *MetricsCollector) Record(status int, latency time.Duration, isNetErr bool) {
	if isNetErr {
		mc.networkErrs.Add(1)
		return
	}

	mc.mu.Lock()
	counter, exists := mc.statusCounts[status]
	if !exists {
		counter = &atomic.Uint64{}
		mc.statusCounts[status] = counter
	}
	mc.latencies = append(mc.latencies, latency)
	mc.mu.Unlock()

	counter.Add(1)
}

func (mc *MetricsCollector) SampleRuntime() {
	current := CaptureRuntimeSnapshot()
	mc.mu.Lock()
	if current.NumGoroutine > mc.peakStats.NumGoroutine {
		mc.peakStats.NumGoroutine = current.NumGoroutine
	}
	if current.HeapAllocBytes > mc.peakStats.HeapAllocBytes {
		mc.peakStats.HeapAllocBytes = current.HeapAllocBytes
	}
	mc.mu.Unlock()
}

func (mc *MetricsCollector) Stop() {
	mc.endTime = time.Now()
	mc.finalStats = CaptureRuntimeSnapshot()
}

type PerformanceSummary struct {
	TotalRequests    int
	Duration         time.Duration
	ThroughputRPS    float64
	MinLatency       time.Duration
	P50Latency       time.Duration
	P90Latency       time.Duration
	P95Latency       time.Duration
	P99Latency       time.Duration
	MaxLatency       time.Duration
	MeanLatency      time.Duration
	StatusCodes      map[int]uint64
	NetworkErrors    uint64
	InitialGoroutine int
	PeakGoroutine    int
	FinalGoroutine   int
	HeapAllocMB      float64
	PeakHeapAllocMB  float64
	GCRuns           uint32
}

func (mc *MetricsCollector) Summary() PerformanceSummary {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	totalReqs := len(mc.latencies) + int(mc.networkErrs.Load())
	duration := mc.endTime.Sub(mc.startTime)
	if duration <= 0 {
		duration = time.Millisecond
	}

	rps := float64(len(mc.latencies)) / duration.Seconds()

	var p50, p90, p95, p99, minLat, maxLat, meanLat time.Duration
	if len(mc.latencies) > 0 {
		sorted := make([]time.Duration, len(mc.latencies))
		copy(sorted, mc.latencies)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i] < sorted[j]
		})

		minLat = sorted[0]
		maxLat = sorted[len(sorted)-1]
		p50 = sorted[int(float64(len(sorted))*0.50)]
		p90 = sorted[int(float64(len(sorted))*0.90)]
		p95 = sorted[int(float64(len(sorted))*0.95)]
		p99 = sorted[int(float64(len(sorted))*0.99)]

		var totalDuration time.Duration
		for _, l := range sorted {
			totalDuration += l
		}
		meanLat = totalDuration / time.Duration(len(sorted))
	}

	statuses := make(map[int]uint64)
	for code, count := range mc.statusCounts {
		val := count.Load()
		if val > 0 {
			statuses[code] = val
		}
	}

	return PerformanceSummary{
		TotalRequests:    totalReqs,
		Duration:         duration,
		ThroughputRPS:    rps,
		MinLatency:       minLat,
		P50Latency:       p50,
		P90Latency:       p90,
		P95Latency:       p95,
		P99Latency:       p99,
		MaxLatency:       maxLat,
		MeanLatency:      meanLat,
		StatusCodes:      statuses,
		NetworkErrors:    mc.networkErrs.Load(),
		InitialGoroutine: mc.initialStats.NumGoroutine,
		PeakGoroutine:    mc.peakStats.NumGoroutine,
		FinalGoroutine:   mc.finalStats.NumGoroutine,
		HeapAllocMB:      float64(mc.finalStats.HeapAllocBytes) / (1024 * 1024),
		PeakHeapAllocMB:  float64(mc.peakStats.HeapAllocBytes) / (1024 * 1024),
		GCRuns:           mc.finalStats.NumGC - mc.initialStats.NumGC,
	}
}

func (s PerformanceSummary) String() string {
	var b strings.Builder
	b.WriteString("\n================================================================================\n")
	b.WriteString("                 CAMPAIGN BROKER - PERFORMANCE TEST REPORT                      \n")
	b.WriteString("================================================================================\n")
	fmt.Fprintf(&b, "Total Requests:        %d\n", s.TotalRequests)
	fmt.Fprintf(&b, "Duration:              %v\n", s.Duration.Round(time.Millisecond))
	fmt.Fprintf(&b, "Throughput:            %.2f RPS\n", s.ThroughputRPS)
	b.WriteString("--------------------------------------------------------------------------------\n")
	b.WriteString("LATENCY PERCENTILES:\n")
	fmt.Fprintf(&b, "  Min:                 %v\n", s.MinLatency)
	fmt.Fprintf(&b, "  Mean:                %v\n", s.MeanLatency)
	fmt.Fprintf(&b, "  p50 (Median):        %v\n", s.P50Latency)
	fmt.Fprintf(&b, "  p90:                 %v\n", s.P90Latency)
	fmt.Fprintf(&b, "  p95:                 %v\n", s.P95Latency)
	fmt.Fprintf(&b, "  p99:                 %v\n", s.P99Latency)
	fmt.Fprintf(&b, "  Max:                 %v\n", s.MaxLatency)
	b.WriteString("--------------------------------------------------------------------------------\n")
	b.WriteString("HTTP STATUS CODES:\n")
	for code, count := range s.StatusCodes {
		fmt.Fprintf(&b, "  HTTP %d:             %d\n", code, count)
	}
	if s.NetworkErrors > 0 {
		fmt.Fprintf(&b, "  Network Errors:      %d\n", s.NetworkErrors)
	}
	b.WriteString("--------------------------------------------------------------------------------\n")
	b.WriteString("RUNTIME & RESOURCE METRICS:\n")
	fmt.Fprintf(&b, "  Goroutines:          Initial: %d | Peak: %d | Final: %d\n", s.InitialGoroutine, s.PeakGoroutine, s.FinalGoroutine)
	fmt.Fprintf(&b, "  Heap Allocation:     Final: %.2f MB | Peak: %.2f MB\n", s.HeapAllocMB, s.PeakHeapAllocMB)
	fmt.Fprintf(&b, "  GC Cycles Completed: %d\n", s.GCRuns)
	b.WriteString("--------------------------------------------------------------------------------\n")
	b.WriteString("SLA VERIFICATION:\n")
	pass50 := s.P50Latency <= 5*time.Millisecond
	pass95 := s.P95Latency <= 20*time.Millisecond
	pass99 := s.P99Latency <= 50*time.Millisecond
	fmt.Fprintf(&b, "  p50 <= 5ms:          [%s] (%v)\n", checkMark(pass50), s.P50Latency)
	fmt.Fprintf(&b, "  p95 <= 20ms:         [%s] (%v)\n", checkMark(pass95), s.P95Latency)
	fmt.Fprintf(&b, "  p99 <= 50ms:         [%s] (%v)\n", checkMark(pass99), s.P99Latency)
	b.WriteString("================================================================================\n")
	return b.String()
}

func checkMark(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
