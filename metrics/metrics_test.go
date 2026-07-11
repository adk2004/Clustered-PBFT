package metrics

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func makeRecord(id string, latencyMs float64, success bool) OperationRecord {
	start := time.Now()
	end := start.Add(time.Duration(latencyMs * float64(time.Millisecond)))
	return OperationRecord{
		OperationID: id,
		StartTime:   start,
		EndTime:     end,
		NodeCount:   12,
		IsGlobal:    true,
		Success:     success,
	}
}

func populateCollector(c *Collector, latencies []float64) {
	for i, l := range latencies {
		c.Add(makeRecord(fmt.Sprintf("op-%d", i), l, true))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1 — LatencyMs of a 500 ms operation = 500.0 ± 1 ms
// ─────────────────────────────────────────────────────────────────────────────

func TestMetricsLatencyMs(t *testing.T) {
	t.Parallel()

	r := makeRecord("op-0", 500.0, true)
	got := r.LatencyMs()
	if math.Abs(got-500.0) > 1.0 {
		t.Errorf("LatencyMs() = %.3f, want 500.0 ± 1.0 ms", got)
	}
}

func TestMetricsLatencyMsZero(t *testing.T) {
	t.Parallel()

	r := OperationRecord{} // zero-value times
	if r.LatencyMs() != 0 {
		t.Errorf("zero-value record LatencyMs() = %.3f, want 0", r.LatencyMs())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2 — Throughput of 100 ops in 1 second = 100 ± 5 ops/s
// ─────────────────────────────────────────────────────────────────────────────

func TestMetricsThroughput(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	base := time.Now()

	// 100 operations each 10 ms long, starting at 1 ms intervals over 1 s.
	for i := 0; i < 100; i++ {
		start := base.Add(time.Duration(i) * 10 * time.Millisecond)
		end := start.Add(10 * time.Millisecond)
		c.Add(OperationRecord{
			OperationID: fmt.Sprintf("op-%d", i),
			StartTime:   start,
			EndTime:     end,
			Success:     true,
		})
	}

	// Window = first start to last end = ~1 second.
	got := c.Throughput()
	// 100 ops / ~1.01 s ≈ 99 ops/s; allow generous ± 20 for timing jitter.
	if math.Abs(got-100) > 20 {
		t.Errorf("Throughput() = %.2f ops/s, want ≈ 100 ± 20", got)
	}
}

func TestMetricsThroughputEmpty(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	if c.Throughput() != 0 {
		t.Errorf("empty collector Throughput() = %.2f, want 0", c.Throughput())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3 — P99Latency of [1..100] ms = 99 ms
// ─────────────────────────────────────────────────────────────────────────────

func TestMetricsP99Latency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	lats := make([]float64, 100)
	for i := range lats {
		lats[i] = float64(i + 1) // 1, 2, …, 100 ms
	}
	populateCollector(c, lats)

	got := c.P99Latency()
	// ceil(99/100 * 100) - 1 = index 98 → value 99
	if math.Abs(got-99.0) > 1.0 {
		t.Errorf("P99Latency() = %.3f, want 99.0 ± 1.0", got)
	}
}

func TestMetricsMeanLatency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	populateCollector(c, []float64{100, 200, 300}) // mean = 200
	got := c.MeanLatency()
	if math.Abs(got-200.0) > 1.0 {
		t.Errorf("MeanLatency() = %.3f, want 200.0 ± 1.0", got)
	}
}

func TestMetricsP50Latency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	lats := make([]float64, 10)
	for i := range lats {
		lats[i] = float64(i+1) * 10 // 10,20,...,100
	}
	populateCollector(c, lats)

	got := c.P50Latency()
	// p50 of [10,20,30,40,50,60,70,80,90,100] = 50 (index ceil(0.5*10)-1 = 4)
	if math.Abs(got-50.0) > 1.0 {
		t.Errorf("P50Latency() = %.3f, want 50.0 ± 1.0", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4 — SaveCSV writes header + one row per record
// ─────────────────────────────────────────────────────────────────────────────

func TestMetricsSaveCSV(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	for i := 0; i < 5; i++ {
		c.Add(makeRecord(fmt.Sprintf("op-%d", i), float64(i+1)*10, true))
	}
	c.Add(makeRecord("op-fail", 999, false))

	path := t.TempDir() + "/metrics_test.csv"
	if err := c.SaveCSV(path); err != nil {
		t.Fatalf("SaveCSV: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	lines := countLines(string(data))
	// 1 header + 6 data rows = 7 lines
	want := 7
	if lines != want {
		t.Errorf("CSV has %d lines, want %d (1 header + 6 rows)", lines, want)
	}

	// Must start with header.
	if len(data) < 10 || string(data[:11]) != "operation_i" {
		t.Errorf("CSV does not start with header row")
	}
}

func countLines(s string) int {
	n := 0
	for _, c := range s {
		if c == '\n' {
			n++
		}
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: SuccessRate, SuccessCount, Count
// ─────────────────────────────────────────────────────────────────────────────

func TestMetricsSuccessRate(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	for i := 0; i < 7; i++ {
		c.Add(makeRecord(fmt.Sprintf("ok-%d", i), 10, true))
	}
	for i := 0; i < 3; i++ {
		c.Add(makeRecord(fmt.Sprintf("fail-%d", i), 10, false))
	}

	if c.Count() != 10 {
		t.Errorf("Count() = %d, want 10", c.Count())
	}
	if c.SuccessCount() != 7 {
		t.Errorf("SuccessCount() = %d, want 7", c.SuccessCount())
	}
	got := c.SuccessRate()
	if math.Abs(got-0.7) > 1e-9 {
		t.Errorf("SuccessRate() = %.4f, want 0.7", got)
	}
}

func TestMetricsReset(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	populateCollector(c, []float64{1, 2, 3})
	if c.Count() != 3 {
		t.Fatalf("Count before reset = %d, want 3", c.Count())
	}
	c.Reset()
	if c.Count() != 0 {
		t.Errorf("Count after Reset() = %d, want 0", c.Count())
	}
}

func TestMetricsConcurrentAdd(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	const n = 200
	done := make(chan struct{}, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			c.Add(makeRecord(fmt.Sprintf("concurrent-%d", idx), 5, true))
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}

	if c.Count() != n {
		t.Errorf("concurrent Add: Count() = %d, want %d", c.Count(), n)
	}
}

func TestMetricsMaxLatency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	populateCollector(c, []float64{10, 50, 200, 1, 75})

	got := c.MaxLatency()
	if math.Abs(got-200.0) > 1.0 {
		t.Errorf("MaxLatency() = %.3f, want 200.0 ± 1.0", got)
	}
}

func TestMetricsOnlyFailedOpsExcludedFromLatency(t *testing.T) {
	t.Parallel()

	c := NewCollector()
	c.Add(makeRecord("fail", 9999, false))
	c.Add(makeRecord("ok", 10, true))

	// Mean latency must be ~10, not ~5005 (failed op excluded).
	got := c.MeanLatency()
	if math.Abs(got-10.0) > 1.0 {
		t.Errorf("MeanLatency() = %.3f (failed ops included?), want ≈10", got)
	}
}