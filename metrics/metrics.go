// Package metrics collects, aggregates and exports performance metrics for
// the vehicular BFT protocol simulation (paper Section XIV).
//
// The paper evaluates (Figures 4-8):
//   - Throughput  (req/s) vs request load at 8, 12, 16, 20 nodes
//   - Latency     (ms)    vs request load
//   - Dynamic testbed: throughput/latency vs tick interval
//
// This package reproduces those metrics using real wall-clock timings from
// the simulation layer.
package metrics

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// OperationRecord
// ─────────────────────────────────────────────────────────────────────────────

// OperationRecord stores timing info for one consensus round.
type OperationRecord struct {
	OperationID string
	StartTime   time.Time
	EndTime     time.Time
	NodeCount   int
	IsGlobal    bool
	Success     bool
}

// LatencyMs returns end-to-end latency in milliseconds.
func (r OperationRecord) LatencyMs() float64 {
	if r.EndTime.IsZero() || r.StartTime.IsZero() {
		return 0
	}
	return float64(r.EndTime.Sub(r.StartTime).Nanoseconds()) / 1e6
}

// ─────────────────────────────────────────────────────────────────────────────
// Collector
// ─────────────────────────────────────────────────────────────────────────────

// Collector accumulates OperationRecords and computes summary statistics.
// All methods are safe for concurrent use.
type Collector struct {
	mu      sync.Mutex
	records []OperationRecord
}

// NewCollector creates an empty Collector.
func NewCollector() *Collector {
	return &Collector{}
}

// Add appends a record. Safe to call from multiple goroutines.
func (c *Collector) Add(r OperationRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
}

// Count returns the total number of recorded operations.
func (c *Collector) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.records)
}

// SuccessCount returns the number of successful operations.
func (c *Collector) SuccessCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.records {
		if r.Success {
			n++
		}
	}
	return n
}

// Throughput returns successful operations per second over the observation window.
// The window is defined as the span from the earliest StartTime to the latest EndTime.
func (c *Collector) Throughput() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.records) == 0 {
		return 0
	}

	var earliest, latest time.Time
	successes := 0

	for _, r := range c.records {
		if earliest.IsZero() || r.StartTime.Before(earliest) {
			earliest = r.StartTime
		}
		if r.EndTime.After(latest) {
			latest = r.EndTime
		}
		if r.Success {
			successes++
		}
	}

	dur := latest.Sub(earliest).Seconds()
	if dur <= 0 {
		return float64(successes)
	}
	return float64(successes) / dur
}

// MeanLatency returns the arithmetic mean latency in ms for successful ops.
func (c *Collector) MeanLatency() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return mean(c.successLatencies())
}

// P99Latency returns the 99th-percentile latency in ms for successful ops.
func (c *Collector) P99Latency() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return percentile(c.successLatencies(), 99)
}

// P50Latency returns the median latency in ms for successful ops.
func (c *Collector) P50Latency() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return percentile(c.successLatencies(), 50)
}

// MaxLatency returns the maximum latency across all successful operations.
func (c *Collector) MaxLatency() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	lats := c.successLatencies()
	if len(lats) == 0 {
		return 0
	}
	return lats[len(lats)-1]
}

// SuccessRate returns the fraction of operations that succeeded (0.0–1.0).
func (c *Collector) SuccessRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.records) == 0 {
		return 0
	}
	n := 0
	for _, r := range c.records {
		if r.Success {
			n++
		}
	}
	return float64(n) / float64(len(c.records))
}

// Report prints a human-readable summary to stdout.
func (c *Collector) Report() {
	fmt.Printf("── Metrics Report ────────────────────────────────\n")
	fmt.Printf("  Total ops    : %d\n", c.Count())
	fmt.Printf("  Successful   : %d  (%.1f%%)\n", c.SuccessCount(), c.SuccessRate()*100)
	fmt.Printf("  Throughput   : %.2f ops/s\n", c.Throughput())
	fmt.Printf("  Mean latency : %.3f ms\n", c.MeanLatency())
	fmt.Printf("  P50  latency : %.3f ms\n", c.P50Latency())
	fmt.Printf("  P99  latency : %.3f ms\n", c.P99Latency())
	fmt.Printf("  Max  latency : %.3f ms\n", c.MaxLatency())
	fmt.Printf("──────────────────────────────────────────────────\n")
}

// SaveCSV writes all records to a CSV file at path.
// Columns: operation_id, start_unix_ns, end_unix_ns, latency_ms,
//          node_count, is_global, success.
func (c *Collector) SaveCSV(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("metrics.SaveCSV: create %s: %w", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header row.
	if err := w.Write([]string{
		"operation_id", "start_unix_ns", "end_unix_ns",
		"latency_ms", "node_count", "is_global", "success",
	}); err != nil {
		return fmt.Errorf("metrics.SaveCSV: write header: %w", err)
	}

	for _, r := range c.records {
		row := []string{
			r.OperationID,
			fmt.Sprintf("%d", r.StartTime.UnixNano()),
			fmt.Sprintf("%d", r.EndTime.UnixNano()),
			fmt.Sprintf("%.4f", r.LatencyMs()),
			fmt.Sprintf("%d", r.NodeCount),
			fmt.Sprintf("%v", r.IsGlobal),
			fmt.Sprintf("%v", r.Success),
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("metrics.SaveCSV: write row: %w", err)
		}
	}
	return nil
}

// Reset clears all records.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = c.records[:0]
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

// successLatencies returns a sorted slice of latency values for successful ops.
// Caller must hold c.mu.
func (c *Collector) successLatencies() []float64 {
	lats := make([]float64, 0, len(c.records))
	for _, r := range c.records {
		if r.Success {
			lats = append(lats, r.LatencyMs())
		}
	}
	sort.Float64s(lats)
	return lats
}

func mean(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}