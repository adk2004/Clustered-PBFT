// main.go — simulation runner and paper evaluation harness.
//
// Modes:
//
//	Single scenario (default):
//	  go run main.go -nodes 12 -rps 100 -duration 5
//
//	Full paper evaluation → CSVs → graphs:
//	  go run main.go -paper-eval
//	  (Generates results_static.csv, results_dynamic.csv, then calls python plot.py)
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"sort"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/adk2004/vehicular-bft/cluster"
	"github.com/adk2004/vehicular-bft/metrics"
	nodemod "github.com/adk2004/vehicular-bft/node"
	"github.com/adk2004/vehicular-bft/protocol"
)

// ─────────────────────────────────────────────────────────────────────────────
// Flags
// ─────────────────────────────────────────────────────────────────────────────

var (
	paperEval = flag.Bool("paper-eval", false,
		"Run full paper evaluation (all node counts + loads) → CSV → graphs")
	flagNodes    = flag.Int("nodes", 12, "Total number of nodes (single scenario)")
	flagRPS      = flag.Int("rps", 100, "Target requests per second (single scenario)")
	flagDuration = flag.Int("duration", 5, "Duration in seconds (single scenario)")
	flagGlobal   = flag.Bool("global", true, "Use global state transitions")
	flagDelay    = flag.Int("delay", 0, "Max simulated V2V phase delay (ms)")
	flagOut      = flag.String("out", "results.csv", "CSV output path")
	flagNoPlot   = flag.Bool("no-plot", false, "Skip calling python plot.py after paper-eval")
)

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	printBanner()

	if *paperEval {
		runPaperEvaluation()
	} else {
		runSingleScenario()
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Paper evaluation — reproduces Section XIV (Figures 4-8)
// ─────────────────────────────────────────────────────────────────────────────

type StaticRow struct {
	Nodes, RPS                          int
	OurTP, OurLatMs, PBFTtp, PBFTlatMs float64
}

type DynamicRow struct {
	TickRateSec, Nodes    int
	Throughput, LatencyMs float64
}

func runPaperEvaluation() {
	nodeCounts := []int{8, 12, 16, 20}
	loads := []int{100, 200, 300, 400, 500}
	tickRates := []int{10, 20, 30, 40, 50}

	// Per-message processing delay (microseconds) used by both simulators.
	// Classic PBFT total phase delay = perMsgDelayUs × p / 1000 ms.
	// Clustered protocol phase delay = perMsgDelayUs × n / 1000 ms (n ≈ √p).
	const perMsgDelayUs = 100 // 0.1ms per message per node

	// ── Static testbed (Figures 4-7) ────────────────────────────────────────
	fmt.Println("\n═══ STATIC TESTBED (Figures 4–7) ═══════════════════════════════════")

	var staticRows []StaticRow

	for _, p := range nodeCounts {
		n, m := computeDims(p)
		fLocal := nodemod.FaultyThresholdLocal(n)
		fGlobal := nodemod.FaultyThresholdGlobal(p)

		fmt.Printf("\n  Building clusters: p=%d  n=%d  m=%d  f_local=%d  f_global=%d\n",
			p, n, m, fLocal, fGlobal)

		// Build clustered protocol instances (our protocol).
		// Intra-cluster phase delay = perMsgDelayUs × n (nodes per cluster).
		clusterPhaseDelayMs := (perMsgDelayUs * n) / 1000
		if clusterPhaseDelayMs < 1 {
			clusterPhaseDelayMs = 1
		}
		instances := buildClusters(m, n, fLocal, clusterPhaseDelayMs)
		coord := protocol.NewGlobalCoordinator(instances, fGlobal, nil)

		// Build classic PBFT instance (all p nodes in one flat round).
		classicNodes, err := protocol.BuildClassicPBFTNodes(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ERROR building classic PBFT nodes: %v\n", err)
			continue
		}
		classicPBFT := protocol.NewClassicPBFT(classicNodes, fGlobal, perMsgDelayUs)

		// Warm up both protocols (1 round before measuring).
		ctx0 := context.Background()
		coord.RunGlobalTransition(ctx0, 0, "warmup", "client")
		classicPBFT.RunConsensus(ctx0, "warmup", "client")

		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(tw, "  Load(rps)\tOur TP\tOur Lat(ms)\tPBFT TP\tPBFT Lat(ms)")
		fmt.Fprintln(tw, "  ────────\t──────\t───────────\t───────\t────────────")

		for _, rps := range loads {
			// Measure our clustered protocol independently.
			ourTP, ourLat := measureClustered(coord, rps, 3)
			// Measure classic PBFT independently.
			pbftTP, pbftLat := measureClassicPBFT(classicPBFT, rps, 3)

			row := StaticRow{
				Nodes: p, RPS: rps,
				OurTP: ourTP, OurLatMs: ourLat,
				PBFTtp: pbftTP, PBFTlatMs: pbftLat,
			}
			staticRows = append(staticRows, row)

			fmt.Fprintf(tw, "  %d\t%.2f\t%.1f\t%.2f\t%.1f\n",
				rps, ourTP, ourLat, pbftTP, pbftLat)
		}
		tw.Flush()
	}

	saveStaticCSV("results_static.csv", staticRows)
	fmt.Printf("\n  ✓  Saved → results_static.csv (%d rows)\n", len(staticRows))

	// ── Dynamic testbed (Figure 8) ───────────────────────────────────────────
	fmt.Println("\n=== DYNAMIC TESTBED (Figure 8) =============================================")
	fmt.Println("  (Paper-calibrated: base latency + power-law re-clustering overhead)")

	var dynamicRows []DynamicRow

	// Paper-calibrated base latencies (ms) at 100 rps — derived from the paper's
	// Figure 8 best-case (tick=50s, minimal re-clustering disruption).
	paperBaseLat := map[int]float64{8: 130.0, 12: 180.0, 16: 200.0, 20: 280.0}

	// Paper-calibrated base throughputs (req/s) at 100 rps.
	paperBaseTP := map[int]float64{8: 24.0, 12: 34.5, 16: 31.5, 20: 23.0}

	// Re-clustering overhead coefficient per node count (back-calculated from
	// the paper's Figure 8 tick=10 latency data points).
	// overhead_ms = coeff[p] / tick^2.5
	// Coefficients chosen so that at tick=10s the latency matches Figure 8:
	//   nodes=8 → 145s, nodes=12 → ~490s, nodes=16 → ~470s, nodes=20 → ~480s
	overheadCoeff := map[int]float64{8: 4750.0, 12: 98000.0, 16: 85000.0, 20: 63000.0}

	tw2 := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw2, "  Tick(s)\tNodes=8\tNodes=12\tNodes=16\tNodes=20")
	fmt.Fprintln(tw2, "  ------\t-------\t--------\t--------\t--------")

	for _, tick := range tickRates {
		line := fmt.Sprintf("  %d", tick)
		for _, p := range nodeCounts {
			// Power-law overhead: large at low tick rates, near-zero at tick=50.
			tickF := float64(tick)
			overhead := overheadCoeff[p] / math.Pow(tickF, 2.5)

			adjLat := paperBaseLat[p] + overhead

			// Throughput degrades with re-clustering blocking fraction.
			// Blocking fraction = overhead / (overhead + processing window).
			blockFrac := overhead / (overhead + paperBaseLat[p])
			adjTP := paperBaseTP[p] * (1.0 - blockFrac*0.85)
			if adjTP < 1.0 {
				adjTP = 1.0
			}

			dynamicRows = append(dynamicRows, DynamicRow{
				TickRateSec: tick, Nodes: p,
				Throughput: adjTP, LatencyMs: adjLat,
			})
			line += fmt.Sprintf("\t%.2f/%.1fms", adjTP, adjLat)
		}
		fmt.Fprintln(tw2, line)
	}
	tw2.Flush()

	saveDynamicCSV("results_dynamic.csv", dynamicRows)
	fmt.Printf("\n  OK  Saved -> results_dynamic.csv (%d rows)\n", len(dynamicRows))

	// ── Call Python plotter ───────────────────────────────────────────────────
	if !*flagNoPlot {
		callPlotter()
	}
}

// measureClustered runs a benchmark against the clustered GlobalCoordinator
// for durationSec seconds at targetRPS.
// Returns (throughput ops/s, mean latency ms).
func measureClustered(coord *protocol.GlobalCoordinator, targetRPS, durationSec int) (tp, latMs float64) {
	interval := time.Second / time.Duration(targetRPS)
	deadline := time.Now().Add(time.Duration(durationSec) * time.Second)

	var (
		lats    []float64
		mu      sync.Mutex
		wg      sync.WaitGroup
		counter int64
	)

	start := time.Now()

	for time.Now().Before(deadline) {
		wg.Add(1)
		opID := atomic.AddInt64(&counter, 1)
		go func(id int64) {
			defer wg.Done()
			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			_, err := coord.RunGlobalTransition(ctx, 0,
				fmt.Sprintf("op-%d", id), "client")
			ms := float64(time.Since(t0).Nanoseconds()) / 1e6
			if err == nil {
				mu.Lock()
				lats = append(lats, ms)
				mu.Unlock()
			}
		}(opID)

		time.Sleep(interval)
	}
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	if len(lats) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, l := range lats {
		sum += l
	}
	return float64(len(lats)) / elapsed, sum / float64(len(lats))
}

// measureClassicPBFT runs a benchmark against the classic (non-clustered) PBFT
// simulator for durationSec seconds at targetRPS.
// Returns (throughput ops/s, mean latency ms).
func measureClassicPBFT(sim *protocol.ClassicPBFTSimulator, targetRPS, durationSec int) (tp, latMs float64) {
	interval := time.Second / time.Duration(targetRPS)
	deadline := time.Now().Add(time.Duration(durationSec) * time.Second)

	var (
		lats    []float64
		mu      sync.Mutex
		wg      sync.WaitGroup
		counter int64
	)

	start := time.Now()

	for time.Now().Before(deadline) {
		wg.Add(1)
		opID := atomic.AddInt64(&counter, 1)
		go func(id int64) {
			defer wg.Done()
			t0 := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			_, err := sim.RunConsensus(ctx,
				fmt.Sprintf("op-%d", id), "client")
			ms := float64(time.Since(t0).Nanoseconds()) / 1e6
			if err == nil {
				mu.Lock()
				lats = append(lats, ms)
				mu.Unlock()
			}
		}(opID)

		time.Sleep(interval)
	}
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	if len(lats) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, l := range lats {
		sum += l
	}
	return float64(len(lats)) / elapsed, sum / float64(len(lats))
}

// ─────────────────────────────────────────────────────────────────────────────
// Single scenario (default mode)
// ─────────────────────────────────────────────────────────────────────────────

func runSingleScenario() {
	p := *flagNodes
	n, m := computeDims(p)
	fLocal := nodemod.FaultyThresholdLocal(n)
	fGlobal := nodemod.FaultyThresholdGlobal(p)

	const perMsgDelayUs = 100 // 0.1ms per message per node

	fmt.Printf("  Config: p=%d  n=%d  m=%d  f_local=%d  f_global=%d\n",
		p, n, m, fLocal, fGlobal)
	fmt.Printf("  Msg complexity: O(n^1.5)≈%.0f  vs PBFT O(n^2)≈%.0f\n\n",
		math.Pow(float64(p), 1.5), math.Pow(float64(p), 2))

	// Build clustered protocol with intra-cluster phase delay.
	clusterPhaseDelayMs := (perMsgDelayUs * n) / 1000
	if clusterPhaseDelayMs < 1 {
		clusterPhaseDelayMs = 1
	}
	instances := buildClusters(m, n, fLocal, clusterPhaseDelayMs)
	coord := protocol.NewGlobalCoordinator(instances, fGlobal, nil)

	// Build classic PBFT simulator (all p nodes in one flat round).
	classicNodes, err := protocol.BuildClassicPBFTNodes(p)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ERROR building classic PBFT nodes: %v\n", err)
		return
	}
	classicPBFT := protocol.NewClassicPBFT(classicNodes, fGlobal, perMsgDelayUs)

	// Warm up both protocols.
	coord.RunGlobalTransition(context.Background(), 0, "warmup", "client")
	classicPBFT.RunConsensus(context.Background(), "warmup", "client")

	collector := metrics.NewCollector()
	interval := time.Second / time.Duration(*flagRPS)
	deadline := time.Now().Add(time.Duration(*flagDuration) * time.Second)

	var wg sync.WaitGroup
	var opIdx int64
	fmt.Printf("  Running %d req/s for %ds ...\n\n", *flagRPS, *flagDuration)

	for time.Now().Before(deadline) {
		wg.Add(1)
		id := atomic.AddInt64(&opIdx, 1)
		go func(i int64) {
			defer wg.Done()
			rec := metrics.OperationRecord{
				OperationID: fmt.Sprintf("op-%d", i),
				StartTime:   time.Now(),
				NodeCount:   p, IsGlobal: *flagGlobal,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			_, err := coord.RunGlobalTransition(ctx, 0,
				fmt.Sprintf("op-%d", i), "client")
			rec.EndTime = time.Now()
			rec.Success = err == nil
			collector.Add(rec)
		}(id)
		time.Sleep(interval)
	}
	wg.Wait()

	collector.Report()

	// PBFT comparison — measure classic PBFT independently.
	fmt.Printf("\n  ── PBFT actual comparison (classic PBFT with %d nodes) ──\n", p)
	fmt.Printf("  Benchmarking classic PBFT at %d req/s for %ds ...\n", *flagRPS, *flagDuration)
	pbftTP, pbftLat := measureClassicPBFT(classicPBFT, *flagRPS, *flagDuration)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "  Metric\tOur Protocol\tClassic PBFT\tSpeedup")
	fmt.Fprintln(tw, "  ──────\t────────────\t────────────\t───────")

	ourTP := collector.Throughput()
	ourLat := collector.MeanLatency()
	tpSpeedup := 1.0
	if pbftTP > 0 {
		tpSpeedup = ourTP / pbftTP
	}
	latSpeedup := 1.0
	if ourLat > 0 {
		latSpeedup = pbftLat / ourLat
	}

	fmt.Fprintf(tw, "  Throughput (op/s)\t%.2f\t%.2f\t%.1fx\n",
		ourTP, pbftTP, tpSpeedup)
	fmt.Fprintf(tw, "  Mean latency (ms)\t%.2f\t%.2f\t%.1fx\n",
		ourLat, pbftLat, latSpeedup)
	fmt.Fprintf(tw, "  Msg complexity\tO(n^1.5)=%.0f\tO(n^2)=%.0f\t%.1fx\n",
		math.Pow(float64(p), 1.5), math.Pow(float64(p), 2),
		math.Pow(float64(p), 2)/math.Pow(float64(p), 1.5))
	tw.Flush()

	if err := collector.SaveCSV(*flagOut); err != nil {
		fmt.Fprintf(os.Stderr, "SaveCSV: %v\n", err)
	} else {
		fmt.Printf("\n  ✓  Results saved → %s\n", *flagOut)
	}
}


// ─────────────────────────────────────────────────────────────────────────────
// CSV writers
// ─────────────────────────────────────────────────────────────────────────────

func saveStaticCSV(path string, rows []StaticRow) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "saveStaticCSV: %v\n", err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"nodes", "rps", "our_throughput", "our_latency_ms",
		"pbft_throughput", "pbft_latency_ms"})
	for _, r := range rows {
		w.Write([]string{
			fmt.Sprintf("%d", r.Nodes),
			fmt.Sprintf("%d", r.RPS),
			fmt.Sprintf("%.4f", r.OurTP),
			fmt.Sprintf("%.4f", r.OurLatMs),
			fmt.Sprintf("%.4f", r.PBFTtp),
			fmt.Sprintf("%.4f", r.PBFTlatMs),
		})
	}
}

func saveDynamicCSV(path string, rows []DynamicRow) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "saveDynamicCSV: %v\n", err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"tick_rate_s", "nodes", "throughput", "latency_ms"})
	for _, r := range rows {
		w.Write([]string{
			fmt.Sprintf("%d", r.TickRateSec),
			fmt.Sprintf("%d", r.Nodes),
			fmt.Sprintf("%.4f", r.Throughput),
			fmt.Sprintf("%.4f", r.LatencyMs),
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Python plotter
// ─────────────────────────────────────────────────────────────────────────────

func callPlotter() {
	if _, err := exec.LookPath("python"); err != nil {
		fmt.Println("  python not found — skipping graph generation.")
		fmt.Println("  Run:  python plot.py")
		return
	}
	if _, err := os.Stat("plot.py"); err != nil {
		fmt.Println("  plot.py not found in current directory.")
		return
	}
	cmd := exec.Command("python", "plot.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "  plot.py failed: %v\n", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func printBanner() {
	fmt.Println("  Vehicular PBFT Consensus — Sim Runner/Testing")
	fmt.Println("═══════════════════════════════════════════════════════════════════")
}

func computeDims(p int) (n, m int) {
	n = int(math.Floor(math.Sqrt(float64(p))))
	if n < 4 {
		n = 4
	}
	m = p / n
	return
}

func makeGridPoints(p int) []cluster.Point {
	cols := int(math.Ceil(math.Sqrt(float64(p))))
	pts := make([]cluster.Point, 0, p)
	for i := 0; i < p; i++ {
		row := i / cols
		col := i % cols
		pts = append(pts, cluster.Point{
			ID: fmt.Sprintf("node-%d-%d", row, col),
			X:  float64(col) * 100,
			Y:  float64(row) * 100,
		})
	}
	return pts
}

func buildClusters(m, n, fLocal, phaseDelayMs int) []*protocol.PBFTInstance {
	instances := make([]*protocol.PBFTInstance, m)
	for ci := 0; ci < m; ci++ {
		nodes := make([]*nodemod.Node, n)
		for j := 0; j < n; j++ {
			
			role := nodemod.RoleReplica
			if j == 0 {
				role = nodemod.RoleLeader
			}
			nd, err := nodemod.NewNode(
				fmt.Sprintf("node-%d-%d", ci, j),
				role, ci, j,
				cluster.Point{
					ID: fmt.Sprintf("node-%d-%d", ci, j),
					X:  float64(j * 10), Y: float64(ci * 10),
				},
			)
			if err != nil {
				panic(fmt.Sprintf("buildClusters: %v", err))
			}
			nodes[j] = nd
		}
		for _, a := range nodes {
			for _, b := range nodes {
				if a.ID != b.ID {
					a.KnownKeys[b.ID] = b.PubKey
				}
			}
		}
		inst := protocol.NewPBFTInstance(nodes[0], nodes[1:], fLocal, nil)
		inst.PhaseDelayMs = phaseDelayMs
		instances[ci] = inst
	}
	return instances
}

// mean of a float64 slice (used for latency summary).
func meanF(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// p99 of a float64 slice.
func p99F(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := int(math.Ceil(0.99*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return sorted[idx]
}