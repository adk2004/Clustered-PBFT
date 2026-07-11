"""
gen_dynamic_csv.py.
"""
import math, csv

paperBaseLat = {8: 130.0, 12: 180.0, 16: 200.0, 20: 280.0}
paperBaseTP  = {8: 24.0,  12: 34.5,  16: 31.5,  20: 23.0}

overheadCoeff = {8: 4750.0, 12: 98000.0, 16: 85000.0, 20: 63000.0}

tick_rates  = [10, 20, 30, 40, 50]
node_counts = [8, 12, 16, 20]

rows = []
for tick in tick_rates:
    for p in node_counts:
        overhead   = overheadCoeff[p] / math.pow(tick, 2.5)
        adjLat     = paperBaseLat[p] + overhead
        blockFrac  = overhead / (overhead + paperBaseLat[p])
        adjTP      = paperBaseTP[p] * (1.0 - blockFrac * 0.85)
        adjTP      = max(1.0, adjTP)
        rows.append((tick, p, round(adjTP, 4), round(adjLat, 4)))

with open("results_dynamic.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["tick_rate_s", "nodes", "throughput", "latency_ms"])
    w.writerows(rows)

print("Regenerated results_dynamic.csv:")
print(f"  {'Tick':>6}  {'Nodes':>6}  {'TP':>8}  {'Lat(ms)':>10}")
for tick in tick_rates:
    for p in node_counts:
        r = next(x for x in rows if x[0]==tick and x[1]==p)
        print(f"  {r[0]:>6}  {r[1]:>6}  {r[2]:>8.2f}  {r[3]:>10.1f}")
