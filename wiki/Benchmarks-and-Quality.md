# Benchmarks and Quality

## Benchmarks

- Compare suites are under `tests/benchmarks/`.
- Output artifacts are in `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.

### Latest Snapshot (FrameKit vs Arquero)

Checked-in snapshot (`BENCH_ROWS=50000`, `BENCH_ITERS=10`, `BENCH_WARMUP=3`):

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) |
| --------- | -------------------: | ------------------: | --------------------------: |
| Filter    |               5.2187 |              1.3451 |                       3.88x |
| Sort      |              34.3772 |              0.1628 |                     211.23x |
| GroupBy   |              11.2867 |              6.1850 |                       1.82x |
| Join      |              37.5740 |             11.9141 |                       3.15x |
| Reshape   |              17.7959 |             40.0524 |                       0.44x |
| Window    |              23.4937 |              1.6053 |                      14.64x |

Raw sources:

- `benchmarks/results/compare-filter.json`
- `benchmarks/results/compare-sort.json`
- `benchmarks/results/compare-groupby.json`
- `benchmarks/results/compare-join.json`
- `benchmarks/results/compare-reshape.json`
- `benchmarks/results/compare-window.json`

### Reproducing Results

```bash
npm run bench:smoke
npm run bench:full
```

## Quality Gates

- Type checking
- Unit and integration tests
- Protected main branch with required checks and review

## Interpretation

Benchmark results are directional and environment-dependent. Re-run on target hardware and runtime before using them as strict production SLAs.
