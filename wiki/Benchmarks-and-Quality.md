# Benchmarks and Quality

## Benchmarks

- Compare suites are under `tests/benchmarks/`.
- Output artifacts are in `benchmarks/results/`.
- CI includes smoke and nightly benchmark workflows.

### Latest Snapshot

Checked-in snapshot (`BENCH_ROWS=50000`, `BENCH_ITERS=10`, `BENCH_WARMUP=3`):

| Operation | Median (ms) | p95 (ms) |
| --------- | ----------: | -------: |
| Filter    |     14.0510 |  57.5378 |
| Sort      |     40.1102 |  48.6977 |
| GroupBy   |     13.5805 |  17.2832 |
| Join      |     43.2021 |  52.3730 |
| Reshape   |     20.1917 |  48.8045 |
| Window    |     28.1787 |  52.4062 |

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
