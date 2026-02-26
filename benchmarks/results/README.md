# Benchmark Results

This folder stores generated benchmark outputs from compare suites.

## Latest Snapshot

| Operation | Median (ms) | p95 (ms) | JSON                   | Markdown             |
| --------- | ----------: | -------: | ---------------------- | -------------------- |
| Filter    |     14.0510 |  57.5378 | `compare-filter.json`  | `compare-filter.md`  |
| Sort      |     40.1102 |  48.6977 | `compare-sort.json`    | `compare-sort.md`    |
| GroupBy   |     13.5805 |  17.2832 | `compare-groupby.json` | `compare-groupby.md` |
| Join      |     43.2021 |  52.3730 | `compare-join.json`    | `compare-join.md`    |
| Reshape   |     20.1917 |  48.8045 | `compare-reshape.json` | `compare-reshape.md` |
| Window    |     28.1787 |  52.4062 | `compare-window.json`  | `compare-window.md`  |

## Re-run

```bash
npm run bench:smoke
npm run bench:full
```

Benchmark numbers are environment-sensitive and should be interpreted as directional.
