# Benchmark Results

This folder stores generated benchmark outputs from compare suites.

## Latest Snapshot (FrameKit vs Arquero)

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) | JSON                   | Markdown             |
| --------- | -------------------: | ------------------: | --------------------------: | ---------------------- | -------------------- |
| Filter    |               5.2187 |              1.3451 |                       3.88x | `compare-filter.json`  | `compare-filter.md`  |
| Sort      |              34.3772 |              0.1628 |                     211.23x | `compare-sort.json`    | `compare-sort.md`    |
| GroupBy   |              11.2867 |              6.1850 |                       1.82x | `compare-groupby.json` | `compare-groupby.md` |
| Join      |              37.5740 |             11.9141 |                       3.15x | `compare-join.json`    | `compare-join.md`    |
| Reshape   |              17.7959 |             40.0524 |                       0.44x | `compare-reshape.json` | `compare-reshape.md` |
| Window    |              23.4937 |              1.6053 |                      14.64x | `compare-window.json`  | `compare-window.md`  |

## Re-run

```bash
npm run bench:smoke
npm run bench:full
```

Benchmark numbers are environment-sensitive and should be interpreted as directional.
