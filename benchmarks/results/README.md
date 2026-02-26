# Benchmark Results

This folder stores generated benchmark outputs from compare suites.

## Latest Snapshot (FrameKit vs Arquero)

| Operation | FrameKit Median (ms) | Arquero Median (ms) | Relative (FrameKit/Arquero) | JSON                   | Markdown             |
| --------- | -------------------: | ------------------: | --------------------------: | ---------------------- | -------------------- |
| Filter    |              26.9228 |             36.6342 |                       0.73x | `compare-filter.json`  | `compare-filter.md`  |
| Sort      |              25.5620 |             50.6992 |                       0.50x | `compare-sort.json`    | `compare-sort.md`    |
| GroupBy   |               3.9674 |              5.0675 |                       0.78x | `compare-groupby.json` | `compare-groupby.md` |
| Join      |              20.4444 |             61.5447 |                       0.33x | `compare-join.json`    | `compare-join.md`    |
| Reshape   |              17.2577 |             65.6563 |                       0.26x | `compare-reshape.json` | `compare-reshape.md` |
| Window    |              22.2838 |             64.1068 |                       0.35x | `compare-window.json`  | `compare-window.md`  |

## Re-run

```bash
npm run bench:smoke
npm run bench:full
```

Benchmark numbers are environment-sensitive and should be interpreted as directional.
