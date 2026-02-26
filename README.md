# FrameKit

FrameKit is a TypeScript-first DataFrame library for Node.js focused on practical analytics, predictable null semantics, and an API that stays ergonomic in both small scripts and production pipelines.

## Why FrameKit

- TypeScript-native DataFrame and expression APIs.
- Eager and lazy execution modes with shared semantics.
- Core API plus `framekit/compat` for Arquero-style migration.
- Built-in I/O for CSV, JSON/NDJSON, Arrow, Excel, Parquet, and SQL export.

## Install

```bash
npm install framekit
```

## Quickstart

```ts
import { DataFrame, col } from 'framekit';

const df = DataFrame.fromRows([
  { product: 'A', qty: 2, price: 10 },
  { product: 'B', qty: 1, price: 25 },
  { product: 'A', qty: 4, price: 10 },
]);

const out = df
  .withColumn('revenue', col<number>('qty').mul(col<number>('price')))
  .groupBy('product')
  .agg({ total: col('revenue').sum() })
  .sortBy('total', 'desc');

console.log(out.toArray());
```

## Eager API

```ts
import { DataFrame } from 'framekit';

const eager = DataFrame.fromRows([
  { city: 'Rome', temp: 26 },
  { city: 'Milan', temp: 31 },
]).filter((r) => (r.temp as number) > 28);
```

## Lazy API

```ts
import { DataFrame, col } from 'framekit';

const lazy = DataFrame.fromRows([
  { city: 'Rome', temp: 26 },
  { city: 'Milan', temp: 31 },
])
  .lazy()
  .filter(col<number>('temp').gt(28))
  .select('city', 'temp')
  .collect();
```

## I/O Support Matrix

| Format             | Read | Write |
| ------------------ | ---- | ----- |
| CSV                | Yes  | Yes   |
| JSON               | Yes  | Yes   |
| NDJSON (streaming) | Yes  | Yes   |
| Arrow IPC          | Yes  | Yes   |
| Excel              | Yes  | Yes   |
| Parquet            | Yes  | Yes   |
| SQL (INSERT text)  | No   | Yes   |

## Core vs Compat API

- `framekit`: core API (`DataFrame`, expressions, lazy plans).
- `framekit/compat`: Arquero-style verbs (`derive`, `rollup`, `fold`, `orderby`) and helpers (`all`, `not`, `range`, `desc`).

See `docs/guides/migration-arquero.md` for side-by-side mappings.

## Benchmark Methodology Disclaimer

Benchmarks in this repository are intended for directional comparison only. Hardware, Node.js version, dataset shape, and warmup strategy can materially change outcomes. Always re-run benchmarks in your own environment before making production decisions.

## Roadmap

- Stabilize compat surface and expression aggregates.
- Expand benchmark coverage and CI regression checks.
- Grow cookbook and API reference depth.

See `CHANGELOG.md` for released changes.

## Contributing

Please read `CONTRIBUTING.md` for local setup, test/typecheck/lint requirements, and commit conventions.
