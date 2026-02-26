# Compat API Reference

Import from `framekit/compat`.

## Verbs

- `derive(df, exprs)`
- `rollup(dfOrGroupBy, exprs)`
- `fold(df, columns, options?)`
- `orderby(df, specs)`

## Helpers

- `all()`
- `not(...columns)`
- `range(start, end)`
- `desc(column)`

## Aggregation Helpers

- `op.sum(values)`
- `op.mean(values)`
- `op.count(values)`
- `op.min(values)`
- `op.max(values)`
- `op.distinct(values)`
- `op.first(values)`
- `op.last(values)`

## Example

```ts
import { derive, rollup, op, desc } from 'framekit/compat';

const withTotal = derive(df, { total: (d) => (d.price as number) * (d.qty as number) });
const summary = rollup(withTotal, { totalRevenue: (d) => op.sum(d.total!) });
```
