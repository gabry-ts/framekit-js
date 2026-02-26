# Expression API Reference

## Factories

- `col(name)`
- `lit(value)`

## Arithmetic / Comparison / Logical

- Arithmetic: `add`, `sub`, `mul`, `div`, `mod`, `pow`
- Comparison: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`
- Logical: `and`, `or`, `not`

## Aggregations

- `sum`, `mean`, `count`, `countDistinct`, `min`, `max`, `std`, `first`, `last`, `list`, `mode`
- `op.corr(col('x'), col('y'))`: Pearson correlation aggregate

## Null Expressions

- `coalesce`, `fillNull`, `isNull`, `isNotNull`

## Window and Row Helpers

- Window methods from expression/window layer (rank, denseRank, rowNumber, cumulative ops)
- `op.rowNumber()`: 0-based row index helper for derive/withColumn style usage

## Example

```ts
import { DataFrame, col, op } from 'framekit';

const out = DataFrame.fromRows([
  { x: 1, y: 2 },
  { x: 2, y: 4 },
])
  .groupBy()
  .agg({ corr: op.corr(col('x'), col('y')) });
```
