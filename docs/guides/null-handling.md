# Null Handling

FrameKit treats `null` as missing data and keeps semantics explicit.

- Arithmetic/comparison expressions propagate `null` when inputs are null.
- Aggregations skip nulls for numeric metrics such as `sum`, `mean`, and `corr`.
- Imputation replaces only null values and keeps existing non-null values unchanged.

```ts
import { DataFrame } from 'framekit';

const df = DataFrame.fromRows([
  { id: 1, v: 10 },
  { id: 2, v: null },
]);

const filled = df.impute({ v: 0 });
```
