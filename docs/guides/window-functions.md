# Window Functions

FrameKit includes window expressions through expression helpers such as `rowNumber`, rank variants, cumulative operations, and shifts.

```ts
import { DataFrame, col } from 'framekit';

const df = DataFrame.fromRows([
  { id: 1, amount: 10 },
  { id: 2, amount: 20 },
]);

const withRn = df.withColumn('rn', col('amount').rowNumber());
```

For derive-style row indexing, use:

```ts
import { op } from 'framekit';

const indexed = df.derive({ idx: () => op.rowNumber() });
```
