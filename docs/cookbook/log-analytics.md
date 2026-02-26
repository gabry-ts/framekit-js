# Cookbook: Log Analytics

```ts
import { DataFrame, col } from 'framekit';

const logs = DataFrame.fromRows([
  { level: 'INFO', ms: 12 },
  { level: 'ERROR', ms: 230 },
  { level: 'INFO', ms: 16 },
]);

const summary = logs
  .groupBy('level')
  .agg({
    count: col('ms').count(),
    avg_ms: col('ms').mean(),
  })
  .sortBy('count', 'desc');
```
