# Lazy vs Eager

Use eager mode for ad-hoc scripts and when immediate materialization is simpler. Use lazy mode for longer pipelines where deferred optimization can reduce work.

```ts
import { DataFrame, col } from 'framekit';

const eager = DataFrame.fromRows([{ x: 1 }, { x: 2 }]).filter(col<number>('x').gt(1));

const lazy = DataFrame.fromRows([{ x: 1 }, { x: 2 }])
  .lazy()
  .filter(col<number>('x').gt(1))
  .collect();
```
