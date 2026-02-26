# Cookbook: Event Pipeline

```ts
import { DataFrame } from 'framekit';

const events = DataFrame.fromRows([
  { user: 'u1', tags: ['signup', 'mobile'] },
  { user: 'u2', tags: ['purchase'] },
]);

const exploded = events.unroll('tags', { index: 'tag_pos' });

const widened = exploded.spread('tags', {
  name: (col, key) => `${col}_${String(key)}`,
  drop: false,
});
```
