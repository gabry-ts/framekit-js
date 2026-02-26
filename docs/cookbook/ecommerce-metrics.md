# Cookbook: Ecommerce Metrics

```ts
import { DataFrame, col } from 'framekit';

const orders = DataFrame.fromRows([
  { date: '2026-01-01', sku: 'A', qty: 2, price: 10 },
  { date: '2026-01-01', sku: 'B', qty: 1, price: 30 },
  { date: '2026-01-02', sku: 'A', qty: 3, price: 10 },
]);

const metrics = orders
  .withColumn('revenue', col<number>('qty').mul(col<number>('price')))
  .groupBy('date')
  .agg({
    revenue: col('revenue').sum(),
    avg_order_value: col('revenue').mean(),
  });
```
