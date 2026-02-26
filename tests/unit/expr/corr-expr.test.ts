import { describe, it, expect } from 'vitest';
import { DataFrame, col, op } from '../../../src';

describe('op.corr', () => {
  it('computes Pearson correlation in global aggregation', () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 2 },
      { x: 2, y: 4 },
      { x: 3, y: 6 },
      { x: 4, y: 8 },
    ]);

    const out = df.groupBy().agg({ corr: op.corr(col('x'), col('y')) });
    expect(out.row(0).corr as number).toBeCloseTo(1, 8);
  });

  it('works in groupBy aggregation context', () => {
    const df = DataFrame.fromRows([
      { g: 'A', x: 1, y: 1 },
      { g: 'A', x: 2, y: 2 },
      { g: 'A', x: 3, y: 3 },
      { g: 'B', x: 1, y: 3 },
      { g: 'B', x: 2, y: 2 },
      { g: 'B', x: 3, y: 1 },
    ]);

    const out = df
      .groupBy('g')
      .agg({ corr: op.corr(col('x'), col('y')) })
      .sortBy('g');
    expect(out.toArray()).toEqual([
      { g: 'A', corr: 1 },
      { g: 'B', corr: -1 },
    ]);
  });

  it('excludes null pairs and returns null when fewer than 2 pairs exist', () => {
    const df = DataFrame.fromRows([
      { g: 'A', x: 1, y: 2 },
      { g: 'A', x: null, y: 4 },
      { g: 'A', x: 3, y: 6 },
      { g: 'B', x: 1, y: null },
      { g: 'B', x: null, y: 5 },
    ]);

    const out = df
      .groupBy('g')
      .agg({ corr: op.corr(col('x'), col('y')) })
      .sortBy('g');
    const rows = out.toArray();
    expect(rows[0]!.corr as number).toBeCloseTo(1, 8);
    expect(rows[1]!.corr).toBeNull();
  });

  it('returns value within [-1, 1]', () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 9 },
      { x: 2, y: 7 },
      { x: 3, y: 8 },
      { x: 4, y: 2 },
      { x: 5, y: 3 },
    ]);

    const out = df.groupBy().agg({ corr: op.corr(col('x'), col('y')) });
    const corr = out.row(0).corr as number;
    expect(corr).toBeGreaterThanOrEqual(-1);
    expect(corr).toBeLessThanOrEqual(1);
  });
});
