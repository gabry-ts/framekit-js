import { describe, it, expect } from 'vitest';
import { DataFrame, op } from '../../../src';

describe('op.rowNumber', () => {
  it('returns 0-based row index in ungrouped context', () => {
    const df = DataFrame.fromRows([{ value: 10 }, { value: 20 }, { value: 30 }]);

    const result = df.withColumn('idx', op.rowNumber());
    expect(result.col('idx').toArray()).toEqual([0, 1, 2]);
  });

  it('can be used in derive', () => {
    const df = DataFrame.fromRows([{ value: 10 }, { value: 20 }, { value: 30 }]);

    const result = df.derive({ idx: () => op.rowNumber() });
    expect(result.col('idx').toArray()).toEqual([0, 1, 2]);
  });

  it('resets per group when used in grouped apply', () => {
    const df = DataFrame.fromRows([
      { g: 'A', value: 1 },
      { g: 'A', value: 2 },
      { g: 'B', value: 3 },
      { g: 'B', value: 4 },
      { g: 'B', value: 5 },
    ]);

    const result = df
      .groupBy('g')
      .apply((group) => group.withColumn('idx', op.rowNumber()))
      .sortBy(['g', 'value']);
    expect(result.toArray()).toEqual([
      { g: 'A', value: 1, idx: 0 },
      { g: 'A', value: 2, idx: 1 },
      { g: 'B', value: 3, idx: 0 },
      { g: 'B', value: 4, idx: 1 },
      { g: 'B', value: 5, idx: 2 },
    ]);
  });
});
