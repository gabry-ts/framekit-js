import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src';

describe('DataFrame.impute', () => {
  it('fills null values with static values', () => {
    const df = DataFrame.fromRows([
      { k1: 'A', k2: 'X', value: 1 },
      { k1: 'A', k2: 'Y', value: null },
      { k1: 'B', k2: 'X', value: null },
    ]);

    const result = df.impute({ value: 0 });

    expect(result.toArray()).toEqual([
      { k1: 'A', k2: 'X', value: 1 },
      { k1: 'A', k2: 'Y', value: 0 },
      { k1: 'B', k2: 'X', value: 0 },
    ]);
  });

  it('supports function values', () => {
    const df = DataFrame.fromRows([
      { k: 'A', value: 10 },
      { k: 'B', value: null },
      { k: 'C', value: 30 },
    ]);

    const result = df.impute({
      value: (d: Record<string, unknown[]>) => {
        const valueCol = d.value ?? [];
        const values = valueCol.filter((v: unknown): v is number => typeof v === 'number');
        return values.reduce((a, b) => a + b, 0) / values.length;
      },
    });

    expect(result.toArray()).toEqual([
      { k: 'A', value: 10 },
      { k: 'B', value: 20 },
      { k: 'C', value: 30 },
    ]);
  });

  it('expands key combinations and fills missing rows', () => {
    const df = DataFrame.fromRows([
      { region: 'North', quarter: 'Q1', value: 10 },
      { region: 'North', quarter: 'Q2', value: null },
      { region: 'South', quarter: 'Q1', value: 30 },
    ]);

    const result = df.impute({ value: 0 }, { expand: ['region', 'quarter'] });

    expect(result.length).toBe(4);
    expect(result.toArray()).toEqual([
      { region: 'North', quarter: 'Q1', value: 10 },
      { region: 'North', quarter: 'Q2', value: 0 },
      { region: 'South', quarter: 'Q1', value: 30 },
      { region: 'South', quarter: 'Q2', value: 0 },
    ]);
  });

  it('preserves non-null values', () => {
    const df = DataFrame.fromRows([
      { id: 1, value: 5 },
      { id: 2, value: null },
    ]);

    const result = df.impute({ value: 99 });

    expect(result.toArray()).toEqual([
      { id: 1, value: 5 },
      { id: 2, value: 99 },
    ]);
  });
});
