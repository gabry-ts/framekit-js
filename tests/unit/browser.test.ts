import { describe, it, expect } from 'vitest';
import {
  DataFrame,
  Series,
  Float64Column,
  col,
  lit,
  DType,
} from '../../src/browser';

describe('Browser compatibility smoke tests', () => {
  it('constructs a DataFrame from rows', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ]);
    expect(df.shape[0]).toBe(3);
    expect(df.shape[1]).toBe(2);
    expect(df.columns).toContain('name');
    expect(df.columns).toContain('age');
  });

  it('filters rows with a callback', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
      { name: 'Charlie', age: 35 },
    ]);
    const filtered = df.filter((row: Record<string, unknown>) => (row['age'] as number) > 28);
    expect(filtered.shape[0]).toBe(2);
  });

  it('selects columns', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', age: 30, city: 'NYC' },
    ]);
    const selected = df.select('name', 'age');
    expect(selected.columns).toEqual(['name', 'age']);
  });

  it('sorts by column', () => {
    const df = DataFrame.fromRows([
      { name: 'Charlie', age: 35 },
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ]);
    const sorted = df.sortBy('age');
    expect(sorted.col('name').toArray()).toEqual(['Bob', 'Alice', 'Charlie']);
  });

  it('performs groupBy aggregation', () => {
    const df = DataFrame.fromRows([
      { dept: 'eng', salary: 100 },
      { dept: 'eng', salary: 120 },
      { dept: 'hr', salary: 90 },
    ]);
    const grouped = df.groupBy('dept').agg({ salary: 'mean' });
    expect(grouped.shape[0]).toBe(2);
  });

  it('creates Series directly', () => {
    const column = Float64Column.from([1, 2, 3]);
    const s = new Series<number>('values', column);
    expect(s.length).toBe(3);
    expect(s.name).toBe('values');
  });

  it('uses expression API', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);
    const filtered = df.filter(col('a').gt(lit(1)));
    expect(filtered.shape[0]).toBe(2);
  });

  it('uses withColumn', () => {
    const df = DataFrame.fromRows([
      { x: 1 },
      { x: 2 },
    ]);
    const result = df.withColumn('doubled', (row: Record<string, unknown>) => (row['x'] as number) * 2);
    expect(result.col('doubled').toArray()).toEqual([2, 4]);
  });

  it('exports core types', () => {
    expect(DType.Float64).toBeDefined();
    expect(DType.Utf8).toBeDefined();
    expect(DType.Boolean).toBeDefined();
    expect(DType.Date).toBeDefined();
  });

  it('does not export parallelism APIs', async () => {
    const browserExports = await import('../../src/browser');
    expect('shouldUseParallel' in browserExports).toBe(false);
  });
});
