import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';

describe('DataFrame.unique()', () => {
  const df = DataFrame.fromRows([
    { name: 'Alice', age: 30, city: 'NY' },
    { name: 'Bob', age: 25, city: 'LA' },
    { name: 'Alice', age: 30, city: 'NY' },
    { name: 'Charlie', age: 25, city: 'NY' },
    { name: 'Bob', age: 40, city: 'SF' },
  ]);

  it('deduplicates based on all columns keeping first occurrence', () => {
    const result = df.unique();
    expect(result.length).toBe(4);
    expect(result.row(0)).toEqual({ name: 'Alice', age: 30, city: 'NY' });
    expect(result.row(1)).toEqual({ name: 'Bob', age: 25, city: 'LA' });
    expect(result.row(2)).toEqual({ name: 'Charlie', age: 25, city: 'NY' });
    expect(result.row(3)).toEqual({ name: 'Bob', age: 40, city: 'SF' });
  });

  it('deduplicates based on a single column', () => {
    const result = df.unique('name');
    expect(result.length).toBe(3);
    expect(result.row(0)).toEqual({ name: 'Alice', age: 30, city: 'NY' });
    expect(result.row(1)).toEqual({ name: 'Bob', age: 25, city: 'LA' });
    expect(result.row(2)).toEqual({ name: 'Charlie', age: 25, city: 'NY' });
  });

  it('deduplicates based on multiple columns', () => {
    const result = df.unique(['name', 'city']);
    expect(result.length).toBe(4);
  });

  it('keeps last occurrence when keep=last', () => {
    const result = df.unique('name', 'last');
    expect(result.length).toBe(3);
    // Alice: last is index 2, Bob: last is index 4, Charlie: index 3
    expect(result.row(0)).toEqual({ name: 'Alice', age: 30, city: 'NY' });
    expect(result.row(1)).toEqual({ name: 'Bob', age: 40, city: 'SF' });
    expect(result.row(2)).toEqual({ name: 'Charlie', age: 25, city: 'NY' });
  });

  it('handles null values in deduplication', () => {
    const dfWithNulls = DataFrame.fromColumns({
      a: [1, null, 1, null],
      b: ['x', 'y', 'x', 'y'],
    });
    const result = dfWithNulls.unique();
    expect(result.length).toBe(2);
  });

  it('returns empty DataFrame when input is empty', () => {
    const empty = DataFrame.empty();
    const result = empty.unique();
    expect(result.length).toBe(0);
  });

  it('throws ColumnNotFoundError for invalid column', () => {
    expect(() => df.unique('nonexistent' as never)).toThrow();
  });
});

describe('DataFrame.sample()', () => {
  const df = DataFrame.fromColumns({
    id: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    val: ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'],
  });

  it('returns n random rows when n >= 1', () => {
    const result = df.sample(3, { seed: 42 });
    expect(result.length).toBe(3);
    expect(result.columns).toEqual(['id', 'val']);
  });

  it('returns proportional sample when 0 < fraction < 1', () => {
    const result = df.sample(0.3, { seed: 42 });
    expect(result.length).toBe(3); // round(0.3 * 10) = 3
  });

  it('produces reproducible results with same seed', () => {
    const r1 = df.sample(5, { seed: 123 });
    const r2 = df.sample(5, { seed: 123 });
    expect(r1.toArray()).toEqual(r2.toArray());
  });

  it('produces different results with different seeds', () => {
    const r1 = df.sample(5, { seed: 1 });
    const r2 = df.sample(5, { seed: 2 });
    // Very unlikely to be the same
    const same = r1.toArray().every((row, i) => {
      const other = r2.toArray()[i];
      return other && row['id' as keyof typeof row] === other['id' as keyof typeof other];
    });
    expect(same).toBe(false);
  });

  it('caps sample at DataFrame length', () => {
    const result = df.sample(100, { seed: 42 });
    expect(result.length).toBe(10);
  });

  it('handles empty DataFrame', () => {
    const empty = DataFrame.empty();
    const result = empty.sample(5);
    expect(result.length).toBe(0);
  });

  it('throws for non-positive sample size', () => {
    expect(() => df.sample(0)).toThrow();
    expect(() => df.sample(-1)).toThrow();
  });

  it('works without seed (random)', () => {
    const result = df.sample(3);
    expect(result.length).toBe(3);
  });

  it('sample with fraction returns at least 1 row', () => {
    const result = df.sample(0.01, { seed: 42 });
    expect(result.length).toBeGreaterThanOrEqual(1);
  });
});
