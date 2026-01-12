import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';

type Row = { name: string; age: number; city: string };

function makeDF() {
  return DataFrame.fromRows<Row>([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'LA' },
    { name: 'Charlie', age: 35, city: 'NYC' },
    { name: 'Diana', age: 25, city: 'SF' },
    { name: 'Eve', age: 40, city: 'LA' },
  ]);
}

describe('DataFrame.filter', () => {
  it('filters rows matching a predicate', () => {
    const df = makeDF();
    const result = df.filter((row) => row.age > 30);
    expect(result.length).toBe(2);
    expect(result.toArray()).toEqual([
      { name: 'Charlie', age: 35, city: 'NYC' },
      { name: 'Eve', age: 40, city: 'LA' },
    ]);
  });

  it('returns empty DataFrame when no rows match', () => {
    const df = makeDF();
    const result = df.filter((row) => row.age > 100);
    expect(result.length).toBe(0);
    expect(result.columns).toEqual(['name', 'age', 'city']);
  });

  it('returns all rows when all match', () => {
    const df = makeDF();
    const result = df.filter((row) => row.age > 0);
    expect(result.length).toBe(5);
  });

  it('filters by string predicate', () => {
    const df = makeDF();
    const result = df.filter((row) => row.city === 'NYC');
    expect(result.length).toBe(2);
    expect(result.col('name' as never).toArray()).toEqual(['Alice', 'Charlie']);
  });

  it('preserves columns on empty result', () => {
    const df = makeDF();
    const result = df.filter(() => false);
    expect(result.shape).toEqual([0, 3]);
    expect(result.columns).toEqual(['name', 'age', 'city']);
  });
});

describe('DataFrame.sortBy', () => {
  it('sorts by single column ascending (default)', () => {
    const df = makeDF();
    const result = df.sortBy('age');
    const ages = result.col('age' as never).toArray();
    expect(ages).toEqual([25, 25, 30, 35, 40]);
  });

  it('sorts by single column descending', () => {
    const df = makeDF();
    const result = df.sortBy('age', 'desc');
    const ages = result.col('age' as never).toArray();
    expect(ages).toEqual([40, 35, 30, 25, 25]);
  });

  it('sorts by string column ascending', () => {
    const df = makeDF();
    const result = df.sortBy('name');
    const names = result.col('name' as never).toArray();
    expect(names).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']);
  });

  it('sorts by string column descending', () => {
    const df = makeDF();
    const result = df.sortBy('name', 'desc');
    const names = result.col('name' as never).toArray();
    expect(names).toEqual(['Eve', 'Diana', 'Charlie', 'Bob', 'Alice']);
  });

  it('sorts by multiple columns with independent orders', () => {
    const df = makeDF();
    // Sort by city ascending, then by age descending
    const result = df.sortBy(['city', 'age'], ['asc', 'desc']);
    const rows = result.toArray();
    expect(rows).toEqual([
      { name: 'Eve', age: 40, city: 'LA' },
      { name: 'Bob', age: 25, city: 'LA' },
      { name: 'Charlie', age: 35, city: 'NYC' },
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: 'Diana', age: 25, city: 'SF' },
    ]);
  });

  it('stable sort preserves relative order of equal elements', () => {
    // Bob and Diana both have age=25; original order is Bob before Diana
    const df = makeDF();
    const result = df.sortBy('age');
    const rows = result.toArray();
    // Among age=25 rows, Bob should come before Diana (stable)
    const age25 = rows.filter((r) => r.age === 25);
    expect(age25).toEqual([
      { name: 'Bob', age: 25, city: 'LA' },
      { name: 'Diana', age: 25, city: 'SF' },
    ]);
  });

  it('sorts with nulls (nulls go last)', () => {
    type NRow = { x: number | null; y: string };
    const df = DataFrame.fromRows<NRow>([
      { x: 3, y: 'c' },
      { x: null, y: 'a' },
      { x: 1, y: 'b' },
      { x: null, y: 'd' },
      { x: 2, y: 'e' },
    ]);
    const result = df.sortBy('x');
    const xs = result.col('x' as never).toArray();
    expect(xs).toEqual([1, 2, 3, null, null]);
  });

  it('throws on invalid column name', () => {
    const df = makeDF();
    expect(() => df.sortBy('nonexistent')).toThrow();
  });

  it('sorts by multiple columns array form', () => {
    const df = makeDF();
    // Sort by age asc, then name asc
    const result = df.sortBy(['age', 'name'], ['asc', 'asc']);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ name: 'Bob', age: 25, city: 'LA' });
    expect(rows[1]).toEqual({ name: 'Diana', age: 25, city: 'SF' });
    expect(rows[2]).toEqual({ name: 'Alice', age: 30, city: 'NYC' });
  });
});
