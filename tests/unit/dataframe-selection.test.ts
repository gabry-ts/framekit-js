import { describe, it, expect } from 'vitest';
import { DataFrame, ColumnNotFoundError } from '../../src/index';

type TestRow = {
  name: string;
  age: number;
  active: boolean;
};

function makeTestDf() {
  return DataFrame.fromColumns<TestRow>({
    name: ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank'],
    age: [30, 25, 35, 28, 22, 40],
    active: [true, false, true, true, false, true],
  });
}

describe('DataFrame.select', () => {
  it('selects a single column', () => {
    const df = makeTestDf();
    const selected = df.select('name');
    expect(selected.columns).toEqual(['name']);
    expect(selected.shape).toEqual([6, 1]);
    expect(selected.col('name').get(0)).toBe('Alice');
  });

  it('selects multiple columns', () => {
    const df = makeTestDf();
    const selected = df.select('name', 'age');
    expect(selected.columns).toEqual(['name', 'age']);
    expect(selected.shape).toEqual([6, 2]);
  });

  it('preserves the requested column order', () => {
    const df = makeTestDf();
    const selected = df.select('active', 'name');
    expect(selected.columns).toEqual(['active', 'name']);
  });

  it('returns a new DataFrame instance', () => {
    const df = makeTestDf();
    const selected = df.select('name');
    expect(selected).not.toBe(df);
  });

  it('throws ColumnNotFoundError for non-existent column', () => {
    const df = makeTestDf();
    expect(() => df.select('nonexistent' as never)).toThrow(ColumnNotFoundError);
  });
});

describe('DataFrame.drop', () => {
  it('drops a single column', () => {
    const df = makeTestDf();
    const dropped = df.drop('active');
    expect(dropped.columns).toEqual(['name', 'age']);
    expect(dropped.shape).toEqual([6, 2]);
  });

  it('drops multiple columns', () => {
    const df = makeTestDf();
    const dropped = df.drop('name', 'active');
    expect(dropped.columns).toEqual(['age']);
    expect(dropped.shape).toEqual([6, 1]);
  });

  it('returns a new DataFrame instance', () => {
    const df = makeTestDf();
    const dropped = df.drop('active');
    expect(dropped).not.toBe(df);
  });

  it('throws ColumnNotFoundError for non-existent column', () => {
    const df = makeTestDf();
    expect(() => df.drop('nonexistent' as never)).toThrow(ColumnNotFoundError);
  });
});

describe('DataFrame.head', () => {
  it('returns first 5 rows by default', () => {
    const df = makeTestDf();
    const result = df.head();
    expect(result.length).toBe(5);
    expect(result.col('name').get(0)).toBe('Alice');
    expect(result.col('name').get(4)).toBe('Eve');
  });

  it('returns first n rows', () => {
    const df = makeTestDf();
    const result = df.head(3);
    expect(result.length).toBe(3);
    expect(result.col('name').get(2)).toBe('Charlie');
  });

  it('returns all rows when n > length', () => {
    const df = makeTestDf();
    const result = df.head(100);
    expect(result.length).toBe(6);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    expect(df.head()).not.toBe(df);
  });
});

describe('DataFrame.tail', () => {
  it('returns last 5 rows by default', () => {
    const df = makeTestDf();
    const result = df.tail();
    expect(result.length).toBe(5);
    expect(result.col('name').get(0)).toBe('Bob');
    expect(result.col('name').get(4)).toBe('Frank');
  });

  it('returns last n rows', () => {
    const df = makeTestDf();
    const result = df.tail(2);
    expect(result.length).toBe(2);
    expect(result.col('name').get(0)).toBe('Eve');
    expect(result.col('name').get(1)).toBe('Frank');
  });

  it('returns all rows when n > length', () => {
    const df = makeTestDf();
    const result = df.tail(100);
    expect(result.length).toBe(6);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    expect(df.tail()).not.toBe(df);
  });
});

describe('DataFrame.slice', () => {
  it('slices with start and end', () => {
    const df = makeTestDf();
    const result = df.slice(1, 4);
    expect(result.length).toBe(3);
    expect(result.col('name').get(0)).toBe('Bob');
    expect(result.col('name').get(2)).toBe('Diana');
  });

  it('slices with only start (goes to end)', () => {
    const df = makeTestDf();
    const result = df.slice(4);
    expect(result.length).toBe(2);
    expect(result.col('name').get(0)).toBe('Eve');
    expect(result.col('name').get(1)).toBe('Frank');
  });

  it('clamps end to length', () => {
    const df = makeTestDf();
    const result = df.slice(4, 100);
    expect(result.length).toBe(2);
  });

  it('clamps start to 0', () => {
    const df = makeTestDf();
    const result = df.slice(-5, 3);
    expect(result.length).toBe(3);
  });

  it('preserves all columns', () => {
    const df = makeTestDf();
    const result = df.slice(0, 2);
    expect(result.columns).toEqual(['name', 'age', 'active']);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    expect(df.slice(0, 3)).not.toBe(df);
  });

  it('returns empty DataFrame when start >= end', () => {
    const df = makeTestDf();
    const result = df.slice(3, 3);
    expect(result.length).toBe(0);
  });
});

describe('DataFrame selection immutability', () => {
  it('original DataFrame is unchanged after select', () => {
    const df = makeTestDf();
    df.select('name');
    expect(df.columns).toEqual(['name', 'age', 'active']);
  });

  it('original DataFrame is unchanged after drop', () => {
    const df = makeTestDf();
    df.drop('name');
    expect(df.columns).toEqual(['name', 'age', 'active']);
  });

  it('original DataFrame is unchanged after head', () => {
    const df = makeTestDf();
    df.head(2);
    expect(df.length).toBe(6);
  });
});
