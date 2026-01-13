import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';

type TestRow = { name: string | null; age: number | null; city: string | null };

function makeTestDf() {
  return DataFrame.fromRows<TestRow>([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: null, city: 'LA' },
    { name: null, age: 25, city: null },
    { name: 'Dave', age: 40, city: 'SF' },
  ]);
}

describe('DataFrame.dropNull', () => {
  it('drops rows where any column has null', () => {
    const df = makeTestDf();
    const result = df.dropNull();
    expect(result.length).toBe(2);
    expect(result.toArray()).toEqual([
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: 'Dave', age: 40, city: 'SF' },
    ]);
  });

  it('drops rows where specific column has null', () => {
    const df = makeTestDf();
    const result = df.dropNull('age');
    expect(result.length).toBe(3);
    expect(result.toArray()).toEqual([
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: null, age: 25, city: null },
      { name: 'Dave', age: 40, city: 'SF' },
    ]);
  });

  it('drops rows where any of specified columns have null', () => {
    const df = makeTestDf();
    const result = df.dropNull(['name', 'city']);
    expect(result.length).toBe(3);
    expect(result.toArray()).toEqual([
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: 'Bob', age: null, city: 'LA' },
      { name: 'Dave', age: 40, city: 'SF' },
    ]);
  });

  it('returns all rows when no nulls exist', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ]);
    const result = df.dropNull();
    expect(result.length).toBe(2);
  });

  it('returns empty DataFrame when all rows have nulls', () => {
    const df = DataFrame.fromRows([
      { a: null, b: 1 },
      { a: 2, b: null },
    ]);
    const result = df.dropNull();
    expect(result.length).toBe(0);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    const result = df.dropNull();
    expect(result).not.toBe(df);
  });
});

describe('DataFrame.fillNull with values', () => {
  it('fills nulls in specified columns with given values', () => {
    const df = makeTestDf();
    const result = df.fillNull({ age: 0, city: 'Unknown' });
    expect(result.toArray()).toEqual([
      { name: 'Alice', age: 30, city: 'NYC' },
      { name: 'Bob', age: 0, city: 'LA' },
      { name: null, age: 25, city: 'Unknown' },
      { name: 'Dave', age: 40, city: 'SF' },
    ]);
  });

  it('does not modify columns not in the fill map', () => {
    const df = makeTestDf();
    const result = df.fillNull({ age: 99 });
    // name column still has null in row 2
    expect(result.row(2)).toEqual({ name: null, age: 25, city: null });
    // age column is filled
    expect(result.row(1)).toEqual({ name: 'Bob', age: 99, city: 'LA' });
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    const result = df.fillNull({ age: 0 });
    expect(result).not.toBe(df);
  });
});

describe('DataFrame.fillNull forward', () => {
  it('forward-fills nulls within each column', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'x' },
      { a: null, b: null },
      { a: null, b: null },
      { a: 4, b: 'y' },
    ]);
    const result = df.fillNull('forward');
    expect(result.toArray()).toEqual([
      { a: 1, b: 'x' },
      { a: 1, b: 'x' },
      { a: 1, b: 'x' },
      { a: 4, b: 'y' },
    ]);
  });

  it('leaves leading nulls as null', () => {
    const df = DataFrame.fromRows([
      { a: null },
      { a: null },
      { a: 3 },
    ]);
    const result = df.fillNull('forward');
    expect(result.toArray()).toEqual([
      { a: null },
      { a: null },
      { a: 3 },
    ]);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    const result = df.fillNull('forward');
    expect(result).not.toBe(df);
  });
});

describe('DataFrame.fillNull backward', () => {
  it('backward-fills nulls within each column', () => {
    const df = DataFrame.fromRows([
      { a: 1, b: 'x' },
      { a: null, b: null },
      { a: null, b: null },
      { a: 4, b: 'y' },
    ]);
    const result = df.fillNull('backward');
    expect(result.toArray()).toEqual([
      { a: 1, b: 'x' },
      { a: 4, b: 'y' },
      { a: 4, b: 'y' },
      { a: 4, b: 'y' },
    ]);
  });

  it('leaves trailing nulls as null', () => {
    const df = DataFrame.fromRows([
      { a: 3 },
      { a: null },
      { a: null },
    ]);
    const result = df.fillNull('backward');
    expect(result.toArray()).toEqual([
      { a: 3 },
      { a: null },
      { a: null },
    ]);
  });

  it('returns new DataFrame instance', () => {
    const df = makeTestDf();
    const result = df.fillNull('backward');
    expect(result).not.toBe(df);
  });
});
