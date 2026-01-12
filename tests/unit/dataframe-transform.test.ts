import { describe, it, expect } from 'vitest';
import { DataFrame, DType, ColumnNotFoundError, ShapeMismatchError } from '../../src';

type Person = { name: string; age: number; active: boolean };

function makeDf(): DataFrame<Person> {
  return DataFrame.fromRows<Person>([
    { name: 'Alice', age: 30, active: true },
    { name: 'Bob', age: 25, active: false },
    { name: 'Charlie', age: 35, active: true },
  ]);
}

describe('DataFrame.withColumn', () => {
  it('adds a new column from an array of values', () => {
    const df = makeDf();
    const result = df.withColumn('score', [100, 200, 300]);
    expect(result.columns).toEqual(['name', 'age', 'active', 'score']);
    expect(result.col('score').toArray()).toEqual([100, 200, 300]);
    expect(result.shape).toEqual([3, 4]);
  });

  it('replaces an existing column from an array of values', () => {
    const df = makeDf();
    const result = df.withColumn('age', [40, 50, 60]);
    expect(result.columns).toEqual(['name', 'age', 'active']);
    expect(result.col('age').toArray()).toEqual([40, 50, 60]);
  });

  it('adds a new column from a mapping function', () => {
    const df = makeDf();
    const result = df.withColumn('ageDoubled', (row: Person) => row.age * 2);
    expect(result.col('ageDoubled').toArray()).toEqual([60, 50, 70]);
    expect(result.columns).toEqual(['name', 'age', 'active', 'ageDoubled']);
  });

  it('withColumn function receives typed row objects', () => {
    const df = makeDf();
    const result = df.withColumn('greeting', (row: Person) => `Hello ${row.name}`);
    expect(result.col('greeting').toArray()).toEqual([
      'Hello Alice',
      'Hello Bob',
      'Hello Charlie',
    ]);
  });

  it('throws ShapeMismatchError when array length does not match', () => {
    const df = makeDf();
    expect(() => df.withColumn('bad', [1, 2])).toThrow(ShapeMismatchError);
  });

  it('returns a new DataFrame instance', () => {
    const df = makeDf();
    const result = df.withColumn('x', [1, 2, 3]);
    expect(result).not.toBe(df);
    expect(df.columns).toEqual(['name', 'age', 'active']);
  });
});

describe('DataFrame.rename', () => {
  it('renames columns according to mapping', () => {
    const df = makeDf();
    const result = df.rename({ name: 'fullName', age: 'years' });
    expect(result.columns).toEqual(['fullName', 'years', 'active']);
  });

  it('preserves data after rename', () => {
    const df = makeDf();
    const result = df.rename({ name: 'fullName' });
    expect(result.col('fullName' as never).toArray()).toEqual(['Alice', 'Bob', 'Charlie']);
  });

  it('throws ColumnNotFoundError for non-existent column', () => {
    const df = makeDf();
    expect(() => df.rename({ nonExistent: 'x' })).toThrow(ColumnNotFoundError);
  });

  it('returns a new DataFrame instance', () => {
    const df = makeDf();
    const result = df.rename({ name: 'fullName' });
    expect(result).not.toBe(df);
    expect(df.columns).toEqual(['name', 'age', 'active']);
  });

  it('handles empty mapping', () => {
    const df = makeDf();
    const result = df.rename({});
    expect(result.columns).toEqual(['name', 'age', 'active']);
  });
});

describe('DataFrame.cast', () => {
  it('casts a column to a new dtype', () => {
    const df = makeDf();
    const result = df.cast({ age: DType.Float64 });
    expect(result.dtypes['age']).toBe(DType.Float64);
    expect(result.col('age').toArray()).toEqual([30, 25, 35]);
  });

  it('casts multiple columns', () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 2 },
      { x: 3, y: 4 },
    ]);
    const result = df.cast({ x: DType.Int32, y: DType.Int32 });
    expect(result.dtypes['x']).toBe(DType.Int32);
    expect(result.dtypes['y']).toBe(DType.Int32);
  });

  it('casts number to string', () => {
    const df = makeDf();
    const result = df.cast({ age: DType.Utf8 });
    expect(result.dtypes['age']).toBe(DType.Utf8);
    expect(result.col('age' as never).toArray()).toEqual(['30', '25', '35']);
  });

  it('throws ColumnNotFoundError for non-existent column', () => {
    const df = makeDf();
    expect(() => df.cast({ nonExistent: DType.Float64 } as never)).toThrow(ColumnNotFoundError);
  });

  it('returns a new DataFrame instance', () => {
    const df = makeDf();
    const result = df.cast({ age: DType.Int32 });
    expect(result).not.toBe(df);
  });

  it('does not modify columns not in the cast mapping', () => {
    const df = makeDf();
    const result = df.cast({ age: DType.Utf8 });
    expect(result.dtypes['name']).toBe(DType.Utf8);
    expect(result.dtypes['active']).toBe(DType.Boolean);
  });
});
