import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';
import { ShapeMismatchError } from '../../src/errors';

describe('DataFrame.assign', () => {
  it('merges columns from another DataFrame', () => {
    const df = DataFrame.fromColumns({ a: [1, 2, 3], b: ['x', 'y', 'z'] });
    const other = DataFrame.fromColumns({ c: [10, 20, 30] });
    const result = df.assign(other);

    expect(result.columns).toEqual(['a', 'b', 'c']);
    expect(result.length).toBe(3);
    expect(result.toArray()).toEqual([
      { a: 1, b: 'x', c: 10 },
      { a: 2, b: 'y', c: 20 },
      { a: 3, b: 'z', c: 30 },
    ]);
  });

  it('overwrites columns with same name from other', () => {
    const df = DataFrame.fromColumns({ a: [1, 2], b: [3, 4] });
    const other = DataFrame.fromColumns({ b: [10, 20], c: [30, 40] });
    const result = df.assign(other);

    expect(result.columns).toEqual(['a', 'b', 'c']);
    expect(result.toArray()).toEqual([
      { a: 1, b: 10, c: 30 },
      { a: 2, b: 20, c: 40 },
    ]);
  });

  it('throws ShapeMismatchError on length mismatch', () => {
    const df = DataFrame.fromColumns({ a: [1, 2, 3] });
    const other = DataFrame.fromColumns({ b: [1, 2] });

    expect(() => df.assign(other)).toThrow(ShapeMismatchError);
    expect(() => df.assign(other)).toThrow(/Cannot assign DataFrame with 2 rows to DataFrame with 3 rows/);
  });

  it('works with zero-column other DataFrame', () => {
    const df = DataFrame.fromColumns({ a: [1, 2] });
    const other = DataFrame.empty();
    const result = df.assign(other);

    expect(result.columns).toEqual(['a']);
    expect(result.toArray()).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it('works with zero-column source DataFrame', () => {
    const df = DataFrame.empty();
    const other = DataFrame.fromColumns({ a: [1, 2] });
    const result = df.assign(other);

    expect(result.columns).toEqual(['a']);
    expect(result.toArray()).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it('works with single-column DataFrames', () => {
    const df = DataFrame.fromColumns({ a: [1, 2, 3] });
    const other = DataFrame.fromColumns({ b: [4, 5, 6] });
    const result = df.assign(other);

    expect(result.columns).toEqual(['a', 'b']);
    expect(result.toArray()).toEqual([
      { a: 1, b: 4 },
      { a: 2, b: 5 },
      { a: 3, b: 6 },
    ]);
  });

  it('preserves column order with overwrites', () => {
    const df = DataFrame.fromColumns({ a: [1], b: [2], c: [3] });
    const other = DataFrame.fromColumns({ b: [20], d: [40] });
    const result = df.assign(other);

    expect(result.columns).toEqual(['a', 'b', 'c', 'd']);
    expect(result.toArray()).toEqual([{ a: 1, b: 20, c: 3, d: 40 }]);
  });

  it('returns a new DataFrame (does not mutate original)', () => {
    const df = DataFrame.fromColumns({ a: [1, 2] });
    const other = DataFrame.fromColumns({ b: [3, 4] });
    const result = df.assign(other);

    expect(result).not.toBe(df);
    expect(df.columns).toEqual(['a']);
    expect(result.columns).toEqual(['a', 'b']);
  });
});
