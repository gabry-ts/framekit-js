import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';
import { derive } from '../../../src/compat/verbs';

describe('compat derive', () => {
  it('creates a new column from an expression', () => {
    const df = DataFrame.fromColumns({ x: [1, 2, 3], y: [10, 20, 30] });
    const result = derive(df, { total: (d) => (d.x as number) + (d.y as number) });

    expect(result.columns).toEqual(['x', 'y', 'total']);
    expect(result.col('total').toArray()).toEqual([11, 22, 33]);
  });

  it('preserves existing columns', () => {
    const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
    const result = derive(df, { c: (d) => (d.a as number) * 2 });

    expect(result.columns).toEqual(['a', 'b', 'c']);
    expect(result.col('a').toArray()).toEqual([1, 2]);
    expect(result.col('b').toArray()).toEqual(['x', 'y']);
    expect(result.col('c').toArray()).toEqual([2, 4]);
  });

  it('supports multiple derived columns', () => {
    const df = DataFrame.fromColumns({ x: [1, 2, 3] });
    const result = derive(df, {
      doubled: (d) => (d.x as number) * 2,
      tripled: (d) => (d.x as number) * 3,
      label: (d) => `val-${String(d.x)}`,
    });

    expect(result.columns).toEqual(['x', 'doubled', 'tripled', 'label']);
    expect(result.col('doubled').toArray()).toEqual([2, 4, 6]);
    expect(result.col('tripled').toArray()).toEqual([3, 6, 9]);
    expect(result.col('label').toArray()).toEqual(['val-1', 'val-2', 'val-3']);
  });

  it('overwrites existing columns when names match', () => {
    const df = DataFrame.fromColumns({ x: [1, 2, 3], y: [10, 20, 30] });
    const result = derive(df, { x: (d) => (d.x as number) * 100 });

    expect(result.columns).toEqual(['x', 'y']);
    expect(result.col('x').toArray()).toEqual([100, 200, 300]);
    expect(result.col('y').toArray()).toEqual([10, 20, 30]);
  });

  it('propagates null values correctly', () => {
    const df = DataFrame.fromColumns({ x: [1, null, 3], y: [10, 20, null] });
    const result = derive(df, {
      sum: (d) => {
        const x = d.x as number | null;
        const y = d.y as number | null;
        if (x === null || y === null) return null;
        return x + y;
      },
    });

    expect(result.col('sum').toArray()).toEqual([11, null, null]);
  });

  it('deterministic output column order', () => {
    const df = DataFrame.fromColumns({ a: [1], b: [2], c: [3] });
    const result = derive(df, {
      z: () => 'z',
      m: () => 'm',
      a_new: () => 'a',
    });

    expect(result.columns).toEqual(['a', 'b', 'c', 'z', 'm', 'a_new']);
  });

  it('works with empty DataFrame', () => {
    const df = DataFrame.fromColumns({ x: [] as number[] });
    const result = derive(df, { y: (d) => (d.x as number) * 2 });

    expect(result.length).toBe(0);
    expect(result.columns).toEqual(['x', 'y']);
  });
});
