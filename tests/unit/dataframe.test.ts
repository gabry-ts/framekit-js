import { describe, it, expect } from 'vitest';
import {
  DataFrame,
  DType,
  ColumnNotFoundError,
  Series,
} from '../../src/index';

describe('DataFrame', () => {
  describe('fromColumns', () => {
    it('creates DataFrame from column-oriented data', () => {
      const df = DataFrame.fromColumns({
        name: ['Alice', 'Bob', 'Charlie'],
        age: [30, 25, 35],
      });
      expect(df.length).toBe(3);
      expect(df.columns).toEqual(['name', 'age']);
    });

    it('auto-detects Float64 for numbers', () => {
      const df = DataFrame.fromColumns({ x: [1.5, 2.5, 3.5] });
      expect(df.dtypes['x']).toBe(DType.Float64);
    });

    it('auto-detects Utf8 for strings', () => {
      const df = DataFrame.fromColumns({ s: ['a', 'b', 'c'] });
      expect(df.dtypes['s']).toBe(DType.Utf8);
    });

    it('auto-detects Boolean for booleans', () => {
      const df = DataFrame.fromColumns({ b: [true, false, true] });
      expect(df.dtypes['b']).toBe(DType.Boolean);
    });

    it('auto-detects Date for Date objects', () => {
      const df = DataFrame.fromColumns({ d: [new Date('2024-01-01'), new Date('2024-02-01')] });
      expect(df.dtypes['d']).toBe(DType.Date);
    });

    it('throws ShapeMismatchError for unequal column lengths', () => {
      expect(() =>
        DataFrame.fromColumns({ a: [1, 2], b: [1, 2, 3] }),
      ).toThrow('length');
    });

    it('handles null values', () => {
      const df = DataFrame.fromColumns({ x: [1, null, 3] });
      expect(df.col('x').get(1)).toBe(null);
      expect(df.col('x').get(0)).toBe(1);
    });

    it('returns empty DataFrame for empty input', () => {
      const df = DataFrame.fromColumns({});
      expect(df.length).toBe(0);
      expect(df.columns).toEqual([]);
    });
  });

  describe('fromRows', () => {
    it('creates DataFrame from array of objects', () => {
      const df = DataFrame.fromRows([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ]);
      expect(df.length).toBe(2);
      expect(df.columns).toEqual(['name', 'age']);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('age').get(1)).toBe(25);
    });

    it('handles missing keys across rows as null', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 'x' },
        { a: 2 },
      ]);
      expect(df.col('b').get(1)).toBe(null);
    });

    it('returns empty DataFrame for empty array', () => {
      const df = DataFrame.fromRows([]);
      expect(df.length).toBe(0);
      expect(df.columns).toEqual([]);
    });
  });

  describe('empty', () => {
    it('creates empty DataFrame', () => {
      const df = DataFrame.empty();
      expect(df.length).toBe(0);
      expect(df.columns).toEqual([]);
      expect(df.shape).toEqual([0, 0]);
    });
  });

  describe('metadata', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob', 'Charlie'],
      age: [30, 25, 35],
      active: [true, false, true],
    });

    it('shape returns [rows, cols]', () => {
      expect(df.shape).toEqual([3, 3]);
    });

    it('columns returns column names', () => {
      expect(df.columns).toEqual(['name', 'age', 'active']);
    });

    it('columns returns a copy (not mutable reference)', () => {
      const cols = df.columns;
      cols.push('extra');
      expect(df.columns).toEqual(['name', 'age', 'active']);
    });

    it('dtypes returns dtype mapping', () => {
      expect(df.dtypes).toEqual({
        name: DType.Utf8,
        age: DType.Float64,
        active: DType.Boolean,
      });
    });

    it('length returns row count', () => {
      expect(df.length).toBe(3);
    });
  });

  describe('col', () => {
    const df = DataFrame.fromColumns({
      x: [1, 2, 3],
      y: ['a', 'b', 'c'],
    });

    it('returns a Series for existing column', () => {
      const s = df.col('x');
      expect(s).toBeInstanceOf(Series);
      expect(s.name).toBe('x');
      expect(s.toArray()).toEqual([1, 2, 3]);
    });

    it('throws ColumnNotFoundError for missing column', () => {
      expect(() => df.col('z' as never)).toThrow(ColumnNotFoundError);
    });

    it('error message includes available columns', () => {
      expect(() => df.col('z' as never)).toThrow('Available columns');
    });
  });

  describe('row', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob'],
      age: [30, 25],
    });

    it('returns plain object at given index', () => {
      expect(df.row(0)).toEqual({ name: 'Alice', age: 30 });
      expect(df.row(1)).toEqual({ name: 'Bob', age: 25 });
    });

    it('throws on out-of-bounds index', () => {
      expect(() => df.row(5)).toThrow('out of bounds');
    });

    it('throws on negative index', () => {
      expect(() => df.row(-1)).toThrow('out of bounds');
    });
  });

  describe('clone', () => {
    it('returns a deep copy', () => {
      const df = DataFrame.fromColumns({
        x: [1, 2, 3],
      });
      const cloned = df.clone();
      expect(cloned.length).toBe(3);
      expect(cloned.col('x').toArray()).toEqual([1, 2, 3]);
      expect(cloned.columns).toEqual(df.columns);
      // Verify it's a different instance
      expect(cloned).not.toBe(df);
    });
  });

  describe('type detection edge cases', () => {
    it('defaults to Float64 for all-null columns', () => {
      const df = DataFrame.fromColumns({ x: [null, null, null] });
      expect(df.dtypes['x']).toBe(DType.Float64);
    });

    it('detects type from first non-null value', () => {
      const df = DataFrame.fromColumns({ x: [null, 'hello', null] });
      expect(df.dtypes['x']).toBe(DType.Utf8);
      expect(df.col('x').get(1)).toBe('hello');
    });
  });
});
