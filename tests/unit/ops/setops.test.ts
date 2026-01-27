import { describe, it, expect } from 'vitest';
import { DataFrame, ShapeMismatchError } from '../../../src/index';

describe('Set operations', () => {
  const dfA = DataFrame.fromRows([
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
  ]);

  const dfB = DataFrame.fromRows([
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
    { id: 4, name: 'Diana' },
  ]);

  describe('union', () => {
    it('should return all unique rows from both DataFrames', () => {
      const result = dfA.union(dfB);
      expect(result.length).toBe(4);
      expect(result.col('id').toArray()).toEqual([1, 2, 3, 4]);
      expect(result.col('name').toArray()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana']);
    });

    it('should deduplicate rows within a single DataFrame', () => {
      const dfDup = DataFrame.fromRows([
        { x: 1 },
        { x: 1 },
        { x: 2 },
      ]);
      const dfOther = DataFrame.fromRows([
        { x: 2 },
        { x: 3 },
      ]);
      const result = dfDup.union(dfOther);
      expect(result.length).toBe(3);
      expect(result.col('x').toArray()).toEqual([1, 2, 3]);
    });

    it('should handle identical DataFrames', () => {
      const result = dfA.union(dfA);
      expect(result.length).toBe(3);
    });

    it('should handle null values', () => {
      const df1 = DataFrame.fromRows([
        { a: 1, b: null },
        { a: 2, b: 'x' },
      ]);
      const df2 = DataFrame.fromRows([
        { a: 1, b: null },
        { a: 3, b: 'y' },
      ]);
      const result = df1.union(df2);
      expect(result.length).toBe(3);
    });
  });

  describe('intersection', () => {
    it('should return only rows present in both DataFrames', () => {
      const result = dfA.intersection(dfB);
      expect(result.length).toBe(2);
      expect(result.col('id').toArray()).toEqual([2, 3]);
      expect(result.col('name').toArray()).toEqual(['Bob', 'Charlie']);
    });

    it('should return empty when no rows overlap', () => {
      const df1 = DataFrame.fromRows([{ x: 1 }, { x: 2 }]);
      const df2 = DataFrame.fromRows([{ x: 3 }, { x: 4 }]);
      const result = df1.intersection(df2);
      expect(result.length).toBe(0);
    });

    it('should deduplicate result rows', () => {
      const df1 = DataFrame.fromRows([{ x: 1 }, { x: 1 }, { x: 2 }]);
      const df2 = DataFrame.fromRows([{ x: 1 }, { x: 1 }]);
      const result = df1.intersection(df2);
      expect(result.length).toBe(1);
      expect(result.col('x').toArray()).toEqual([1]);
    });
  });

  describe('difference', () => {
    it('should return rows in left that are not in right', () => {
      const result = dfA.difference(dfB);
      expect(result.length).toBe(1);
      expect(result.col('id').toArray()).toEqual([1]);
      expect(result.col('name').toArray()).toEqual(['Alice']);
    });

    it('should return all rows when no overlap', () => {
      const df1 = DataFrame.fromRows([{ x: 1 }, { x: 2 }]);
      const df2 = DataFrame.fromRows([{ x: 3 }, { x: 4 }]);
      const result = df1.difference(df2);
      expect(result.length).toBe(2);
    });

    it('should return empty when all rows exist in right', () => {
      const result = dfB.difference(dfA.union(dfB));
      expect(result.length).toBe(0);
    });

    it('should deduplicate result rows', () => {
      const df1 = DataFrame.fromRows([{ x: 1 }, { x: 1 }, { x: 2 }]);
      const df2 = DataFrame.fromRows([{ x: 2 }]);
      const result = df1.difference(df2);
      expect(result.length).toBe(1);
      expect(result.col('x').toArray()).toEqual([1]);
    });
  });

  describe('schema validation', () => {
    it('should throw ShapeMismatchError if column counts differ', () => {
      const df1 = DataFrame.fromRows([{ a: 1, b: 2 }]);
      const df2 = DataFrame.fromRows([{ a: 1 }]);
      expect(() => df1.union(df2)).toThrow(ShapeMismatchError);
      expect(() => df1.intersection(df2)).toThrow(ShapeMismatchError);
      expect(() => df1.difference(df2)).toThrow(ShapeMismatchError);
    });

    it('should throw ShapeMismatchError if column names differ', () => {
      const df1 = DataFrame.fromRows([{ a: 1 }]);
      const df2 = DataFrame.fromRows([{ b: 1 }]);
      expect(() => df1.union(df2)).toThrow(ShapeMismatchError);
    });

    it('should throw ShapeMismatchError if column types differ', () => {
      const df1 = DataFrame.fromRows([{ a: 1 }]);
      const df2 = DataFrame.fromRows([{ a: 'x' }]);
      expect(() => df1.union(df2)).toThrow(ShapeMismatchError);
    });
  });
});
