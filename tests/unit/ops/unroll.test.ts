import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { ColumnNotFoundError } from '../../../src/errors';

describe('DataFrame.unroll', () => {
  describe('single column', () => {
    it('expands array elements into individual rows', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        tags: [['a', 'b', 'c'], ['d', 'e']],
      });
      const result = df.unroll('tags');

      expect(result.length).toBe(5);
      expect(result.toArray()).toEqual([
        { id: 1, tags: 'a' },
        { id: 1, tags: 'b' },
        { id: 1, tags: 'c' },
        { id: 2, tags: 'd' },
        { id: 2, tags: 'e' },
      ]);
    });

    it('repeats non-array columns for each expanded row', () => {
      const df = DataFrame.fromColumns({
        name: ['Alice', 'Bob'],
        score: [100, 200],
        items: [['x', 'y'], ['z']],
      });
      const result = df.unroll('items');

      expect(result.toArray()).toEqual([
        { name: 'Alice', score: 100, items: 'x' },
        { name: 'Alice', score: 100, items: 'y' },
        { name: 'Bob', score: 200, items: 'z' },
      ]);
    });
  });

  describe('multi-column unroll', () => {
    it('aligns arrays element-wise across columns', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        a: [[10, 20], [30, 40]],
        b: [['x', 'y'], ['z', 'w']],
      });
      const result = df.unroll(['a', 'b']);

      expect(result.length).toBe(4);
      expect(result.toArray()).toEqual([
        { id: 1, a: 10, b: 'x' },
        { id: 1, a: 20, b: 'y' },
        { id: 2, a: 30, b: 'z' },
        { id: 2, a: 40, b: 'w' },
      ]);
    });

    it('pads shorter arrays with null in multi-column unroll', () => {
      const df = DataFrame.fromColumns({
        id: [1],
        a: [[10, 20, 30]],
        b: [['x']],
      });
      const result = df.unroll(['a', 'b']);

      expect(result.length).toBe(3);
      expect(result.toArray()).toEqual([
        { id: 1, a: 10, b: 'x' },
        { id: 1, a: 20, b: null },
        { id: 1, a: 30, b: null },
      ]);
    });
  });

  describe('index option', () => {
    it('adds an index column tracking position within original array', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[10, 20], [30, 40, 50]],
      });
      const result = df.unroll('vals', { index: 'pos' });

      expect(result.columns).toContain('pos');
      expect(result.toArray()).toEqual([
        { id: 1, vals: 10, pos: 0 },
        { id: 1, vals: 20, pos: 1 },
        { id: 2, vals: 30, pos: 0 },
        { id: 2, vals: 40, pos: 1 },
        { id: 2, vals: 50, pos: 2 },
      ]);
    });
  });

  describe('empty arrays', () => {
    it('produces no rows for input rows with empty arrays', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        vals: [[10, 20], [], [30]],
      });
      const result = df.unroll('vals');

      expect(result.length).toBe(3);
      expect(result.toArray()).toEqual([
        { id: 1, vals: 10 },
        { id: 1, vals: 20 },
        { id: 3, vals: 30 },
      ]);
    });

    it('returns empty DataFrame when all arrays are empty', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[], []],
      });
      const result = df.unroll('vals');

      expect(result.length).toBe(0);
    });
  });

  describe('null handling', () => {
    it('treats null values as empty (produces no rows)', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        vals: [[10, 20], null, [30]],
      });
      const result = df.unroll('vals');

      expect(result.length).toBe(3);
      expect(result.toArray()).toEqual([
        { id: 1, vals: 10 },
        { id: 1, vals: 20 },
        { id: 3, vals: 30 },
      ]);
    });

    it('handles non-array values as having zero length', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[10, 20], 'not-an-array'],
      });
      const result = df.unroll('vals');

      // non-array value produces 0 rows for that input row
      expect(result.length).toBe(2);
      expect(result.toArray()).toEqual([
        { id: 1, vals: 10 },
        { id: 1, vals: 20 },
      ]);
    });
  });

  describe('error handling', () => {
    it('throws ColumnNotFoundError for non-existent column', () => {
      const df = DataFrame.fromColumns({ id: [1] });
      expect(() => df.unroll('nonexistent')).toThrow(ColumnNotFoundError);
    });
  });

  describe('edge cases', () => {
    it('works with empty DataFrame', () => {
      const df = DataFrame.fromColumns({
        id: [] as number[],
        vals: [] as unknown[],
      });
      const result = df.unroll('vals');

      expect(result.length).toBe(0);
      expect(result.columns).toContain('id');
      expect(result.columns).toContain('vals');
    });

    it('works with single-element arrays', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[10], [20]],
      });
      const result = df.unroll('vals');

      expect(result.length).toBe(2);
      expect(result.toArray()).toEqual([
        { id: 1, vals: 10 },
        { id: 2, vals: 20 },
      ]);
    });
  });
});
