import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { ColumnNotFoundError } from '../../../src/errors';

describe('DataFrame.spread', () => {
  describe('array expansion', () => {
    it('expands array values into indexed columns', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        tags: [['a', 'b'], ['c', 'd'], ['e', 'f']],
      });
      const result = df.spread('tags');

      expect(result.columns).toEqual(['id', 'tags', 'tags_0', 'tags_1']);
      expect(result.toArray()).toEqual([
        { id: 1, tags: ['a', 'b'], tags_0: 'a', tags_1: 'b' },
        { id: 2, tags: ['c', 'd'], tags_0: 'c', tags_1: 'd' },
        { id: 3, tags: ['e', 'f'], tags_0: 'e', tags_1: 'f' },
      ]);
    });

    it('pads short arrays with nulls', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        vals: [[10, 20, 30], [40], [50, 60]],
      });
      const result = df.spread('vals');

      expect(result.columns).toEqual(['id', 'vals', 'vals_0', 'vals_1', 'vals_2']);
      expect(result.toArray()).toEqual([
        { id: 1, vals: [10, 20, 30], vals_0: 10, vals_1: 20, vals_2: 30 },
        { id: 2, vals: [40], vals_0: 40, vals_1: null, vals_2: null },
        { id: 3, vals: [50, 60], vals_0: 50, vals_1: 60, vals_2: null },
      ]);
    });

    it('handles null values in array column', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        vals: [[1, 2], null, [3, 4]],
      });
      const result = df.spread('vals');

      expect(result.toArray()).toEqual([
        { id: 1, vals: [1, 2], vals_0: 1, vals_1: 2 },
        { id: 2, vals: null, vals_0: null, vals_1: null },
        { id: 3, vals: [3, 4], vals_0: 3, vals_1: 4 },
      ]);
    });

    it('respects limit option', () => {
      const df = DataFrame.fromColumns({
        id: [1],
        vals: [[10, 20, 30, 40]],
      });
      const result = df.spread('vals', { limit: 2 });

      expect(result.columns).toEqual(['id', 'vals', 'vals_0', 'vals_1']);
      expect(result.toArray()).toEqual([
        { id: 1, vals: [10, 20, 30, 40], vals_0: 10, vals_1: 20 },
      ]);
    });

    it('supports custom naming function', () => {
      const df = DataFrame.fromColumns({
        id: [1],
        vals: [[10, 20]],
      });
      const result = df.spread('vals', {
        name: (col, idx) => `${col}__${String(idx)}`,
      });

      expect(result.columns).toEqual(['id', 'vals', 'vals__0', 'vals__1']);
    });

    it('supports drop option to remove source column', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[10, 20], [30, 40]],
      });
      const result = df.spread('vals', { drop: true });

      expect(result.columns).toEqual(['id', 'vals_0', 'vals_1']);
      expect(result.toArray()).toEqual([
        { id: 1, vals_0: 10, vals_1: 20 },
        { id: 2, vals_0: 30, vals_1: 40 },
      ]);
    });

    it('handles empty arrays (no expansion columns)', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [[], []],
      });
      const result = df.spread('vals');

      expect(result.columns).toEqual(['id', 'vals']);
      expect(result.length).toBe(2);
    });
  });

  describe('object expansion', () => {
    it('expands object fields into named columns', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        meta: [{ x: 10, y: 20 }, { x: 30, y: 40 }],
      });
      const result = df.spread('meta');

      expect(result.columns).toEqual(['id', 'meta', 'meta_x', 'meta_y']);
      expect(result.toArray()).toEqual([
        { id: 1, meta: { x: 10, y: 20 }, meta_x: 10, meta_y: 20 },
        { id: 2, meta: { x: 30, y: 40 }, meta_x: 30, meta_y: 40 },
      ]);
    });

    it('handles objects with different keys (pads missing with null)', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2, 3],
        meta: [{ a: 1 }, { b: 2 }, { a: 3, b: 4 }],
      });
      const result = df.spread('meta');

      expect(result.toArray()).toEqual([
        { id: 1, meta: { a: 1 }, meta_a: 1, meta_b: null },
        { id: 2, meta: { b: 2 }, meta_a: null, meta_b: 2 },
        { id: 3, meta: { a: 3, b: 4 }, meta_a: 3, meta_b: 4 },
      ]);
    });

    it('handles null values in object column', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        meta: [{ x: 10 }, null],
      });
      const result = df.spread('meta');

      expect(result.toArray()).toEqual([
        { id: 1, meta: { x: 10 }, meta_x: 10 },
        { id: 2, meta: null, meta_x: null },
      ]);
    });

    it('respects limit option for objects', () => {
      const df = DataFrame.fromColumns({
        id: [1],
        meta: [{ a: 1, b: 2, c: 3 }],
      });
      const result = df.spread('meta', { limit: 2 });

      // Only first 2 keys
      const spreadCols = result.columns.filter(c => c.startsWith('meta_'));
      expect(spreadCols.length).toBe(2);
    });

    it('supports custom naming for objects', () => {
      const df = DataFrame.fromColumns({
        id: [1],
        meta: [{ x: 10 }],
      });
      const result = df.spread('meta', {
        name: (col, key) => `${col}.${String(key)}`,
      });

      expect(result.columns).toContain('meta.x');
    });
  });

  describe('error handling', () => {
    it('throws ColumnNotFoundError for non-existent column', () => {
      const df = DataFrame.fromColumns({ id: [1] });
      expect(() => df.spread('nonexistent')).toThrow(ColumnNotFoundError);
    });
  });

  describe('edge cases', () => {
    it('works with empty DataFrame', () => {
      const df = DataFrame.fromColumns({
        id: [] as number[],
        vals: [] as unknown[],
      });
      const result = df.spread('vals');

      expect(result.length).toBe(0);
      expect(result.columns).toEqual(['id', 'vals']);
    });

    it('handles all-null source column', () => {
      const df = DataFrame.fromColumns({
        id: [1, 2],
        vals: [null, null],
      });
      const result = df.spread('vals');

      // No expansion columns since all values are null
      expect(result.columns).toEqual(['id', 'vals']);
    });
  });
});
