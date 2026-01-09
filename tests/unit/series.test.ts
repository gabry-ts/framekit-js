import { describe, it, expect } from 'vitest';
import { Series } from '../../src/series';
import { Float64Column, Int32Column } from '../../src/storage/numeric';
import { Utf8Column } from '../../src/storage/string';
import { BooleanColumn } from '../../src/storage/boolean';
import { DType } from '../../src/types/dtype';
import { TypeMismatchError } from '../../src/errors';

describe('Series', () => {
  describe('basic properties', () => {
    it('has name, dtype, length', () => {
      const col = Float64Column.from([1, 2, 3]);
      const s = new Series<number>('x', col);
      expect(s.name).toBe('x');
      expect(s.dtype).toBe(DType.Float64);
      expect(s.length).toBe(3);
    });

    it('reports nullCount', () => {
      const col = Float64Column.from([1, null, 3]);
      const s = new Series<number>('x', col);
      expect(s.nullCount).toBe(1);
    });

    it('exposes underlying column', () => {
      const col = Float64Column.from([1, 2]);
      const s = new Series<number>('x', col);
      expect(s.column).toBe(col);
    });
  });

  describe('get and toArray', () => {
    it('get returns value or null', () => {
      const s = new Series<number>('x', Float64Column.from([10, null, 30]));
      expect(s.get(0)).toBe(10);
      expect(s.get(1)).toBeNull();
      expect(s.get(2)).toBe(30);
    });

    it('toArray returns all values', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      expect(s.toArray()).toEqual([1, null, 3]);
    });
  });

  describe('numeric methods', () => {
    it('sum skips nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      expect(s.sum()).toBe(4);
    });

    it('mean skips nulls', () => {
      const s = new Series<number>('x', Float64Column.from([2, null, 4]));
      expect(s.mean()).toBe(3);
    });

    it('mean returns null for all-null', () => {
      const s = new Series<number>('x', Float64Column.from([null, null]));
      expect(s.mean()).toBeNull();
    });

    it('min and max skip nulls', () => {
      const s = new Series<number>('x', Float64Column.from([5, null, 1, 3]));
      expect(s.min()).toBe(1);
      expect(s.max()).toBe(5);
    });

    it('std computes sample standard deviation', () => {
      const s = new Series<number>('x', Float64Column.from([2, 4, 4, 4, 5, 5, 7, 9]));
      const result = s.std();
      expect(result).not.toBeNull();
      expect(result!).toBeCloseTo(2.138, 2);
    });

    it('std returns null for fewer than 2 values', () => {
      const s = new Series<number>('x', Float64Column.from([5]));
      expect(s.std()).toBeNull();
    });

    it('median returns middle value for odd count', () => {
      const s = new Series<number>('x', Float64Column.from([3, 1, 2]));
      expect(s.median()).toBe(2);
    });

    it('median returns average of two middle values for even count', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3, 4]));
      expect(s.median()).toBe(2.5);
    });

    it('median returns null for all-null', () => {
      const s = new Series<number>('x', Float64Column.from([null, null]));
      expect(s.median()).toBeNull();
    });

    it('works with Int32Column', () => {
      const s = new Series<number>('x', Int32Column.from([10, 20, 30]));
      expect(s.sum()).toBe(60);
      expect(s.mean()).toBe(20);
    });

    it('throws TypeMismatchError for non-numeric series', () => {
      const s = new Series<string>('x', Utf8Column.from(['a', 'b']));
      expect(() => s.sum()).toThrow(TypeMismatchError);
    });
  });

  describe('comparison methods', () => {
    it('eq returns boolean series', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3, null]));
      const result = s.eq(2);
      expect(result.toArray()).toEqual([false, true, false, null]);
    });

    it('neq returns boolean series', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      const result = s.neq(2);
      expect(result.toArray()).toEqual([true, false, true]);
    });

    it('gt, gte, lt, lte', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      expect(s.gt(2).toArray()).toEqual([false, false, true]);
      expect(s.gte(2).toArray()).toEqual([false, true, true]);
      expect(s.lt(2).toArray()).toEqual([true, false, false]);
      expect(s.lte(2).toArray()).toEqual([true, true, false]);
    });

    it('eq works with strings', () => {
      const s = new Series<string>('x', Utf8Column.from(['a', 'b', 'a']));
      expect(s.eq('a').toArray()).toEqual([true, false, true]);
    });
  });

  describe('isIn', () => {
    it('returns true for values in the set', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3, 4]));
      const result = s.isIn([2, 4]);
      expect(result.toArray()).toEqual([false, true, false, true]);
    });

    it('returns null for null values', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const result = s.isIn([1, 3]);
      expect(result.toArray()).toEqual([true, null, true]);
    });
  });

  describe('isNull and isNotNull', () => {
    it('isNull identifies nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      expect(s.isNull().toArray()).toEqual([false, true, false]);
    });

    it('isNotNull identifies non-nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      expect(s.isNotNull().toArray()).toEqual([true, false, true]);
    });
  });

  describe('fillNull', () => {
    it('replaces nulls with given value', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const filled = s.fillNull(0);
      expect(filled.toArray()).toEqual([1, 0, 3]);
      expect(filled.nullCount).toBe(0);
    });

    it('preserves name', () => {
      const s = new Series<string>('name', Utf8Column.from([null, 'b']));
      expect(s.fillNull('?').name).toBe('name');
    });
  });

  describe('unique and nUnique', () => {
    it('unique removes duplicates', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 2, 3, 1]));
      const u = s.unique();
      expect(u.toArray()).toEqual([1, 2, 3]);
    });

    it('unique preserves null (once)', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, null, 2]));
      const u = s.unique();
      expect(u.toArray()).toEqual([1, null, 2]);
    });

    it('nUnique counts distinct values including null', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 2, null]));
      expect(s.nUnique()).toBe(3);
    });
  });

  describe('cast', () => {
    it('casts number to string', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      const casted = s.cast<string>(DType.Utf8);
      expect(casted.dtype).toBe(DType.Utf8);
      expect(casted.toArray()).toEqual(['1', '2', '3']);
    });

    it('casts string to number', () => {
      const s = new Series<string>('x', Utf8Column.from(['1', '2.5']));
      const casted = s.cast<number>(DType.Float64);
      expect(casted.dtype).toBe(DType.Float64);
      expect(casted.toArray()).toEqual([1, 2.5]);
    });

    it('preserves nulls during cast', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const casted = s.cast<string>(DType.Utf8);
      expect(casted.toArray()).toEqual(['1', null, '3']);
    });

    it('casts boolean to number', () => {
      const s = new Series<boolean>('x', BooleanColumn.from([true, false, true]));
      const casted = s.cast<number>(DType.Float64);
      expect(casted.toArray()).toEqual([1, 0, 1]);
    });
  });

  describe('apply', () => {
    it('maps values through function', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      const doubled = s.apply<number>((v) => (v !== null ? v * 2 : null));
      expect(doubled.toArray()).toEqual([2, 4, 6]);
    });

    it('can change type', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      const strings = s.apply<string>((v) => (v !== null ? String(v) : null));
      expect(strings.dtype).toBe(DType.Utf8);
      expect(strings.toArray()).toEqual(['1', '2', '3']);
    });

    it('preserves nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const result = s.apply<number>((v) => (v !== null ? v + 10 : null));
      expect(result.toArray()).toEqual([11, null, 13]);
    });
  });

  describe('immutability', () => {
    it('fillNull returns a new series', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const filled = s.fillNull(0);
      expect(filled).not.toBe(s);
      expect(s.get(1)).toBeNull();
    });

    it('unique returns a new series', () => {
      const s = new Series<number>('x', Float64Column.from([1, 1, 2]));
      const u = s.unique();
      expect(u).not.toBe(s);
      expect(s.length).toBe(3);
    });
  });
});
