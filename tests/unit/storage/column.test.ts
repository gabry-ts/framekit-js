import { describe, it, expect } from 'vitest';
import { Float64Column } from '../../../src/storage/numeric';
import { DType } from '../../../src/types/dtype';
import { BitArray } from '../../../src/storage/bitarray';
import { Column } from '../../../src/storage/column';

describe('Float64Column', () => {
  describe('construction and properties', () => {
    it('should create from Float64Array', () => {
      const col = new Float64Column(new Float64Array([1, 2, 3]));
      expect(col.length).toBe(3);
      expect(col.dtype).toBe(DType.Float64);
      expect(col.nullCount).toBe(0);
    });

    it('should report nullCount from BitArray mask', () => {
      const mask = new BitArray(3);
      mask.set(0, true);
      mask.set(1, false);
      mask.set(2, true);
      const col = new Float64Column(new Float64Array([1, 0, 3]), mask);
      expect(col.nullCount).toBe(1);
    });
  });

  describe('get', () => {
    it('should return values at valid indices', () => {
      const col = Float64Column.from([10, 20, 30]);
      expect(col.get(0)).toBe(10);
      expect(col.get(1)).toBe(20);
      expect(col.get(2)).toBe(30);
    });

    it('should return null for null values', () => {
      const col = Float64Column.from([1, null, 3]);
      expect(col.get(0)).toBe(1);
      expect(col.get(1)).toBeNull();
      expect(col.get(2)).toBe(3);
    });

    it('should throw on out-of-bounds access', () => {
      const col = Float64Column.from([1, 2]);
      expect(() => col.get(-1)).toThrow();
      expect(() => col.get(2)).toThrow();
    });
  });

  describe('from factory', () => {
    it('should create column from array with nulls', () => {
      const col = Float64Column.from([1, null, 3, null, 5]);
      expect(col.length).toBe(5);
      expect(col.nullCount).toBe(2);
      expect(col.get(0)).toBe(1);
      expect(col.get(1)).toBeNull();
      expect(col.get(2)).toBe(3);
      expect(col.get(3)).toBeNull();
      expect(col.get(4)).toBe(5);
    });

    it('should create column from empty array', () => {
      const col = Float64Column.from([]);
      expect(col.length).toBe(0);
      expect(col.nullCount).toBe(0);
    });
  });

  describe('aggregations', () => {
    it('sum should skip nulls', () => {
      const col = Float64Column.from([1, null, 3, null, 5]);
      expect(col.sum()).toBe(9);
    });

    it('mean should skip nulls', () => {
      const col = Float64Column.from([2, null, 4]);
      expect(col.mean()).toBe(3);
    });

    it('mean returns null for all-null column', () => {
      const col = Float64Column.from([null, null]);
      expect(col.mean()).toBeNull();
    });

    it('min should skip nulls', () => {
      const col = Float64Column.from([3, null, 1, null, 5]);
      expect(col.min()).toBe(1);
    });

    it('max should skip nulls', () => {
      const col = Float64Column.from([3, null, 1, null, 5]);
      expect(col.max()).toBe(5);
    });

    it('min/max returns null for all-null column', () => {
      const col = Float64Column.from([null, null]);
      expect(col.min()).toBeNull();
      expect(col.max()).toBeNull();
    });
  });

  describe('slice', () => {
    it('should return a sliced column', () => {
      const col = Float64Column.from([1, 2, 3, 4, 5]);
      const sliced = col.slice(1, 4);
      expect(sliced.length).toBe(3);
      expect(sliced.get(0)).toBe(2);
      expect(sliced.get(1)).toBe(3);
      expect(sliced.get(2)).toBe(4);
    });

    it('should preserve null mask in slice', () => {
      const col = Float64Column.from([1, null, 3, null, 5]);
      const sliced = col.slice(0, 3);
      expect(sliced.get(0)).toBe(1);
      expect(sliced.get(1)).toBeNull();
      expect(sliced.get(2)).toBe(3);
    });
  });

  describe('clone', () => {
    it('should create an independent copy', () => {
      const col = Float64Column.from([1, null, 3]);
      const cloned = col.clone();
      expect(cloned.length).toBe(3);
      expect(cloned.get(0)).toBe(1);
      expect(cloned.get(1)).toBeNull();
      expect(cloned.get(2)).toBe(3);
    });
  });

  describe('take', () => {
    it('should return rows at given indices', () => {
      const col = Float64Column.from([10, 20, 30, 40, 50]);
      const taken = col.take(new Int32Array([0, 2, 4]));
      expect(taken.length).toBe(3);
      expect(taken.get(0)).toBe(10);
      expect(taken.get(1)).toBe(30);
      expect(taken.get(2)).toBe(50);
    });

    it('should preserve nulls in take', () => {
      const col = Float64Column.from([1, null, 3]);
      const taken = col.take(new Int32Array([1, 2]));
      expect(taken.get(0)).toBeNull();
      expect(taken.get(1)).toBe(3);
    });
  });

  describe('Column abstract class', () => {
    it('Float64Column is instance of Column', () => {
      const col = Float64Column.from([1, 2, 3]);
      expect(col).toBeInstanceOf(Column);
    });
  });
});
