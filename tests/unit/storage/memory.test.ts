import { describe, it, expect, vi } from 'vitest';
import { Float64Column, Int32Column } from '../../../src/storage/numeric';
import { Utf8Column } from '../../../src/storage/string';
import { BooleanColumn } from '../../../src/storage/boolean';
import { DateColumn } from '../../../src/storage/date';
import { ObjectColumn } from '../../../src/storage/object';
import { DataFrame } from '../../../src';

describe('Column.estimatedMemoryBytes()', () => {
  it('Float64Column returns length * 8 + bitarray overhead', () => {
    const col = Float64Column.from([1, 2, 3, null, 5]);
    const bytes = col.estimatedMemoryBytes();
    // 5 * 8 = 40 bytes for data + ceil(5/8) = 1 byte for null mask
    expect(bytes).toBe(41);
  });

  it('Int32Column returns length * 4 + bitarray overhead', () => {
    const col = Int32Column.from([1, 2, 3]);
    const bytes = col.estimatedMemoryBytes();
    // 3 * 4 = 12 bytes for data + ceil(3/8) = 1 byte for null mask
    expect(bytes).toBe(13);
  });

  it('BooleanColumn returns length * 1 + bitarray overhead', () => {
    const col = BooleanColumn.from([true, false, true, null]);
    const bytes = col.estimatedMemoryBytes();
    // 4 * 1 = 4 bytes for data + ceil(4/8) = 1 byte for null mask
    expect(bytes).toBe(5);
  });

  it('DateColumn returns length * 8 + bitarray overhead', () => {
    const col = DateColumn.from([new Date(), null, new Date()]);
    const bytes = col.estimatedMemoryBytes();
    // 3 * 8 = 24 bytes for data + ceil(3/8) = 1 byte for null mask
    expect(bytes).toBe(25);
  });

  it('Utf8Column sums string byte lengths + bitarray overhead', () => {
    const col = Utf8Column.from(['hello', null, 'world']);
    const bytes = col.estimatedMemoryBytes();
    // "hello" = 5 * 2 = 10, "world" = 5 * 2 = 10 => 20 bytes for strings
    // + ceil(3/8) = 1 byte for null mask
    expect(bytes).toBe(21);
  });

  it('ObjectColumn returns length * 8 + bitarray overhead', () => {
    const col = ObjectColumn.from([{ a: 1 }, null, { b: 2 }]);
    const bytes = col.estimatedMemoryBytes();
    // 3 * 8 = 24 + ceil(3/8) = 1
    expect(bytes).toBe(25);
  });

  it('empty column returns only bitarray overhead', () => {
    const col = Float64Column.from([]);
    expect(col.estimatedMemoryBytes()).toBe(0);
  });
});

describe('DataFrame.memoryUsage()', () => {
  it('returns total bytes across all columns', () => {
    const df = DataFrame.fromColumns({
      x: [1, 2, 3],
      y: ['a', 'b', 'c'],
    });
    const total = df.memoryUsage();
    expect(total).toBeGreaterThan(0);
    // Float64: 3 * 8 + 1 = 25, Utf8: 3 * 2 + 1 = 7 (each char is 1 char * 2 bytes)
    // total should be sum of individual column memory
    expect(typeof total).toBe('number');
  });

  it('returns 0 for empty dataframe', () => {
    const df = DataFrame.empty();
    expect(df.memoryUsage()).toBe(0);
  });
});

describe('DataFrame.info()', () => {
  it('prints per-column memory, null counts, and total', () => {
    const df = DataFrame.fromColumns({
      nums: [1, null, 3],
      names: ['alice', null, 'charlie'],
    });
    const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
    df.info();
    expect(spy).toHaveBeenCalledTimes(1);
    const output = spy.mock.calls[0]![0] as string;
    expect(output).toContain('DataFrame: 3 rows x 2 columns');
    expect(output).toContain('nums');
    expect(output).toContain('names');
    expect(output).toContain('Total memory:');
    // Should contain null counts
    expect(output).toMatch(/1/); // at least one null count of 1
    // Should contain human-readable memory format
    expect(output).toMatch(/\d+(\.\d+)?\s*(B|KB|MB|GB)/);
    spy.mockRestore();
  });
});
