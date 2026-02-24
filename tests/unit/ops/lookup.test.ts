import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { ColumnNotFoundError } from '../../../src/errors';

describe('DataFrame.lookup', () => {
  it('enriches with all non-key columns by default', () => {
    const df = DataFrame.fromColumns({ id: [1, 2, 3], value: ['a', 'b', 'c'] });
    const other = DataFrame.fromColumns({ id: [1, 2, 3], name: ['Alice', 'Bob', 'Charlie'], age: [30, 25, 35] });
    const result = df.lookup(other, 'id');

    expect(result.columns).toEqual(['id', 'value', 'name', 'age']);
    expect(result.toArray()).toEqual([
      { id: 1, value: 'a', name: 'Alice', age: 30 },
      { id: 2, value: 'b', name: 'Bob', age: 25 },
      { id: 3, value: 'c', name: 'Charlie', age: 35 },
    ]);
  });

  it('enriches with only specified value columns', () => {
    const df = DataFrame.fromColumns({ id: [1, 2, 3] });
    const other = DataFrame.fromColumns({ id: [1, 2, 3], name: ['Alice', 'Bob', 'Charlie'], age: [30, 25, 35] });
    const result = df.lookup(other, 'id', ['name']);

    expect(result.columns).toEqual(['id', 'name']);
    expect(result.toArray()).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ]);
  });

  it('produces one output row per left row (no multiplication)', () => {
    const df = DataFrame.fromColumns({ id: [1, 2, 3] });
    const other = DataFrame.fromColumns({ id: [1, 1, 2], label: ['x', 'y', 'z'] });
    const result = df.lookup(other, 'id');

    expect(result.length).toBe(3);
    expect(result.toArray()).toEqual([
      { id: 1, label: 'x' },  // first match wins
      { id: 2, label: 'z' },
      { id: 3, label: null },  // missing key
    ]);
  });

  it('fills null for missing keys in lookup table', () => {
    const df = DataFrame.fromColumns({ id: [1, 2, 3, 4] });
    const other = DataFrame.fromColumns({ id: [1, 3], score: [100, 300] });
    const result = df.lookup(other, 'id');

    expect(result.toArray()).toEqual([
      { id: 1, score: 100 },
      { id: 2, score: null },
      { id: 3, score: 300 },
      { id: 4, score: null },
    ]);
  });

  it('first match wins for duplicate keys in lookup table', () => {
    const df = DataFrame.fromColumns({ id: [1, 2] });
    const other = DataFrame.fromColumns({ id: [1, 1, 2, 2], val: ['first', 'second', 'third', 'fourth'] });
    const result = df.lookup(other, 'id');

    expect(result.toArray()).toEqual([
      { id: 1, val: 'first' },
      { id: 2, val: 'third' },
    ]);
  });

  it('handles string keys', () => {
    const df = DataFrame.fromColumns({ key: ['a', 'b', 'c'] });
    const other = DataFrame.fromColumns({ key: ['b', 'a'], info: [20, 10] });
    const result = df.lookup(other, 'key');

    expect(result.toArray()).toEqual([
      { key: 'a', info: 10 },
      { key: 'b', info: 20 },
      { key: 'c', info: null },
    ]);
  });

  it('handles null keys in left (produce null values)', () => {
    const df = DataFrame.fromColumns({ id: [1, null, 3] });
    const other = DataFrame.fromColumns({ id: [1, 3], name: ['Alice', 'Charlie'] });
    const result = df.lookup(other, 'id');

    expect(result.toArray()).toEqual([
      { id: 1, name: 'Alice' },
      { id: null, name: null },
      { id: 3, name: 'Charlie' },
    ]);
  });

  it('handles null keys in right (ignored)', () => {
    const df = DataFrame.fromColumns({ id: [1, 2] });
    const other = DataFrame.fromColumns({ id: [null, 1, 2], name: ['Ghost', 'Alice', 'Bob'] });
    const result = df.lookup(other, 'id');

    expect(result.toArray()).toEqual([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
  });

  it('throws ColumnNotFoundError for missing key in left', () => {
    const df = DataFrame.fromColumns({ a: [1] });
    const other = DataFrame.fromColumns({ id: [1], val: [10] });
    expect(() => df.lookup(other, 'id')).toThrow(ColumnNotFoundError);
  });

  it('throws ColumnNotFoundError for missing key in right', () => {
    const df = DataFrame.fromColumns({ id: [1] });
    const other = DataFrame.fromColumns({ a: [1], val: [10] });
    expect(() => df.lookup(other, 'id')).toThrow(ColumnNotFoundError);
  });

  it('throws ColumnNotFoundError for missing value column', () => {
    const df = DataFrame.fromColumns({ id: [1] });
    const other = DataFrame.fromColumns({ id: [1], val: [10] });
    expect(() => df.lookup(other, 'id', ['nonexistent'])).toThrow(ColumnNotFoundError);
  });

  it('works with empty left DataFrame', () => {
    const df = DataFrame.fromColumns({ id: [] as number[], val: [] as string[] });
    const other = DataFrame.fromColumns({ id: [1], name: ['Alice'] });
    const result = df.lookup(other, 'id');

    expect(result.length).toBe(0);
    expect(result.columns).toEqual(['id', 'val', 'name']);
  });

  it('works with empty lookup table', () => {
    const df = DataFrame.fromColumns({ id: [1, 2] });
    const other = DataFrame.fromColumns({ id: [] as number[], name: [] as string[] });
    const result = df.lookup(other, 'id');

    expect(result.toArray()).toEqual([
      { id: 1, name: null },
      { id: 2, name: null },
    ]);
  });
});
