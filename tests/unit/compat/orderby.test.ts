import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';
import { orderby } from '../../../src/compat/verbs';
import { desc } from '../../../src/compat/helpers';

describe('compat orderby', () => {
  it('sorts ascending by default', () => {
    const df = DataFrame.fromColumns({
      name: ['charlie', 'alice', 'bob'],
      age: [30, 25, 28],
    });
    const result = orderby(df, ['name']);

    expect(result.col('name').toArray()).toEqual(['alice', 'bob', 'charlie']);
    expect(result.col('age').toArray()).toEqual([25, 28, 30]);
  });

  it('sorts descending with desc()', () => {
    const df = DataFrame.fromColumns({
      name: ['alice', 'bob', 'charlie'],
      score: [80, 95, 70],
    });
    const result = orderby(df, [desc('score')]);

    expect(result.col('score').toArray()).toEqual([95, 80, 70]);
    expect(result.col('name').toArray()).toEqual(['bob', 'alice', 'charlie']);
  });

  it('supports multiple columns for tie-breaking', () => {
    const df = DataFrame.fromColumns({
      group: ['a', 'a', 'b', 'b'],
      value: [2, 1, 4, 3],
    });
    const result = orderby(df, ['group', 'value']);

    expect(result.col('group').toArray()).toEqual(['a', 'a', 'b', 'b']);
    expect(result.col('value').toArray()).toEqual([1, 2, 3, 4]);
  });

  it('supports mixed asc and desc across columns', () => {
    const df = DataFrame.fromColumns({
      group: ['a', 'a', 'b', 'b'],
      value: [2, 1, 4, 3],
    });
    const result = orderby(df, [desc('group'), 'value']);

    expect(result.col('group').toArray()).toEqual(['b', 'b', 'a', 'a']);
    expect(result.col('value').toArray()).toEqual([3, 4, 1, 2]);
  });

  it('handles null values (nulls sort last)', () => {
    const df = DataFrame.fromColumns({
      x: [3, null, 1, null, 2],
    });
    const result = orderby(df, ['x']);

    expect(result.col('x').toArray()).toEqual([1, 2, 3, null, null]);
  });

  it('handles null values with descending sort', () => {
    const df = DataFrame.fromColumns({
      x: [3, null, 1, null, 2],
    });
    const result = orderby(df, [desc('x')]);

    expect(result.col('x').toArray()).toEqual([3, 2, 1, null, null]);
  });

  it('sorts numeric values correctly', () => {
    const df = DataFrame.fromColumns({
      val: [100, 20, 3, 40, 5],
    });
    const result = orderby(df, ['val']);

    expect(result.col('val').toArray()).toEqual([3, 5, 20, 40, 100]);
  });

  it('preserves all columns in output', () => {
    const df = DataFrame.fromColumns({
      a: [2, 1, 3],
      b: ['x', 'y', 'z'],
      c: [true, false, true],
    });
    const result = orderby(df, ['a']);

    expect(result.columns).toEqual(['a', 'b', 'c']);
    expect(result.col('a').toArray()).toEqual([1, 2, 3]);
    expect(result.col('b').toArray()).toEqual(['y', 'x', 'z']);
  });

  it('works with single-row DataFrame', () => {
    const df = DataFrame.fromColumns({ x: [42] });
    const result = orderby(df, ['x']);

    expect(result.length).toBe(1);
    expect(result.col('x').toArray()).toEqual([42]);
  });
});
