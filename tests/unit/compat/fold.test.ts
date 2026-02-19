import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/index';
import { fold } from '../../../src/compat/verbs';

describe('compat fold', () => {
  it('unpivots specified columns into key/value pairs', () => {
    const df = DataFrame.fromColumns({
      id: ['a', 'b'],
      x: [1, 2],
      y: [3, 4],
    });
    const result = fold(df, ['x', 'y']);

    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['id', 'key', 'value']);
    expect(result.col('id').toArray()).toEqual(['a', 'a', 'b', 'b']);
    expect(result.col('key').toArray()).toEqual(['x', 'y', 'x', 'y']);
    expect(result.col('value').toArray()).toEqual([1, 3, 2, 4]);
  });

  it('supports custom key/value column names via as option', () => {
    const df = DataFrame.fromColumns({
      name: ['alice'],
      score1: [80],
      score2: [90],
    });
    const result = fold(df, ['score1', 'score2'], { as: ['metric', 'measurement'] });

    expect(result.columns).toEqual(['name', 'metric', 'measurement']);
    expect(result.col('metric').toArray()).toEqual(['score1', 'score2']);
    expect(result.col('measurement').toArray()).toEqual([80, 90]);
  });

  it('handles null values in folded columns', () => {
    const df = DataFrame.fromColumns({
      id: ['a', 'b'],
      x: [1, null],
      y: [null, 4],
    });
    const result = fold(df, ['x', 'y']);

    expect(result.length).toBe(4);
    expect(result.col('value').toArray()).toEqual([1, null, null, 4]);
  });

  it('folds a single column', () => {
    const df = DataFrame.fromColumns({
      id: ['a', 'b', 'c'],
      val: [10, 20, 30],
    });
    const result = fold(df, ['val']);

    expect(result.length).toBe(3);
    expect(result.col('key').toArray()).toEqual(['val', 'val', 'val']);
    expect(result.col('value').toArray()).toEqual([10, 20, 30]);
  });

  it('preserves non-folded columns as id vars', () => {
    const df = DataFrame.fromColumns({
      region: ['east', 'west'],
      type: ['a', 'b'],
      x: [1, 2],
      y: [3, 4],
    });
    const result = fold(df, ['x', 'y']);

    expect(result.columns).toEqual(['region', 'type', 'key', 'value']);
    expect(result.col('region').toArray()).toEqual(['east', 'east', 'west', 'west']);
    expect(result.col('type').toArray()).toEqual(['a', 'a', 'b', 'b']);
  });

  it('works with empty DataFrame', () => {
    const df = DataFrame.fromColumns({
      id: [] as string[],
      x: [] as number[],
      y: [] as number[],
    });
    const result = fold(df, ['x', 'y']);

    expect(result.length).toBe(0);
    expect(result.columns).toEqual(['id', 'key', 'value']);
  });

  it('folds all columns when no id vars remain', () => {
    const df = DataFrame.fromColumns({
      a: [1, 2],
      b: [3, 4],
    });
    const result = fold(df, ['a', 'b']);

    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['key', 'value']);
    expect(result.col('key').toArray()).toEqual(['a', 'b', 'a', 'b']);
    expect(result.col('value').toArray()).toEqual([1, 3, 2, 4]);
  });
});
