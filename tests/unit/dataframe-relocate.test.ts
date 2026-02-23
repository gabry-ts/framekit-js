import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src/dataframe';
import { ColumnNotFoundError, FrameKitError } from '../../src/errors';

describe('DataFrame.relocate', () => {
  const df = DataFrame.fromColumns({
    a: [1, 2, 3],
    b: [4, 5, 6],
    c: [7, 8, 9],
    d: [10, 11, 12],
  });

  it('moves columns before an anchor', () => {
    const result = df.relocate(['c', 'd'], { before: 'a' });
    expect(result.columns).toEqual(['c', 'd', 'a', 'b']);
    expect(result.col('c').toArray()).toEqual([7, 8, 9]);
  });

  it('moves columns after an anchor', () => {
    const result = df.relocate(['a'], { after: 'c' });
    expect(result.columns).toEqual(['b', 'c', 'a', 'd']);
    expect(result.col('a').toArray()).toEqual([1, 2, 3]);
  });

  it('moves a single column before another', () => {
    const result = df.relocate(['d'], { before: 'b' });
    expect(result.columns).toEqual(['a', 'd', 'b', 'c']);
  });

  it('moves a single column after the last column (no-op anchor)', () => {
    const result = df.relocate(['a'], { after: 'd' });
    expect(result.columns).toEqual(['b', 'c', 'd', 'a']);
  });

  it('moves column to same position (no-op)', () => {
    const result = df.relocate(['a'], { before: 'b' });
    expect(result.columns).toEqual(['a', 'b', 'c', 'd']);
  });

  it('throws when both before and after are specified', () => {
    expect(() => df.relocate(['a'], { before: 'b', after: 'c' })).toThrow(FrameKitError);
    expect(() => df.relocate(['a'], { before: 'b', after: 'c' })).toThrow('Cannot specify both');
  });

  it('throws when neither before nor after is specified', () => {
    expect(() => df.relocate(['a'], {})).toThrow(FrameKitError);
    expect(() => df.relocate(['a'], {})).toThrow('Must specify either');
  });

  it('throws when source column does not exist', () => {
    expect(() => df.relocate(['x'], { before: 'a' })).toThrow(ColumnNotFoundError);
  });

  it('throws when anchor column does not exist', () => {
    expect(() => df.relocate(['a'], { before: 'x' })).toThrow(ColumnNotFoundError);
  });

  it('preserves data after relocate', () => {
    const result = df.relocate(['c', 'a'], { after: 'd' });
    expect(result.columns).toEqual(['b', 'd', 'c', 'a']);
    expect(result.col('a').toArray()).toEqual([1, 2, 3]);
    expect(result.col('b').toArray()).toEqual([4, 5, 6]);
    expect(result.col('c').toArray()).toEqual([7, 8, 9]);
    expect(result.col('d').toArray()).toEqual([10, 11, 12]);
  });

  it('handles moving the anchor column relative to itself', () => {
    // Moving 'b' before 'b' — 'b' is removed from remaining, anchor shifts
    const result = df.relocate(['b'], { before: 'c' });
    expect(result.columns).toEqual(['a', 'b', 'c', 'd']);
  });
});
