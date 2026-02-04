import { describe, it, expect } from 'vitest';
import { DataFrame, col } from '../../../src';

describe('Rolling window functions', () => {
  const df = DataFrame.fromRows([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
    { value: 5 },
    { value: 6 },
    { value: 7 },
  ]);

  function getValues(result: DataFrame, colName: string): (number | null)[] {
    const vals: (number | null)[] = [];
    for (let i = 0; i < result.length; i++) {
      vals.push(result.col(colName).get(i) as number | null);
    }
    return vals;
  }

  describe('rollingMean()', () => {
    it('should compute 3-row moving average', () => {
      const result = df.withColumn('rm', col('value').rollingMean(3));
      const vals = getValues(result, 'rm');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      expect(vals[2]).toBeCloseTo(2); // (1+2+3)/3
      expect(vals[3]).toBeCloseTo(3); // (2+3+4)/3
      expect(vals[4]).toBeCloseTo(4); // (3+4+5)/3
      expect(vals[5]).toBeCloseTo(5); // (4+5+6)/3
      expect(vals[6]).toBeCloseTo(6); // (5+6+7)/3
    });

    it('should handle window of 1', () => {
      const result = df.withColumn('rm', col('value').rollingMean(1));
      const vals = getValues(result, 'rm');
      expect(vals).toEqual([1, 2, 3, 4, 5, 6, 7]);
    });
  });

  describe('rollingSum()', () => {
    it('should compute 3-row moving sum', () => {
      const result = df.withColumn('rs', col('value').rollingSum(3));
      const vals = getValues(result, 'rs');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      expect(vals[2]).toBe(6);  // 1+2+3
      expect(vals[3]).toBe(9);  // 2+3+4
      expect(vals[4]).toBe(12); // 3+4+5
      expect(vals[5]).toBe(15); // 4+5+6
      expect(vals[6]).toBe(18); // 5+6+7
    });
  });

  describe('rollingStd()', () => {
    it('should compute 3-row moving standard deviation (sample)', () => {
      const result = df.withColumn('rstd', col('value').rollingStd(3));
      const vals = getValues(result, 'rstd');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      // std of [1,2,3] = 1.0 (sample std)
      expect(vals[2]).toBeCloseTo(1.0);
      // std of [2,3,4] = 1.0
      expect(vals[3]).toBeCloseTo(1.0);
    });
  });

  describe('rollingMin()', () => {
    it('should compute 3-row moving minimum', () => {
      const result = df.withColumn('rmin', col('value').rollingMin(3));
      const vals = getValues(result, 'rmin');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      expect(vals[2]).toBe(1); // min(1,2,3)
      expect(vals[3]).toBe(2); // min(2,3,4)
      expect(vals[4]).toBe(3); // min(3,4,5)
    });
  });

  describe('rollingMax()', () => {
    it('should compute 3-row moving maximum', () => {
      const result = df.withColumn('rmax', col('value').rollingMax(3));
      const vals = getValues(result, 'rmax');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      expect(vals[2]).toBe(3); // max(1,2,3)
      expect(vals[3]).toBe(4); // max(2,3,4)
      expect(vals[4]).toBe(5); // max(3,4,5)
    });
  });

  describe('rollingMean() with nulls', () => {
    it('should handle null values in window', () => {
      const dfWithNulls = DataFrame.fromRows([
        { value: 1 },
        { value: null },
        { value: 3 },
        { value: 4 },
      ]);
      const result = dfWithNulls.withColumn('rm', col('value').rollingMean(3));
      const vals = getValues(result, 'rm');
      expect(vals[0]).toBeNull();
      expect(vals[1]).toBeNull();
      // window [1, null, 3] → mean of non-null: (1+3)/2 = 2
      expect(vals[2]).toBeCloseTo(2);
      // window [null, 3, 4] → mean of non-null: (3+4)/2 = 3.5
      expect(vals[3]).toBeCloseTo(3.5);
    });
  });
});

describe('Exponential weighted moving average', () => {
  function getValues(result: DataFrame, colName: string): (number | null)[] {
    const vals: (number | null)[] = [];
    for (let i = 0; i < result.length; i++) {
      vals.push(result.col(colName).get(i) as number | null);
    }
    return vals;
  }

  it('should compute ewm with alpha=0.5', () => {
    const df = DataFrame.fromRows([
      { value: 1 },
      { value: 2 },
      { value: 3 },
      { value: 4 },
    ]);
    const result = df.withColumn('e', col('value').ewm(0.5));
    const vals = getValues(result, 'e');

    // ewma[0] = 1
    expect(vals[0]).toBeCloseTo(1);
    // ewma[1] = 0.5*2 + 0.5*1 = 1.5
    expect(vals[1]).toBeCloseTo(1.5);
    // ewma[2] = 0.5*3 + 0.5*1.5 = 2.25
    expect(vals[2]).toBeCloseTo(2.25);
    // ewma[3] = 0.5*4 + 0.5*2.25 = 3.125
    expect(vals[3]).toBeCloseTo(3.125);
  });

  it('should compute ewm with alpha=1.0 (no smoothing)', () => {
    const df = DataFrame.fromRows([
      { value: 10 },
      { value: 20 },
      { value: 30 },
    ]);
    const result = df.withColumn('e', col('value').ewm(1.0));
    const vals = getValues(result, 'e');
    // alpha=1 means ewma = current value always
    expect(vals).toEqual([10, 20, 30]);
  });

  it('should handle null values gracefully', () => {
    const df = DataFrame.fromRows([
      { value: 10 },
      { value: null },
      { value: 30 },
    ]);
    const result = df.withColumn('e', col('value').ewm(0.5));
    const vals = getValues(result, 'e');
    // ewma[0] = 10
    expect(vals[0]).toBeCloseTo(10);
    // ewma[1] = null skipped, carry forward = 10
    expect(vals[1]).toBeCloseTo(10);
    // ewma[2] = 0.5*30 + 0.5*10 = 20
    expect(vals[2]).toBeCloseTo(20);
  });
});
