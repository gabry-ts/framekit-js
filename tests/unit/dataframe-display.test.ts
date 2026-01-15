import { describe, it, expect, vi } from 'vitest';
import { DataFrame } from '../../src';

describe('DataFrame display utilities', () => {
  const df = DataFrame.fromColumns<{ name: string; age: number; active: boolean }>({
    name: ['Alice', 'Bob', 'Charlie'],
    age: [30, 25, 35],
    active: [true, false, true],
  });

  describe('toString()', () => {
    it('returns formatted ASCII table', () => {
      const result = df.toString();
      expect(result).toContain('name');
      expect(result).toContain('age');
      expect(result).toContain('active');
      expect(result).toContain('Alice');
      expect(result).toContain('30');
      expect(result).toContain('3 rows x 3 columns');
    });

    it('uses box-drawing characters', () => {
      const result = df.toString();
      expect(result).toContain('┌');
      expect(result).toContain('┐');
      expect(result).toContain('└');
      expect(result).toContain('┘');
      expect(result).toContain('│');
      expect(result).toContain('─');
    });

    it('truncates large DataFrames with ... separator', () => {
      const bigData: Record<string, number[]> = {
        x: Array.from({ length: 20 }, (_, i) => i),
      };
      const bigDf = DataFrame.fromColumns(bigData);
      const result = bigDf.toString({ maxRows: 6 });
      expect(result).toContain('...');
      expect(result).toContain('20 rows x 1 columns');
      // Should show first 3 and last 3 rows
      expect(result).toContain(' 0');
      expect(result).toContain(' 1');
      expect(result).toContain(' 2');
      expect(result).toContain('17');
      expect(result).toContain('18');
      expect(result).toContain('19');
    });

    it('truncates columns when maxCols exceeded', () => {
      const data: Record<string, number[]> = {};
      for (let i = 0; i < 15; i++) {
        data[`col${i}`] = [i];
      }
      const wideDf = DataFrame.fromColumns(data);
      const result = wideDf.toString({ maxCols: 4 });
      expect(result).toContain('...');
      expect(result).toContain('col0');
      expect(result).toContain('col14');
    });

    it('handles empty DataFrame', () => {
      const empty = DataFrame.empty();
      const result = empty.toString();
      expect(result).toContain('Empty DataFrame');
      expect(result).toContain('0 rows x 0 columns');
    });

    it('shows row index', () => {
      const result = df.toString();
      expect(result).toContain('0');
      expect(result).toContain('1');
      expect(result).toContain('2');
    });

    it('displays null values', () => {
      const dfWithNull = DataFrame.fromColumns({
        val: [1, null, 3],
      });
      const result = dfWithNull.toString();
      expect(result).toContain('null');
    });
  });

  describe('print()', () => {
    it('outputs toString to stdout', () => {
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      df.print();
      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy.mock.calls[0]![0]).toContain('Alice');
      expect(spy.mock.calls[0]![0]).toContain('3 rows x 3 columns');
      spy.mockRestore();
    });

    it('passes options through', () => {
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      df.print({ maxRows: 2 });
      expect(spy).toHaveBeenCalledTimes(1);
      spy.mockRestore();
    });
  });

  describe('describe()', () => {
    it('returns DataFrame with stats for numeric columns', () => {
      const result = df.describe();
      const stats = result.col('stat' as never).toArray() as string[];
      expect(stats).toEqual(['count', 'mean', 'std', 'min', 'max']);
    });

    it('includes stats for numeric columns only', () => {
      const result = df.describe();
      const cols = result.columns;
      expect(cols).toContain('stat');
      expect(cols).toContain('age');
      expect(cols).not.toContain('name');
      expect(cols).not.toContain('active');
    });

    it('computes correct values', () => {
      const numDf = DataFrame.fromColumns({ x: [10, 20, 30] });
      const result = numDf.describe();
      const xCol = result.col('x' as never).toArray() as (number | null)[];
      expect(xCol[0]).toBe(3); // count
      expect(xCol[1]).toBe(20); // mean
      expect(xCol[3]).toBe(10); // min
      expect(xCol[4]).toBe(30); // max
    });

    it('handles DataFrame with no numeric columns', () => {
      const strDf = DataFrame.fromColumns({ name: ['a', 'b'] });
      const result = strDf.describe();
      expect(result.columns).toEqual(['stat']);
    });
  });

  describe('info()', () => {
    it('prints column info to stdout', () => {
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      df.info();
      expect(spy).toHaveBeenCalledTimes(1);
      const output = spy.mock.calls[0]![0] as string;
      expect(output).toContain('DataFrame: 3 rows x 3 columns');
      expect(output).toContain('name');
      expect(output).toContain('age');
      expect(output).toContain('active');
      expect(output).toContain('utf8');
      expect(output).toContain('f64');
      expect(output).toContain('bool');
      expect(output).toContain('Total memory');
      spy.mockRestore();
    });

    it('shows null counts', () => {
      const dfWithNull = DataFrame.fromColumns({
        x: [1, null, 3],
        y: ['a', 'b', null],
      });
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      dfWithNull.info();
      const output = spy.mock.calls[0]![0] as string;
      expect(output).toContain('1'); // null counts
      spy.mockRestore();
    });

    it('displays memory in human-readable format', () => {
      const spy = vi.spyOn(console, 'log').mockImplementation(() => {});
      df.info();
      const output = spy.mock.calls[0]![0] as string;
      expect(output).toMatch(/\d+ B|\d+\.\d+ KB/);
      spy.mockRestore();
    });
  });
});
