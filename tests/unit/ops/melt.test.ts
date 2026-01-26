import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

describe('melt', () => {
  it('should melt wide to long format with explicit valueVars', () => {
    const df = DataFrame.fromRows([
      { region: 'East', Q1: 100, Q2: 200 },
      { region: 'West', Q1: 300, Q2: 400 },
    ]);

    const result = df.melt({
      idVars: ['region'],
      valueVars: ['Q1', 'Q2'],
      varName: 'quarter',
      valueName: 'revenue',
    });

    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['region', 'quarter', 'revenue']);

    const rows = result.toArray();
    expect(rows[0]).toEqual({ region: 'East', quarter: 'Q1', revenue: 100 });
    expect(rows[1]).toEqual({ region: 'East', quarter: 'Q2', revenue: 200 });
    expect(rows[2]).toEqual({ region: 'West', quarter: 'Q1', revenue: 300 });
    expect(rows[3]).toEqual({ region: 'West', quarter: 'Q2', revenue: 400 });
  });

  it('should use default varName and valueName', () => {
    const df = DataFrame.fromRows([
      { id: 1, a: 10, b: 20 },
    ]);

    const result = df.melt({ idVars: ['id'] });

    expect(result.columns).toEqual(['id', 'variable', 'value']);
    expect(result.length).toBe(2);

    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: 1, variable: 'a', value: 10 });
    expect(rows[1]).toEqual({ id: 1, variable: 'b', value: 20 });
  });

  it('should melt all non-idVars when valueVars not specified', () => {
    const df = DataFrame.fromRows([
      { name: 'Alice', math: 90, science: 85, english: 92 },
      { name: 'Bob', math: 78, science: 88, english: 76 },
    ]);

    const result = df.melt({
      idVars: ['name'],
      varName: 'subject',
      valueName: 'score',
    });

    expect(result.length).toBe(6);
    expect(result.columns).toEqual(['name', 'subject', 'score']);

    const rows = result.toArray();
    expect(rows[0]).toEqual({ name: 'Alice', subject: 'math', score: 90 });
    expect(rows[1]).toEqual({ name: 'Alice', subject: 'science', score: 85 });
    expect(rows[2]).toEqual({ name: 'Alice', subject: 'english', score: 92 });
    expect(rows[3]).toEqual({ name: 'Bob', subject: 'math', score: 78 });
    expect(rows[4]).toEqual({ name: 'Bob', subject: 'science', score: 88 });
    expect(rows[5]).toEqual({ name: 'Bob', subject: 'english', score: 76 });
  });

  it('should handle multiple idVars', () => {
    const df = DataFrame.fromRows([
      { region: 'East', year: 2023, Q1: 100, Q2: 200 },
      { region: 'West', year: 2024, Q1: 300, Q2: 400 },
    ]);

    const result = df.melt({
      idVars: ['region', 'year'],
      valueVars: ['Q1', 'Q2'],
    });

    expect(result.length).toBe(4);
    expect(result.columns).toEqual(['region', 'year', 'variable', 'value']);

    const rows = result.toArray();
    expect(rows[0]).toEqual({ region: 'East', year: 2023, variable: 'Q1', value: 100 });
    expect(rows[1]).toEqual({ region: 'East', year: 2023, variable: 'Q2', value: 200 });
  });

  it('should handle null values in value columns', () => {
    const df = DataFrame.fromRows([
      { id: 1, a: 10, b: null },
      { id: 2, a: null, b: 30 },
    ]);

    const result = df.melt({ idVars: ['id'], valueVars: ['a', 'b'] });

    expect(result.length).toBe(4);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: 1, variable: 'a', value: 10 });
    expect(rows[1]).toEqual({ id: 1, variable: 'b', value: null });
    expect(rows[2]).toEqual({ id: 2, variable: 'a', value: null });
    expect(rows[3]).toEqual({ id: 2, variable: 'b', value: 30 });
  });

  it('should handle string idVars (not array)', () => {
    const df = DataFrame.fromRows([
      { id: 1, x: 10, y: 20 },
    ]);

    const result = df.melt({ idVars: 'id' });

    expect(result.length).toBe(2);
    expect(result.columns).toEqual(['id', 'variable', 'value']);
  });

  it('should handle string valueVars (not array)', () => {
    const df = DataFrame.fromRows([
      { id: 1, x: 10, y: 20 },
    ]);

    const result = df.melt({ idVars: 'id', valueVars: 'x' });

    expect(result.length).toBe(1);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: 1, variable: 'x', value: 10 });
  });

  it('should throw ColumnNotFoundError for invalid idVars', () => {
    const df = DataFrame.fromRows([{ a: 1, b: 2 }]);

    expect(() => df.melt({ idVars: ['nonexistent'] })).toThrow('nonexistent');
  });

  it('should throw ColumnNotFoundError for invalid valueVars', () => {
    const df = DataFrame.fromRows([{ a: 1, b: 2 }]);

    expect(() => df.melt({ idVars: ['a'], valueVars: ['nonexistent'] })).toThrow('nonexistent');
  });

  it('should handle string value columns', () => {
    const df = DataFrame.fromRows([
      { id: 1, first: 'Alice', last: 'Smith' },
      { id: 2, first: 'Bob', last: 'Jones' },
    ]);

    const result = df.melt({
      idVars: ['id'],
      valueVars: ['first', 'last'],
      varName: 'field',
      valueName: 'name',
    });

    expect(result.length).toBe(4);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: 1, field: 'first', name: 'Alice' });
    expect(rows[1]).toEqual({ id: 1, field: 'last', name: 'Smith' });
    expect(rows[2]).toEqual({ id: 2, field: 'first', name: 'Bob' });
    expect(rows[3]).toEqual({ id: 2, field: 'last', name: 'Jones' });
  });

  it('should handle single row', () => {
    const df = DataFrame.fromRows([
      { id: 1, a: 10, b: 20, c: 30 },
    ]);

    const result = df.melt({ idVars: ['id'] });

    expect(result.length).toBe(3);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: 1, variable: 'a', value: 10 });
    expect(rows[1]).toEqual({ id: 1, variable: 'b', value: 20 });
    expect(rows[2]).toEqual({ id: 1, variable: 'c', value: 30 });
  });

  it('should preserve id column values with nulls', () => {
    const df = DataFrame.fromRows([
      { id: null, a: 10, b: 20 },
      { id: 2, a: 30, b: 40 },
    ]);

    const result = df.melt({ idVars: ['id'], valueVars: ['a', 'b'] });

    expect(result.length).toBe(4);
    const rows = result.toArray();
    expect(rows[0]).toEqual({ id: null, variable: 'a', value: 10 });
    expect(rows[1]).toEqual({ id: null, variable: 'b', value: 20 });
  });

  it('should be inverse of pivot (roundtrip)', () => {
    const df = DataFrame.fromRows([
      { region: 'East', Q1: 100, Q2: 200 },
      { region: 'West', Q1: 300, Q2: 400 },
    ]);

    const melted = df.melt({
      idVars: ['region'],
      valueVars: ['Q1', 'Q2'],
      varName: 'quarter',
      valueName: 'revenue',
    });

    const pivoted = melted.pivot({
      index: 'region',
      columns: 'quarter',
      values: 'revenue',
    });

    expect(pivoted.length).toBe(2);
    expect(pivoted.columns).toEqual(['region', 'Q1', 'Q2']);

    const rows = pivoted.toArray();
    expect(rows[0]).toEqual({ region: 'East', Q1: 100, Q2: 200 });
    expect(rows[1]).toEqual({ region: 'West', Q1: 300, Q2: 400 });
  });
});
