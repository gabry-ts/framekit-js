import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

describe('DataFrame.toSQL()', () => {
  it('generates INSERT INTO with correct column names and values', () => {
    const df = DataFrame.fromRows([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);
    const sql = df.toSQL('users');
    expect(sql).toContain('INSERT INTO "users"');
    expect(sql).toContain('"id"');
    expect(sql).toContain('"name"');
    expect(sql).toContain("'Alice'");
    expect(sql).toContain("'Bob'");
    expect(sql).toContain('1');
    expect(sql).toContain('2');
  });

  it('escapes single quotes in string values', () => {
    const df = DataFrame.fromRows([{ name: "O'Brien" }]);
    const sql = df.toSQL('people');
    expect(sql).toContain("'O''Brien'");
  });

  it('outputs NULL for null values', () => {
    const df = DataFrame.fromRows([
      { id: 1, name: 'Alice' },
      { id: 2, name: null },
    ]);
    const sql = df.toSQL('users');
    expect(sql).toContain('NULL');
  });

  it('outputs dates in ISO 8601 format', () => {
    const date = new Date('2024-01-15T10:30:00.000Z');
    const df = DataFrame.fromRows([{ created: date }]);
    const sql = df.toSQL('events');
    expect(sql).toContain("'2024-01-15T10:30:00.000Z'");
  });

  it('outputs boolean values as TRUE/FALSE', () => {
    const df = DataFrame.fromRows([
      { active: true },
      { active: false },
    ]);
    const sql = df.toSQL('flags');
    expect(sql).toContain('TRUE');
    expect(sql).toContain('FALSE');
  });

  it('batches rows with default batch size of 1000', () => {
    const rows = Array.from({ length: 2500 }, (_, i) => ({ id: i }));
    const df = DataFrame.fromRows(rows);
    const sql = df.toSQL('big_table');
    const insertCount = (sql.match(/INSERT INTO/g) ?? []).length;
    expect(insertCount).toBe(3);
  });

  it('batches rows with custom batch size', () => {
    const rows = Array.from({ length: 10 }, (_, i) => ({ id: i }));
    const df = DataFrame.fromRows(rows);
    const sql = df.toSQL('small_table', { batchSize: 3 });
    const insertCount = (sql.match(/INSERT INTO/g) ?? []).length;
    expect(insertCount).toBe(4); // 3+3+3+1
  });

  it('returns empty string for empty DataFrame', () => {
    const df = DataFrame.empty();
    const sql = df.toSQL('empty_table');
    expect(sql).toBe('');
  });

  it('handles numeric values including NaN/Infinity as NULL', () => {
    const df = DataFrame.fromRows([
      { val: 42 },
      { val: NaN },
      { val: Infinity },
    ]);
    const sql = df.toSQL('nums');
    expect(sql).toContain('42');
    // NaN and Infinity should become NULL
    expect((sql.match(/NULL/g) ?? []).length).toBe(2);
  });

  it('escapes double quotes in column names', () => {
    const df = DataFrame.fromRows([{ 'col"name': 1 }]);
    const sql = df.toSQL('t');
    expect(sql).toContain('"col""name"');
  });

  it('escapes double quotes in table name', () => {
    const df = DataFrame.fromRows([{ id: 1 }]);
    const sql = df.toSQL('my"table');
    expect(sql).toContain('"my""table"');
  });
});
