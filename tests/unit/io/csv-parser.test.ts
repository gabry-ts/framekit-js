import { describe, it, expect } from 'vitest';
import { parseCSV } from '../../../src/io/csv/parser';
import { DType } from '../../../src/types/dtype';

describe('parseCSV', () => {
  it('parses basic CSV with header', () => {
    const csv = 'name,age,active\nAlice,30,true\nBob,25,false';
    const result = parseCSV(csv);
    expect(result.header).toEqual(['name', 'age', 'active']);
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
    expect(result.columns['age']).toEqual(['30', '25']);
    expect(result.columns['active']).toEqual(['true', 'false']);
  });

  it('infers numeric types (Int32 vs Float64)', () => {
    const csv = 'int_col,float_col\n1,1.5\n2,2.7\n3,3.0';
    const result = parseCSV(csv);
    expect(result.inferredTypes['int_col']).toBe(DType.Int32);
    expect(result.inferredTypes['float_col']).toBe(DType.Float64);
  });

  it('infers boolean type', () => {
    const csv = 'flag\ntrue\nfalse\nTrue\nFalse';
    const result = parseCSV(csv);
    expect(result.inferredTypes['flag']).toBe(DType.Boolean);
  });

  it('infers date type from ISO strings', () => {
    const csv = 'date\n2024-01-01\n2024-06-15\n2024-12-31';
    const result = parseCSV(csv);
    expect(result.inferredTypes['date']).toBe(DType.Date);
  });

  it('handles quoted fields with commas inside', () => {
    const csv = 'name,address\nAlice,"123 Main St, Apt 4"\nBob,"456 Oak Ave"';
    const result = parseCSV(csv);
    expect(result.columns['address']).toEqual(['123 Main St, Apt 4', '456 Oak Ave']);
  });

  it('handles escaped double quotes (RFC 4180)', () => {
    const csv = 'name,quote\nAlice,"She said ""hello"""\nBob,"a ""b"" c"';
    const result = parseCSV(csv);
    expect(result.columns['quote']).toEqual(['She said "hello"', 'a "b" c']);
  });

  it('handles multiline values in quoted fields', () => {
    const csv = 'name,note\nAlice,"line1\nline2"\nBob,simple';
    const result = parseCSV(csv);
    expect(result.columns['note']).toEqual(['line1\nline2', 'simple']);
  });

  it('auto-detects semicolon delimiter', () => {
    const csv = 'name;age;city\nAlice;30;Rome\nBob;25;Turin';
    const result = parseCSV(csv);
    expect(result.header).toEqual(['name', 'age', 'city']);
    expect(result.columns['city']).toEqual(['Rome', 'Turin']);
  });

  it('auto-detects tab delimiter', () => {
    const csv = 'name\tage\nAlice\t30\nBob\t25';
    const result = parseCSV(csv);
    expect(result.header).toEqual(['name', 'age']);
    expect(result.columns['age']).toEqual(['30', '25']);
  });

  it('auto-detects pipe delimiter', () => {
    const csv = 'name|age\nAlice|30\nBob|25';
    const result = parseCSV(csv);
    expect(result.header).toEqual(['name', 'age']);
  });

  it('respects explicit delimiter option', () => {
    const csv = 'name;age\nAlice;30';
    const result = parseCSV(csv, { delimiter: ';' });
    expect(result.header).toEqual(['name', 'age']);
  });

  it('handles nullValues option', () => {
    const csv = 'name,age\nAlice,30\nBob,NA\nCharlie,';
    const result = parseCSV(csv, { nullValues: ['NA', ''] });
    expect(result.columns['age']).toEqual(['30', null, null]);
  });

  it('handles default null values', () => {
    const csv = 'name,val\nAlice,null\nBob,NULL\nCharlie,NA\nDave,N/A\nEve,NaN';
    const result = parseCSV(csv);
    expect(result.columns['val']).toEqual([null, null, null, null, null]);
  });

  it('respects skipRows option', () => {
    const csv = 'comment line\nname,age\nAlice,30\nBob,25';
    const result = parseCSV(csv, { skipRows: 1 });
    expect(result.header).toEqual(['name', 'age']);
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('respects nRows option', () => {
    const csv = 'name,age\nAlice,30\nBob,25\nCharlie,35';
    const result = parseCSV(csv, { nRows: 2 });
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('respects columns option to select subset', () => {
    const csv = 'name,age,city\nAlice,30,Rome\nBob,25,Turin';
    const result = parseCSV(csv, { columns: ['name', 'city'] });
    expect(result.header).toEqual(['name', 'city']);
    expect(result.columns['age']).toBeUndefined();
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
    expect(result.columns['city']).toEqual(['Rome', 'Turin']);
  });

  it('supports custom header when hasHeader is false', () => {
    const csv = 'Alice,30\nBob,25';
    const result = parseCSV(csv, { hasHeader: false, header: ['name', 'age'] });
    expect(result.header).toEqual(['name', 'age']);
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('generates column_N headers when no header and none provided', () => {
    const csv = 'Alice,30\nBob,25';
    const result = parseCSV(csv, { hasHeader: false });
    expect(result.header).toEqual(['column_0', 'column_1']);
    expect(result.columns['column_0']).toEqual(['Alice', 'Bob']);
  });

  it('handles empty content', () => {
    const result = parseCSV('');
    expect(result.header).toEqual([]);
    expect(result.columns).toEqual({});
  });

  it('handles header-only CSV', () => {
    const csv = 'name,age';
    const result = parseCSV(csv);
    expect(result.header).toEqual(['name', 'age']);
    expect(result.columns['name']).toEqual([]);
  });

  it('handles comment lines', () => {
    const csv = '# this is a comment\nname,age\nAlice,30\n# another comment\nBob,25';
    const result = parseCSV(csv, { comment: '#' });
    expect(result.header).toEqual(['name', 'age']);
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('handles CRLF line endings', () => {
    const csv = 'name,age\r\nAlice,30\r\nBob,25';
    const result = parseCSV(csv);
    expect(result.columns['name']).toEqual(['Alice', 'Bob']);
  });

  it('respects dtypes override in options', () => {
    const csv = 'id,name\n1,Alice\n2,Bob';
    const result = parseCSV(csv, { dtypes: { id: DType.Utf8 } });
    expect(result.inferredTypes['id']).toBe(DType.Utf8);
  });

  it('disables number parsing when parseNumbers is false', () => {
    const csv = 'val\n123\n456';
    const result = parseCSV(csv, { parseNumbers: false });
    expect(result.inferredTypes['val']).toBe(DType.Utf8);
  });

  it('disables date parsing when parseDates is false', () => {
    const csv = 'date\n2024-01-01\n2024-06-15';
    const result = parseCSV(csv, { parseDates: false });
    expect(result.inferredTypes['date']).toBe(DType.Utf8);
  });

  it('handles rows with fewer fields than header', () => {
    const csv = 'name,age,city\nAlice,30\nBob,25,Turin';
    const result = parseCSV(csv);
    expect(result.columns['city']).toEqual([null, 'Turin']);
  });
});
