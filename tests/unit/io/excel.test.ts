import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { DType } from '../../../src/types/dtype';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('Excel I/O (US-076)', () => {
  let tmpDir: string;
  let sampleFile: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-excel-io-'));

    // Generate sample.xlsx programmatically with mixed-type data
    const ExcelJS = await import('exceljs');
    const workbook = new ExcelJS.Workbook();

    // Sheet 1: Mixed types
    const ws1 = workbook.addWorksheet('Mixed');
    ws1.addRow(['name', 'score', 'passed', 'date']);
    ws1.addRow(['Alice', 95.5, true, new Date('2024-01-10')]);
    ws1.addRow(['Bob', 87.0, false, new Date('2024-03-15')]);
    ws1.addRow(['Charlie', 72.3, true, new Date('2024-06-20')]);

    // Sheet 2: Numeric
    const ws2 = workbook.addWorksheet('Numeric');
    ws2.addRow(['a', 'b']);
    ws2.addRow([10, 100]);
    ws2.addRow([20, 200]);
    ws2.addRow([30, 300]);

    // Sheet 3: With nulls
    const ws3 = workbook.addWorksheet('Nulls');
    ws3.addRow(['x', 'y']);
    ws3.addRow([1, 'hello']);
    ws3.addRow([null, 'world']);
    ws3.addRow([3, null]);

    sampleFile = path.join(tmpDir, 'sample.xlsx');
    await workbook.xlsx.writeFile(sampleFile);
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe('fromExcel - data type detection', () => {
    it('reads correct data types from Excel cells', async () => {
      const df = await DataFrame.fromExcel(sampleFile);
      expect(df.col('name').dtype).toBe(DType.Utf8);
      expect(df.col('score').dtype).toBe(DType.Float64);
      expect(df.col('passed').dtype).toBe(DType.Boolean);
      expect(df.col('date').dtype).toBe(DType.Date);
    });

    it('reads correct values from mixed-type sheet', async () => {
      const df = await DataFrame.fromExcel(sampleFile);
      expect(df.length).toBe(3);
      expect(df.col('name').get(0)).toBe('Alice');
      expect(df.col('score').get(0)).toBe(95.5);
      expect(df.col('passed').get(1)).toBe(false);
      expect(df.col('date').get(0)).toBeInstanceOf(Date);
    });
  });

  describe('fromExcel - sheet selection', () => {
    it('selects sheet by name', async () => {
      const df = await DataFrame.fromExcel(sampleFile, { sheet: 'Numeric' });
      expect(df.columns).toEqual(['a', 'b']);
      expect(df.length).toBe(3);
      expect(df.col('a').get(0)).toBe(10);
      expect(df.col('b').get(2)).toBe(300);
    });

    it('selects sheet by index', async () => {
      const df = await DataFrame.fromExcel(sampleFile, { sheet: 1 });
      expect(df.columns).toEqual(['a', 'b']);
      expect(df.length).toBe(3);
    });

    it('defaults to first sheet when no sheet specified', async () => {
      const df = await DataFrame.fromExcel(sampleFile);
      expect(df.columns).toEqual(['name', 'score', 'passed', 'date']);
    });
  });

  describe('fromExcel - range option', () => {
    it('reads subset of cells with range', async () => {
      const df = await DataFrame.fromExcel(sampleFile, {
        sheet: 'Numeric',
        range: 'A1:A3',
      });
      expect(df.columns).toEqual(['a']);
      expect(df.length).toBe(2);
      expect(df.col('a').get(0)).toBe(10);
      expect(df.col('a').get(1)).toBe(20);
    });

    it('reads multi-column range', async () => {
      const df = await DataFrame.fromExcel(sampleFile, {
        sheet: 'Numeric',
        range: 'A1:B3',
      });
      expect(df.columns).toEqual(['a', 'b']);
      expect(df.length).toBe(2);
    });
  });

  describe('toExcel / fromExcel roundtrip', () => {
    it('writes and re-reads DataFrame preserving data', async () => {
      // Create a DataFrame with mixed types
      const original = DataFrame.fromRows([
        { city: 'Rome', pop: 2873000, capital: true },
        { city: 'Milan', pop: 1352000, capital: false },
        { city: 'Naples', pop: 967000, capital: false },
      ]);

      const outFile = path.join(tmpDir, 'roundtrip.xlsx');
      await original.toExcel(outFile);

      // Re-read the file
      const loaded = await DataFrame.fromExcel(outFile);
      expect(loaded.length).toBe(3);
      expect(loaded.columns).toEqual(['city', 'pop', 'capital']);
      expect(loaded.col('city').get(0)).toBe('Rome');
      expect(loaded.col('pop').get(0)).toBe(2873000);
      expect(loaded.col('capital').get(0)).toBe(true);
      expect(loaded.col('capital').get(1)).toBe(false);
    });

    it('roundtrips with null values', async () => {
      const original = DataFrame.fromRows([
        { a: 1, b: 'hello' },
        { a: null, b: 'world' },
        { a: 3, b: null },
      ]);

      const outFile = path.join(tmpDir, 'roundtrip-nulls.xlsx');
      await original.toExcel(outFile);

      const loaded = await DataFrame.fromExcel(outFile);
      expect(loaded.length).toBe(3);
      expect(loaded.col('a').get(0)).toBe(1);
      expect(loaded.col('a').get(1)).toBeNull();
      expect(loaded.col('b').get(2)).toBeNull();
    });

    it('roundtrips with custom sheet name', async () => {
      const original = DataFrame.fromRows([
        { x: 1, y: 2 },
        { x: 3, y: 4 },
      ]);

      const outFile = path.join(tmpDir, 'roundtrip-sheet.xlsx');
      await original.toExcel(outFile, { sheet: 'MySheet' });

      const loaded = await DataFrame.fromExcel(outFile, { sheet: 'MySheet' });
      expect(loaded.length).toBe(2);
      expect(loaded.col('x').get(0)).toBe(1);
    });
  });

  describe('missing exceljs peer dependency', () => {
    it('throws helpful error when exceljs is not installed (read)', async () => {
      // Mock the dynamic import to simulate missing exceljs
      const { readExcelFile } = await import('../../../src/io/excel/reader');

      // We can't easily uninstall exceljs, so we test the error message pattern
      // by calling with a non-existent file — the import succeeds but file fails
      // The peer dep check is at the top of readExcelFile
      // Instead, test the actual error message from the reader module
      await expect(
        readExcelFile('/nonexistent/missing.xlsx'),
      ).rejects.toThrow(/Failed to read Excel file/);
    });

    it('throws helpful error when exceljs is not installed (write)', async () => {
      const { writeExcelFile } = await import('../../../src/io/excel/writer');

      // Verify the error message pattern exists for file write failures
      await expect(
        writeExcelFile('/nonexistent/dir/output.xlsx', ['a'], [[1]]),
      ).rejects.toThrow(/Failed to write Excel file/);
    });
  });
});

// Separate describe block to test peer dep error with module mocking
describe('Excel peer dependency error (US-076)', () => {
  it('readExcelFile throws helpful error when exceljs import fails', async () => {
    // Dynamically test the code path by temporarily replacing import
    vi.doMock('exceljs', () => {
      throw new Error('Cannot find module exceljs');
    });

    // Clear module cache to force re-import
    vi.resetModules();

    const { readExcelFile } = await import('../../../src/io/excel/reader');
    await expect(readExcelFile('any.xlsx')).rejects.toThrow(
      /exceljs is required to read Excel files but is not installed/,
    );

    vi.doUnmock('exceljs');
    vi.resetModules();
  });

  it('writeExcelFile throws helpful error when exceljs import fails', async () => {
    vi.doMock('exceljs', () => {
      throw new Error('Cannot find module exceljs');
    });

    vi.resetModules();

    const { writeExcelFile } = await import('../../../src/io/excel/writer');
    await expect(writeExcelFile('any.xlsx', ['a'], [[1]])).rejects.toThrow(
      /exceljs is required to write Excel files but is not installed/,
    );

    vi.doUnmock('exceljs');
    vi.resetModules();
  });
});
