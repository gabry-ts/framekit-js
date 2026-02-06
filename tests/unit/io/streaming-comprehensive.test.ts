import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';
import { DType } from '../../../src/types/dtype';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';

describe('Streaming CSV and NDJSON comprehensive tests (US-067)', () => {
  let tmpDir: string;

  beforeAll(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'framekit-stream-067-'));
  });

  afterAll(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  function writeTmpFile(name: string, content: string): Promise<string> {
    const filePath = path.join(tmpDir, name);
    return fs.writeFile(filePath, content, 'utf-8').then(() => filePath);
  }

  /** Generate a CSV string with N data rows: id,name,value,active */
  function generateCSV(rowCount: number): string {
    const header = 'id,name,value,active';
    const rows: string[] = [header];
    for (let i = 0; i < rowCount; i++) {
      rows.push(`${i},item_${i},${(i * 1.5).toFixed(2)},${i % 2 === 0}`);
    }
    return rows.join('\n') + '\n';
  }

  /** Generate an NDJSON string with N records */
  function generateNDJSON(rowCount: number): string {
    const lines: string[] = [];
    for (let i = 0; i < rowCount; i++) {
      lines.push(JSON.stringify({
        id: i,
        name: `item_${i}`,
        value: i * 1.5,
        active: i % 2 === 0,
      }));
    }
    return lines.join('\n') + '\n';
  }

  describe('streamCSV with 1000+ row fixture', () => {
    const ROW_COUNT = 1200;
    let csvPath: string;

    beforeAll(async () => {
      csvPath = await writeTmpFile('large.csv', generateCSV(ROW_COUNT));
    });

    it('yields correct number of chunks based on chunkSize', async () => {
      const chunkSize = 100;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(csvPath, { chunkSize })) {
        chunks.push(chunk);
      }

      // 1200 rows / 100 per chunk = 12 chunks
      expect(chunks.length).toBe(Math.ceil(ROW_COUNT / chunkSize));

      // Verify total rows across all chunks
      const totalRows = chunks.reduce((sum, c) => sum + c.shape[0], 0);
      expect(totalRows).toBe(ROW_COUNT);
    });

    it('yields correct chunk count with non-even division', async () => {
      const chunkSize = 250;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(csvPath, { chunkSize })) {
        chunks.push(chunk);
      }

      // 1200 / 250 = 4 full chunks + 1 partial (200 rows)
      expect(chunks.length).toBe(Math.ceil(ROW_COUNT / chunkSize));

      const lastChunk = chunks[chunks.length - 1]!;
      expect(lastChunk.shape[0]).toBe(ROW_COUNT % chunkSize);
    });

    it('concatenating all chunks equals fromCSV result', async () => {
      const chunkSize = 200;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(csvPath, { chunkSize })) {
        chunks.push(chunk);
      }

      // Load the same file with fromCSV (non-streaming)
      const full = await DataFrame.fromCSV(csvPath);

      // Same total shape
      const totalRows = chunks.reduce((sum, c) => sum + c.shape[0], 0);
      expect(totalRows).toBe(full.shape[0]);
      expect(chunks[0]!.columns).toEqual(full.columns);

      // Verify data matches row-by-row by collecting all streamed values
      let rowOffset = 0;
      for (const chunk of chunks) {
        for (let r = 0; r < chunk.shape[0]; r++) {
          for (const colName of chunk.columns) {
            const streamVal = chunk.col(colName).get(r);
            const fullVal = full.col(colName).get(rowOffset + r);
            expect(streamVal).toEqual(fullVal);
          }
        }
        rowOffset += chunk.shape[0];
      }
    });

    it('produces consistent schema across all chunks', async () => {
      const chunkSize = 150;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(csvPath, { chunkSize })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBeGreaterThan(1);

      const expectedColumns = chunks[0]!.columns;
      const expectedDtypes = expectedColumns.map((c) => chunks[0]!.col(c).dtype);

      for (let i = 1; i < chunks.length; i++) {
        const chunk = chunks[i]!;
        // Same column names
        expect(chunk.columns).toEqual(expectedColumns);
        // Same dtypes
        const dtypes = expectedColumns.map((c) => chunk.col(c).dtype);
        expect(dtypes).toEqual(expectedDtypes);
      }
    });

    it('correctly infers types for the large fixture', async () => {
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamCSV(csvPath, { chunkSize: 500 })) {
        chunks.push(chunk);
      }

      const first = chunks[0]!;
      expect(first.col('id').dtype).toBe(DType.Int32);
      expect(first.col('name').dtype).toBe(DType.Utf8);
      expect(first.col('value').dtype).toBe(DType.Float64);
      expect(first.col('active').dtype).toBe(DType.Boolean);
    });
  });

  describe('streamNDJSON with fixture', () => {
    const ROW_COUNT = 1000;
    let ndjsonPath: string;

    beforeAll(async () => {
      ndjsonPath = await writeTmpFile('large.ndjson', generateNDJSON(ROW_COUNT));
    });

    it('streams NDJSON file in correct number of chunks', async () => {
      const chunkSize = 200;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(ndjsonPath, { chunkSize })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBe(Math.ceil(ROW_COUNT / chunkSize));

      const totalRows = chunks.reduce((sum, c) => sum + c.shape[0], 0);
      expect(totalRows).toBe(ROW_COUNT);
    });

    it('concatenating all NDJSON chunks matches fromNDJSON result', async () => {
      const chunkSize = 300;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(ndjsonPath, { chunkSize })) {
        chunks.push(chunk);
      }

      const full = await DataFrame.fromNDJSON(ndjsonPath);

      const totalRows = chunks.reduce((sum, c) => sum + c.shape[0], 0);
      expect(totalRows).toBe(full.shape[0]);
      expect(chunks[0]!.columns.sort()).toEqual(full.columns.sort());

      // Spot-check first and last values
      const firstChunk = chunks[0]!;
      expect(firstChunk.col('id').get(0)).toEqual(full.col('id').get(0));
      expect(firstChunk.col('name').get(0)).toEqual(full.col('name').get(0));

      // Check last row of last chunk
      const lastChunk = chunks[chunks.length - 1]!;
      const lastRowInChunk = lastChunk.shape[0] - 1;
      const lastRowInFull = full.shape[0] - 1;
      expect(lastChunk.col('id').get(lastRowInChunk)).toEqual(full.col('id').get(lastRowInFull));
    });

    it('produces consistent schema across NDJSON chunks', async () => {
      const chunkSize = 150;
      const chunks: DataFrame[] = [];
      for await (const chunk of DataFrame.streamNDJSON(ndjsonPath, { chunkSize })) {
        chunks.push(chunk);
      }

      expect(chunks.length).toBeGreaterThan(1);

      const expectedColumns = chunks[0]!.columns.sort();
      for (let i = 1; i < chunks.length; i++) {
        expect(chunks[i]!.columns.sort()).toEqual(expectedColumns);
      }
    });

    it('handles NDJSON with varied chunk sizes', async () => {
      for (const chunkSize of [1, 50, 999, 1000, 5000]) {
        const chunks: DataFrame[] = [];
        for await (const chunk of DataFrame.streamNDJSON(ndjsonPath, { chunkSize })) {
          chunks.push(chunk);
        }

        const totalRows = chunks.reduce((sum, c) => sum + c.shape[0], 0);
        expect(totalRows).toBe(ROW_COUNT);
      }
    });
  });
});
