/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call */
import { describe, it, expect, beforeAll } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

let arrowAvailable = false;

beforeAll(async () => {
  try {
    await import('apache-arrow');
    arrowAvailable = true;
  } catch {
    arrowAvailable = false;
  }
});

describe('Arrow IPC serialization (US-073, US-078)', () => {
  describe('toArrowIPC', () => {
    it('should return a Uint8Array buffer', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromRows([
        { x: 1, y: 10 },
        { x: 2, y: 20 },
      ]);

      const buffer = await df.toArrowIPC();
      expect(buffer).toBeInstanceOf(Uint8Array);
      expect(buffer.byteLength).toBeGreaterThan(0);
    });

    it('should serialize string columns', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromRows([{ name: 'alice' }, { name: 'bob' }]);

      const buffer = await df.toArrowIPC();
      expect(buffer).toBeInstanceOf(Uint8Array);
      expect(buffer.byteLength).toBeGreaterThan(0);
    });

    it('should handle null values', async () => {
      if (!arrowAvailable) return;

      const df = DataFrame.fromColumns({ val: [1, null, 3] });

      const buffer = await df.toArrowIPC();
      expect(buffer).toBeInstanceOf(Uint8Array);
    });
  });

  describe('fromArrowIPC', () => {
    it('should create a DataFrame from an IPC buffer', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { x: 1, y: 10 },
        { x: 2, y: 20 },
        { x: 3, y: 30 },
      ]);

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.shape).toEqual([3, 2]);
      expect(restored.columns).toEqual(['x', 'y']);
    });
  });

  describe('round-trip', () => {
    it('should round-trip numeric data through Arrow IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { a: 1.5, b: 10 },
        { a: 2.5, b: 20 },
        { a: 3.5, b: 30 },
      ]);

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
      expect(restored.col('a').toArray()).toEqual([1.5, 2.5, 3.5]);
      expect(restored.col('b').toArray()).toEqual([10, 20, 30]);
    });

    it('should round-trip string data through Arrow IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { name: 'alice', age: 30 },
        { name: 'bob', age: 25 },
      ]);

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.col('name').toArray()).toEqual(['alice', 'bob']);
      expect(restored.col('age').toArray()).toEqual([30, 25]);
    });

    it('should round-trip data with nulls through Arrow IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromColumns({ val: [1, null, 3] });

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.col('val').toArray()).toEqual([1, null, 3]);
    });

    it('fromArrowIPC(df.toArrowIPC()) produces equivalent DataFrame', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { id: 1, name: 'alice', active: true },
        { id: 2, name: 'bob', active: false },
        { id: 3, name: 'charlie', active: true },
      ]);

      const restored = await DataFrame.fromArrowIPC(await original.toArrowIPC());

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
      for (const colName of original.columns) {
        expect(restored.col(colName).toArray()).toEqual(original.col(colName).toArray());
      }
    });

    it('should round-trip boolean data through Arrow IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromColumns({ flag: [true, false, true] });

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.col('flag').toArray()).toEqual([true, false, true]);
    });

    it('should round-trip all data types together through IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromRows([
        { id: 1, name: 'alice', active: true, score: 9.5 },
        { id: 2, name: 'bob', active: false, score: 7.2 },
      ]);

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.shape).toEqual(original.shape);
      expect(restored.columns).toEqual(original.columns);
      for (const colName of original.columns) {
        expect(restored.col(colName).toArray()).toEqual(original.col(colName).toArray());
      }
    });

    it('should round-trip multiple columns with nulls through IPC', async () => {
      if (!arrowAvailable) return;

      const original = DataFrame.fromColumns({
        a: [1, null, 3],
        b: ['x', 'y', null],
      });

      const buffer = await original.toArrowIPC();
      const restored = await DataFrame.fromArrowIPC(buffer);

      expect(restored.col('a').toArray()).toEqual([1, null, 3]);
      expect(restored.col('b').toArray()).toEqual(['x', 'y', null]);
    });
  });
});
