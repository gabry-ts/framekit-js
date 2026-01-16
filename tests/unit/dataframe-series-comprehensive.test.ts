import { describe, it, expect } from 'vitest';
import {
  DataFrame,
  Series,
  DType,
  ColumnNotFoundError,
  TypeMismatchError,
} from '../../src/index';
import { Float64Column, Int32Column } from '../../src/storage/numeric';
import { Utf8Column } from '../../src/storage/string';
import { BooleanColumn } from '../../src/storage/boolean';
import { DateColumn } from '../../src/storage/date';

// =============================================================================
// Series comprehensive tests
// =============================================================================

describe('Series — comprehensive', () => {
  describe('construction', () => {
    it('constructs from Float64Column', () => {
      const s = new Series<number>('val', Float64Column.from([1.1, 2.2, 3.3]));
      expect(s.name).toBe('val');
      expect(s.dtype).toBe(DType.Float64);
      expect(s.length).toBe(3);
      expect(s.nullCount).toBe(0);
    });

    it('constructs from Int32Column', () => {
      const s = new Series<number>('ints', Int32Column.from([10, 20, 30]));
      expect(s.dtype).toBe(DType.Int32);
      expect(s.toArray()).toEqual([10, 20, 30]);
    });

    it('constructs from Utf8Column', () => {
      const s = new Series<string>('names', Utf8Column.from(['a', 'b', null]));
      expect(s.dtype).toBe(DType.Utf8);
      expect(s.nullCount).toBe(1);
    });

    it('constructs from BooleanColumn', () => {
      const s = new Series<boolean>('flags', BooleanColumn.from([true, false, null]));
      expect(s.dtype).toBe(DType.Boolean);
      expect(s.toArray()).toEqual([true, false, null]);
    });

    it('constructs from DateColumn', () => {
      const d = new Date('2024-01-15');
      const s = new Series<Date>('dates', DateColumn.from([d, null]));
      expect(s.dtype).toBe(DType.Date);
      expect(s.get(0)!.getTime()).toBe(d.getTime());
      expect(s.get(1)).toBeNull();
    });

    it('exposes underlying column via getter', () => {
      const col = Float64Column.from([1, 2]);
      const s = new Series<number>('x', col);
      expect(s.column).toBe(col);
    });
  });

  describe('numeric ops', () => {
    const s = new Series<number>('x', Float64Column.from([10, 20, null, 40]));

    it('sum skips nulls', () => {
      expect(s.sum()).toBe(70);
    });

    it('mean skips nulls', () => {
      expect(s.mean()).toBeCloseTo(70 / 3);
    });

    it('min skips nulls', () => {
      expect(s.min()).toBe(10);
    });

    it('max skips nulls', () => {
      expect(s.max()).toBe(40);
    });

    it('std computes sample std dev', () => {
      const vals = new Series<number>('v', Float64Column.from([2, 4, 4, 4, 5, 5, 7, 9]));
      expect(vals.std()!).toBeCloseTo(2.138, 2);
    });

    it('std returns null for single element', () => {
      expect(new Series<number>('v', Float64Column.from([5])).std()).toBeNull();
    });

    it('std returns null for all-null', () => {
      expect(new Series<number>('v', Float64Column.from([null, null])).std()).toBeNull();
    });

    it('median odd count', () => {
      const s2 = new Series<number>('v', Float64Column.from([5, 1, 3]));
      expect(s2.median()).toBe(3);
    });

    it('median even count', () => {
      const s2 = new Series<number>('v', Float64Column.from([1, 2, 3, 4]));
      expect(s2.median()).toBe(2.5);
    });

    it('median returns null for all-null', () => {
      expect(new Series<number>('v', Float64Column.from([null])).median()).toBeNull();
    });

    it('works with Int32Column', () => {
      const i = new Series<number>('i', Int32Column.from([5, 10, 15]));
      expect(i.sum()).toBe(30);
      expect(i.mean()).toBe(10);
      expect(i.min()).toBe(5);
      expect(i.max()).toBe(15);
    });

    it('throws TypeMismatchError for non-numeric', () => {
      const str = new Series<string>('s', Utf8Column.from(['a']));
      expect(() => str.sum()).toThrow(TypeMismatchError);
      expect(() => str.mean()).toThrow(TypeMismatchError);
      expect(() => str.min()).toThrow(TypeMismatchError);
      expect(() => str.max()).toThrow(TypeMismatchError);
    });
  });

  describe('comparison ops', () => {
    const s = new Series<number>('x', Float64Column.from([1, 2, 3, null, 5]));

    it('eq with null propagation', () => {
      expect(s.eq(2).toArray()).toEqual([false, true, false, null, false]);
    });

    it('neq', () => {
      expect(s.neq(2).toArray()).toEqual([true, false, true, null, true]);
    });

    it('gt', () => {
      expect(s.gt(3).toArray()).toEqual([false, false, false, null, true]);
    });

    it('gte', () => {
      expect(s.gte(3).toArray()).toEqual([false, false, true, null, true]);
    });

    it('lt', () => {
      expect(s.lt(3).toArray()).toEqual([true, true, false, null, false]);
    });

    it('lte', () => {
      expect(s.lte(3).toArray()).toEqual([true, true, true, null, false]);
    });

    it('eq with strings', () => {
      const str = new Series<string>('s', Utf8Column.from(['a', 'b', 'a', null]));
      expect(str.eq('a').toArray()).toEqual([true, false, true, null]);
    });
  });

  describe('isNull / isNotNull', () => {
    const s = new Series<number>('x', Float64Column.from([1, null, 3, null]));

    it('isNull', () => {
      expect(s.isNull().toArray()).toEqual([false, true, false, true]);
    });

    it('isNotNull', () => {
      expect(s.isNotNull().toArray()).toEqual([true, false, true, false]);
    });
  });

  describe('fillNull', () => {
    it('replaces nulls and preserves non-nulls', () => {
      const s = new Series<number>('x', Float64Column.from([null, 2, null, 4]));
      const filled = s.fillNull(0);
      expect(filled.toArray()).toEqual([0, 2, 0, 4]);
      expect(filled.nullCount).toBe(0);
    });

    it('preserves name', () => {
      const s = new Series<string>('col', Utf8Column.from([null, 'b']));
      expect(s.fillNull('X').name).toBe('col');
    });

    it('returns new series (immutable)', () => {
      const s = new Series<number>('x', Float64Column.from([null, 1]));
      const filled = s.fillNull(0);
      expect(filled).not.toBe(s);
      expect(s.get(0)).toBeNull();
    });
  });

  describe('unique / nUnique', () => {
    it('removes duplicates preserving order', () => {
      const s = new Series<number>('x', Float64Column.from([3, 1, 2, 1, 3]));
      expect(s.unique().toArray()).toEqual([3, 1, 2]);
    });

    it('preserves null once', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, null, 2]));
      expect(s.unique().toArray()).toEqual([1, null, 2]);
    });

    it('nUnique counts including null', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 2, null, null]));
      expect(s.nUnique()).toBe(3); // 1, 2, null
    });

    it('nUnique with no nulls', () => {
      const s = new Series<string>('x', Utf8Column.from(['a', 'b', 'a']));
      expect(s.nUnique()).toBe(2);
    });
  });

  describe('apply', () => {
    it('maps values', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3]));
      const result = s.apply<number>((v) => (v !== null ? v * 10 : null));
      expect(result.toArray()).toEqual([10, 20, 30]);
    });

    it('changes dtype', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2]));
      const result = s.apply<string>((v) => (v !== null ? `num_${v}` : null));
      expect(result.dtype).toBe(DType.Utf8);
      expect(result.toArray()).toEqual(['num_1', 'num_2']);
    });

    it('propagates nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const result = s.apply<number>((v) => (v !== null ? v + 1 : null));
      expect(result.toArray()).toEqual([2, null, 4]);
    });

    it('returns to boolean', () => {
      const s = new Series<number>('x', Float64Column.from([0, 1, 2]));
      const result = s.apply<boolean>((v) => (v !== null ? v > 0 : null));
      expect(result.dtype).toBe(DType.Boolean);
      expect(result.toArray()).toEqual([false, true, true]);
    });
  });

  describe('cast', () => {
    it('Float64 → Utf8', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2.5]));
      const c = s.cast<string>(DType.Utf8);
      expect(c.dtype).toBe(DType.Utf8);
      expect(c.toArray()).toEqual(['1', '2.5']);
    });

    it('Utf8 → Float64', () => {
      const s = new Series<string>('x', Utf8Column.from(['3.14', '2']));
      const c = s.cast<number>(DType.Float64);
      expect(c.toArray()).toEqual([3.14, 2]);
    });

    it('Boolean → Float64', () => {
      const s = new Series<boolean>('x', BooleanColumn.from([true, false]));
      const c = s.cast<number>(DType.Float64);
      expect(c.toArray()).toEqual([1, 0]);
    });

    it('preserves nulls', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      const c = s.cast<string>(DType.Utf8);
      expect(c.toArray()).toEqual(['1', null, '3']);
    });

    it('Number → Date', () => {
      const epoch = new Date('2024-06-01').getTime();
      const s = new Series<number>('x', Float64Column.from([epoch]));
      const c = s.cast<Date>(DType.Date);
      expect(c.dtype).toBe(DType.Date);
      expect(c.get(0)!.getTime()).toBe(epoch);
    });
  });

  describe('isIn', () => {
    it('matches values', () => {
      const s = new Series<number>('x', Float64Column.from([1, 2, 3, 4]));
      expect(s.isIn([2, 4]).toArray()).toEqual([false, true, false, true]);
    });

    it('null returns null', () => {
      const s = new Series<number>('x', Float64Column.from([1, null, 3]));
      expect(s.isIn([1]).toArray()).toEqual([true, null, false]);
    });
  });
});

// =============================================================================
// DataFrame comprehensive tests
// =============================================================================

describe('DataFrame — comprehensive', () => {
  // ------- Construction -------
  describe('construction', () => {
    it('fromColumns basic', () => {
      const df = DataFrame.fromColumns({ a: [1, 2], b: ['x', 'y'] });
      expect(df.shape).toEqual([2, 2]);
      expect(df.columns).toEqual(['a', 'b']);
    });

    it('fromRows basic', () => {
      const df = DataFrame.fromRows([
        { x: 1, y: 'a' },
        { x: 2, y: 'b' },
      ]);
      expect(df.length).toBe(2);
      expect(df.col('x').toArray()).toEqual([1, 2]);
    });

    it('fromRows fills missing keys with null', () => {
      const df = DataFrame.fromRows([{ a: 1, b: 2 }, { a: 3 }]);
      expect(df.col('b').get(1)).toBeNull();
    });

    it('empty', () => {
      const df = DataFrame.empty();
      expect(df.shape).toEqual([0, 0]);
    });

    it('fromColumns auto-detects dtypes', () => {
      const df = DataFrame.fromColumns({
        n: [1, 2],
        s: ['a', 'b'],
        b: [true, false],
        d: [new Date('2024-01-01'), new Date('2024-02-01')],
      });
      expect(df.dtypes['n']).toBe(DType.Float64);
      expect(df.dtypes['s']).toBe(DType.Utf8);
      expect(df.dtypes['b']).toBe(DType.Boolean);
      expect(df.dtypes['d']).toBe(DType.Date);
    });

    it('all-null column defaults to Float64', () => {
      const df = DataFrame.fromColumns({ x: [null, null] });
      expect(df.dtypes['x']).toBe(DType.Float64);
    });
  });

  // ------- Metadata -------
  describe('metadata', () => {
    const df = DataFrame.fromColumns({
      name: ['Alice', 'Bob', 'Charlie'],
      age: [30, 25, 35],
    });

    it('shape', () => expect(df.shape).toEqual([3, 2]));
    it('columns', () => expect(df.columns).toEqual(['name', 'age']));
    it('dtypes', () =>
      expect(df.dtypes).toEqual({ name: DType.Utf8, age: DType.Float64 }));
    it('length', () => expect(df.length).toBe(3));
  });

  // ------- col / row / toArray / iterator -------
  describe('col / row / toArray / iterator', () => {
    const df = DataFrame.fromRows([
      { x: 1, y: 'a' },
      { x: 2, y: 'b' },
      { x: 3, y: 'c' },
    ]);

    it('col returns Series', () => {
      const s = df.col('x');
      expect(s).toBeInstanceOf(Series);
      expect(s.toArray()).toEqual([1, 2, 3]);
    });

    it('col throws ColumnNotFoundError', () => {
      expect(() => df.col('z' as never)).toThrow(ColumnNotFoundError);
    });

    it('row returns object', () => {
      expect(df.row(1)).toEqual({ x: 2, y: 'b' });
    });

    it('row throws on out-of-bounds', () => {
      expect(() => df.row(99)).toThrow();
    });

    it('toArray returns all rows', () => {
      const arr = df.toArray();
      expect(arr).toHaveLength(3);
      expect(arr[0]).toEqual({ x: 1, y: 'a' });
    });

    it('iterator supports for...of', () => {
      const collected: unknown[] = [];
      for (const row of df) {
        collected.push(row);
      }
      expect(collected).toHaveLength(3);
      expect(collected[2]).toEqual({ x: 3, y: 'c' });
    });

    it('spread operator', () => {
      const rows = [...df];
      expect(rows).toHaveLength(3);
    });
  });

  // ------- Selection -------
  describe('selection', () => {
    type Row = { a: number; b: string; c: boolean };
    const df = DataFrame.fromRows<Row>([
      { a: 1, b: 'x', c: true },
      { a: 2, b: 'y', c: false },
      { a: 3, b: 'z', c: true },
      { a: 4, b: 'w', c: false },
      { a: 5, b: 'v', c: true },
    ]);

    it('select picks columns', () => {
      const s = df.select('a', 'c');
      expect(s.columns).toEqual(['a', 'c']);
      expect(s.length).toBe(5);
    });

    it('drop removes columns', () => {
      const d = df.drop('c');
      expect(d.columns).toEqual(['a', 'b']);
    });

    it('head default 5', () => {
      expect(df.head().length).toBe(5);
    });

    it('head(2)', () => {
      expect(df.head(2).length).toBe(2);
      expect(df.head(2).col('a').toArray()).toEqual([1, 2]);
    });

    it('tail(2)', () => {
      expect(df.tail(2).length).toBe(2);
      expect(df.tail(2).col('a').toArray()).toEqual([4, 5]);
    });

    it('slice', () => {
      const sliced = df.slice(1, 3);
      expect(sliced.length).toBe(2);
      expect(sliced.col('a').toArray()).toEqual([2, 3]);
    });

    it('select throws ColumnNotFoundError for missing column', () => {
      expect(() => df.select('a', 'missing' as never)).toThrow(ColumnNotFoundError);
    });
  });

  // ------- Transform -------
  describe('transform', () => {
    type Row = { name: string; age: number };
    const df = DataFrame.fromRows<Row>([
      { name: 'Alice', age: 30 },
      { name: 'Bob', age: 25 },
    ]);

    it('withColumn adds column from array', () => {
      const df2 = df.withColumn('score', [90, 85]);
      expect(df2.columns).toContain('score');
      expect(df2.col('score').toArray()).toEqual([90, 85]);
    });

    it('withColumn adds column from function', () => {
      const df2 = df.withColumn('senior', (row: Row) => row.age >= 30);
      expect(df2.col('senior').toArray()).toEqual([true, false]);
    });

    it('withColumn replaces existing column', () => {
      const df2 = df.withColumn('age', [100, 200]);
      expect(df2.col('age').toArray()).toEqual([100, 200]);
      expect(df2.columns).toEqual(['name', 'age']); // order preserved
    });

    it('rename columns', () => {
      const df2 = df.rename({ name: 'fullName' });
      expect(df2.columns).toContain('fullName');
      expect(df2.columns).not.toContain('name');
      expect(df2.col('fullName' as never).toArray()).toEqual(['Alice', 'Bob']);
    });

    it('cast column dtype', () => {
      const df2 = df.cast({ age: DType.Utf8 });
      expect(df2.col('age').dtype).toBe(DType.Utf8);
      expect(df2.col('age').toArray()).toEqual(['30', '25']);
    });
  });

  // ------- Filter -------
  describe('filter', () => {
    type Row = { x: number; y: string };
    const df = DataFrame.fromRows<Row>([
      { x: 1, y: 'a' },
      { x: 2, y: 'b' },
      { x: 3, y: 'c' },
      { x: 4, y: 'd' },
    ]);

    it('predicate filter', () => {
      const filtered = df.filter((row: Row) => row.x > 2);
      expect(filtered.length).toBe(2);
      expect(filtered.col('y').toArray()).toEqual(['c', 'd']);
    });

    it('empty result', () => {
      const filtered = df.filter((row: Row) => row.x > 100);
      expect(filtered.length).toBe(0);
      expect(filtered.columns).toEqual(['x', 'y']);
    });

    it('preserves dtypes', () => {
      const filtered = df.filter((row: Row) => row.x === 1);
      expect(filtered.dtypes).toEqual(df.dtypes);
    });

    it('all rows match', () => {
      const filtered = df.filter(() => true);
      expect(filtered.length).toBe(4);
    });
  });

  // ------- Sort -------
  describe('sortBy', () => {
    type Row = { name: string; age: number; city: string };
    const df = DataFrame.fromRows<Row>([
      { name: 'Charlie', age: 35, city: 'NYC' },
      { name: 'Alice', age: 30, city: 'LA' },
      { name: 'Bob', age: 30, city: 'NYC' },
      { name: 'Diana', age: 25, city: 'LA' },
    ]);

    it('single column ascending', () => {
      const sorted = df.sortBy('age');
      expect(sorted.col('name').toArray()).toEqual(['Diana', 'Alice', 'Bob', 'Charlie']);
    });

    it('single column descending', () => {
      const sorted = df.sortBy('age', 'desc');
      expect(sorted.col('age').toArray()).toEqual([35, 30, 30, 25]);
    });

    it('multi-column sort', () => {
      const sorted = df.sortBy(['age', 'name'], ['asc', 'asc']);
      expect(sorted.col('name').toArray()).toEqual(['Diana', 'Alice', 'Bob', 'Charlie']);
    });

    it('sort is stable', () => {
      const data = DataFrame.fromRows([
        { k: 1, v: 'a' },
        { k: 1, v: 'b' },
        { k: 1, v: 'c' },
      ]);
      const sorted = data.sortBy('k');
      expect(sorted.col('v').toArray()).toEqual(['a', 'b', 'c']);
    });
  });

  // ------- Null handling -------
  describe('null handling', () => {
    type Row = { a: number | null; b: string | null; c: number | null };

    it('dropNull removes rows with any null', () => {
      const df = DataFrame.fromRows<Row>([
        { a: 1, b: 'x', c: 10 },
        { a: null, b: 'y', c: 20 },
        { a: 3, b: null, c: 30 },
        { a: 4, b: 'w', c: 40 },
      ]);
      const dropped = df.dropNull();
      expect(dropped.length).toBe(2);
      expect(dropped.col('a').toArray()).toEqual([1, 4]);
    });

    it('dropNull with specific column', () => {
      const df = DataFrame.fromRows<Row>([
        { a: 1, b: 'x', c: null },
        { a: null, b: 'y', c: 20 },
        { a: 3, b: 'z', c: 30 },
      ]);
      const dropped = df.dropNull('a');
      expect(dropped.length).toBe(2);
    });

    it('dropNull with multiple columns', () => {
      const df = DataFrame.fromRows<Row>([
        { a: 1, b: null, c: 10 },
        { a: null, b: 'y', c: null },
        { a: 3, b: 'z', c: 30 },
      ]);
      const dropped = df.dropNull(['a', 'b']);
      expect(dropped.length).toBe(1);
      expect(dropped.col('a').get(0)).toBe(3);
    });

    it('fillNull with value map', () => {
      const df = DataFrame.fromRows<Row>([
        { a: null, b: null, c: 10 },
        { a: 2, b: 'y', c: null },
      ]);
      const filled = df.fillNull({ a: 0, b: 'N/A' });
      expect(filled.col('a').toArray()).toEqual([0, 2]);
      expect(filled.col('b').toArray()).toEqual(['N/A', 'y']);
      // c was not in the map, should still have null
      expect(filled.col('c').get(1)).toBeNull();
    });

    it('fillNull forward', () => {
      const df = DataFrame.fromRows<Row>([
        { a: 1, b: 'x', c: 10 },
        { a: null, b: null, c: 20 },
        { a: 3, b: null, c: 30 },
      ]);
      const filled = df.fillNull('forward');
      expect(filled.col('a').toArray()).toEqual([1, 1, 3]);
      expect(filled.col('b').toArray()).toEqual(['x', 'x', 'x']);
    });

    it('fillNull backward', () => {
      const df = DataFrame.fromRows<Row>([
        { a: null, b: null, c: 10 },
        { a: null, b: 'y', c: 20 },
        { a: 3, b: 'z', c: 30 },
      ]);
      const filled = df.fillNull('backward');
      expect(filled.col('a').toArray()).toEqual([3, 3, 3]);
      expect(filled.col('b').toArray()).toEqual(['y', 'y', 'z']);
    });
  });

  // ------- Unique -------
  describe('unique', () => {
    it('deduplicates all columns', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 'x' },
        { a: 1, b: 'x' },
        { a: 2, b: 'y' },
      ]);
      const u = df.unique();
      expect(u.length).toBe(2);
    });

    it('deduplicates by specific column', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 'x' },
        { a: 1, b: 'y' },
        { a: 2, b: 'z' },
      ]);
      const u = df.unique('a');
      expect(u.length).toBe(2);
    });

    it('keep first by default', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 'first' },
        { a: 1, b: 'second' },
      ]);
      const u = df.unique('a');
      expect(u.col('b').get(0)).toBe('first');
    });

    it('keep last', () => {
      const df = DataFrame.fromRows([
        { a: 1, b: 'first' },
        { a: 1, b: 'second' },
      ]);
      const u = df.unique('a', 'last');
      expect(u.col('b').get(0)).toBe('second');
    });
  });

  // ------- Sample -------
  describe('sample', () => {
    const df = DataFrame.fromRows(
      Array.from({ length: 100 }, (_, i) => ({ id: i, val: i * 10 })),
    );

    it('sample integer count', () => {
      const s = df.sample(10);
      expect(s.length).toBe(10);
    });

    it('sample fractional', () => {
      const s = df.sample(0.1);
      expect(s.length).toBe(10);
    });

    it('seeded sample is reproducible', () => {
      const s1 = df.sample(5, { seed: 42 });
      const s2 = df.sample(5, { seed: 42 });
      expect(s1.col('id').toArray()).toEqual(s2.col('id').toArray());
    });

    it('different seeds give different results', () => {
      const s1 = df.sample(10, { seed: 1 });
      const s2 = df.sample(10, { seed: 2 });
      // Extremely unlikely to be equal with different seeds
      const eq = JSON.stringify(s1.col('id').toArray()) === JSON.stringify(s2.col('id').toArray());
      expect(eq).toBe(false);
    });
  });
});
