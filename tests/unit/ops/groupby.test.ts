import { describe, it, expect } from 'vitest';
import { DataFrame, GroupBy } from '../../../src';

describe('GroupBy', () => {
  const data = [
    { region: 'East', product: 'A', amount: 100 },
    { region: 'West', product: 'B', amount: 200 },
    { region: 'East', product: 'B', amount: 150 },
    { region: 'West', product: 'A', amount: 300 },
    { region: 'East', product: 'A', amount: 250 },
  ];

  type Row = typeof data[0];
  const df = DataFrame.fromRows<Row>(data);

  describe('single-key groupBy', () => {
    it('returns a GroupBy instance', () => {
      const gb = df.groupBy('region');
      expect(gb).toBeInstanceOf(GroupBy);
    });

    it('nGroups returns number of distinct groups', () => {
      const gb = df.groupBy('region');
      expect(gb.nGroups()).toBe(2);
    });

    it('keys returns the group key columns', () => {
      const gb = df.groupBy('region');
      expect(gb.keys).toEqual(['region']);
    });

    it('groups returns Map of sub-DataFrames', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();
      expect(grps.size).toBe(2);

      // Find East group
      let eastDf: DataFrame | undefined;
      let westDf: DataFrame | undefined;
      for (const [, subDf] of grps) {
        const firstRegion = subDf.row(0).region;
        if (firstRegion === 'East') eastDf = subDf;
        if (firstRegion === 'West') westDf = subDf;
      }

      expect(eastDf).toBeDefined();
      expect(eastDf!.length).toBe(3);
      expect(westDf).toBeDefined();
      expect(westDf!.length).toBe(2);
    });

    it('sub-DataFrames preserve all columns', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();
      for (const [, subDf] of grps) {
        expect(subDf.columns).toEqual(df.columns);
      }
    });

    it('sub-DataFrames contain correct rows', () => {
      const gb = df.groupBy('region');
      const grps = gb.groups();

      for (const [, subDf] of grps) {
        const rows = subDf.toArray();
        const region = rows[0]!.region;
        for (const row of rows) {
          expect(row.region).toBe(region);
        }
      }
    });
  });

  describe('multi-key groupBy', () => {
    it('supports grouping by multiple columns', () => {
      const gb = df.groupBy('region', 'product');
      expect(gb.nGroups()).toBe(4); // East-A, East-B, West-A, West-B
    });

    it('keys returns all group key columns', () => {
      const gb = df.groupBy('region', 'product');
      expect(gb.keys).toEqual(['region', 'product']);
    });

    it('groups returns correct sub-DataFrames', () => {
      const gb = df.groupBy('region', 'product');
      const grps = gb.groups();
      expect(grps.size).toBe(4);

      for (const [, subDf] of grps) {
        const rows = subDf.toArray();
        const region = rows[0]!.region;
        const product = rows[0]!.product;
        for (const row of rows) {
          expect(row.region).toBe(region);
          expect(row.product).toBe(product);
        }
      }
    });

    it('East-A group has 2 rows', () => {
      const gb = df.groupBy('region', 'product');
      const grps = gb.groups();

      let eastACount = 0;
      for (const [, subDf] of grps) {
        if (subDf.row(0).region === 'East' && subDf.row(0).product === 'A') {
          eastACount = subDf.length;
        }
      }
      expect(eastACount).toBe(2);
    });
  });

  describe('edge cases', () => {
    it('throws ColumnNotFoundError for invalid column', () => {
      expect(() => df.groupBy('invalid' as never)).toThrow();
    });

    it('groupBy on single-row DataFrame', () => {
      const singleDf = DataFrame.fromRows<Row>([data[0]!]);
      const gb = singleDf.groupBy('region');
      expect(gb.nGroups()).toBe(1);
    });

    it('groupBy where every row is unique', () => {
      const gb = df.groupBy('amount' as never);
      expect(gb.nGroups()).toBe(5);
    });

    it('groupBy with null values in key', () => {
      type NullRow = { region: string | null; amount: number };
      const nullDf = DataFrame.fromRows<NullRow>([
        { region: 'East', amount: 100 },
        { region: null, amount: 200 },
        { region: 'East', amount: 150 },
        { region: null, amount: 300 },
      ]);
      const gb = nullDf.groupBy('region');
      expect(gb.nGroups()).toBe(2); // East and null
    });

    it('groupMap is accessible', () => {
      const gb = df.groupBy('region');
      const map = gb.groupMap;
      expect(map).toBeInstanceOf(Map);
      let totalRows = 0;
      for (const [, indices] of map) {
        totalRows += indices.length;
      }
      expect(totalRows).toBe(df.length);
    });

    it('dataframe getter returns source DataFrame', () => {
      const gb = df.groupBy('region');
      expect(gb.dataframe).toBe(df);
    });
  });
});
