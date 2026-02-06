import { describe, it, expect } from 'vitest';
import { DataFrame, col, LazyFrame } from '../../../../src';

describe('LazyFrame integration tests', () => {
  const rows = [
    { name: 'Alice', age: 30, city: 'NYC', amount: 150 },
    { name: 'Bob', age: 25, city: 'LA', amount: 200 },
    { name: 'Charlie', age: 35, city: 'NYC', amount: 50 },
    { name: 'Diana', age: 28, city: 'LA', amount: 300 },
    { name: 'Eve', age: 30, city: 'NYC', amount: 100 },
  ];
  const df = DataFrame.fromRows(rows);

  describe('lazy() returns LazyFrame without executing', () => {
    it('returns a LazyFrame instance', () => {
      const lf = df.lazy();
      expect(lf).toBeInstanceOf(LazyFrame);
    });

    it('does not produce data until collect() is called', () => {
      // Building a complex lazy chain does not trigger computation
      const lf = df
        .lazy()
        .filter(col('age').gt(25))
        .select('name', 'city')
        .sort('name');

      // lf is just a plan — verify it has the right plan type
      expect(lf._plan.type).toBe('sort');
      // No DataFrame result yet; collect() returns a Promise
      const result = lf.collect();
      expect(result).toBeInstanceOf(Promise);
    });
  });

  describe('collect() produces same result as eager equivalent', () => {
    it('filter: lazy matches eager', async () => {
      const eager = df.filter(col('age').gt(28));
      const lazy = await df.lazy().filter(col('age').gt(28)).collect();

      expect(lazy.length).toBe(eager.length);
      expect(lazy.columns).toEqual(eager.columns);
      expect(lazy.col('name').toArray()).toEqual(eager.col('name').toArray());
      expect(lazy.col('age').toArray()).toEqual(eager.col('age').toArray());
    });

    it('select: lazy matches eager', async () => {
      const eager = df.select('name', 'amount');
      const lazy = await df.lazy().select('name', 'amount').collect();

      expect(lazy.length).toBe(eager.length);
      expect(lazy.columns).toEqual(eager.columns);
      expect(lazy.col('name').toArray()).toEqual(eager.col('name').toArray());
      expect(lazy.col('amount').toArray()).toEqual(eager.col('amount').toArray());
    });

    it('sortBy: lazy matches eager', async () => {
      const eager = df.sortBy('age');
      const lazy = await df.lazy().sort('age').collect();

      expect(lazy.col('age').toArray()).toEqual(eager.col('age').toArray());
      expect(lazy.col('name').toArray()).toEqual(eager.col('name').toArray());
    });

    it('head: lazy matches eager', async () => {
      const eager = df.head(3);
      const lazy = await df.lazy().head(3).collect();

      expect(lazy.length).toBe(eager.length);
      expect(lazy.col('name').toArray()).toEqual(eager.col('name').toArray());
    });

    it('filter + select: lazy matches eager', async () => {
      const eager = df.filter(col('age').gt(28)).select('name', 'city');
      const lazy = await df
        .lazy()
        .filter(col('age').gt(28))
        .select('name', 'city')
        .collect();

      expect(lazy.length).toBe(eager.length);
      expect(lazy.columns).toEqual(eager.columns);
      expect(lazy.col('name').toArray()).toEqual(eager.col('name').toArray());
    });
  });

  describe('predicate pushdown: filter after select applies filter early', () => {
    it('filter placed after select is pushed down in optimized plan', () => {
      const lf = df
        .lazy()
        .select('name', 'age')
        .filter(col('age').gt(28));

      const plan = lf.explain();
      // Original plan: filter on top of select
      expect(plan).toContain('ORIGINAL:');
      const originalSection = plan.split('OPTIMIZED:')[0]!;
      const originalLines = originalSection.split('\n');
      const originalFilterIdx = originalLines.findIndex(l => l.includes('FILTER'));
      const originalSelectIdx = originalLines.findIndex(l => l.includes('SELECT'));
      expect(originalFilterIdx).toBeLessThan(originalSelectIdx);

      // Optimized plan: filter pushed below select
      expect(plan).toContain('OPTIMIZED:');
      const optimizedSection = plan.split('OPTIMIZED:')[1]!;
      const optimizedLines = optimizedSection.split('\n');
      const optFilterIdx = optimizedLines.findIndex(l => l.includes('FILTER'));
      const optSelectIdx = optimizedLines.findIndex(l => l.includes('SELECT'));
      expect(optSelectIdx).toBeLessThan(optFilterIdx);
    });
  });

  describe('projection pushdown: only needed columns loaded', () => {
    it('scan node receives projection with only selected columns', () => {
      const lf = df.lazy().select('name', 'age');
      const plan = lf.explain();
      const optimizedSection = plan.split('OPTIMIZED:')[1]!;
      // Scan node should show cols= with only the needed columns
      expect(optimizedSection).toContain('cols=');
      expect(optimizedSection).toContain('age');
      expect(optimizedSection).toContain('name');
      // Should not include columns not in the select
      expect(optimizedSection).not.toContain('amount');
      expect(optimizedSection).not.toContain('city');
    });

    it('filter dependencies are included in scan projection', () => {
      const lf = df
        .lazy()
        .filter(col('age').gt(28))
        .select('name');

      const plan = lf.explain();
      const optimizedSection = plan.split('OPTIMIZED:')[1]!;
      // Scan should load both 'name' (selected) and 'age' (filter dependency)
      expect(optimizedSection).toContain('age');
      expect(optimizedSection).toContain('name');
    });
  });

  describe('explain() output shows readable plan', () => {
    it('shows both ORIGINAL and OPTIMIZED sections', () => {
      const plan = df.lazy().filter(col('age').gt(25)).explain();
      expect(plan).toContain('ORIGINAL:');
      expect(plan).toContain('OPTIMIZED:');
    });

    it('shows human-readable node descriptions', () => {
      const plan = df
        .lazy()
        .filter(col('amount').gt(100))
        .select('name', 'amount')
        .sort('amount', true)
        .limit(10)
        .explain();

      expect(plan).toContain('FILTER [(amount > 100)]');
      expect(plan).toContain('SELECT [name, amount]');
      expect(plan).toContain('SORT [amount DESC]');
      expect(plan).toContain('LIMIT [10]');
      expect(plan).toMatch(/SCAN \[id=\d+/);
    });

    it('shows groupBy with keys and aggregation specs', () => {
      const plan = df
        .lazy()
        .groupBy('city')
        .agg(col('amount').sum())
        .explain();

      expect(plan).toContain('GROUPBY [keys: city; aggs: sum(amount)]');
    });
  });

  describe('complex chain: filter -> select -> groupBy -> sort -> collect', () => {
    it('produces correct results through the full pipeline', async () => {
      // Filter: amount > 50 (excludes Charlie with amount=50)
      // Select: city, amount
      // GroupBy city, sum amount
      // Sort by city
      const result = await df
        .lazy()
        .filter(col('amount').gt(50))
        .select('city', 'amount')
        .groupBy('city')
        .agg(col('amount').sum())
        .sort('city')
        .collect();

      expect(result.columns).toContain('city');
      const cities = result.col('city').toArray();
      // Both NYC and LA should be present
      expect(cities).toContain('NYC');
      expect(cities).toContain('LA');

      // LA: Bob(200) + Diana(300) = 500
      // NYC: Alice(150) + Eve(100) = 250
      const laIdx = cities.indexOf('LA');
      const nycIdx = cities.indexOf('NYC');
      const amounts = result.col('amount').toArray();
      expect(amounts[laIdx]).toBe(500);
      expect(amounts[nycIdx]).toBe(250);
    });

    it('explain shows the full plan tree for complex chain', () => {
      const plan = df
        .lazy()
        .filter(col('amount').gt(50))
        .select('city', 'amount')
        .groupBy('city')
        .agg(col('amount').sum())
        .sort('city')
        .explain();

      expect(plan).toContain('SORT');
      expect(plan).toContain('GROUPBY');
      expect(plan).toContain('SELECT');
      expect(plan).toContain('FILTER');
      expect(plan).toContain('SCAN');
    });
  });
});
