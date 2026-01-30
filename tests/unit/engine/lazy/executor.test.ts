import { describe, it, expect } from 'vitest';
import { DataFrame, col } from '../../../../src';

describe('LazyFrame executor (collect)', () => {
  const rows = [
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'LA' },
    { name: 'Charlie', age: 35, city: 'NYC' },
    { name: 'Diana', age: 28, city: 'LA' },
    { name: 'Eve', age: 30, city: 'NYC' },
  ];
  const source = DataFrame.fromRows(rows);

  it('scan node returns the source DataFrame', async () => {
    const result = await source.lazy().collect();
    expect(result.length).toBe(5);
    expect(result.columns).toEqual(['name', 'age', 'city']);
  });

  it('filter node applies predicate', async () => {
    const result = await source
      .lazy()
      .filter(col('age').gt(28))
      .collect();
    expect(result.length).toBe(3);
    const names = result.col('name').toArray();
    expect(names).toEqual(['Alice', 'Charlie', 'Eve']);
  });

  it('select node picks columns', async () => {
    const result = await source
      .lazy()
      .select('name', 'city')
      .collect();
    expect(result.columns).toEqual(['name', 'city']);
    expect(result.length).toBe(5);
  });

  it('sort node sorts input', async () => {
    const result = await source
      .lazy()
      .sort('age')
      .collect();
    const ages = result.col('age').toArray();
    expect(ages).toEqual([25, 28, 30, 30, 35]);
  });

  it('sort descending', async () => {
    const result = await source
      .lazy()
      .sort('age', true)
      .collect();
    const ages = result.col('age').toArray();
    expect(ages[0]).toBe(35);
    expect(ages[ages.length - 1]).toBe(25);
  });

  it('limit node takes first N rows', async () => {
    const result = await source
      .lazy()
      .limit(3)
      .collect();
    expect(result.length).toBe(3);
  });

  it('groupBy node performs aggregation', async () => {
    const result = await source
      .lazy()
      .groupBy('city')
      .agg(col('age').mean())
      .collect();
    expect(result.length).toBe(2);
    const cityCol = result.col('city').toArray();
    expect(cityCol).toContain('NYC');
    expect(cityCol).toContain('LA');
  });

  it('chained operations: filter + sort + limit', async () => {
    const result = await source
      .lazy()
      .filter(col('age').gte(28))
      .sort('age')
      .limit(2)
      .collect();
    expect(result.length).toBe(2);
    const ages = result.col('age').toArray();
    expect(ages).toEqual([28, 30]);
  });

  it('chained operations: select + sort', async () => {
    const result = await source
      .lazy()
      .select('name', 'age')
      .sort('age')
      .collect();
    expect(result.columns).toEqual(['name', 'age']);
    const ages = result.col('age').toArray();
    expect(ages).toEqual([25, 28, 30, 30, 35]);
  });

  it('distinct removes duplicates', async () => {
    const result = await source
      .lazy()
      .distinct(['city'])
      .collect();
    expect(result.length).toBe(2);
  });
});
