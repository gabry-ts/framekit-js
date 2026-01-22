import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../../src/dataframe';

describe('Join with different column names (US-034)', () => {
  const customers = DataFrame.fromRows([
    { cust_id: 1, name: 'Alice' },
    { cust_id: 2, name: 'Bob' },
    { cust_id: 3, name: 'Charlie' },
  ]);

  const orders = DataFrame.fromRows([
    { id: 1, product: 'Widget' },
    { id: 2, product: 'Gadget' },
    { id: 4, product: 'Doohickey' },
  ]);

  it('joins on different column names via { left, right } mapping', () => {
    const result = customers.join(orders, { left: 'cust_id', right: 'id' }, 'inner');
    expect(result.length).toBe(2);
    // Output key column uses left key name
    expect(result.columns).toContain('cust_id');
    expect(result.col('cust_id').toArray()).toEqual([1, 2]);
    expect(result.col('name').toArray()).toEqual(['Alice', 'Bob']);
    expect(result.col('product').toArray()).toEqual(['Widget', 'Gadget']);
  });

  it('left join with different column names preserves unmatched left rows', () => {
    const result = customers.join(orders, { left: 'cust_id', right: 'id' }, 'left');
    expect(result.length).toBe(3);
    expect(result.col('cust_id').toArray()).toEqual([1, 2, 3]);
    expect(result.col('name').toArray()).toEqual(['Alice', 'Bob', 'Charlie']);
    expect(result.col('product').toArray()).toEqual(['Widget', 'Gadget', null]);
  });

  it('right join with different column names preserves unmatched right rows', () => {
    const result = customers.join(orders, { left: 'cust_id', right: 'id' }, 'right');
    expect(result.length).toBe(3);
    expect(result.col('product').toArray()).toEqual(['Widget', 'Gadget', 'Doohickey']);
  });

  it('outer join with different column names preserves all rows', () => {
    const result = customers.join(orders, { left: 'cust_id', right: 'id' }, 'outer');
    expect(result.length).toBe(4);
  });
});

describe('Composite key with different names (US-034)', () => {
  const left = DataFrame.fromRows([
    { a: 1, b: 'x', val: 100 },
    { a: 1, b: 'y', val: 200 },
    { a: 2, b: 'x', val: 300 },
  ]);

  const right = DataFrame.fromRows([
    { x: 1, y: 'x', score: 10 },
    { x: 2, y: 'x', score: 20 },
    { x: 3, y: 'z', score: 30 },
  ]);

  it('joins on composite keys with different names', () => {
    const result = left.join(right, { left: ['a', 'b'], right: ['x', 'y'] }, 'inner');
    expect(result.length).toBe(2);
    expect(result.col('a').toArray()).toEqual([1, 2]);
    expect(result.col('b').toArray()).toEqual(['x', 'x']);
    expect(result.col('val').toArray()).toEqual([100, 300]);
    expect(result.col('score').toArray()).toEqual([10, 20]);
  });

  it('left join with composite different-name keys', () => {
    const result = left.join(right, { left: ['a', 'b'], right: ['x', 'y'] }, 'left');
    expect(result.length).toBe(3);
    expect(result.col('a').toArray()).toEqual([1, 1, 2]);
    expect(result.col('score').toArray()).toEqual([10, null, 20]);
  });
});

describe('Custom suffix for overlapping columns (US-034)', () => {
  const left = DataFrame.fromRows([
    { id: 1, value: 'left_a' },
    { id: 2, value: 'left_b' },
  ]);

  const right = DataFrame.fromRows([
    { id: 1, value: 'right_a' },
    { id: 2, value: 'right_b' },
  ]);

  it('uses _right suffix by default for overlapping non-key columns', () => {
    const result = left.join(right, 'id', 'inner');
    expect(result.columns).toEqual(['id', 'value', 'value_right']);
    expect(result.col('value').toArray()).toEqual(['left_a', 'left_b']);
    expect(result.col('value_right').toArray()).toEqual(['right_a', 'right_b']);
  });

  it('accepts custom suffix via options parameter', () => {
    const result = left.join(right, 'id', 'inner', { suffix: '_r' });
    expect(result.columns).toEqual(['id', 'value', 'value_r']);
    expect(result.col('value').toArray()).toEqual(['left_a', 'left_b']);
    expect(result.col('value_r').toArray()).toEqual(['right_a', 'right_b']);
  });

  it('custom suffix with left join', () => {
    const result = left.join(right, 'id', 'left', { suffix: '_other' });
    expect(result.columns).toContain('value_other');
    expect(result.col('value_other').toArray()).toEqual(['right_a', 'right_b']);
  });

  it('custom suffix with different column name mapping', () => {
    const customers = DataFrame.fromRows([
      { cust_id: 1, name: 'Alice', status: 'active' },
    ]);
    const accounts = DataFrame.fromRows([
      { account_id: 1, name: 'Account-1', status: 'open' },
    ]);

    const result = customers.join(
      accounts,
      { left: 'cust_id', right: 'account_id' },
      'inner',
      { suffix: '_acct' },
    );
    expect(result.columns).toEqual(['cust_id', 'name', 'status', 'name_acct', 'status_acct']);
    expect(result.col('name').toArray()).toEqual(['Alice']);
    expect(result.col('name_acct').toArray()).toEqual(['Account-1']);
    expect(result.col('status').toArray()).toEqual(['active']);
    expect(result.col('status_acct').toArray()).toEqual(['open']);
  });
});

describe('Semi/anti join with different column names (US-034)', () => {
  const left = DataFrame.fromRows([
    { cust_id: 1, name: 'Alice' },
    { cust_id: 2, name: 'Bob' },
    { cust_id: 3, name: 'Charlie' },
  ]);

  const right = DataFrame.fromRows([
    { id: 1, product: 'Widget' },
    { id: 3, product: 'Gadget' },
  ]);

  it('semi join with different column names', () => {
    const result = left.join(right, { left: 'cust_id', right: 'id' }, 'semi');
    expect(result.length).toBe(2);
    expect(result.col('cust_id').toArray()).toEqual([1, 3]);
    expect(result.columns).toEqual(['cust_id', 'name']);
  });

  it('anti join with different column names', () => {
    const result = left.join(right, { left: 'cust_id', right: 'id' }, 'anti');
    expect(result.length).toBe(1);
    expect(result.col('cust_id').toArray()).toEqual([2]);
  });
});
