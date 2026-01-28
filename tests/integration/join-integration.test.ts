import { describe, it, expect } from 'vitest';
import { DataFrame } from '../../src';

/**
 * Helper to create DataFrames from row objects for integration tests.
 */
function rows<S extends Record<string, unknown>>(data: S[]): DataFrame<S> {
  return DataFrame.fromRows(data);
}

describe('Join Integration Tests (US-045)', () => {
  // Fixture data: employees and departments
  const employees = () =>
    rows([
      { empId: 1, name: 'Alice', deptId: 10, salary: 90000 },
      { empId: 2, name: 'Bob', deptId: 20, salary: 80000 },
      { empId: 3, name: 'Charlie', deptId: 10, salary: 95000 },
      { empId: 4, name: 'Diana', deptId: 30, salary: 70000 },
      { empId: 5, name: 'Eve', deptId: null, salary: 60000 },
    ]);

  const departments = () =>
    rows([
      { deptId: 10, deptName: 'Engineering' },
      { deptId: 20, deptName: 'Marketing' },
      { deptId: 40, deptName: 'Sales' },
    ]);

  describe('inner join', () => {
    it('should return only matching rows', () => {
      const result = employees().join(departments(), 'deptId', 'inner');

      // Only empIds 1,2,3 match (deptId 10,20); Diana (30) and Eve (null) don't match
      expect(result.length).toBe(3);

      const names = result.col('name').toArray().sort();
      expect(names).toEqual(['Alice', 'Bob', 'Charlie']);

      // Verify department names came through
      const deptNames = result.col('deptName').toArray().sort();
      expect(deptNames).toEqual(['Engineering', 'Engineering', 'Marketing']);
    });

    it('should not include null key rows', () => {
      const result = employees().join(departments(), 'deptId', 'inner');
      const empIds = result.col('empId').toArray();
      expect(empIds).not.toContain(5); // Eve has null deptId
    });
  });

  describe('left join', () => {
    it('should preserve all left rows with nulls for non-matches', () => {
      const result = employees().join(departments(), 'deptId', 'left');

      // All 5 employees should appear
      expect(result.length).toBe(5);

      const names = result.col('name').toArray();
      expect(names.sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve']);

      // Diana (deptId=30) and Eve (deptId=null) should have null deptName
      for (let i = 0; i < result.length; i++) {
        const name = result.col('name').get(i);
        const deptName = result.col('deptName').get(i);
        if (name === 'Diana' || name === 'Eve') {
          expect(deptName).toBeNull();
        } else {
          expect(deptName).not.toBeNull();
        }
      }
    });
  });

  describe('right join', () => {
    it('should preserve all right rows with nulls for non-matches', () => {
      const result = employees().join(departments(), 'deptId', 'right');

      // Matches: Alice(10), Charlie(10), Bob(20) + unmatched: Sales(40)
      expect(result.length).toBe(4);

      // Sales department should appear with null employee fields
      let salesIdx = -1;
      for (let i = 0; i < result.length; i++) {
        if (result.col('deptName').get(i) === 'Sales') {
          salesIdx = i;
          break;
        }
      }
      expect(salesIdx).not.toBe(-1);
      expect(result.col('name').get(salesIdx)).toBeNull();
      expect(result.col('salary').get(salesIdx)).toBeNull();
    });
  });

  describe('outer join', () => {
    it('should include all rows from both sides', () => {
      const result = employees().join(departments(), 'deptId', 'outer');

      // Matches: Alice(10), Charlie(10), Bob(20) = 3
      // Left only: Diana(30), Eve(null) = 2
      // Right only: Sales(40) = 1
      // Total = 6
      expect(result.length).toBe(6);

      // All employee names should be present (or null for right-only)
      const names = result.col('name').toArray();
      expect(names.filter((n) => n !== null).sort()).toEqual([
        'Alice',
        'Bob',
        'Charlie',
        'Diana',
        'Eve',
      ]);

      // All department names should be present (or null for left-only)
      const deptNames = result.col('deptName').toArray();
      expect(deptNames.filter((n) => n !== null).sort()).toEqual([
        'Engineering',
        'Engineering',
        'Marketing',
        'Sales',
      ]);
    });
  });

  describe('cross join', () => {
    it('should produce cartesian product with correct row count', () => {
      const left = rows([
        { id: 1, color: 'red' },
        { id: 2, color: 'blue' },
      ]);
      const right = rows([
        { size: 'S' },
        { size: 'M' },
        { size: 'L' },
      ]);

      const result = left.join(right, '', 'cross');

      // 2 * 3 = 6 rows
      expect(result.length).toBe(6);

      // Each left row should appear 3 times
      const ids = result.col('id').toArray();
      expect(ids.filter((v) => v === 1).length).toBe(3);
      expect(ids.filter((v) => v === 2).length).toBe(3);

      // Each right value should appear 2 times
      const sizes = result.col('size').toArray();
      expect(sizes.filter((v) => v === 'S').length).toBe(2);
      expect(sizes.filter((v) => v === 'M').length).toBe(2);
      expect(sizes.filter((v) => v === 'L').length).toBe(2);
    });
  });

  describe('semi join', () => {
    it('should return left rows that have a match, without right columns', () => {
      const result = employees().join(departments(), 'deptId', 'semi');

      // Alice(10), Bob(20), Charlie(10) match
      expect(result.length).toBe(3);

      const names = result.col('name').toArray().sort();
      expect(names).toEqual(['Alice', 'Bob', 'Charlie']);

      // No right columns should be added (only left columns)
      expect(result.columns).toEqual(['empId', 'name', 'deptId', 'salary']);
      expect(result.columns).not.toContain('deptName');
    });
  });

  describe('anti join', () => {
    it('should return left rows that have no match', () => {
      const result = employees().join(departments(), 'deptId', 'anti');

      // Diana(30) and Eve(null) have no match
      expect(result.length).toBe(2);

      const names = result.col('name').toArray().sort();
      expect(names).toEqual(['Diana', 'Eve']);

      // Only left columns
      expect(result.columns).toEqual(['empId', 'name', 'deptId', 'salary']);
    });
  });

  describe('composite keys', () => {
    it('should join on multiple key columns', () => {
      const orders = rows([
        { region: 'East', productId: 1, qty: 10 },
        { region: 'East', productId: 2, qty: 5 },
        { region: 'West', productId: 1, qty: 8 },
        { region: 'West', productId: 3, qty: 12 },
      ]);

      const prices = rows([
        { region: 'East', productId: 1, price: 100 },
        { region: 'East', productId: 2, price: 200 },
        { region: 'West', productId: 1, price: 110 },
      ]);

      const result = orders.join(prices, ['region', 'productId'], 'inner');

      // Only 3 matches: East/1, East/2, West/1 — West/3 has no match
      expect(result.length).toBe(3);

      // Verify a specific row
      for (let i = 0; i < result.length; i++) {
        if (
          result.col('region').get(i) === 'East' &&
          result.col('productId').get(i) === 1
        ) {
          expect(result.col('qty').get(i)).toBe(10);
          expect(result.col('price').get(i)).toBe(100);
        }
      }

      // West/3 should not be in results
      const regions = result.col('region').toArray();
      const products = result.col('productId').toArray();
      const hasWest3 = regions.some(
        (r, idx) => r === 'West' && products[idx] === 3,
      );
      expect(hasWest3).toBe(false);
    });
  });

  describe('different column names via { left, right } syntax', () => {
    it('should join with different key column names', () => {
      const emps = rows([
        { id: 1, name: 'Alice', dept: 10 },
        { id: 2, name: 'Bob', dept: 20 },
        { id: 3, name: 'Charlie', dept: 30 },
      ]);

      const depts = rows([
        { deptCode: 10, deptName: 'Engineering' },
        { deptCode: 20, deptName: 'Marketing' },
      ]);

      const result = emps.join(
        depts,
        { left: 'dept', right: 'deptCode' },
        'left',
      );

      // All 3 employees should appear
      expect(result.length).toBe(3);

      // Key column uses left name ('dept')
      expect(result.columns).toContain('dept');

      // Charlie (dept=30) should have null deptName
      for (let i = 0; i < result.length; i++) {
        if (result.col('name').get(i) === 'Charlie') {
          expect(result.col('deptName').get(i)).toBeNull();
        } else {
          expect(result.col('deptName').get(i)).not.toBeNull();
        }
      }
    });

    it('should work with composite keys using different names', () => {
      const left = rows([
        { a: 1, b: 'x', val: 100 },
        { a: 2, b: 'y', val: 200 },
      ]);

      const right = rows([
        { c: 1, d: 'x', info: 'match' },
        { c: 2, d: 'z', info: 'no-match' },
      ]);

      const result = left.join(
        right,
        { left: ['a', 'b'], right: ['c', 'd'] },
        'inner',
      );

      // Only a=1,b='x' matches c=1,d='x'
      expect(result.length).toBe(1);
      expect(result.col('val').get(0)).toBe(100);
      expect(result.col('info').get(0)).toBe('match');
    });
  });
});
