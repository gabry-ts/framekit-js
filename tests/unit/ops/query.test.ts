import { describe, it, expect } from 'vitest';
import { DataFrame, ParseError } from '../../../src';

describe('DataFrame.query() — SQL-like query interface', () => {
  const df = DataFrame.fromRows([
    { name: 'Alice', age: 30, city: 'NYC', salary: 70000 },
    { name: 'Bob', age: 25, city: 'LA', salary: 60000 },
    { name: 'Charlie', age: 35, city: 'NYC', salary: 90000 },
    { name: 'Diana', age: 28, city: 'LA', salary: 65000 },
    { name: 'Eve', age: 40, city: 'NYC', salary: 120000 },
  ]);

  describe('SELECT', () => {
    it('selects specific columns', () => {
      const result = df.query('SELECT name, age FROM this');
      expect(result.columns).toEqual(['name', 'age']);
      expect(result.length).toBe(5);
    });

    it('selects all columns with *', () => {
      const result = df.query('SELECT * FROM this');
      expect(result.columns).toEqual(['name', 'age', 'city', 'salary']);
      expect(result.length).toBe(5);
    });

    it('supports column aliases with AS', () => {
      const result = df.query('SELECT name AS person, age AS years FROM this');
      expect(result.columns).toEqual(['person', 'years']);
      expect(result.length).toBe(5);
      expect(result.row(0)).toEqual({ person: 'Alice', years: 30 });
    });
  });

  describe('WHERE', () => {
    it('filters with > operator', () => {
      const result = df.query('SELECT name, age FROM this WHERE age > 30');
      expect(result.length).toBe(2);
      expect(result.toArray().map((r) => r.name)).toEqual(['Charlie', 'Eve']);
    });

    it('filters with = operator', () => {
      const result = df.query("SELECT name FROM this WHERE city = 'NYC'");
      expect(result.length).toBe(3);
    });

    it('filters with != operator', () => {
      const result = df.query("SELECT name FROM this WHERE city != 'LA'");
      expect(result.length).toBe(3);
    });

    it('filters with >= operator', () => {
      const result = df.query('SELECT name FROM this WHERE age >= 35');
      expect(result.length).toBe(2);
    });

    it('filters with <= operator', () => {
      const result = df.query('SELECT name FROM this WHERE age <= 28');
      expect(result.length).toBe(2);
    });

    it('filters with < operator', () => {
      const result = df.query('SELECT name FROM this WHERE age < 30');
      expect(result.length).toBe(2);
    });

    it('supports AND conditions', () => {
      const result = df.query("SELECT name FROM this WHERE age > 25 AND city = 'NYC'");
      expect(result.length).toBe(3);
      expect(result.toArray().map((r) => r.name)).toEqual(['Alice', 'Charlie', 'Eve']);
    });

    it('supports OR conditions', () => {
      const result = df.query("SELECT name FROM this WHERE age > 35 OR city = 'LA'");
      expect(result.length).toBe(3);
    });

    it('supports IN operator', () => {
      const result = df.query("SELECT name FROM this WHERE city IN ('NYC', 'LA')");
      expect(result.length).toBe(5);
    });

    it('supports NOT IN operator', () => {
      const result = df.query("SELECT name FROM this WHERE city NOT IN ('LA')");
      expect(result.length).toBe(3);
    });

    it('supports LIKE operator with %', () => {
      const result = df.query("SELECT name FROM this WHERE name LIKE 'A%'");
      expect(result.length).toBe(1);
      expect(result.row(0)).toEqual({ name: 'Alice' });
    });

    it('supports LIKE operator with _', () => {
      const result = df.query("SELECT name FROM this WHERE name LIKE 'B_b'");
      expect(result.length).toBe(1);
      expect(result.row(0)).toEqual({ name: 'Bob' });
    });

    it('supports IS NULL', () => {
      const dfNull = DataFrame.fromRows([
        { name: 'Alice', score: 100 },
        { name: 'Bob', score: null },
        { name: 'Charlie', score: 90 },
      ]);
      const result = dfNull.query('SELECT name FROM this WHERE score IS NULL');
      expect(result.length).toBe(1);
      expect(result.row(0)).toEqual({ name: 'Bob' });
    });

    it('supports IS NOT NULL', () => {
      const dfNull = DataFrame.fromRows([
        { name: 'Alice', score: 100 },
        { name: 'Bob', score: null },
        { name: 'Charlie', score: 90 },
      ]);
      const result = dfNull.query('SELECT name FROM this WHERE score IS NOT NULL');
      expect(result.length).toBe(2);
    });

    it('supports parenthesized conditions', () => {
      const result = df.query("SELECT name FROM this WHERE (age > 30 OR age < 26) AND city = 'NYC'");
      expect(result.length).toBe(2);
    });
  });

  describe('ORDER BY', () => {
    it('sorts ascending by default', () => {
      const result = df.query('SELECT name, age FROM this ORDER BY age');
      expect(result.toArray().map((r) => r.name)).toEqual(['Bob', 'Diana', 'Alice', 'Charlie', 'Eve']);
    });

    it('sorts descending with DESC', () => {
      const result = df.query('SELECT name, age FROM this ORDER BY age DESC');
      expect(result.toArray().map((r) => r.name)).toEqual(['Eve', 'Charlie', 'Alice', 'Diana', 'Bob']);
    });

    it('sorts ascending with explicit ASC', () => {
      const result = df.query('SELECT name, age FROM this ORDER BY age ASC');
      expect(result.toArray().map((r) => r.name)).toEqual(['Bob', 'Diana', 'Alice', 'Charlie', 'Eve']);
    });
  });

  describe('LIMIT', () => {
    it('limits result rows', () => {
      const result = df.query('SELECT name FROM this LIMIT 3');
      expect(result.length).toBe(3);
    });

    it('works with ORDER BY', () => {
      const result = df.query('SELECT name, age FROM this ORDER BY age DESC LIMIT 2');
      expect(result.length).toBe(2);
      expect(result.toArray().map((r) => r.name)).toEqual(['Eve', 'Charlie']);
    });
  });

  describe('GROUP BY', () => {
    it('groups and aggregates with SUM', () => {
      const result = df.query('SELECT city, SUM(salary) AS total_salary FROM this GROUP BY city');
      expect(result.columns).toContain('city');
      expect(result.columns).toContain('total_salary');
      // NYC: 70000 + 90000 + 120000 = 280000
      const nycRow = result.toArray().find((r) => r.city === 'NYC');
      expect(nycRow?.total_salary).toBe(280000);
    });

    it('groups and aggregates with COUNT', () => {
      const result = df.query('SELECT city, COUNT(name) AS cnt FROM this GROUP BY city');
      const nycRow = result.toArray().find((r) => r.city === 'NYC');
      expect(nycRow?.cnt).toBe(3);
    });

    it('groups and aggregates with AVG', () => {
      const result = df.query('SELECT city, AVG(age) AS avg_age FROM this GROUP BY city');
      const laRow = result.toArray().find((r) => r.city === 'LA');
      expect(laRow?.avg_age).toBeCloseTo(26.5);
    });

    it('groups and aggregates with MIN and MAX', () => {
      const result = df.query('SELECT city, MIN(age) AS min_age, MAX(age) AS max_age FROM this GROUP BY city');
      const nycRow = result.toArray().find((r) => r.city === 'NYC');
      expect(nycRow?.min_age).toBe(30);
      expect(nycRow?.max_age).toBe(40);
    });
  });

  describe('HAVING', () => {
    it('filters aggregated results', () => {
      const result = df.query('SELECT city, COUNT(name) AS cnt FROM this GROUP BY city HAVING COUNT(name) > 2');
      expect(result.length).toBe(1);
      expect(result.row(0).city).toBe('NYC');
    });
  });

  describe('combined queries', () => {
    it('handles SELECT + WHERE + ORDER BY + LIMIT', () => {
      const result = df.query("SELECT name, age FROM this WHERE city = 'NYC' ORDER BY age DESC LIMIT 2");
      expect(result.length).toBe(2);
      expect(result.toArray().map((r) => r.name)).toEqual(['Eve', 'Charlie']);
    });
  });

  describe('error handling', () => {
    it('throws ParseError for missing SELECT', () => {
      expect(() => df.query('name FROM this')).toThrow(ParseError);
    });

    it('throws ParseError for invalid FROM target', () => {
      expect(() => df.query('SELECT name FROM other')).toThrow(ParseError);
    });

    it('throws ParseError for unterminated string', () => {
      expect(() => df.query("SELECT name FROM this WHERE city = 'NYC")).toThrow(ParseError);
    });

    it('throws ParseError for unexpected token', () => {
      expect(() => df.query('SELECT name FROM this WHERE')).toThrow(ParseError);
    });

    it('includes position in error message', () => {
      try {
        df.query("SELECT name FROM this WHERE city @ 'NYC'");
        expect.fail('should have thrown');
      } catch (err) {
        expect(err).toBeInstanceOf(ParseError);
        expect((err as ParseError).message).toMatch(/position/i);
      }
    });
  });
});
