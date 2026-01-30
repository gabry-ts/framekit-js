import { describe, it, expect } from 'vitest';
import { optimize } from '../../../../src/engine/lazy/optimizer';
import { createScanNode, explainPlan } from '../../../../src/engine/lazy/plan';
import type { PlanNode } from '../../../../src/engine/lazy/plan';
import { col } from '../../../../src/expr/expr';

describe('optimize – predicate pushdown', () => {
  const scan = createScanNode();

  it('pushes filter below select', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: { type: 'select', input: scan, columns: ['a', 'b'] },
      predicate: col('a').gt(10),
    };
    const result = optimize(plan);
    // Filter should be below select
    expect(result.type).toBe('select');
    if (result.type === 'select') {
      expect(result.input.type).toBe('filter');
    }
  });

  it('pushes filter below project', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: { type: 'project', input: scan, exprs: [col('a')] },
      predicate: col('a').gt(5),
    };
    const result = optimize(plan);
    expect(result.type).toBe('project');
    if (result.type === 'project') {
      expect(result.input.type).toBe('filter');
    }
  });

  it('pushes filter below sort', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: { type: 'sort', input: scan, by: 'a', descending: false },
      predicate: col('a').gt(0),
    };
    const result = optimize(plan);
    expect(result.type).toBe('sort');
    if (result.type === 'sort') {
      expect(result.input.type).toBe('filter');
    }
  });

  it('does NOT push filter below groupby', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: { type: 'groupby', input: scan, keys: ['a'], aggs: [] },
      predicate: col('a').gt(0),
    };
    const result = optimize(plan);
    // Filter stays on top
    expect(result.type).toBe('filter');
    if (result.type === 'filter') {
      expect(result.input.type).toBe('groupby');
    }
  });

  it('merges consecutive filters into single AND filter', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: {
        type: 'filter',
        input: scan,
        predicate: col('a').gt(0),
      },
      predicate: col('b').lt(100),
    };
    const result = optimize(plan);
    // Should be a single filter with merged predicate
    expect(result.type).toBe('filter');
    if (result.type === 'filter') {
      expect(result.input.type).toBe('scan');
    }
  });

  it('pushes multiple filters through select and merges them', () => {
    const plan: PlanNode = {
      type: 'filter',
      input: {
        type: 'filter',
        input: { type: 'select', input: scan, columns: ['a', 'b'] },
        predicate: col('a').gt(0),
      },
      predicate: col('b').lt(100),
    };
    const result = optimize(plan);
    // Both filters should be pushed below select and merged
    expect(result.type).toBe('select');
    if (result.type === 'select') {
      expect(result.input.type).toBe('filter');
      if (result.input.type === 'filter') {
        // Merged: only one filter node, input is scan
        expect(result.input.input.type).toBe('scan');
      }
    }
  });

  it('explain shows both original and optimized plans', () => {
    const original: PlanNode = {
      type: 'filter',
      input: { type: 'sort', input: scan, by: 'a', descending: false },
      predicate: col('a').gt(0),
    };
    const optimized = optimize(original);
    const originalExplain = explainPlan(original);
    const optimizedExplain = explainPlan(optimized);
    // Original has filter on top
    expect(originalExplain).toContain('FILTER');
    expect(originalExplain.indexOf('FILTER')).toBeLessThan(originalExplain.indexOf('SORT'));
    // Optimized has sort on top
    expect(optimizedExplain).toContain('SORT');
    expect(optimizedExplain.indexOf('SORT')).toBeLessThan(optimizedExplain.indexOf('FILTER'));
  });

  it('handles deeply nested pushdown', () => {
    // filter -> select -> sort -> scan => select -> sort -> filter -> scan
    const plan: PlanNode = {
      type: 'filter',
      input: {
        type: 'select',
        input: { type: 'sort', input: scan, by: 'a', descending: true },
        columns: ['a'],
      },
      predicate: col('a').gt(0),
    };
    const result = optimize(plan);
    expect(result.type).toBe('select');
    if (result.type === 'select') {
      expect(result.input.type).toBe('sort');
      if (result.input.type === 'sort') {
        expect(result.input.input.type).toBe('filter');
      }
    }
  });

  it('leaves plan unchanged when no optimization applies', () => {
    const plan: PlanNode = {
      type: 'select',
      input: { type: 'filter', input: scan, predicate: col('a').gt(0) },
      columns: ['a'],
    };
    const result = optimize(plan);
    expect(result.type).toBe('select');
    if (result.type === 'select') {
      expect(result.input.type).toBe('filter');
    }
  });
});
