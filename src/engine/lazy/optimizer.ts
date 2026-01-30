import type { PlanNode, FilterNode } from './plan';

/**
 * Optimize a query plan by applying predicate pushdown rules:
 * 1. Push filters below select/project nodes
 * 2. Push filters below sort nodes
 * 3. Merge consecutive filters into a single AND filter
 * 4. Do NOT push filters below groupBy (changes semantics)
 */
export function optimize(plan: PlanNode): PlanNode {
  // First, recursively optimize children, then apply rules bottom-up
  const optimized = pushDown(plan);
  return mergeFilters(optimized);
}

/**
 * Push filter nodes below select, project, and sort nodes where safe.
 */
function pushDown(node: PlanNode): PlanNode {
  switch (node.type) {
    case 'scan':
      return node;

    case 'filter': {
      // First, recursively optimize the input
      const input = pushDown(node.input);
      const filter: FilterNode = { type: 'filter', input, predicate: node.predicate };
      return pushFilterDown(filter);
    }

    case 'select':
      return { type: 'select', input: pushDown(node.input), columns: node.columns };

    case 'project':
      return { type: 'project', input: pushDown(node.input), exprs: node.exprs };

    case 'groupby':
      return { type: 'groupby', input: pushDown(node.input), keys: node.keys, aggs: node.aggs };

    case 'sort':
      return { type: 'sort', input: pushDown(node.input), by: node.by, descending: node.descending };

    case 'limit':
      return { type: 'limit', input: pushDown(node.input), n: node.n };

    case 'distinct':
      return { type: 'distinct', input: pushDown(node.input), subset: node.subset };

    case 'join':
      return { type: 'join', left: pushDown(node.left), right: pushDown(node.right), on: node.on, how: node.how };
  }
}

/**
 * Try to push a filter node past its immediate child when safe.
 */
function pushFilterDown(filter: FilterNode): PlanNode {
  const child = filter.input;

  switch (child.type) {
    // Safe to push filter below select/project
    case 'select':
      return {
        type: 'select',
        input: pushFilterDown({ type: 'filter', input: child.input, predicate: filter.predicate }),
        columns: child.columns,
      };

    case 'project':
      return {
        type: 'project',
        input: pushFilterDown({ type: 'filter', input: child.input, predicate: filter.predicate }),
        exprs: child.exprs,
      };

    // Safe to push filter below sort
    case 'sort':
      return {
        type: 'sort',
        input: pushFilterDown({ type: 'filter', input: child.input, predicate: filter.predicate }),
        by: child.by,
        descending: child.descending,
      };

    // NOT safe to push below groupby — changes semantics
    case 'groupby':
      return filter;

    // For all other node types, keep filter in place
    default:
      return filter;
  }
}

/**
 * Merge consecutive filter nodes into a single filter with AND predicate.
 */
function mergeFilters(node: PlanNode): PlanNode {
  switch (node.type) {
    case 'scan':
      return node;

    case 'filter': {
      const input = mergeFilters(node.input);
      // If input is also a filter, merge predicates with AND
      if (input.type === 'filter') {
        return {
          type: 'filter',
          input: input.input,
          predicate: input.predicate.and(node.predicate),
        };
      }
      return { type: 'filter', input, predicate: node.predicate };
    }

    case 'select':
      return { type: 'select', input: mergeFilters(node.input), columns: node.columns };

    case 'project':
      return { type: 'project', input: mergeFilters(node.input), exprs: node.exprs };

    case 'groupby':
      return { type: 'groupby', input: mergeFilters(node.input), keys: node.keys, aggs: node.aggs };

    case 'sort':
      return { type: 'sort', input: mergeFilters(node.input), by: node.by, descending: node.descending };

    case 'limit':
      return { type: 'limit', input: mergeFilters(node.input), n: node.n };

    case 'distinct':
      return { type: 'distinct', input: mergeFilters(node.input), subset: node.subset };

    case 'join':
      return { type: 'join', left: mergeFilters(node.left), right: mergeFilters(node.right), on: node.on, how: node.how };
  }
}
