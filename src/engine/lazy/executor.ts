import type { DataFrame } from '../../dataframe';
import type { PlanNode } from './plan';
import type { AggExpr } from '../../expr/expr';
import { Expr } from '../../expr/expr';

/**
 * Eagerly execute a query plan tree, returning the resulting DataFrame.
 */
export function execute<S extends Record<string, unknown>>(
  plan: PlanNode,
  source: DataFrame<S>,
): DataFrame<S> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  type AnyDF = DataFrame<any>;

  function run(node: PlanNode): AnyDF {
    switch (node.type) {
      case 'scan':
        if (node.projection) {
          return source.select(...node.projection);
        }
        return source;

      case 'filter': {
        const input = run(node.input);
        return input.filter(node.predicate as Expr<boolean>);
      }

      case 'select': {
        const input = run(node.input);
        return input.select(...node.columns);
      }

      case 'project': {
        const input = run(node.input);
        // Project applies expressions to produce new columns
        let result = input;
        for (const expr of node.exprs) {
          const series = expr.evaluate(input as DataFrame<Record<string, unknown>>);
          result = result.withColumn(series.name, expr as Expr<unknown>);
        }
        return result;
      }

      case 'groupby': {
        const input = run(node.input);
        const gb = input.groupBy(...node.keys);
        // Convert AnyExpr[] to Record<string, AggExpr> for GroupBy.agg()
        const specs: Record<string, AggExpr<unknown>> = {};
        for (const aggExpr of node.aggs) {
          const agg = aggExpr as AggExpr<unknown>;
          // Use the column name as the output key
          const deps = agg.dependencies;
          const outputName = deps[0] ?? 'value';
          specs[outputName] = agg;
        }
        return gb.agg(specs);
      }

      case 'sort': {
        const input = run(node.input);
        return input.sortBy(node.by, node.descending ? 'desc' : 'asc');
      }

      case 'limit': {
        const input = run(node.input);
        return input.head(node.n);
      }

      case 'distinct': {
        const input = run(node.input);
        return input.unique(node.subset);
      }

      case 'join': {
        const left = run(node.left);
        const right = run(node.right);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return left.join(right as DataFrame<any>, node.on, node.how as 'inner' | 'left' | 'right' | 'outer');
      }
    }
  }

  return run(plan) as DataFrame<S>;
}
