/**
 * Compat verb functions — Arquero-style derive, rollup, fold, orderby, etc.
 */

import { DataFrame } from '../dataframe';
import type { ExprRecord } from './types';

/**
 * Arquero-style derive: create new columns from row-level expressions.
 *
 * ```ts
 * derive(df, { total: d => d.price * d.qty, label: d => `item-${d.id}` })
 * ```
 *
 * Existing columns are preserved. New columns are appended in insertion order.
 * If a derived column name matches an existing column, the existing column is overwritten.
 */
export function derive(df: DataFrame, exprs: ExprRecord): DataFrame {
  const len = df.length;
  const exprEntries = Object.entries(exprs);

  // Pre-compute derived column values
  const derivedColumns: Record<string, unknown[]> = {};
  for (const [name] of exprEntries) {
    derivedColumns[name] = new Array(len);
  }

  for (let i = 0; i < len; i++) {
    const row = df.row(i);
    for (const [name, fn] of exprEntries) {
      derivedColumns[name]![i] = fn(row);
    }
  }

  // Build result: start with original df, then add/overwrite derived columns
  let result: DataFrame = df;
  for (const [name, values] of Object.entries(derivedColumns)) {
    result = result.withColumn(name, values);
  }

  return result;
}
