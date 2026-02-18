/**
 * Compat-specific type definitions for Arquero-style API compatibility.
 */

/** A column selector that matches all columns. */
export interface AllSelector {
  readonly kind: 'all';
}

/** A column selector that excludes specified columns. */
export interface NotSelector {
  readonly kind: 'not';
  readonly columns: readonly string[];
}

/** A column selector for a contiguous range of columns. */
export interface RangeSelector {
  readonly kind: 'range';
  readonly start: string;
  readonly end: string;
}

/** A descending sort specification. */
export interface DescSpec {
  readonly kind: 'desc';
  readonly column: string;
}

/** Union of all column selector types. */
export type ColumnSelector = AllSelector | NotSelector | RangeSelector;

/** A sort specification: plain column name (ascending) or DescSpec. */
export type SortSpec = string | DescSpec;

/** Expression function passed to derive/rollup. */
export type ExprFn<T = unknown> = (d: Record<string, unknown>) => T;

/** A record of named expression functions. */
export type ExprRecord = Record<string, ExprFn>;
