/**
 * Browser-compatible entry point for FrameKit.
 *
 * Excludes Node.js-specific APIs:
 * - CLI tool
 * - Worker thread parallelism (os, worker_threads)
 * - Streaming file scanners (fs.createReadStream)
 *
 * File I/O methods on DataFrame (fromCSV with file path, toCSV with file path, etc.)
 * are still available but will throw IOError at runtime if `fs` is not available.
 */

// Types
export { DType } from './types/dtype';
export type { DTypeToTS } from './types/dtype';
export type { DataType, Schema } from './types/schema';
export type {
  CSVReadOptions,
  CSVWriteOptions,
  JSONReadOptions,
  JSONWriteOptions,
  NDJSONReadOptions,
  PrintOptions,
  SampleOptions,
  ExcelReadOptions,
  ExcelWriteOptions,
  ParquetReadOptions,
  ParquetWriteOptions,
  SQLWriteOptions,
} from './types/options';

// Errors
export {
  ErrorCode,
  FrameKitError,
  ColumnNotFoundError,
  TypeMismatchError,
  ShapeMismatchError,
  ParseError,
  IOError,
} from './errors';

// Storage
export { BitArray } from './storage/bitarray';
export { Column } from './storage/column';
export { Float64Column, Int32Column } from './storage/numeric';
export { Utf8Column } from './storage/string';
export { BooleanColumn } from './storage/boolean';
export { DateColumn } from './storage/date';
export { ObjectColumn } from './storage/object';

// Series & Accessors
export { Series } from './series';
export { StringAccessor } from './accessors/string-accessor';
export { DateAccessor } from './accessors/date-accessor';
export type { TimeUnit } from './accessors/date-accessor';

// DataFrame
export { DataFrame } from './dataframe';

// GroupBy
export { GroupBy } from './ops/groupby';

// Join, Pivot, Melt types
export type { JoinType, JoinOnMapping, JoinOptions } from './ops/join';
export type { PivotOptions, PivotAggFunc } from './ops/pivot';
export type { MeltOptions } from './ops/melt';

// Expressions
export {
  Expr,
  NamedExpr,
  col,
  lit,
  AggExpr,
  SumAggExpr,
  MeanAggExpr,
  CountAggExpr,
  CountDistinctAggExpr,
  MinAggExpr,
  MaxAggExpr,
  StdAggExpr,
  FirstAggExpr,
  LastAggExpr,
  ListAggExpr,
  ModeAggExpr,
} from './expr/expr';
export { when, WhenBuilder, ThenBuilder } from './expr/conditional-expr';

// Lazy
export { LazyFrame, LazyGroupBy } from './lazy';
export type {
  PlanNode,
  ScanNode,
  FilterNode,
  SelectNode,
  ProjectNode,
  GroupByNode,
  JoinNode,
  SortNode,
  LimitNode,
  DistinctNode,
} from './engine/lazy/plan';
export { execute } from './engine/lazy/executor';
export { optimize } from './engine/lazy/optimizer';

// Expression accessors
export { StringExprAccessor } from './expr/string-expr';
export { DateExprAccessor } from './expr/date-expr';

// Window functions
export {
  WindowRankExpr,
  WindowDenseRankExpr,
  WindowRowNumberExpr,
  WindowPercentRankExpr,
  WindowNtileExpr,
  CumSumExpr,
  CumMaxExpr,
  CumMinExpr,
  CumProdExpr,
  CumCountExpr,
  ShiftExpr,
  DiffExpr,
  PctChangeExpr,
  PartitionedWindowExpr,
  OrderedWindowExpr,
} from './ops/window';

// Side-effect imports for prototype augmentation
import './expr/expr-accessors';
import './ops/window';

import { DataFrame } from './dataframe';

/**
 * Shorthand factory for creating a DataFrame from an array of objects.
 * Equivalent to DataFrame.fromRows(rows).
 */
export function df<S extends Record<string, unknown> = Record<string, unknown>>(
  rows: object[],
): DataFrame<S> {
  return DataFrame.fromRows<S>(rows);
}
