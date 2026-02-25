import type { DataFrame } from '../dataframe';
import { Column } from '../storage/column';
import { ObjectColumn } from '../storage/object';
import { ColumnNotFoundError } from '../errors';

export interface UnrollOptions {
  /** If provided, adds an index column tracking position within each original array */
  index?: string;
}

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export function unroll<S extends Record<string, unknown>>(
  df: DataFrame<S>,
  columns: string | string[],
  options: UnrollOptions = {},
): DataFrame<Record<string, unknown>> {
  const cols = Array.isArray(columns) ? columns : [columns];

  // Validate all columns exist
  for (const c of cols) {
    if (!df.columns.includes(c)) {
      throw new ColumnNotFoundError(c, df.columns);
    }
  }

  const len = df.length;
  const nonUnrollCols = df.columns.filter(c => !cols.includes(c));

  // For each row, determine the output row count (max array length across unroll columns)
  const rowCounts: number[] = new Array<number>(len);
  for (let i = 0; i < len; i++) {
    let maxLen = 0;
    for (const c of cols) {
      const val = df.col(c as string & keyof S).column.get(i);
      if (Array.isArray(val)) {
        maxLen = Math.max(maxLen, val.length);
      }
    }
    // Empty arrays => 0 rows for this input row
    rowCounts[i] = maxLen;
  }

  const totalRows = rowCounts.reduce((a, b) => a + b, 0);

  // Build output data arrays
  const resultData: Record<string, unknown[]> = {};
  for (const c of nonUnrollCols) {
    resultData[c] = new Array(totalRows);
  }
  for (const c of cols) {
    resultData[c] = new Array(totalRows);
  }

  let indexData: number[] | undefined;
  if (options.index) {
    indexData = new Array(totalRows);
  }

  let outIdx = 0;
  for (let i = 0; i < len; i++) {
    const count = rowCounts[i]!;
    if (count === 0) continue;

    for (let j = 0; j < count; j++) {
      // Repeat non-unroll columns
      for (const c of nonUnrollCols) {
        resultData[c]![outIdx] = df.col(c as string & keyof S).column.get(i);
      }

      // Expand unroll columns element-wise; shorter arrays padded with null
      for (const c of cols) {
        const val = df.col(c as string & keyof S).column.get(i);
        if (Array.isArray(val) && j < val.length) {
          resultData[c]![outIdx] = val[j] as unknown;
        } else {
          resultData[c]![outIdx] = null;
        }
      }

      if (indexData) {
        indexData[outIdx] = j;
      }

      outIdx++;
    }
  }

  // Build result columns
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  for (const c of df.columns) {
    resultColumns.set(c, ObjectColumn.from(resultData[c]!));
    columnOrder.push(c);
  }

  if (options.index && indexData) {
    resultColumns.set(options.index, ObjectColumn.from(indexData));
    columnOrder.push(options.index);
  }

  const Ctor = df.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
