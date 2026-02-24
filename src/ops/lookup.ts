import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column, Int32Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ObjectColumn } from '../storage/object';
import { ColumnNotFoundError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

function serializeKey(columns: Column<unknown>[], index: number): string | null {
  const parts: string[] = [];
  for (const column of columns) {
    const v = column.get(index);
    if (v === null) {
      return null;
    } else if (v instanceof Date) {
      parts.push(`\0d${v.getTime()}`);
    } else if (typeof v === 'number' || typeof v === 'string' || typeof v === 'boolean') {
      parts.push(`\0${typeof v}${String(v)}`);
    } else {
      parts.push(`\0obj${JSON.stringify(v)}`);
    }
  }
  return parts.join('\x01');
}

function buildColumnFromValues(dtype: DType, values: unknown[]): Column<unknown> {
  switch (dtype) {
    case DType.Float64:
      return Float64Column.from(values as (number | null)[]);
    case DType.Int32:
      return Int32Column.from(values as (number | null)[]);
    case DType.Utf8:
      return Utf8Column.from(values as (string | null)[]);
    case DType.Boolean:
      return BooleanColumn.from(values as (boolean | null)[]);
    case DType.Date:
      return DateColumn.from(values as (Date | null)[]);
    default:
      return new ObjectColumn(values);
  }
}

export function lookup<
  L extends Record<string, unknown>,
  R extends Record<string, unknown>,
>(
  left: DataFrame<L>,
  right: DataFrame<R>,
  on: string,
  values?: string[],
): DataFrame<Record<string, unknown>> {
  // Validate key column exists in both DataFrames
  if (!left.columns.includes(on)) {
    throw new ColumnNotFoundError(on, left.columns);
  }
  if (!right.columns.includes(on)) {
    throw new ColumnNotFoundError(on, right.columns);
  }

  // Determine which columns to pull from right
  const valueCols = values ?? right.columns.filter((c) => c !== on);
  for (const col of valueCols) {
    if (!right.columns.includes(col)) {
      throw new ColumnNotFoundError(col, right.columns);
    }
  }

  // Build hash map from right: key -> first matching row index
  const rightKeyCol = [right.col(on).column];
  const lookupMap = new Map<string, number>();
  for (let i = 0; i < right.length; i++) {
    const keyStr = serializeKey(rightKeyCol, i);
    if (keyStr === null) continue;
    // First match wins — don't overwrite
    if (!lookupMap.has(keyStr)) {
      lookupMap.set(keyStr, i);
    }
  }

  // Probe left side and build result columns
  const leftKeyCol = [left.col(on).column];
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  // Copy all left columns
  for (const colName of left.columns) {
    resultColumns.set(colName, left.col(colName).column);
    columnOrder.push(colName);
  }

  // Build value columns from right
  for (const colName of valueCols) {
    const srcCol = right.col(colName).column;
    const vals: unknown[] = new Array(left.length);
    for (let i = 0; i < left.length; i++) {
      const keyStr = serializeKey(leftKeyCol, i);
      if (keyStr !== null) {
        const ri = lookupMap.get(keyStr);
        vals[i] = ri !== undefined ? srcCol.get(ri) : null;
      } else {
        vals[i] = null;
      }
    }
    const outputName = left.columns.includes(colName) ? colName : colName;
    resultColumns.set(outputName, buildColumnFromValues(srcCol.dtype, vals));
    columnOrder.push(outputName);
  }

  const Ctor = left.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
