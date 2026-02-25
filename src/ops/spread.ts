import type { DataFrame } from '../dataframe';
import { Column } from '../storage/column';
import { ObjectColumn } from '../storage/object';
import { ColumnNotFoundError } from '../errors';

export interface SpreadOptions {
  /** Maximum number of output columns (default: unlimited) */
  limit?: number;
  /** Custom naming function: (sourceCol, index|key) => outputColName */
  name?: (sourceCol: string, indexOrKey: string | number) => string;
  /** Whether to drop the source column from the result (default: false) */
  drop?: boolean;
}

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export function spread<S extends Record<string, unknown>>(
  df: DataFrame<S>,
  column: string,
  options: SpreadOptions = {},
): DataFrame<Record<string, unknown>> {
  if (!df.columns.includes(column)) {
    throw new ColumnNotFoundError(column, df.columns);
  }

  const { limit, name: nameFn, drop = false } = options;
  const srcCol = df.col(column as string & keyof S).column;
  const len = df.length;

  // Scan values to determine expansion mode (array vs object) and max width
  let isObject = false;
  let maxArrayLen = 0;
  const objectKeys = new Set<string>();

  for (let i = 0; i < len; i++) {
    const val = srcCol.get(i);
    if (val === null) continue;
    if (Array.isArray(val)) {
      maxArrayLen = Math.max(maxArrayLen, val.length);
    } else if (typeof val === 'object') {
      isObject = true;
      for (const key of Object.keys(val as Record<string, unknown>)) {
        objectKeys.add(key);
      }
    }
  }

  // Build output columns
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  // Copy existing columns (optionally dropping source)
  for (const colName of df.columns) {
    if (drop && colName === column) continue;
    resultColumns.set(colName, df.col(colName as string & keyof S).column);
    columnOrder.push(colName);
  }

  if (isObject) {
    // Object expansion: one column per unique key
    const keys = [...objectKeys];
    const effectiveKeys = limit !== undefined ? keys.slice(0, limit) : keys;

    for (const key of effectiveKeys) {
      const outName = nameFn ? nameFn(column, key) : `${column}_${key}`;
      const values: unknown[] = new Array(len);
      for (let i = 0; i < len; i++) {
        const val = srcCol.get(i);
        if (val === null || typeof val !== 'object' || Array.isArray(val)) {
          values[i] = null;
        } else {
          const obj = val as Record<string, unknown>;
          values[i] = key in obj ? obj[key] : null;
        }
      }
      resultColumns.set(outName, ObjectColumn.from(values));
      columnOrder.push(outName);
    }
  } else {
    // Array expansion: one column per index position
    const width = limit !== undefined ? Math.min(maxArrayLen, limit) : maxArrayLen;

    for (let idx = 0; idx < width; idx++) {
      const outName = nameFn ? nameFn(column, idx) : `${column}_${String(idx)}`;
      const values: unknown[] = new Array(len);
      for (let i = 0; i < len; i++) {
        const val = srcCol.get(i);
        if (val === null || !Array.isArray(val) || idx >= val.length) {
          values[i] = null;
        } else {
          values[i] = val[idx] as unknown;
        }
      }
      resultColumns.set(outName, ObjectColumn.from(values));
      columnOrder.push(outName);
    }
  }

  const Ctor = df.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
