import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column, Int32Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ShapeMismatchError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

function serializeRow(df: DataFrame<Record<string, unknown>>, index: number, cols: string[]): string {
  const parts: string[] = [];
  for (const name of cols) {
    const v = df.col(name).get(index);
    if (v === null) {
      parts.push('\0null');
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

function validateMatchingSchemas(
  left: DataFrame<Record<string, unknown>>,
  right: DataFrame<Record<string, unknown>>,
): void {
  const leftCols = left.columns;
  const rightCols = right.columns;

  if (leftCols.length !== rightCols.length) {
    throw new ShapeMismatchError(
      `Schema mismatch: left has ${leftCols.length} columns, right has ${rightCols.length} columns`,
    );
  }

  const leftDtypes = left.dtypes;
  const rightDtypes = right.dtypes;

  for (const name of leftCols) {
    if (!(name in rightDtypes)) {
      throw new ShapeMismatchError(
        `Schema mismatch: column '${name}' not found in right DataFrame`,
      );
    }
    if (leftDtypes[name] !== rightDtypes[name]) {
      throw new ShapeMismatchError(
        `Schema mismatch: column '${name}' has dtype '${leftDtypes[name]}' in left but '${rightDtypes[name]}' in right`,
      );
    }
  }
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
      return Float64Column.from(values as (number | null)[]);
  }
}

function collectRows(
  df: DataFrame<Record<string, unknown>>,
  indices: number[],
): DataFrame<Record<string, unknown>> {
  const cols = df.columns;
  const dtypes = df.dtypes;
  const resultColumns = new Map<string, Column<unknown>>();

  for (const name of cols) {
    const series = df.col(name);
    const values: unknown[] = [];
    for (const idx of indices) {
      values.push(series.get(idx));
    }
    resultColumns.set(name, buildColumnFromValues(dtypes[name]!, values));
  }

  const Ctor = df.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, [...cols]);
}

export function union(
  left: DataFrame<Record<string, unknown>>,
  right: DataFrame<Record<string, unknown>>,
): DataFrame<Record<string, unknown>> {
  validateMatchingSchemas(left, right);

  const cols = left.columns;
  const dtypes = left.dtypes;
  const seen = new Set<string>();

  // Collect values per column
  const valueArrays = new Map<string, unknown[]>();
  for (const name of cols) {
    valueArrays.set(name, []);
  }

  // Add unique rows from left
  for (let i = 0; i < left.length; i++) {
    const key = serializeRow(left, i, cols);
    if (!seen.has(key)) {
      seen.add(key);
      for (const name of cols) {
        valueArrays.get(name)!.push(left.col(name).get(i));
      }
    }
  }

  // Add unique rows from right
  for (let i = 0; i < right.length; i++) {
    const key = serializeRow(right, i, cols);
    if (!seen.has(key)) {
      seen.add(key);
      for (const name of cols) {
        valueArrays.get(name)!.push(right.col(name).get(i));
      }
    }
  }

  const resultColumns = new Map<string, Column<unknown>>();
  for (const name of cols) {
    resultColumns.set(name, buildColumnFromValues(dtypes[name]!, valueArrays.get(name)!));
  }

  const Ctor = left.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, [...cols]);
}

export function intersection(
  left: DataFrame<Record<string, unknown>>,
  right: DataFrame<Record<string, unknown>>,
): DataFrame<Record<string, unknown>> {
  validateMatchingSchemas(left, right);

  const cols = left.columns;

  // Build set of keys from right
  const rightKeys = new Set<string>();
  for (let i = 0; i < right.length; i++) {
    rightKeys.add(serializeRow(right, i, cols));
  }

  // Find rows in left that exist in right (with deduplication)
  const seen = new Set<string>();
  const indices: number[] = [];
  for (let i = 0; i < left.length; i++) {
    const key = serializeRow(left, i, cols);
    if (rightKeys.has(key) && !seen.has(key)) {
      seen.add(key);
      indices.push(i);
    }
  }

  return collectRows(left, indices);
}

export function difference(
  left: DataFrame<Record<string, unknown>>,
  right: DataFrame<Record<string, unknown>>,
): DataFrame<Record<string, unknown>> {
  validateMatchingSchemas(left, right);

  const cols = left.columns;

  // Build set of keys from right
  const rightKeys = new Set<string>();
  for (let i = 0; i < right.length; i++) {
    rightKeys.add(serializeRow(right, i, cols));
  }

  // Find rows in left that do NOT exist in right (with deduplication)
  const seen = new Set<string>();
  const indices: number[] = [];
  for (let i = 0; i < left.length; i++) {
    const key = serializeRow(left, i, cols);
    if (!rightKeys.has(key) && !seen.has(key)) {
      seen.add(key);
      indices.push(i);
    }
  }

  return collectRows(left, indices);
}
