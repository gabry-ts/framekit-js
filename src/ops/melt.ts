import type { DataFrame } from '../dataframe';
import { DType } from '../types/dtype';
import { Column } from '../storage/column';
import { Float64Column, Int32Column } from '../storage/numeric';
import { Utf8Column } from '../storage/string';
import { BooleanColumn } from '../storage/boolean';
import { DateColumn } from '../storage/date';
import { ColumnNotFoundError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export interface MeltOptions {
  idVars: string | string[];
  valueVars?: string | string[];
  varName?: string;
  valueName?: string;
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

function detectDType(values: unknown[]): DType {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') return Number.isInteger(v) ? DType.Float64 : DType.Float64;
    if (typeof v === 'string') return DType.Utf8;
    if (typeof v === 'boolean') return DType.Boolean;
    if (v instanceof Date) return DType.Date;
  }
  return DType.Float64;
}

export function melt(
  df: DataFrame<Record<string, unknown>>,
  options: MeltOptions,
): DataFrame<Record<string, unknown>> {
  const idVars = Array.isArray(options.idVars) ? options.idVars : [options.idVars];
  const varName = options.varName ?? 'variable';
  const valueName = options.valueName ?? 'value';

  // Validate idVars exist
  for (const col of idVars) {
    if (!df.columns.includes(col)) {
      throw new ColumnNotFoundError(col, df.columns);
    }
  }

  // Determine valueVars
  let valueVars: string[];
  if (options.valueVars) {
    valueVars = Array.isArray(options.valueVars) ? options.valueVars : [options.valueVars];
    // Validate valueVars exist
    for (const col of valueVars) {
      if (!df.columns.includes(col)) {
        throw new ColumnNotFoundError(col, df.columns);
      }
    }
  } else {
    // All non-idVars columns
    const idSet = new Set(idVars);
    valueVars = df.columns.filter((c) => !idSet.has(c));
  }

  const nRows = df.length;
  const nValueVars = valueVars.length;
  const resultLength = nRows * nValueVars;

  // Build id columns (each value repeated nValueVars times)
  const resultColumns = new Map<string, Column<unknown>>();
  const columnOrder: string[] = [];

  for (const idCol of idVars) {
    const sourceSeries = df.col(idCol);
    const vals: unknown[] = new Array(resultLength);
    for (let i = 0; i < nRows; i++) {
      const v = sourceSeries.get(i);
      for (let j = 0; j < nValueVars; j++) {
        vals[i * nValueVars + j] = v;
      }
    }
    resultColumns.set(idCol, buildColumnFromValues(sourceSeries.column.dtype, vals));
    columnOrder.push(idCol);
  }

  // Build varName column (column names repeated for each row)
  const varValues: (string | null)[] = new Array<string | null>(resultLength);
  for (let i = 0; i < nRows; i++) {
    for (let j = 0; j < nValueVars; j++) {
      varValues[i * nValueVars + j] = valueVars[j]!;
    }
  }
  resultColumns.set(varName, Utf8Column.from(varValues));
  columnOrder.push(varName);

  // Build valueName column (actual values from each value column)
  const valueValues: unknown[] = new Array(resultLength);
  for (let i = 0; i < nRows; i++) {
    for (let j = 0; j < nValueVars; j++) {
      valueValues[i * nValueVars + j] = df.col(valueVars[j]!).get(i);
    }
  }

  // Detect common dtype for value column
  const valueDtype = detectDType(valueValues);
  resultColumns.set(valueName, buildColumnFromValues(valueDtype, valueValues));
  columnOrder.push(valueName);

  const Ctor = df.constructor as DataFrameConstructor;
  return new Ctor<Record<string, unknown>>(resultColumns, columnOrder);
}
