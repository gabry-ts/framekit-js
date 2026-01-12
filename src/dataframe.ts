import { DType } from './types/dtype';
import { ColumnNotFoundError, ErrorCode, FrameKitError, ShapeMismatchError } from './errors';
import { Column } from './storage/column';
import { Float64Column, Int32Column } from './storage/numeric';
import { Utf8Column } from './storage/string';
import { BooleanColumn } from './storage/boolean';
import { DateColumn } from './storage/date';
import { Series } from './series';

export class DataFrame<S extends Record<string, unknown> = Record<string, unknown>> {
  private readonly _columns: Map<string, Column<unknown>>;
  private readonly _columnOrder: string[];

  constructor(columns: Map<string, Column<unknown>>, columnOrder: string[]) {
    this._columns = columns;
    this._columnOrder = columnOrder;
  }

  get shape(): [number, number] {
    return [this.length, this._columnOrder.length];
  }

  get columns(): string[] {
    return [...this._columnOrder];
  }

  get dtypes(): Record<string, DType> {
    const result: Record<string, DType> = {};
    for (const name of this._columnOrder) {
      result[name] = this._columns.get(name)!.dtype;
    }
    return result;
  }

  get length(): number {
    if (this._columnOrder.length === 0) return 0;
    return this._columns.get(this._columnOrder[0]!)!.length;
  }

  col<K extends string & keyof S>(name: K): Series<S[K]> {
    const column = this._columns.get(name);
    if (!column) {
      throw new ColumnNotFoundError(name, this._columnOrder);
    }
    return new Series<S[K]>(name, column as Column<S[K]>);
  }

  row(index: number): S {
    if (index < 0 || index >= this.length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Row index ${index} out of bounds for DataFrame with ${this.length} rows`,
      );
    }
    const result: Record<string, unknown> = {};
    for (const name of this._columnOrder) {
      result[name] = this._columns.get(name)!.get(index);
    }
    return result as S;
  }

  *[Symbol.iterator](): Iterator<S> {
    for (let i = 0; i < this.length; i++) {
      yield this.row(i);
    }
  }

  rows(): Iterator<S> {
    return this[Symbol.iterator]();
  }

  toArray(): S[] {
    const result: S[] = [];
    for (let i = 0; i < this.length; i++) {
      result.push(this.row(i));
    }
    return result;
  }

  clone(): DataFrame<S> {
    const clonedColumns = new Map<string, Column<unknown>>();
    for (const [name, col] of this._columns) {
      clonedColumns.set(name, col.clone());
    }
    return new DataFrame<S>(clonedColumns, [...this._columnOrder]);
  }

  select<K extends string & keyof S>(...columns: K[]): DataFrame<Pick<S, K>> {
    for (const name of columns) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of columns) {
      newColumns.set(name, this._columns.get(name)!);
    }
    return new DataFrame<Pick<S, K>>(newColumns, [...columns]);
  }

  drop<K extends string & keyof S>(...columns: K[]): DataFrame<Omit<S, K>> {
    for (const name of columns) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }
    const dropSet = new Set<string>(columns);
    const newColumns = new Map<string, Column<unknown>>();
    const newOrder: string[] = [];
    for (const name of this._columnOrder) {
      if (!dropSet.has(name)) {
        newColumns.set(name, this._columns.get(name)!);
        newOrder.push(name);
      }
    }
    return new DataFrame<Omit<S, K>>(newColumns, newOrder);
  }

  head(n = 5): DataFrame<S> {
    return this.slice(0, Math.min(n, this.length));
  }

  tail(n = 5): DataFrame<S> {
    const start = Math.max(0, this.length - n);
    return this.slice(start, this.length);
  }

  slice(start: number, end?: number): DataFrame<S> {
    const resolvedEnd = end === undefined ? this.length : Math.min(end, this.length);
    const resolvedStart = Math.max(0, start);
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.slice(resolvedStart, resolvedEnd));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  withColumn<K extends string, V>(
    name: K,
    valuesOrFn: V[] | ((row: S) => V),
  ): DataFrame<S & Record<K, V>> {
    type Result = S & Record<K, V>;
    let columnValues: V[];

    if (typeof valuesOrFn === 'function') {
      const fn = valuesOrFn as (row: S) => V;
      columnValues = [];
      for (let i = 0; i < this.length; i++) {
        columnValues.push(fn(this.row(i)));
      }
    } else {
      columnValues = valuesOrFn;
      if (columnValues.length !== this.length) {
        throw new ShapeMismatchError(
          `Column '${name}' has length ${columnValues.length}, expected ${this.length}`,
        );
      }
    }

    const dtype = detectDType(columnValues as unknown[]);
    const col = buildColumn(dtype, columnValues as unknown[]);

    const newColumns = new Map<string, Column<unknown>>(this._columns);
    newColumns.set(name, col);

    const newOrder = this._columnOrder.includes(name)
      ? [...this._columnOrder]
      : [...this._columnOrder, name];

    return new DataFrame<Result>(newColumns, newOrder);
  }

  rename(mapping: Record<string, string>): DataFrame<S> {
    // Validate that all source columns exist
    for (const oldName of Object.keys(mapping)) {
      if (!this._columns.has(oldName)) {
        throw new ColumnNotFoundError(oldName, this._columnOrder);
      }
    }

    const newColumns = new Map<string, Column<unknown>>();
    const newOrder: string[] = [];

    for (const name of this._columnOrder) {
      const newName = mapping[name] ?? name;
      newColumns.set(newName, this._columns.get(name)!);
      newOrder.push(newName);
    }

    return new DataFrame<S>(newColumns, newOrder);
  }

  filter(predicate: (row: S) => boolean): DataFrame<S> {
    const indices: number[] = [];
    for (let i = 0; i < this.length; i++) {
      if (predicate(this.row(i))) {
        indices.push(i);
      }
    }
    const int32Indices = new Int32Array(indices);
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.take(int32Indices));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  sortBy(
    columns: string | string[],
    order?: 'asc' | 'desc' | ('asc' | 'desc')[],
  ): DataFrame<S> {
    const cols = Array.isArray(columns) ? columns : [columns];
    const orders = Array.isArray(order) ? order : cols.map(() => order ?? 'asc');

    for (const name of cols) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(name, this._columnOrder);
      }
    }

    // Build arrays of raw values for each sort column (avoids repeated row() calls)
    const sortValues: unknown[][] = cols.map((name) => {
      const col = this._columns.get(name)!;
      const vals: unknown[] = [];
      for (let i = 0; i < this.length; i++) {
        vals.push(col.get(i));
      }
      return vals;
    });

    // Create index array and sort it (stable sort in V8/modern JS engines)
    const indices = Array.from({ length: this.length }, (_, i) => i);
    indices.sort((a, b) => {
      for (let c = 0; c < cols.length; c++) {
        const va = sortValues[c]![a]!;
        const vb = sortValues[c]![b]!;
        const aIsNull = va === null || va === undefined;
        const bIsNull = vb === null || vb === undefined;

        if (aIsNull && bIsNull) continue;
        if (aIsNull) return 1; // nulls last
        if (bIsNull) return -1;

        let cmp = 0;
        if (va instanceof Date && vb instanceof Date) {
          cmp = va.getTime() - vb.getTime();
        } else if (typeof va === 'string' && typeof vb === 'string') {
          cmp = va < vb ? -1 : va > vb ? 1 : 0;
        } else if (typeof va === 'number' && typeof vb === 'number') {
          cmp = va - vb;
        } else if (typeof va === 'boolean' && typeof vb === 'boolean') {
          cmp = (va ? 1 : 0) - (vb ? 1 : 0);
        }

        if (cmp !== 0) {
          return orders[c] === 'desc' ? -cmp : cmp;
        }
      }
      return 0;
    });

    const int32Indices = new Int32Array(indices);
    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      newColumns.set(name, this._columns.get(name)!.take(int32Indices));
    }
    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  cast(dtypes: Partial<Record<string & keyof S, DType>>): DataFrame<S> {
    for (const colName of Object.keys(dtypes)) {
      if (!this._columns.has(colName)) {
        throw new ColumnNotFoundError(colName, this._columnOrder);
      }
    }

    const newColumns = new Map<string, Column<unknown>>();
    for (const name of this._columnOrder) {
      const targetDType = (dtypes as Record<string, DType>)[name];
      if (targetDType) {
        const series = new Series<unknown>(name, this._columns.get(name)!);
        const casted = series.cast(targetDType);
        newColumns.set(name, casted.column);
      } else {
        newColumns.set(name, this._columns.get(name)!);
      }
    }

    return new DataFrame<S>(newColumns, [...this._columnOrder]);
  }

  static fromColumns<S extends Record<string, unknown> = Record<string, unknown>>(
    data: Record<string, unknown[]>,
  ): DataFrame<S> {
    const keys = Object.keys(data);
    if (keys.length === 0) {
      return DataFrame.empty<S>();
    }

    // Validate all columns have same length
    const firstKey = keys[0]!;
    const rowCount = data[firstKey]!.length;
    for (const key of keys) {
      if (data[key]!.length !== rowCount) {
        throw new ShapeMismatchError(
          `Column '${key}' has length ${data[key]!.length}, expected ${rowCount}`,
        );
      }
    }

    const columns = new Map<string, Column<unknown>>();
    for (const key of keys) {
      const values = data[key]!;
      const dtype = detectDType(values);
      const col = buildColumn(dtype, values);
      columns.set(key, col);
    }

    return new DataFrame<S>(columns, keys);
  }

  static fromRows<S extends Record<string, unknown> = Record<string, unknown>>(
    rows: object[],
  ): DataFrame<S> {
    if (rows.length === 0) {
      return DataFrame.empty<S>();
    }

    // Collect all column names from all rows (preserving order from first row)
    const columnSet = new Set<string>();
    for (const row of rows) {
      for (const key of Object.keys(row)) {
        columnSet.add(key);
      }
    }
    const columnNames = [...columnSet];

    // Build arrays per column
    const data: Record<string, unknown[]> = {};
    for (const name of columnNames) {
      data[name] = [];
    }
    for (const row of rows) {
      const r = row as Record<string, unknown>;
      for (const name of columnNames) {
        const value = name in r ? r[name] : null;
        data[name]!.push(value === undefined ? null : value);
      }
    }

    return DataFrame.fromColumns<S>(data);
  }

  static empty<S extends Record<string, unknown> = Record<string, unknown>>(): DataFrame<S> {
    return new DataFrame<S>(new Map(), []);
  }
}

function detectDType(values: unknown[]): DType {
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') return DType.Float64;
    if (typeof v === 'string') return DType.Utf8;
    if (typeof v === 'boolean') return DType.Boolean;
    if (v instanceof Date) return DType.Date;
  }
  // Default to Float64 if all nulls
  return DType.Float64;
}

function buildColumn(dtype: DType, values: unknown[]): Column<unknown> {
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
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Unsupported dtype for column construction: ${dtype}`,
      );
  }
}
