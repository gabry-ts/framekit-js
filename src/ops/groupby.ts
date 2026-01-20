import type { DataFrame } from '../dataframe';
import { Column } from '../storage/column';
import { ColumnNotFoundError } from '../errors';

type DataFrameConstructor = new <S extends Record<string, unknown>>(
  columns: Map<string, Column<unknown>>,
  columnOrder: string[],
) => DataFrame<S>;

export class GroupBy<
  S extends Record<string, unknown> = Record<string, unknown>,
  GroupKeys extends string = string,
> {
  private readonly _df: DataFrame<S>;
  private readonly _keys: GroupKeys[];
  private readonly _groupMap: Map<string, number[]>;

  constructor(df: DataFrame<S>, keys: GroupKeys[]) {
    this._df = df;
    this._keys = keys;

    // Validate keys exist
    for (const key of keys) {
      if (!df.columns.includes(key)) {
        throw new ColumnNotFoundError(key, df.columns);
      }
    }

    // Build hash map: serialized-key -> row-index-array
    this._groupMap = new Map<string, number[]>();
    const columns: Column<unknown>[] = keys.map((k) => df.col(k).column);

    for (let i = 0; i < df.length; i++) {
      const keyStr = this._serializeKey(columns, i);
      const group = this._groupMap.get(keyStr);
      if (group) {
        group.push(i);
      } else {
        this._groupMap.set(keyStr, [i]);
      }
    }
  }

  get keys(): GroupKeys[] {
    return [...this._keys];
  }

  get dataframe(): DataFrame<S> {
    return this._df;
  }

  get groupMap(): Map<string, number[]> {
    return this._groupMap;
  }

  nGroups(): number {
    return this._groupMap.size;
  }

  groups(): Map<string, DataFrame<S>> {
    const result = new Map<string, DataFrame<S>>();
    for (const [key, indices] of this._groupMap) {
      result.set(key, this._buildSubFrame(indices));
    }
    return result;
  }

  private _serializeKey(columns: Column<unknown>[], index: number): string {
    const parts: string[] = [];
    for (const column of columns) {
      const v = column.get(index);
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

  private _buildSubFrame(indices: number[]): DataFrame<S> {
    const int32Indices = new Int32Array(indices);
    const newColumns = new Map<string, Column<unknown>>();
    const columnOrder = this._df.columns;
    for (const name of columnOrder) {
      newColumns.set(name, this._df.col(name).column.take(int32Indices));
    }
    // Use the same constructor as the source DataFrame
    const Ctor = this._df.constructor as DataFrameConstructor;
    return new Ctor<S>(newColumns, columnOrder);
  }
}
