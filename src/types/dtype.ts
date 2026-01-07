export enum DType {
  Float64 = 'f64',
  Int32 = 'i32',
  Int64 = 'i64',
  Utf8 = 'utf8',
  Boolean = 'bool',
  Date = 'date',
  DateTime = 'datetime',
  Null = 'null',
  BigInt = 'bigint',
  Object = 'object',
}

export type DTypeToTS = {
  [DType.Float64]: number;
  [DType.Int32]: number;
  [DType.Int64]: number;
  [DType.Utf8]: string;
  [DType.Boolean]: boolean;
  [DType.Date]: Date;
  [DType.DateTime]: Date;
  [DType.Null]: null;
  [DType.BigInt]: bigint;
  [DType.Object]: unknown;
};
