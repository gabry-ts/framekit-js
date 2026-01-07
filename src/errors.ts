export enum ErrorCode {
  COLUMN_NOT_FOUND = 'COLUMN_NOT_FOUND',
  TYPE_MISMATCH = 'TYPE_MISMATCH',
  SHAPE_MISMATCH = 'SHAPE_MISMATCH',
  PARSE_ERROR = 'PARSE_ERROR',
  IO_ERROR = 'IO_ERROR',
  OUT_OF_MEMORY = 'OUT_OF_MEMORY',
  INVALID_OPERATION = 'INVALID_OPERATION',
}

export class FrameKitError extends Error {
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = 'FrameKitError';
    this.code = code;
  }
}

export class ColumnNotFoundError extends FrameKitError {
  constructor(column: string, available: string[]) {
    super(
      ErrorCode.COLUMN_NOT_FOUND,
      `Column '${column}' not found. Available columns: [${available.join(', ')}]`,
    );
    this.name = 'ColumnNotFoundError';
  }
}

export class TypeMismatchError extends FrameKitError {
  constructor(message: string) {
    super(ErrorCode.TYPE_MISMATCH, message);
    this.name = 'TypeMismatchError';
  }
}

export class ShapeMismatchError extends FrameKitError {
  constructor(message: string) {
    super(ErrorCode.SHAPE_MISMATCH, message);
    this.name = 'ShapeMismatchError';
  }
}

export class ParseError extends FrameKitError {
  constructor(message: string) {
    super(ErrorCode.PARSE_ERROR, message);
    this.name = 'ParseError';
  }
}

export class IOError extends FrameKitError {
  constructor(message: string) {
    super(ErrorCode.IO_ERROR, message);
    this.name = 'IOError';
  }
}
