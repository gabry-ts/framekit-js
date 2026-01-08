import { DType } from '../types/dtype';
import { ErrorCode, FrameKitError } from '../errors';
import { BitArray } from './bitarray';
import { Column } from './column';

export class Utf8Column extends Column<string> {
  readonly dtype = DType.Utf8;
  private readonly _data: string[];

  constructor(data: string[], nullMask?: BitArray) {
    super(data.length, nullMask);
    this._data = data;
  }

  get(index: number): string | null {
    this._boundsCheck(index);
    if (!this._nullMask.get(index)) {
      return null;
    }
    return this._data[index]!;
  }

  slice(start: number, end: number): Utf8Column {
    const sliced = this._data.slice(start, end);
    const mask = new BitArray(sliced.length);
    for (let i = 0; i < sliced.length; i++) {
      mask.set(i, this._nullMask.get(start + i));
    }
    return new Utf8Column(sliced, mask);
  }

  clone(): Utf8Column {
    return new Utf8Column([...this._data], this._nullMask.clone());
  }

  filter(mask: Column<boolean>): Utf8Column {
    if (mask.length !== this._length) {
      throw new FrameKitError(
        ErrorCode.SHAPE_MISMATCH,
        `Filter mask length ${mask.length} does not match column length ${this._length}`,
      );
    }
    const indices: number[] = [];
    for (let i = 0; i < mask.length; i++) {
      if (mask.get(i) === true) {
        indices.push(i);
      }
    }
    return this._takeByIndices(indices);
  }

  take(indices: Int32Array): Utf8Column {
    const idxArray: number[] = [];
    for (let i = 0; i < indices.length; i++) {
      idxArray.push(indices[i]!);
    }
    return this._takeByIndices(idxArray);
  }

  static from(values: (string | null)[]): Utf8Column {
    const data: string[] = new Array<string>(values.length);
    const mask = new BitArray(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        data[i] = v;
        mask.set(i, true);
      } else {
        data[i] = '';
      }
    }
    return new Utf8Column(data, mask);
  }

  private _boundsCheck(index: number): void {
    if (index < 0 || index >= this._length) {
      throw new FrameKitError(
        ErrorCode.INVALID_OPERATION,
        `Index ${index} out of bounds for column of length ${this._length}`,
      );
    }
  }

  private _takeByIndices(indices: number[]): Utf8Column {
    const data: string[] = new Array<string>(indices.length);
    const mask = new BitArray(indices.length);
    for (let i = 0; i < indices.length; i++) {
      const idx = indices[i]!;
      this._boundsCheck(idx);
      data[i] = this._data[idx]!;
      mask.set(i, this._nullMask.get(idx));
    }
    return new Utf8Column(data, mask);
  }
}
