import { DType } from '../types/dtype';
import { ErrorCode, FrameKitError } from '../errors';
import { BitArray } from './bitarray';
import { Column } from './column';

/**
 * String interning storage for low-cardinality columns.
 * Stores unique strings in a dictionary + index array instead of string[].
 */
interface InternedStorage {
  dictionary: string[];
  indices: Uint32Array;
}

export class Utf8Column extends Column<string> {
  readonly dtype = DType.Utf8;
  private readonly _data: string[] | null;
  private readonly _interned: InternedStorage | null;
  private readonly _allValid: boolean;

  constructor(data: string[], nullMask?: BitArray, interned?: InternedStorage) {
    super(data.length, nullMask);
    if (interned) {
      this._data = null;
      this._interned = interned;
    } else {
      this._data = data;
      this._interned = null;
    }
    this._allValid = nullMask === undefined;
  }

  /** Whether this column uses interned (dictionary-encoded) storage. */
  get isInterned(): boolean {
    return this._interned !== null;
  }

  /** Raw interned storage — exposed for fast-path operations (filter, groupby, sort). */
  get internedStorage(): Readonly<InternedStorage> | null {
    return this._interned;
  }

  /** Whether all values are non-null. */
  get allValid(): boolean {
    return this._allValid;
  }

  /** Raw null mask. */
  get nullMask(): BitArray {
    return this._nullMask;
  }

  /** Raw data array (only for non-interned columns). */
  get rawData(): string[] | null {
    return this._data;
  }

  get(index: number): string | null {
    this._boundsCheck(index);
    if (this._allValid) {
      if (this._interned) {
        return this._interned.dictionary[this._interned.indices[index]!]!;
      }
      return this._data![index]!;
    }
    if (!this._nullMask.getUnsafe(index)) {
      return null;
    }
    if (this._interned) {
      return this._interned.dictionary[this._interned.indices[index]!]!;
    }
    return this._data![index]!;
  }

  slice(start: number, end: number): Utf8Column {
    const len = end - start;
    if (this._allValid) {
      if (this._interned) {
        const slicedIndices = this._interned.indices.slice(start, end);
        return new Utf8Column(new Array<string>(len), undefined, {
          dictionary: this._interned.dictionary,
          indices: slicedIndices,
        });
      }
      return new Utf8Column(this._data!.slice(start, end));
    }
    const mask = new BitArray(len);
    for (let i = 0; i < len; i++) {
      mask.setUnsafe(i, this._nullMask.getUnsafe(start + i));
    }
    if (this._interned) {
      const slicedIndices = this._interned.indices.slice(start, end);
      return new Utf8Column(new Array<string>(len), mask, {
        dictionary: this._interned.dictionary,
        indices: slicedIndices,
      });
    }
    const sliced = this._data!.slice(start, end);
    return new Utf8Column(sliced, mask);
  }

  clone(): Utf8Column {
    if (this._allValid) {
      if (this._interned) {
        return new Utf8Column(new Array<string>(this._length), undefined, {
          dictionary: [...this._interned.dictionary],
          indices: new Uint32Array(this._interned.indices),
        });
      }
      return new Utf8Column([...this._data!]);
    }
    if (this._interned) {
      return new Utf8Column(new Array<string>(this._length), this._nullMask.clone(), {
        dictionary: [...this._interned.dictionary],
        indices: new Uint32Array(this._interned.indices),
      });
    }
    return new Utf8Column([...this._data!], this._nullMask.clone());
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
    const len = indices.length;
    const mask = this._allValid ? undefined : new BitArray(len);
    if (this._interned) {
      const newIndices = new Uint32Array(len);
      for (let i = 0; i < len; i++) {
        const idx = indices[i]!;
        newIndices[i] = this._interned.indices[idx]!;
        if (mask) {
          mask.setUnsafe(i, this._nullMask.getUnsafe(idx));
        }
      }
      return new Utf8Column(new Array<string>(len), mask, {
        dictionary: this._interned.dictionary,
        indices: newIndices,
      });
    }

    const data: string[] = new Array<string>(len);
    for (let i = 0; i < len; i++) {
      const idx = indices[i]!;
      data[i] = this._data![idx]!;
      if (mask) {
        mask.setUnsafe(i, this._nullMask.getUnsafe(idx));
      }
    }
    return new Utf8Column(data, mask);
  }

  estimatedMemoryBytes(): number {
    let bytes = this._nullMask.byteLength;
    if (this._interned) {
      // Dictionary + Uint32Array indices
      for (const s of this._interned.dictionary) {
        bytes += s.length * 2;
      }
      bytes += this._interned.indices.byteLength;
    } else {
      for (let i = 0; i < this._length; i++) {
        if (this._nullMask.get(i)) {
          bytes += this._data![i]!.length * 2;
        }
      }
    }
    return bytes;
  }

  /**
   * Single-pass construction from values array.
   * Builds dictionary, indices, and null mask in one loop.
   * Falls back to plain storage if cardinality > 50%.
   */
  static from(values: (string | null)[]): Utf8Column {
    const len = values.length;
    if (len === 0) {
      return new Utf8Column([]);
    }

    // Single-pass: build dictionary + indices + null mask simultaneously
    const dictMap = new Map<string, number>();
    const dictionary: string[] = [];
    const indices = new Uint32Array(len);
    const mask = new BitArray(len);
    let hasNull = false;
    let dictSize = 0;

    for (let i = 0; i < len; i++) {
      const v = values[i];
      if (v !== null && v !== undefined) {
        mask.set(i, true);
        let idx = dictMap.get(v);
        if (idx === undefined) {
          idx = dictSize++;
          dictMap.set(v, idx);
          dictionary.push(v);
        }
        indices[i] = idx;
      } else {
        hasNull = true;
      }
    }

    // Decide interning: if unique < 50% of rows, use interned storage
    if (dictSize > 0 && dictSize < len * 0.5) {
      return hasNull
        ? new Utf8Column(new Array<string>(len), mask, { dictionary, indices })
        : new Utf8Column(new Array<string>(len), undefined, { dictionary, indices });
    }

    // High cardinality — use plain storage
    const data: string[] = new Array<string>(len);
    for (let i = 0; i < len; i++) {
      data[i] = values[i] ?? '';
    }

    return hasNull ? new Utf8Column(data, mask) : new Utf8Column(data);
  }

  /**
   * Construct from pre-interned data.
   * Used when caller already knows the dictionary (e.g., from CSV parser).
   */
  static fromPreInterned(
    dictionary: string[],
    indices: Uint32Array,
    nullMask?: BitArray,
  ): Utf8Column {
    const len = indices.length;
    return new Utf8Column(new Array<string>(len), nullMask, { dictionary, indices });
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
    const len = indices.length;
    const mask = new BitArray(len);

    if (this._interned) {
      const newIndices = new Uint32Array(len);
      for (let i = 0; i < len; i++) {
        const idx = indices[i]!;
        newIndices[i] = this._interned.indices[idx]!;
        mask.setUnsafe(i, this._nullMask.getUnsafe(idx));
      }
      return new Utf8Column(new Array<string>(len), mask, {
        dictionary: this._interned.dictionary,
        indices: newIndices,
      });
    }

    const data: string[] = new Array<string>(len);
    for (let i = 0; i < len; i++) {
      const idx = indices[i]!;
      data[i] = this._data![idx]!;
      mask.setUnsafe(i, this._nullMask.getUnsafe(idx));
    }
    return new Utf8Column(data, mask);
  }
}
