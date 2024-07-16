// SPDX-FileCopyrightText: 2024 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD,
  INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  type InternalValueRepresentation,
  type ValueType,
  ValueTypeProvider,
} from '@jvalue/jayvee-language-server';
import { type pl } from 'nodejs-polars';

export abstract class TableColumn {
  abstract get valueType(): ValueType;
  abstract get name(): string;
  abstract set name(newName: string);
  abstract get length(): number;
  abstract nth(n: number): InternalValueRepresentation | undefined | null;
  abstract clone(): TableColumn;

  abstract isPolars(): this is PolarsTableColumn;
  abstract isTypescript(): this is TsTableColumn;
}

export class PolarsTableColumn extends TableColumn {
  private _valueType: ValueType;
  constructor(
    private _series: pl.Series,
    valueType: ValueType | ValueTypeProvider,
  ) {
    super();
    valueType =
      valueType instanceof ValueTypeProvider
        ? valueType.fromPolarsDType(this._series.dtype)
        : valueType;
    this._valueType = valueType;
  }

  override get valueType(): ValueType {
    return this._valueType;
  }

  override get name(): string {
    return this._series.name;
  }

  override set name(newName: string) {
    this._series.name = newName;
  }

  override get length(): number {
    return this._series.length;
  }

  override nth(n: number): InternalValueRepresentation | undefined | null {
    const nth = this._series.getIndex(n) as unknown;
    if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(nth)) {
      return nth;
    }
    if (nth == null) {
      return null;
    }
    throw new Error(
      `Expected InternalRepresentation not ${JSON.stringify(
        nth,
      )} (${typeof nth})`,
    );
  }

  override clone(): PolarsTableColumn {
    return new PolarsTableColumn(this._series.clone(), this.valueType);
  }

  override isPolars(): this is PolarsTableColumn {
    return true;
  }

  override isTypescript(): this is TsTableColumn {
    return false;
  }

  get series(): Readonly<pl.Series> {
    return this._series;
  }
}

export class TsTableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> extends TableColumn {
  constructor(
    private _name: string,
    private _valueType: ValueType<T>,
    private _values: T[] = [],
  ) {
    super();
  }

  override get valueType(): ValueType<T> {
    return this._valueType;
  }

  override get name(): string {
    return this._name;
  }

  override set name(newName: string) {
    this._name = newName;
  }

  override get length(): number {
    return this._values.length;
  }

  override nth(n: number): T | undefined {
    return this._values.at(n);
  }

  override clone(): TsTableColumn {
    // HACK: This feels wrong, but I didn't find any other solution
    const clonedName: unknown = JSON.parse(JSON.stringify(this._name));
    assert(typeof clonedName === 'string');
    const clonedValues: unknown = JSON.parse(JSON.stringify(this._values));
    assert(
      INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD(clonedValues) &&
        clonedValues.every((e) =>
          this._valueType.isInternalValueRepresentation(e),
        ),
    );
    // INFO: `this.valueType` must not be cloned, because the typesystem depends
    // on it being the same object
    return new TsTableColumn(clonedName, this._valueType, clonedValues);
  }

  override isPolars(): this is PolarsTableColumn {
    return false;
  }

  override isTypescript(): this is TsTableColumn<T> {
    return true;
  }

  push(x: T) {
    if (this._valueType.isInternalValueRepresentation(x)) {
      this._values.push(x);
    }
  }

  drop(rowIdx: number) {
    return this._values.splice(rowIdx, 1);
  }
}
