// SPDX-FileCopyrightText: 2024 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD,
  type InternalValueRepresentation,
  type ValueType,
} from '@jvalue/jayvee-language-server';
import { type pl } from 'nodejs-polars';

export abstract class TableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> {
  abstract get valueType(): ValueType<T>;
  abstract get name(): string;
  abstract set name(newName: string);
  abstract get length(): number;
  abstract clone(): TableColumn<T>;

  abstract isPolars(): this is PolarsTableColumn<T>;
  abstract isTypescript(): this is TsTableColumn<T>;
}

export class PolarsTableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> extends TableColumn<T> {
  constructor(private _series: pl.Series, private _valueType: ValueType<T>) {
    super();
  }

  override get valueType(): ValueType<T> {
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

  override clone(): PolarsTableColumn<T> {
    return new PolarsTableColumn<T>(this._series.clone(), this._valueType);
  }

  override isPolars(): this is PolarsTableColumn<T> {
    return true;
  }

  override isTypescript(): this is TsTableColumn<T> {
    return false;
  }

  get series(): Readonly<pl.Series> {
    return this._series;
  }
}

export class TsTableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> extends TableColumn<T> {
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

  override clone(): TsTableColumn<T> {
    // HACK: This is not optimal, but we didn't find any other solution
    const clonedName: unknown = JSON.parse(JSON.stringify(this._name));
    assert(typeof clonedName === 'string');
    const clonedValues: unknown = JSON.parse(JSON.stringify(this._values));
    assert(
      INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD(clonedValues) &&
        this._valueType.isArrayInternalValueRepresentation(clonedValues),
    );
    // INFO: `this.valueType` must not be cloned, because the typesystem depends
    // on it being the same object
    return new TsTableColumn<T>(clonedName, this._valueType, clonedValues);
  }

  override isPolars(): this is PolarsTableColumn<T> {
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

  at(n: number): T | undefined {
    return this._values.at(n);
  }

  drop(rowIdx: number): T | undefined {
    const dropped = this._values.splice(rowIdx, 1);
    assert(dropped.length <= 1);
    return dropped[0];
  }
}
