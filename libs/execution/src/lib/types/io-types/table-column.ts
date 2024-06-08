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
  type ValueTypeProvider,
} from '@jvalue/jayvee-language-server';
import { type pl } from 'nodejs-polars';

export abstract class TableColumn {
  abstract getValueType(provider: ValueTypeProvider): ValueType;
  abstract getName(): string;
  abstract nth(n: number): InternalValueRepresentation | undefined | null;
  abstract clone(): TableColumn;

  abstract isPolars(): this is PolarsTableColumn;
  abstract isTypescript(): this is TsTableColumn;
}

export class PolarsTableColumn extends TableColumn {
  constructor(private series: pl.Series) {
    super();
  }

  override getValueType(provider: ValueTypeProvider): ValueType {
    return provider.fromPolarsDType(this.series.dtype);
  }

  override getName(): string {
    return this.series.name;
  }

  override nth(n: number): InternalValueRepresentation | undefined | null {
    const nth = this.series.getIndex(n) as unknown;
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
    return new PolarsTableColumn(this.series.clone());
  }

  override isPolars(): this is PolarsTableColumn {
    return true;
  }

  override isTypescript(): this is TsTableColumn {
    return false;
  }

  getSeries(): Readonly<pl.Series> {
    return this.series;
  }
}

export class TsTableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> extends TableColumn {
  constructor(
    public name: string,
    public valueType: ValueType<T>,
    public values: T[] = [],
  ) {
    super();
  }

  override getValueType(): ValueType<T> {
    return this.valueType;
  }

  override getName(): string {
    return this.name;
  }

  override nth(n: number): T | undefined {
    return this.values.at(n);
  }

  override clone(): TsTableColumn {
    // HACK: This feels wrong, but I didn't find any other solution
    const clonedName: unknown = JSON.parse(JSON.stringify(this.name));
    assert(typeof clonedName === 'string');
    const clonedValues: unknown = JSON.parse(JSON.stringify(this.values));
    assert(
      INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD(clonedValues) &&
        clonedValues.every((e) =>
          this.valueType.isInternalValueRepresentation(e),
        ),
    );
    // INFO: `this.valueType` must not be cloned, because the typesystem depends
    // on it being the same object
    return new TsTableColumn(clonedName, this.valueType, clonedValues);
  }

  override isPolars(): this is PolarsTableColumn {
    return false;
  }

  override isTypescript(): this is TsTableColumn<T> {
    return true;
  }

  push(x: T) {
    if (this.valueType.isInternalValueRepresentation(x)) {
      this.values.push(x);
    }
  }
}
