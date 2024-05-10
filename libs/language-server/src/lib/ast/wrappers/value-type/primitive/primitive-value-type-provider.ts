// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import {
  type InternalValueRepresentation,
  type PolarsInternalValueRepresentation,
  type TsInternalValueRepresentation,
} from '../../../expressions';
import { type ValueType } from '../value-type';

import {
  PolarsBoolenValuetype,
  TsBooleanValuetype,
} from './boolean-value-type';
import { CellRangeValuetype } from './cell-range-value-type';
import { CollectionValueType } from './collection/collection-value-type';
import { EmptyCollectionValueType } from './collection/empty-collection-value-type';
import { ConstraintValuetype } from './constraint-value-type';
import {
  PolarsDecimalValuetype,
  TsDecimalValuetype,
} from './decimal-value-type';
import {
  PolarsIntegerValuetype,
  TsIntegerValuetype,
} from './integer-value-type';
import { type PrimitiveValueType } from './primitive-value-type';
import { RegexValuetype } from './regex-value-type';
import { PolarsTextValuetype, TsTextValuetype } from './text-value-type';
import { TransformValuetype } from './transform-value-type';
import { ValuetypeAssignmentValuetype } from './value-type-assignment-value-type';

export abstract class ValueTypeProvider {
  EmptyCollection = new EmptyCollectionValueType();
  public abstract Primitives:
    | PolarsPrimitiveValueTypeProvider
    | TsPrimitiveValueTypeProvider;

  createCollectionValueTypeOf<I extends InternalValueRepresentation>(
    input: ValueType<I>,
  ): CollectionValueType<I> {
    return new CollectionValueType(input);
  }

  abstract isPolars(): this is PolarsValueTypeProvider;
  abstract isTypescript(): this is TsValueTypeProvider;
}

/**
 * Should be created as singleton due to the equality comparison of primitive value types.
 * Exported for testing purposes.
 */
export class PolarsValueTypeProvider extends ValueTypeProvider {
  public override Primitives = new PolarsPrimitiveValueTypeProvider();

  override createCollectionValueTypeOf<I extends InternalValueRepresentation>(
    input: ValueType<I>,
  ): CollectionValueType<I> {
    return new CollectionValueType(input);
  }

  override isPolars(): this is PolarsValueTypeProvider {
    return true;
  }

  override isTypescript(): this is TsValueTypeProvider {
    return false;
  }
}

/**
 * Should be created as singleton due to the equality comparison of primitive value types.
 * Exported for testing purposes.
 */
export class TsValueTypeProvider extends ValueTypeProvider {
  public override Primitives = new TsPrimitiveValueTypeProvider();

  override createCollectionValueTypeOf<I extends InternalValueRepresentation>(
    input: ValueType<I>,
  ): CollectionValueType<I> {
    return new CollectionValueType(input);
  }

  override isPolars(): this is PolarsValueTypeProvider {
    return false;
  }

  override isTypescript(): this is TsValueTypeProvider {
    return true;
  }
}

export class PolarsPrimitiveValueTypeProvider {
  Boolean = new PolarsBoolenValuetype();
  Integer = new PolarsIntegerValuetype();
  Decimal = new PolarsDecimalValuetype();
  Text = new PolarsTextValuetype();

  // TODO: Port this to Polars
  Regex = new RegexValuetype();
  CellRange = new CellRangeValuetype();
  Constraint = new ConstraintValuetype();
  ValuetypeAssignment = new ValuetypeAssignmentValuetype();

  Transform = new TransformValuetype();

  getAll(): PrimitiveValueType<PolarsInternalValueRepresentation>[] {
    return [
      this.Boolean,
      this.Integer,
      this.Decimal,
      this.Text,
      this.Regex,
      this.CellRange,
      this.Constraint,
      this.ValuetypeAssignment,
    ];
  }
}

export class TsPrimitiveValueTypeProvider {
  Decimal = new TsDecimalValuetype();
  Boolean = new TsBooleanValuetype();
  Integer = new TsIntegerValuetype();
  Text = new TsTextValuetype();

  Regex = new RegexValuetype();
  CellRange = new CellRangeValuetype();
  Constraint = new ConstraintValuetype();
  ValuetypeAssignment = new ValuetypeAssignmentValuetype();

  Transform = new TransformValuetype();

  getAll(): PrimitiveValueType<TsInternalValueRepresentation>[] {
    return [
      this.Boolean,
      this.Decimal,
      this.Integer,
      this.Text,
      this.Regex,
      this.CellRange,
      this.Constraint,
      this.ValuetypeAssignment,
      this.Transform,
    ];
  }
}
