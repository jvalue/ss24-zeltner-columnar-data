// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type DataType as PlDType, pl } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions';
import { type ValueType } from '../value-type';

import { BooleanValuetype } from './boolean-value-type';
import { CellRangeValuetype } from './cell-range-value-type';
import { CollectionValueType } from './collection/collection-value-type';
import { EmptyCollectionValueType } from './collection/empty-collection-value-type';
import { ConstraintValuetype } from './constraint-value-type';
import { DecimalValuetype } from './decimal-value-type';
import { IntegerValuetype } from './integer-value-type';
import { type PrimitiveValueType } from './primitive-value-type';
import { RegexValuetype } from './regex-value-type';
import { TextValuetype } from './text-value-type';
import { TransformValuetype } from './transform-value-type';
import { ValuetypeAssignmentValuetype } from './value-type-assignment-value-type';

/**
 * Should be created as singleton due to the equality comparison of primitive value types.
 * Exported for testing purposes.
 */
export class ValueTypeProvider {
  Primitives = new PrimitiveValueTypeProvider();
  EmptyCollection = new EmptyCollectionValueType();

  createCollectionValueTypeOf<I extends InternalValueRepresentation>(
    input: ValueType<I>,
  ): CollectionValueType<I> {
    return new CollectionValueType(input);
  }

  fromPolarsDType(dtype: PlDType): ValueType {
    if (dtype.equals(pl.Bool)) {
      return this.Primitives.Boolean;
    } else if (dtype.equals(pl.Float32) || dtype.equals(pl.Float64)) {
      return this.Primitives.Decimal;
    } else if (
      dtype.equals(pl.Int8) ||
      dtype.equals(pl.Int16) ||
      dtype.equals(pl.Int32) ||
      dtype.equals(pl.Int64) ||
      dtype.equals(pl.UInt8) ||
      dtype.equals(pl.UInt16) ||
      dtype.equals(pl.UInt32) ||
      dtype.equals(pl.UInt64)
    ) {
      return this.Primitives.Integer;
    } else if (dtype.equals(pl.String) || dtype.equals(pl.Utf8)) {
      return this.Primitives.Text;
    }
    // TODO:
    // pl.Categorical
    // pl.Date
    // pl.DateTime
    // pl.List
    // pl.Null
    // pl.Struct

    throw new Error(`${dtype.variant} is not supported yet`);
  }
}

export class PrimitiveValueTypeProvider {
  Decimal = new DecimalValuetype();
  Boolean = new BooleanValuetype();
  Integer = new IntegerValuetype();
  Text = new TextValuetype();

  Regex = new RegexValuetype();
  CellRange = new CellRangeValuetype();
  Constraint = new ConstraintValuetype();
  ValuetypeAssignment = new ValuetypeAssignmentValuetype();

  Transform = new TransformValuetype();

  getAll(): PrimitiveValueType[] {
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
