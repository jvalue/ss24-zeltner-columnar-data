// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { BooleanValuetype } from './boolean-value-type';
import { CellRangeValuetype } from './cell-range-value-type';
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
export class PrimitiveValueTypeProvider {
  Primitives = new PrimitiveValueTypeContainer();
  EmptyCollection = new EmptyCollectionValueType();
}

export class PrimitiveValueTypeContainer {
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
