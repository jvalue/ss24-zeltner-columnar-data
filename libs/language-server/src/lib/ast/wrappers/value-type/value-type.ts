// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type InternalValueRepresentation } from '../../expressions/internal-value-representation';

import { type AtomicValueType } from './atomic-value-type';
import {
  type CellRangeValuetype,
  type CollectionValueType,
  type ConstraintValuetype,
  type EmptyCollectionValueType,
  type PolarsBoolenValuetype,
  type PolarsDecimalValuetype,
  type PolarsIntegerValuetype,
  type PolarsTextValuetype,
  type RegexValuetype,
  type TransformValuetype,
  type TsBooleanValuetype,
  type TsDecimalValuetype,
  type TsIntegerValuetype,
  type TsTextValuetype,
  type ValuetypeAssignmentValuetype,
} from './primitive';

export interface VisitableValueType {
  acceptVisitor(visitor: ValueTypeVisitor): void;
}

export interface ValueType<
  I extends InternalValueRepresentation = InternalValueRepresentation,
> extends VisitableValueType {
  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R;

  /**
   * The subtype relation reflects the hierarchy of value types.
   * Primitive value types are never a subtype of another value type.
   * Atomic value types may form a hierarchy below a primitive value type.
   */
  isSubtypeOf(other: ValueType): boolean;

  /**
   * The supertype relation reflects the hierarchy of value types.
   * Primitive value types never have a supertype.
   * Atomic value types may have a atomic or primitive value type as supertype.
   */
  getSupertype(): ValueType | undefined;

  /**
   * The convertible relation reflects the ability of primitive types to
   * convert into another primitive value type in a loss-less way (e.g., int to decimal).
   * Atomic value types inherit (@see isSubtypeOf) the conversion behavior of their primitive value type.
   */
  isConvertibleTo(target: ValueType): boolean;

  /**
   * Flag if value type can be referenced by users.
   * Examples:
   *   - Users can (not) reference a value type to extend it in a value type definition
   *   - Users can (not) reference a value type to parse values in the TableInterpreter block
   */
  isReferenceableByUser(): boolean;

  /**
   * Typeguard to validate whether a given value is in the correct internal representation of this value type.
   * For example, a TextValuetype has the internal representation string.
   */
  isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is I;

  /**
   * Checks if there is a cycle in the supertype relation.
   */
  hasSupertypeCycle(visited?: ValueType[]): boolean;

  isAllowedAsRuntimeParameter(): boolean;
  getName(): string;

  equals(target: ValueType): boolean;
}

export abstract class ValueTypeVisitor<R = unknown> {
  abstract visitTsBoolean(valueType: TsBooleanValuetype): R;
  abstract visitTsDecimal(valueType: TsDecimalValuetype): R;
  abstract visitTsInteger(valueType: TsIntegerValuetype): R;
  abstract visitPolarsBoolean(valueType: PolarsBoolenValuetype): R;
  abstract visitPolarsInteger(valueType: PolarsIntegerValuetype): R;
  abstract visitPolarsDecimal(valueType: PolarsDecimalValuetype): R;
  abstract visitPolarsText(valueType: PolarsTextValuetype): R;
  abstract visitTsText(valueType: TsTextValuetype): R;

  abstract visitCellRange(valueType: CellRangeValuetype): R;
  abstract visitRegex(valueType: RegexValuetype): R;
  abstract visitConstraint(valueType: ConstraintValuetype): R;
  abstract visitValuetypeAssignment(valueType: ValuetypeAssignmentValuetype): R;
  abstract visitCollection(
    valueType: CollectionValueType | EmptyCollectionValueType,
  ): R;
  abstract visitTransform(valueType: TransformValuetype): R;

  abstract visitAtomicValueType(valueType: AtomicValueType): R;
}
