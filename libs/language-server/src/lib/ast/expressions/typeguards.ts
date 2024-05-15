// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import {
  type BlockTypeProperty,
  type CellRangeLiteral,
  type ConstraintDefinition,
  type TransformDefinition,
  type ValuetypeAssignment,
  isBlockTypeProperty,
  isCellRangeLiteral,
  isConstraintDefinition,
  isTransformDefinition,
  isValuetypeAssignment,
} from '../generated/ast';

import {
  type AtomicInternalValueRepresentation,
  type InternalValueRepresentation,
  type InternalValueRepresentationTypeguard,
} from './internal-value-representation';

export const NUMBER_TYPEGUARD: InternalValueRepresentationTypeguard<number> = (
  value: unknown,
): value is number => {
  return typeof value === 'number';
};
export const BOOLEAN_TYPEGUARD: InternalValueRepresentationTypeguard<
  boolean
> = (value: unknown): value is boolean => {
  return typeof value === 'boolean';
};
export const STRING_TYPEGUARD: InternalValueRepresentationTypeguard<string> = (
  value: unknown,
): value is string => {
  return typeof value === 'string';
};

export const REGEXP_TYPEGUARD: InternalValueRepresentationTypeguard<RegExp> = (
  value: unknown,
): value is RegExp => {
  return value instanceof RegExp;
};

export const CELL_RANGE_LITERAL_TYPEGUARD: InternalValueRepresentationTypeguard<
  CellRangeLiteral
> = (value: unknown): value is CellRangeLiteral => {
  return isCellRangeLiteral(value);
};

export const CONSTRAINT_DEFINITION_TYPEGUARD: InternalValueRepresentationTypeguard<
  ConstraintDefinition
> = (value: unknown): value is ConstraintDefinition => {
  return isConstraintDefinition(value);
};

export const VALUETYPE_ASSIGNMENT_TYPEGUARD: InternalValueRepresentationTypeguard<
  ValuetypeAssignment
> = (value: unknown): value is ValuetypeAssignment => {
  return isValuetypeAssignment(value);
};

export const BLOCKTYPE_PROPERTY_TYPEGUARD: InternalValueRepresentationTypeguard<
  BlockTypeProperty
> = (value: unknown): value is BlockTypeProperty => {
  return isBlockTypeProperty(value);
};

export const TRANSFORM_DEFINITION_TYPEGUARD: InternalValueRepresentationTypeguard<
  TransformDefinition
> = (value: unknown): value is TransformDefinition => {
  return isTransformDefinition(value);
};

export const ATOMIC_TYPE_REPRESENTAION_TYPEGUARD: InternalValueRepresentationTypeguard<
  AtomicInternalValueRepresentation
> = (value: unknown): value is AtomicInternalValueRepresentation => {
  return (
    BOOLEAN_TYPEGUARD(value) ||
    NUMBER_TYPEGUARD(value) ||
    STRING_TYPEGUARD(value) ||
    REGEXP_TYPEGUARD(value) ||
    CELL_RANGE_LITERAL_TYPEGUARD(value) ||
    CONSTRAINT_DEFINITION_TYPEGUARD(value) ||
    VALUETYPE_ASSIGNMENT_TYPEGUARD(value) ||
    BLOCKTYPE_PROPERTY_TYPEGUARD(value) ||
    TRANSFORM_DEFINITION_TYPEGUARD(value)
  );
};

export const INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD: InternalValueRepresentationTypeguard<
  InternalValueRepresentation[]
> = (value: unknown): value is InternalValueRepresentation[] => {
  if (!Array.isArray(value)) {
    return false;
  }
  return everyValueInternalRepresentationTypeguard(
    value,
    INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  );
};

export const INTERNAL_VALUE_REPRESENTATION_TYPEGUARD: InternalValueRepresentationTypeguard<
  InternalValueRepresentation
> = (value: unknown): value is InternalValueRepresentation => {
  return (
    INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD(value) ||
    ATOMIC_TYPE_REPRESENTAION_TYPEGUARD(value)
  );
};

export function everyValueInternalRepresentationTypeguard<
  T extends InternalValueRepresentation,
>(
  array: unknown[],
  ELEMENT_TYPEGUARD: InternalValueRepresentationTypeguard<T>,
): array is T[] {
  return array.every((value) => ELEMENT_TYPEGUARD(value));
}

export function isEveryValueDefined<T>(array: (T | undefined)[]): array is T[] {
  return array.every((value) => value !== undefined);
}
