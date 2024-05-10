// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { DataType as PolarsDType } from 'nodejs-polars';

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
  type InternalValueRepresentation,
  type InternalValueRepresentationTypeguard,
  type PolarsAtomicInternalValueRepresentation,
  type PolarsInternalValueRepresentation,
  type TsAtomicInternalValueRepresentation,
  type TsInternalValueRepresentation,
} from './internal-value-representation';

export const INTERNAL_VALUE_REPRESENTATION_TYPEGUARD: InternalValueRepresentationTypeguard<
  InternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is InternalValueRepresentation => {
  return true;
};

export const POLARS_VALUE_REPRESENTATION_TYPEGUARD: InternalValueRepresentationTypeguard<
  PolarsInternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is PolarsInternalValueRepresentation => {
  return value instanceof PolarsDType;
};

export const NUMBER_TYPEGUARD: InternalValueRepresentationTypeguard<number> = (
  value: InternalValueRepresentation,
): value is number => {
  return typeof value === 'number';
};
export const BOOLEAN_TYPEGUARD: InternalValueRepresentationTypeguard<
  boolean
> = (value: InternalValueRepresentation): value is boolean => {
  return typeof value === 'boolean';
};
export const STRING_TYPEGUARD: InternalValueRepresentationTypeguard<string> = (
  value: InternalValueRepresentation,
): value is string => {
  return typeof value === 'string';
};

export const REGEXP_TYPEGUARD: InternalValueRepresentationTypeguard<RegExp> = (
  value: InternalValueRepresentation,
): value is RegExp => {
  return value instanceof RegExp;
};

export const CELL_RANGE_LITERAL_TYPEGUARD: InternalValueRepresentationTypeguard<
  CellRangeLiteral
> = (value: InternalValueRepresentation): value is CellRangeLiteral => {
  return isCellRangeLiteral(value);
};

export const CONSTRAINT_DEFINITION_TYPEGUARD: InternalValueRepresentationTypeguard<
  ConstraintDefinition
> = (value: InternalValueRepresentation): value is ConstraintDefinition => {
  return isConstraintDefinition(value);
};

export const VALUETYPE_ASSIGNMENT_TYPEGUARD: InternalValueRepresentationTypeguard<
  ValuetypeAssignment
> = (value: InternalValueRepresentation): value is ValuetypeAssignment => {
  return isValuetypeAssignment(value);
};

export const BLOCKTYPE_PROPERTY_TYPEGUARD: InternalValueRepresentationTypeguard<
  BlockTypeProperty
> = (value: InternalValueRepresentation): value is BlockTypeProperty => {
  return isBlockTypeProperty(value);
};

export const TRANSFORM_DEFINITION_TYPEGUARD: InternalValueRepresentationTypeguard<
  TransformDefinition
> = (value: InternalValueRepresentation): value is TransformDefinition => {
  return isTransformDefinition(value);
};

export const POLARS_DTYPE_TYPEGUARD: InternalValueRepresentationTypeguard<
  PolarsDType
> = (value: InternalValueRepresentation): value is PolarsDType => {
  return value instanceof PolarsDType;
};

export const POLARS_ATOMIC_TYPE_REPRESENTAION: InternalValueRepresentationTypeguard<
  PolarsAtomicInternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is PolarsAtomicInternalValueRepresentation => {
  return (
    POLARS_DTYPE_TYPEGUARD(value) ||
    REGEXP_TYPEGUARD(value) ||
    CELL_RANGE_LITERAL_TYPEGUARD(value) ||
    CONSTRAINT_DEFINITION_TYPEGUARD(value) ||
    VALUETYPE_ASSIGNMENT_TYPEGUARD(value) ||
    BLOCKTYPE_PROPERTY_TYPEGUARD(value) ||
    TRANSFORM_DEFINITION_TYPEGUARD(value)
  );
};

export const POLARS_TYPE_REPRESENTAION: InternalValueRepresentationTypeguard<
  PolarsInternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is PolarsInternalValueRepresentation => {
  if (Array.isArray(value)) {
    return (
      value.length === 0 || value.every((e) => POLARS_TYPE_REPRESENTAION(e))
    );
  }
  return POLARS_ATOMIC_TYPE_REPRESENTAION(value);
};

export const TS_ATOMIC_TYPE_REPRESENTAION: InternalValueRepresentationTypeguard<
  TsAtomicInternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is TsAtomicInternalValueRepresentation => {
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

export const TS_TYPE_REPRESENTAION: InternalValueRepresentationTypeguard<
  TsInternalValueRepresentation
> = (
  value: InternalValueRepresentation,
): value is TsInternalValueRepresentation => {
  if (Array.isArray(value)) {
    return value.length === 0 || value.every((e) => TS_TYPE_REPRESENTAION(e));
  }
  return TS_TYPE_REPRESENTAION(value);
};

export function INTERNAL_ARRAY_TYPEGUARD<T extends InternalValueRepresentation>(
  array: InternalValueRepresentation[],
  ELEMENT_TYPEGUARD: InternalValueRepresentationTypeguard<T>,
): array is T[] {
  return array.every((value) => ELEMENT_TYPEGUARD(value));
}

export function noMixedImplementations(
  array: (InternalValueRepresentation | undefined)[],
): array is
  | PolarsInternalValueRepresentation[]
  | TsInternalValueRepresentation[] {
  if (array[0] === undefined || !isEveryValueDefined(array)) {
    return false;
  }

  if (POLARS_TYPE_REPRESENTAION(array[0])) {
    return INTERNAL_ARRAY_TYPEGUARD(array, POLARS_TYPE_REPRESENTAION);
  } else if (TS_TYPE_REPRESENTAION(array[0])) {
    return INTERNAL_ARRAY_TYPEGUARD(array, TS_TYPE_REPRESENTAION);
  }
  throw new Error('Niether polars nor typescript representation');
}

export function isEveryValueDefined<T>(array: (T | undefined)[]): array is T[] {
  return array.every((value) => value !== undefined);
}
