// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import {
  type BlockTypeProperty,
  type ConstraintDefinition,
  type TransformDefinition,
  type ValuetypeAssignment,
  isBlockTypeProperty,
  isConstraintDefinition,
  isTransformDefinition,
  isValuetypeAssignment,
} from '../generated/ast';
// eslint-disable-next-line import/no-cycle
import { type CellRangeWrapper, isCellRangeWrapper } from '../wrappers';

export type InternalValueRepresentation =
  | AtomicInternalValueRepresentation
  | InternalValueRepresentation[]
  | [];

export type AtomicInternalValueRepresentation =
  | boolean
  | number
  | string
  | RegExp
  | CellRangeWrapper
  | ConstraintDefinition
  | ValuetypeAssignment
  | BlockTypeProperty
  | TransformDefinition;

export type InternalValueRepresentationTypeguard<
  T extends InternalValueRepresentation,
> = (value: InternalValueRepresentation) => value is T;

export function internalValueToString(
  valueRepresentation: InternalValueRepresentation,
): string {
  if (Array.isArray(valueRepresentation)) {
    return (
      '[ ' +
      valueRepresentation
        .map((item) => internalValueToString(item))
        .join(', ') +
      ' ]'
    );
  }

  if (typeof valueRepresentation === 'boolean') {
    return String(valueRepresentation);
  }
  if (typeof valueRepresentation === 'number') {
    if (valueRepresentation === Number.POSITIVE_INFINITY) {
      return Number.MAX_VALUE.toLocaleString('fullwide', {
        useGrouping: false,
      });
    }
    if (valueRepresentation === Number.NEGATIVE_INFINITY) {
      return Number.MIN_VALUE.toLocaleString('fullwide', {
        useGrouping: false,
      });
    }
    return `${valueRepresentation}`;
  }
  if (typeof valueRepresentation === 'string') {
    return `"${valueRepresentation}"`;
  }
  if (valueRepresentation instanceof RegExp) {
    return valueRepresentation.source;
  }
  if (isCellRangeWrapper(valueRepresentation)) {
    return valueRepresentation.toString();
  }
  if (isConstraintDefinition(valueRepresentation)) {
    return valueRepresentation.name;
  }
  if (isValuetypeAssignment(valueRepresentation)) {
    return valueRepresentation.name;
  }
  if (isTransformDefinition(valueRepresentation)) {
    return valueRepresentation.name;
  }
  if (isBlockTypeProperty(valueRepresentation)) {
    return valueRepresentation.name;
  }
  throw new Error(
    'Convert of this InternalValueRepresentation is not implemented',
  );
}
