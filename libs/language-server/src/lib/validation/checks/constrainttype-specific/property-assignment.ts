// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type Int64 } from 'nodejs-polars';

import {
  type PropertyAssignment,
  type PropertySpecification,
  evaluatePropertyValue,
} from '../../../ast';
import { type JayveeValidationProps } from '../../validation-registry';

export function checkConstraintTypeSpecificProperties(
  property: PropertyAssignment,
  propertySpec: PropertySpecification,
  props: JayveeValidationProps,
) {
  const propName = property.name;
  const propValue = evaluatePropertyValue(
    property,
    props.evaluationContext,
    props.wrapperFactories,
    propertySpec.type,
  );
  if (propValue === undefined) {
    return;
  }

  switch (property.$container.$container.type.ref?.name) {
    case 'LengthConstraint':
      return checkLengthConstraintProperty(propName, property, props);
    default:
  }
}

function checkLengthConstraintProperty(
  propName: string,
  property: PropertyAssignment,
  props: JayveeValidationProps,
) {
  if (propName === 'minLength') {
    checkNonNegative(property, props);
  }
  if (propName === 'maxLength') {
    checkNonNegative(property, props);
  }
}

function checkNonNegative(
  property: PropertyAssignment,
  props: JayveeValidationProps,
) {
  let value = evaluatePropertyValue<number | Int64>(
    property,
    props.evaluationContext,
    props.wrapperFactories,
    props.valueTypeProvider.Primitives.Integer,
  );
  if (value === undefined) {
    return;
  }

  if (typeof value !== 'number') {
    value = Number.parseInt(value.toString(), 10);
  }

  if (value < 0) {
    props.validationContext.accept(
      'error',
      `Bounds for "${property.name}" need to be equal or greater than zero`,
      {
        node: property.value,
      },
    );
  }
}
