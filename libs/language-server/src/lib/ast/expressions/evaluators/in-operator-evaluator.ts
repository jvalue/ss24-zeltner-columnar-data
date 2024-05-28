// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type BinaryExpression } from '../..';
import { type ValidationContext } from '../../..';
import {
  type InternalValueRepresentation,
  type InternalValueRepresentationTypeguard,
  type PolarsInternal,
} from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD, STRING_TYPEGUARD } from '../typeguards';

export class InOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  InternalValueRepresentation,
  InternalValueRepresentation[],
  boolean
> {
  constructor() {
    super(
      'in',
      isLeftOperandMatchingValueRepresentationTypeguard,
      isRightOperandMatchingValueRepresentationTypeguard,
    );
  }
  override doEvaluate(
    left: string | number,
    right: (string | number)[],
  ): boolean {
    return right.includes(left);
  }
  override polarsDoEvaluate(
    left: InternalValueRepresentation | PolarsInternal,
    right: InternalValueRepresentation[] | PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): boolean | PolarsInternal | undefined {
    if (
      isLeftOperandMatchingValueRepresentationTypeguard(left) &&
      isRightOperandMatchingValueRepresentationTypeguard(right)
    ) {
      return this.doEvaluate(left, right);
    }
    return super.polarsDoEvaluate(left, right, expression, context);
  }
}

const isLeftOperandMatchingValueRepresentationTypeguard: InternalValueRepresentationTypeguard<
  string | number
> = (value: unknown): value is string | number => {
  return STRING_TYPEGUARD(value) || NUMBER_TYPEGUARD(value);
};

const isRightOperandMatchingValueRepresentationTypeguard: InternalValueRepresentationTypeguard<
  (string | number)[]
> = (value: unknown): value is (string | number)[] => {
  return (
    Array.isArray(value) &&
    value.every(isLeftOperandMatchingValueRepresentationTypeguard)
  );
};
