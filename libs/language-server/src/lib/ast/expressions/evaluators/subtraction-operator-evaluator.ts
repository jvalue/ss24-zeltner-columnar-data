// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only
import { type ValidationContext } from '../../../validation/validation-context';
import { type BinaryExpression } from '../../generated/ast';
import { type PolarsInternal } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class SubtractionOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  number
> {
  constructor() {
    super('-', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }
  override doEvaluate(leftValue: number, rightValue: number): number {
    return leftValue - rightValue;
  }
  override polarsDoEvaluate(
    left: number | PolarsInternal,
    right: number | PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | PolarsInternal {
    if (NUMBER_TYPEGUARD(left)) {
      if (NUMBER_TYPEGUARD(right)) {
        return this.doEvaluate(left, right);
      }
      context?.accept(
        'warning',
        `<someNumber> - <someColumn> is not fully supported yet. Using a hack`,
        {
          node: expression,
        },
      );
      // HACK:
      const zero = right.mul(0);
      return zero.add(left).minus(right);
    }
    return left.minus(right);
  }
}
