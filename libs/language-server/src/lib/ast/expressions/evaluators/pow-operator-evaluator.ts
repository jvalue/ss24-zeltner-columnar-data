// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type ValidationContext } from '../../../validation/validation-context';
import { type BinaryExpression } from '../../generated/ast';
import { type PolarsInternal } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class PowOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  number
> {
  constructor() {
    super('pow', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }

  override doEvaluate(
    leftValue: number,
    rightValue: number,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | undefined {
    const resultingValue = leftValue ** rightValue;

    if (!isFinite(resultingValue)) {
      if (leftValue === 0 && rightValue < 0) {
        context?.accept(
          'error',
          'Arithmetic error: zero raised to a negative number',
          { node: expression },
        );
      } else {
        context?.accept('error', 'Unknown arithmetic error', {
          node: expression,
        });
      }
      return undefined;
    }
    return resultingValue;
  }

  override polarsDoEvaluate(
    leftValue: number | PolarsInternal,
    rightValue: number | PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | PolarsInternal | undefined {
    if (NUMBER_TYPEGUARD(leftValue)) {
      if (NUMBER_TYPEGUARD(rightValue)) {
        return this.doEvaluate(leftValue, rightValue, expression, context);
      }
      context?.accept(
        'error',
        '<someNumber> pow <someColumn> is not supported yet',
        {
          node: expression.right,
        },
      );
      return undefined;
    }
    if (NUMBER_TYPEGUARD(rightValue)) {
      return leftValue.pow(rightValue);
    }
    context?.accept(
      'error',
      '<someColmun> pow <someOtherColumn> is not supported yet',
      {
        node: expression,
      },
    );
    return undefined;
  }
}
