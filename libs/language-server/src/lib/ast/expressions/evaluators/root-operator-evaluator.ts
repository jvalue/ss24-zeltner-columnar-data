// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { PolarsInternal } from '..';
import { type ValidationContext } from '../../../validation/validation-context';
import { type BinaryExpression } from '../../generated/ast';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class RootOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  number
> {
  constructor() {
    super('root', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }

  override doEvaluate(
    leftValue: number,
    rightValue: number,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | undefined {
    const resultingValue = leftValue ** (1 / rightValue);

    if (!isFinite(resultingValue)) {
      if (leftValue === 0 && rightValue < 0) {
        context?.accept(
          'error',
          'Arithmetic error: root of zero with negative degree',
          { node: expression },
        );
      } else if (rightValue === 0) {
        context?.accept('error', 'Arithmetic error: root of degree zero', {
          node: expression,
        });
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
        '<someNumber> root <someColumn> is not supported yet',
        {
          node: expression.right,
        },
      );
      return undefined;
    }
    if (NUMBER_TYPEGUARD(rightValue)) {
      return leftValue.pow(1 / rightValue);
    }
    context?.accept(
      'error',
      '<someColmun> root <someOtherColumn> is not supported yet',
      {
        node: expression,
      },
    );
    return undefined;
  }
}
