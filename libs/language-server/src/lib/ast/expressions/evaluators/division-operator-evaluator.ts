// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import { pl } from 'nodejs-polars';
import { type ValidationContext } from '../../../validation/validation-context';
import { type BinaryExpression } from '../../generated/ast';
import { type PolarsInternal } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class DivisionOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  number
> {
  constructor() {
    super('/', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }

  override doEvaluate(
    leftValue: number,
    rightValue: number,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | undefined {
    const resultingValue = leftValue / rightValue;

    if (!isFinite(resultingValue)) {
      assert(rightValue === 0);
      context?.accept('error', 'Arithmetic error: division by zero', {
        node: expression,
      });
      return undefined;
    }
    return resultingValue;
  }

  override polarsDoEvaluate(
    left: number | PolarsInternal,
    right: number | PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): number | PolarsInternal | undefined {
    if (NUMBER_TYPEGUARD(left)) {
      if (NUMBER_TYPEGUARD(right)) {
        return this.doEvaluate(left, right, expression, context);
      }
      context?.accept(
        'warning',
        `<someNumber> / <someColumn> is not fully supported yet. Using a hack`,
        {
          node: expression,
        },
      );
      // HACK:
      const one = right.div(right);
      return one.mul(left).div(right);
    }
    return left.div(right);
  }
}
