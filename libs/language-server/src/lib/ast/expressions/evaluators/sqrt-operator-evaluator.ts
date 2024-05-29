// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import { type ValidationContext } from '../../../validation/validation-context';
import { type UnaryExpression } from '../../generated/ast';
import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class SqrtOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  number,
  number
> {
  constructor() {
    super('sqrt', NUMBER_TYPEGUARD);
  }
  override doEvaluate(
    operandValue: number,
    expression: UnaryExpression,
    context: ValidationContext | undefined,
  ): number | undefined {
    const resultingValue = Math.sqrt(operandValue);

    if (!isFinite(resultingValue)) {
      assert(operandValue < 0);
      context?.accept(
        'error',
        'Arithmetic error: square root of negative number',
        { node: expression },
      );
      return undefined;
    }
    return resultingValue;
  }

  protected override polarsDoEvaluate(operand: PolarsInternal): PolarsInternal {
    // HACK: Typst does not have a root expression
    return operand.pow(1 / 2);
  }
}
