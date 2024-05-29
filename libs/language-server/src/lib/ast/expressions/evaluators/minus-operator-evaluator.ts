// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class MinusOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  number,
  number
> {
  constructor() {
    super('-', NUMBER_TYPEGUARD);
  }
  override doEvaluate(operandValue: number): number {
    return -operandValue;
  }
  protected override polarsDoEvaluate(operand: PolarsInternal): PolarsInternal {
    // HACK: Polars does not support neg
    return operand.mul(-1);
  }
}
