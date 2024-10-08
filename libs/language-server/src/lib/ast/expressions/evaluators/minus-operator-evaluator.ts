// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type pl } from 'nodejs-polars';

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
  protected override polarsDoEvaluate(operand: pl.Expr): pl.Expr {
    return operand.mul(-1);
  }
}
