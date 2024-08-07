// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type pl } from 'nodejs-polars';

import { BooleanShortCircuitOperatorEvaluator } from '../operator-evaluator';

export class AndOperatorEvaluator extends BooleanShortCircuitOperatorEvaluator {
  constructor() {
    super('and');
  }
  override canSkipRightOperandEvaluation(leftValue: boolean): boolean {
    return leftValue === false;
  }

  override getShortCircuitValue(): boolean {
    return false;
  }

  override doEvaluate(leftValue: boolean, rightValue: boolean): boolean {
    return leftValue && rightValue;
  }
  override polarsDoEvaluate(leftValue: pl.Expr, rightValue: pl.Expr): pl.Expr {
    return leftValue.and(rightValue);
  }
}
