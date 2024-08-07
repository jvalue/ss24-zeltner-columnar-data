// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type pl } from 'nodejs-polars';

import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class MultiplicationOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  number
> {
  constructor() {
    super('*', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }
  override doEvaluate(leftValue: number, rightValue: number): number {
    return leftValue * rightValue;
  }
  override polarsDoEvaluate(left: pl.Expr, right: pl.Expr): pl.Expr {
    return left.mul(right);
  }
}
