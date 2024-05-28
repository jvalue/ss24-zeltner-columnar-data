// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
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
  override polarsDoEvaluate(
    left: number | PolarsInternal,
    right: number | PolarsInternal,
  ): number | PolarsInternal {
    if (NUMBER_TYPEGUARD(left)) {
      return NUMBER_TYPEGUARD(right)
        ? this.doEvaluate(left, right)
        : right.mul(left);
    }
    return left.mul(right);
  }
}
