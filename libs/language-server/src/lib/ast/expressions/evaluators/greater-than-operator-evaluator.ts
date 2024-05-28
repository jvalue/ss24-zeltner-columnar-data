// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class GreaterThanOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  number,
  number,
  boolean
> {
  constructor() {
    super('>', NUMBER_TYPEGUARD, NUMBER_TYPEGUARD);
  }
  override doEvaluate(leftValue: number, rightValue: number): boolean {
    return leftValue > rightValue;
  }
  override polarsDoEvaluate(
    left: number | PolarsInternal,
    right: number | PolarsInternal,
  ): boolean | PolarsInternal {
    if (NUMBER_TYPEGUARD(left)) {
      return NUMBER_TYPEGUARD(right)
        ? this.doEvaluate(left, right)
        : right.ltEq(left);
    }
    return left.gt(right);
  }
}
