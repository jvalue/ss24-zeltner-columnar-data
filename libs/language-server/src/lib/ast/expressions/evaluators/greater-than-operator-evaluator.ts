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
    left: PolarsInternal,
    right: PolarsInternal,
  ): PolarsInternal {
    return left.gt(right);
  }
}
