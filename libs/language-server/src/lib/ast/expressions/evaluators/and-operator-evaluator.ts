// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { BooleanShortCircuitOperatorEvaluator } from '../operator-evaluator';
import { BOOLEAN_TYPEGUARD } from '../typeguards';

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
  override polarsDoEvaluate(
    leftValue: boolean | PolarsInternal,
    rightValue: boolean | PolarsInternal,
  ): boolean | PolarsInternal {
    if (BOOLEAN_TYPEGUARD(leftValue)) {
      if (BOOLEAN_TYPEGUARD(rightValue)) {
        return this.polarsDoEvaluate(leftValue, rightValue);
      }
      return rightValue.and(leftValue);
    }
    return leftValue.and(rightValue);
  }
}
