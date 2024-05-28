// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { BOOLEAN_TYPEGUARD } from '../typeguards';

export class XorOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  boolean,
  boolean,
  boolean
> {
  constructor() {
    super('xor', BOOLEAN_TYPEGUARD, BOOLEAN_TYPEGUARD);
  }
  override doEvaluate(leftValue: boolean, rightValue: boolean): boolean {
    return (leftValue && !rightValue) || (!leftValue && rightValue);
  }
  override polarsDoEvaluate(
    leftValue: boolean | PolarsInternal,
    rightValue: boolean | PolarsInternal,
  ): boolean | PolarsInternal {
    // HACK: There should be an xor expression in polars
    if (BOOLEAN_TYPEGUARD(leftValue)) {
      return BOOLEAN_TYPEGUARD(rightValue)
        ? this.doEvaluate(leftValue, rightValue)
        : rightValue.and(!leftValue).or(rightValue.not().and(leftValue));
    }
    if (BOOLEAN_TYPEGUARD(rightValue)) {
      return leftValue.and(!rightValue).or(leftValue.not().and(rightValue));
    }
    return leftValue.and(rightValue.not()).or(leftValue.not().and(rightValue));
  }
}
