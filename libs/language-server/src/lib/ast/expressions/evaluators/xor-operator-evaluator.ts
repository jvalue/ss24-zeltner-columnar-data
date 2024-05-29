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
    leftValue: PolarsInternal,
    rightValue: PolarsInternal,
  ): PolarsInternal {
    // HACK: There should be an xor expression in polars
    return leftValue.and(rightValue.not()).or(leftValue.not().and(rightValue));
  }
}
