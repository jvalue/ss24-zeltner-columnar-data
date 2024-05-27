// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { BOOLEAN_TYPEGUARD, PL_EXPR_TYPEGUARD } from '../typeguards';

export class NotOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  boolean,
  boolean
> {
  constructor() {
    super('not', BOOLEAN_TYPEGUARD);
  }
  override doEvaluate(operandValue: boolean): boolean {
    return !operandValue;
  }

  override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    if (PL_EXPR_TYPEGUARD(col)) {
      return col.not();
    }
    throw new Error("pola.rs doesn't support `not` for `pl.Series` yet.");
  }
}
