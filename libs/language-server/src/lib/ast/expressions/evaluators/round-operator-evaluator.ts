// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class RoundOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  number,
  number
> {
  constructor() {
    super('round', NUMBER_TYPEGUARD);
  }
  override doEvaluate(operandValue: number): number {
    return Math.round(operandValue);
  }

  override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    return col.round(0);
  }
}
