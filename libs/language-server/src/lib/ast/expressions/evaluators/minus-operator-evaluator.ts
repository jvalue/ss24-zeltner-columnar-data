// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class MinusOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  number,
  number
> {
  constructor() {
    super('-', NUMBER_TYPEGUARD);
  }
  override doEvaluate(operandValue: number): number {
    return -operandValue;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('`uppercase` is not supported yet');
  }
}
