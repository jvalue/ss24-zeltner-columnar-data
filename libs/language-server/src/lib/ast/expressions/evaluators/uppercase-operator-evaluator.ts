// SPDX-FileCopyrightText: 2024 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { STRING_TYPEGUARD } from '../typeguards';

export class UppercaseOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  string,
  string
> {
  constructor() {
    super('uppercase', STRING_TYPEGUARD);
  }
  override doEvaluate(operandValue: string): string {
    return operandValue.toUpperCase();
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('`uppercase` is not supported yet');
  }
}
