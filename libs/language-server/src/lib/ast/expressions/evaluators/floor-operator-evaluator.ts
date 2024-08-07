// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type pl } from 'nodejs-polars';

import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { NUMBER_TYPEGUARD } from '../typeguards';

export class FloorOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  number,
  number
> {
  constructor() {
    super('floor', NUMBER_TYPEGUARD);
  }
  override doEvaluate(operandValue: number): number {
    return Math.floor(operandValue);
  }

  override polarsDoEvaluate(col: pl.Expr): pl.Expr {
    return col.floor();
  }
}
