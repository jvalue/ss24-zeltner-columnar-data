// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type pl } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../internal-value-representation';
import { DefaultBinaryOperatorEvaluator } from '../operator-evaluator';
import { INTERNAL_VALUE_REPRESENTATION_TYPEGUARD } from '../typeguards';

export class InequalityOperatorEvaluator extends DefaultBinaryOperatorEvaluator<
  InternalValueRepresentation,
  InternalValueRepresentation,
  boolean
> {
  constructor() {
    super(
      '!=',
      INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
      INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
    );
  }
  override doEvaluate(
    left: InternalValueRepresentation,
    right: InternalValueRepresentation,
  ): boolean {
    return left !== right;
  }
  override polarsDoEvaluate(left: pl.Expr, right: pl.Expr): pl.Expr {
    return left.neq(right);
  }
}
