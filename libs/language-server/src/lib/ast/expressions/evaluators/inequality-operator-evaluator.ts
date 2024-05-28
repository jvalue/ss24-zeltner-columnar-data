// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import {
  type InternalValueRepresentation,
  type PolarsInternal,
} from '../internal-value-representation';
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
  override polarsDoEvaluate(
    left: InternalValueRepresentation | PolarsInternal,
    right: InternalValueRepresentation | PolarsInternal,
  ): boolean | PolarsInternal {
    if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(left)) {
      return INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(right)
        ? this.doEvaluate(left, right)
        : right.neq(left);
    }
    return left.neq(right);
  }
}
