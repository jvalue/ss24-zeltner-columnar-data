// SPDX-FileCopyrightText: 2024 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type ValueTypeProvider } from '../../wrappers';
import { type PolarsInternal } from '../internal-value-representation';
import { DefaultUnaryOperatorEvaluator } from '../operator-evaluator';
import { STRING_TYPEGUARD } from '../typeguards';

export class AsTextOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  string,
  string
> {
  constructor(private readonly valueTypeProvider: ValueTypeProvider) {
    super('asText', STRING_TYPEGUARD);
  }
  override doEvaluate(operandValue: string): string {
    return this.valueTypeProvider.Primitives.Text.fromString(operandValue);
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('pola.rs does not support parsing values, currently');
  }
}

export class AsDecimalOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  string,
  number
> {
  constructor(private readonly valueTypeProvider: ValueTypeProvider) {
    super('asDecimal', STRING_TYPEGUARD);
  }
  override doEvaluate(operandValue: string): number {
    const dec =
      this.valueTypeProvider.Primitives.Decimal.fromString(operandValue);
    if (dec === undefined) {
      throw new Error(`Could not parse "${operandValue}" into a Decimal`);
    }
    return dec;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('pola.rs does not support parsing values, currently');
  }
}

export class AsIntegerOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  string,
  number
> {
  constructor(private readonly valueTypeProvider: ValueTypeProvider) {
    super('asInteger', STRING_TYPEGUARD);
  }
  override doEvaluate(operandValue: string): number {
    const int =
      this.valueTypeProvider.Primitives.Integer.fromString(operandValue);
    if (int === undefined) {
      throw new Error(`Could not parse "${operandValue}" into an Integer`);
    }
    return int;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('pola.rs does not support parsing values, currently');
  }
}

export class AsBooleanOperatorEvaluator extends DefaultUnaryOperatorEvaluator<
  string,
  boolean
> {
  constructor(private readonly valueTypeProvider: ValueTypeProvider) {
    super('asBoolean', STRING_TYPEGUARD);
  }
  override doEvaluate(operandValue: string): boolean {
    const bool =
      this.valueTypeProvider.Primitives.Boolean.fromString(operandValue);
    if (bool === undefined) {
      throw new Error(`Could not parse "${operandValue}" into a Boolean`);
    }
    return bool;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected override polarsDoEvaluate(col: PolarsInternal): PolarsInternal {
    throw new Error('pola.rs does not support parsing values, currently');
  }
}
