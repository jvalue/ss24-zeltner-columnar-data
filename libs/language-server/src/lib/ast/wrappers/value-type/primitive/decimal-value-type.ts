// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type Float64 } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions/internal-value-representation';
import { type ValueTypeVisitor } from '../value-type';

import { PrimitiveValueType } from './primitive-value-type';

export class TsDecimalValuetype extends PrimitiveValueType<number> {
  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitTsDecimal(this);
  }

  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }

  override getName(): 'decimal' {
    return 'decimal';
  }

  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is number {
    return typeof operandValue === 'number' && Number.isFinite(operandValue);
  }

  override isReferenceableByUser() {
    return true;
  }

  override getUserDoc(): string {
    return `
A decimal value.
Example: 3.14
`.trim();
  }
}

export class PolarsDecimalValuetype extends PrimitiveValueType<Float64> {
  override acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitPolarsDecimal(this);
  }
  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }
  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is Float64 {
    return operandValue?.toString() === 'Float64';
  }
  override getName(): 'int' {
    return 'int';
  }
}
