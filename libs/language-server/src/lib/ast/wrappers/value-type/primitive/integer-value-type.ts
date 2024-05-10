// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type Int64 } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions/internal-value-representation';
import { type ValueType, type ValueTypeVisitor } from '../value-type';

import { TsDecimalValuetype } from './decimal-value-type';
import { PrimitiveValueType } from './primitive-value-type';

export class TsIntegerValuetype extends PrimitiveValueType<number> {
  override isConvertibleTo(target: ValueType): boolean {
    return (
      super.isConvertibleTo(target) || target instanceof TsDecimalValuetype
    );
  }

  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitTsInteger(this);
  }

  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }

  override getName(): 'integer' {
    return 'integer';
  }

  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is number {
    return typeof operandValue === 'number' && Number.isInteger(operandValue);
  }

  override isReferenceableByUser() {
    return true;
  }

  override getUserDoc(): string {
    return `
An integer value.
Example: 3
`.trim();
  }
}

export class PolarsIntegerValuetype extends PrimitiveValueType<Int64> {
  override acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitPolarsInteger(this);
  }
  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }
  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is Int64 {
    return operandValue?.toString() === 'Int64';
  }
  override getName(): 'float' {
    return 'float';
  }
}
