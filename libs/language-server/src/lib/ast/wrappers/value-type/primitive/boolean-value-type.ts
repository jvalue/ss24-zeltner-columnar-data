// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type Bool } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions/internal-value-representation';
import { type ValueTypeVisitor } from '../value-type';

import { PrimitiveValueType } from './primitive-value-type';

export class TsBooleanValuetype extends PrimitiveValueType<boolean> {
  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitTsBoolean(this);
  }

  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }

  override getName(): 'boolean' {
    return 'boolean';
  }

  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is boolean {
    return typeof operandValue === 'boolean';
  }

  override isReferenceableByUser() {
    return true;
  }

  override getUserDoc(): string {
    return `
A boolean value.
Examples: true, false
`.trim();
  }
}

export class PolarsBoolenValuetype extends PrimitiveValueType<Bool> {
  override acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitPolarsBoolean(this);
  }
  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }
  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is Bool {
    return operandValue?.toString() === 'Bool';
  }
  override getName(): 'bool' {
    return 'bool';
  }
}
