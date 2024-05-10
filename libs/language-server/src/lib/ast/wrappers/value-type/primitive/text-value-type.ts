// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { type String as PolarsString } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions/internal-value-representation';
import { type ValueTypeVisitor } from '../value-type';

import { PrimitiveValueType } from './primitive-value-type';

export class TsTextValuetype extends PrimitiveValueType<string> {
  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitTsText(this);
  }

  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }

  override getName(): 'text' {
    return 'text';
  }

  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is string {
    return typeof operandValue === 'string';
  }

  override isReferenceableByUser() {
    return true;
  }

  override getUserDoc(): string {
    return `
A text value. 
Example: "Hello World"
`.trim();
  }
}

export class PolarsTextValuetype extends PrimitiveValueType<PolarsString> {
  override acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitPolarsText(this);
  }
  override isAllowedAsRuntimeParameter(): boolean {
    return true;
  }
  override isInternalValueRepresentation(
    operandValue: InternalValueRepresentation | undefined,
  ): operandValue is PolarsString {
    return operandValue?.toString() === 'String';
  }
  override getName(): 'string' {
    return 'string';
  }
}
