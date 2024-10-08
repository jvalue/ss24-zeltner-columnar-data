// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { DataType as PlDType } from 'nodejs-polars';

import { type InternalValueRepresentation } from '../../../expressions/internal-value-representation';
import { type ValueTypeVisitor } from '../value-type';

import { PrimitiveValueType } from './primitive-value-type';

const TRUE_REGEX = /^true$/i;
const FALSE_REGEX = /^false$/i;

export class BooleanValuetype extends PrimitiveValueType<boolean> {
  acceptVisitor<R>(visitor: ValueTypeVisitor<R>): R {
    return visitor.visitBoolean(this);
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

  override fromString(s: string): boolean | undefined {
    if (TRUE_REGEX.test(s)) {
      return true;
    } else if (FALSE_REGEX.test(s)) {
      return false;
    }
    return undefined;
  }

  override toPolarsDataType(): PlDType {
    return PlDType.Bool;
  }
}
