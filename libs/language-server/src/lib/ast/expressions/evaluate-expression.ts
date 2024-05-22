// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import * as fp from 'fp-ts';
import { assertUnreachable } from 'langium';
import { pl } from 'nodejs-polars';

import { type ValidationContext } from '../../validation';
import {
  type Expression,
  type PropertyAssignment,
  type ValueLiteral,
  isBinaryExpression,
  isBlockTypeProperty,
  isCellRangeLiteral,
  isCollectionLiteral,
  isExpression,
  isExpressionLiteral,
  isFreeVariableLiteral,
  isRegexLiteral,
  isRuntimeParameterLiteral,
  isTernaryExpression,
  isUnaryExpression,
  isValueLiteral,
} from '../generated/ast';
import { type ValueType, type WrapperFactoryProvider } from '../wrappers';

import { type EvaluationContext } from './evaluation-context';
import { EvaluationStrategy } from './evaluation-strategy';
import { type InternalValueRepresentation } from './internal-value-representation';
import {
  INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  isEveryValueDefined,
} from './typeguards';

export function evaluatePropertyValue<T extends InternalValueRepresentation>(
  property: PropertyAssignment,
  evaluationContext: EvaluationContext,
  wrapperFactories: WrapperFactoryProvider,
  valueType: ValueType<T>,
): T | undefined {
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  const propertyValue = property?.value;
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  assert(propertyValue !== undefined);

  if (isBlockTypeProperty(propertyValue)) {
    // Properties of block types are always undefined
    // because they are set in the block that instantiates the block type
    return undefined;
  }

  let result: InternalValueRepresentation | undefined;
  if (isRuntimeParameterLiteral(propertyValue)) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const runtimeParameterName = propertyValue?.name;
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (runtimeParameterName === undefined) {
      result = undefined;
    } else {
      result = evaluationContext.getValueForRuntimeParameter(
        runtimeParameterName,
        valueType,
      );
    }
  } else if (isExpression(propertyValue)) {
    result = evaluateExpression(
      propertyValue,
      evaluationContext,
      wrapperFactories,
    );
  } else {
    assertUnreachable(propertyValue);
  }

  assert(
    result === undefined || valueType.isInternalValueRepresentation(result),
    `Evaluation result ${
      result?.toString() ?? 'undefined'
    } is not valid: Neither undefined, nor of type ${valueType.getName()}`,
  );
  return result;
}

export function extendPolarsExpression(
  expression: Expression | undefined,
  evaluationContext: EvaluationContext,
  wrapperFactories: WrapperFactoryProvider,
  colLen: number,
  context: ValidationContext | undefined = undefined,
  strategy: EvaluationStrategy = EvaluationStrategy.LAZY,
): fp.either.Either<pl.Expr, pl.Series> | undefined {
  if (expression === undefined) {
    return undefined;
  }
  if (isExpressionLiteral(expression)) {
    if (isFreeVariableLiteral(expression)) {
      const fv = evaluationContext.getValueFor(expression);
      if (fv === undefined) {
        return undefined;
      }
      if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(fv)) {
        return fp.either.right(pl.repeat(fv, colLen));
      }
      return fp.either.left(fv);
    } else if (isValueLiteral(expression)) {
      const lit = evaluateValueLiteral(
        expression,
        evaluationContext,
        wrapperFactories,
        context,
        strategy,
      );
      return fp.either.right(pl.repeat(lit, colLen));
    }
    assertUnreachable(expression);
  }
  if (isUnaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.unary[operator];
    throw new Error('Unary expressions are not supported yet');
  }
  if (isBinaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.binary[operator];
    throw new Error('Unary expressions are not supported yet');
  }
  if (isTernaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.ternary[operator];

    throw new Error('Unary expressions are not supported yet');
  }
  assertUnreachable(expression);
}

export function evaluateExpression(
  expression: Expression | undefined,
  evaluationContext: EvaluationContext,
  wrapperFactories: WrapperFactoryProvider,
  context: ValidationContext | undefined = undefined,
  strategy: EvaluationStrategy = EvaluationStrategy.LAZY,
): InternalValueRepresentation | undefined {
  if (expression === undefined) {
    return undefined;
  }
  if (isExpressionLiteral(expression)) {
    if (isFreeVariableLiteral(expression)) {
      const fv = evaluationContext.getValueFor(expression);
      assert(fv === undefined || INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(fv));
      return fv;
    } else if (isValueLiteral(expression)) {
      return evaluateValueLiteral(
        expression,
        evaluationContext,
        wrapperFactories,
        context,
        strategy,
      );
    }
    assertUnreachable(expression);
  }
  if (isUnaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.unary[operator];
    return evaluator.evaluate(
      expression,
      evaluationContext,
      wrapperFactories,
      strategy,
      context,
    );
  }
  if (isBinaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.binary[operator];
    return evaluator.evaluate(
      expression,
      evaluationContext,
      wrapperFactories,
      strategy,
      context,
    );
  }
  if (isTernaryExpression(expression)) {
    const operator = expression.operator;
    const evaluator = evaluationContext.operatorRegistry.ternary[operator];
    return evaluator.evaluate(
      expression,
      evaluationContext,
      wrapperFactories,
      strategy,
      context,
    );
  }
  assertUnreachable(expression);
}

function evaluateValueLiteral(
  expression: ValueLiteral,
  evaluationContext: EvaluationContext,
  wrapperFactories: WrapperFactoryProvider,
  validationContext: ValidationContext | undefined = undefined,
  strategy: EvaluationStrategy = EvaluationStrategy.LAZY,
): InternalValueRepresentation | undefined {
  if (isCollectionLiteral(expression)) {
    const evaluatedCollection = expression.values.map((v) =>
      evaluateExpression(
        v,
        evaluationContext,
        wrapperFactories,
        validationContext,
        strategy,
      ),
    );
    if (!isEveryValueDefined(evaluatedCollection)) {
      return undefined;
    }
    return evaluatedCollection;
  }
  if (isCellRangeLiteral(expression)) {
    if (!wrapperFactories.CellRange.canWrap(expression)) {
      return undefined;
    }
    return expression;
  }
  if (isRegexLiteral(expression)) {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (expression?.value === undefined) {
      return undefined;
    }
    return new RegExp(expression.value);
  }
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
  return expression?.value;
}
