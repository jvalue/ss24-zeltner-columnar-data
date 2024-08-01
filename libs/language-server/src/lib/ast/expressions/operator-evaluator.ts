// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import { type ValidationContext } from '../../validation/validation-context';
import {
  type BinaryExpression,
  type TernaryExpression,
  type UnaryExpression,
} from '../generated/ast';
import { type WrapperFactoryProvider } from '../wrappers';

import {
  evaluateExpression,
  polarsEvaluateExpression,
} from './evaluate-expression';
import { type EvaluationContext } from './evaluation-context';
import { EvaluationStrategy } from './evaluation-strategy';
import {
  type InternalValueRepresentation,
  type InternalValueRepresentationTypeguard,
  type PolarsInternal,
} from './internal-value-representation';
import {
  type BinaryExpressionOperator,
  type TernaryExpressionOperator,
  type UnaryExpressionOperator,
} from './operator-types';

export interface OperatorEvaluator<
  E extends UnaryExpression | BinaryExpression | TernaryExpression,
> {
  readonly operator: E['operator'];

  /**
   * @return the value the expression evaluates to or `undefined` if the evaluation failed.
   */
  evaluate(
    expression: E,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): InternalValueRepresentation | undefined;
}

export interface PolarsOperatorEvaluator<
  E extends UnaryExpression | BinaryExpression | TernaryExpression,
> extends OperatorEvaluator<E> {
  readonly operator: E['operator'];

  polarsEvaluate(
    expression: E,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): PolarsInternal | undefined;
}

export abstract class DefaultUnaryOperatorEvaluator<
  O extends InternalValueRepresentation,
  T extends InternalValueRepresentation,
> implements PolarsOperatorEvaluator<UnaryExpression>
{
  constructor(
    public readonly operator: UnaryExpressionOperator,
    private readonly operandValueTypeguard: InternalValueRepresentationTypeguard<O>,
  ) {}
  polarsEvaluate(
    expression: UnaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    assert(expression.operator === this.operator);
    const operandValue = polarsEvaluateExpression(
      expression.expression,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (operandValue === undefined) {
      return undefined;
    }

    return this.polarsDoEvaluate(operandValue, expression, validationContext);
  }

  protected abstract doEvaluate(
    operandValue: O,
    expression: UnaryExpression,
    context: ValidationContext | undefined,
  ): T | undefined;

  protected polarsDoEvaluate(
    _operand: PolarsInternal,
    expression: UnaryExpression,
    context: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    context?.accept('error', `${expression.operator} is not supported yet.`, {
      node: expression,
    });
    return undefined;
  }

  evaluate(
    expression: UnaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): T | undefined {
    assert(expression.operator === this.operator);
    const operandValue = evaluateExpression(
      expression.expression,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (operandValue === undefined) {
      return undefined;
    }

    assert(this.operandValueTypeguard(operandValue));

    return this.doEvaluate(operandValue, expression, validationContext);
  }
}

export abstract class DefaultBinaryOperatorEvaluator<
  L extends InternalValueRepresentation,
  R extends InternalValueRepresentation,
  T extends InternalValueRepresentation,
> implements PolarsOperatorEvaluator<BinaryExpression>
{
  constructor(
    public readonly operator: BinaryExpressionOperator,
    private readonly leftValueTypeguard: InternalValueRepresentationTypeguard<L>,
    private readonly rightValueTypeguard: InternalValueRepresentationTypeguard<R>,
  ) {}

  protected abstract doEvaluate(
    leftValue: L,
    rightValue: R,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): T | undefined;

  protected polarsDoEvaluate(
    _left: PolarsInternal,
    _right: PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    context?.accept('error', `${expression.operator} is not supported yet.`, {
      node: expression,
    });
    return undefined;
  }

  evaluate(
    expression: BinaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): T | undefined {
    assert(expression.operator === this.operator);
    const leftValue = evaluateExpression(
      expression.left,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (strategy === EvaluationStrategy.LAZY && leftValue === undefined) {
      return undefined;
    }
    const rightValue = evaluateExpression(
      expression.right,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (leftValue === undefined || rightValue === undefined) {
      return undefined;
    }

    assert(this.leftValueTypeguard(leftValue));
    assert(this.rightValueTypeguard(rightValue));

    return this.doEvaluate(
      leftValue,
      rightValue,
      expression,
      validationContext,
    );
  }

  polarsEvaluate(
    expression: BinaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    assert(expression.operator === this.operator);
    const leftValue = polarsEvaluateExpression(
      expression.left,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (strategy === EvaluationStrategy.LAZY && leftValue === undefined) {
      return undefined;
    }
    const rightValue = polarsEvaluateExpression(
      expression.right,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (leftValue === undefined || rightValue === undefined) {
      return undefined;
    }

    return this.polarsDoEvaluate(
      leftValue,
      rightValue,
      expression,
      validationContext,
    );
  }
}

/**
 * This class serves as a base for boolean operators that support short-circuit evaluation.
 * Short-circuit evaluation means that the right operand is not evaluated
 * if the resulting value can be determined by solely evaluating the left operand.
 */
export abstract class BooleanShortCircuitOperatorEvaluator
  implements PolarsOperatorEvaluator<BinaryExpression>
{
  constructor(public readonly operator: 'and' | 'or') {}

  protected abstract canSkipRightOperandEvaluation(leftValue: boolean): boolean;
  protected abstract getShortCircuitValue(): boolean;

  protected abstract doEvaluate(
    leftValue: boolean,
    rightValue: boolean,
  ): boolean;

  protected polarsDoEvaluate(
    _left: PolarsInternal,
    _right: PolarsInternal,
    expression: BinaryExpression,
    context: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    context?.accept('error', `${expression.operator} is not supported yet.`, {
      node: expression,
    });
    return undefined;
  }

  evaluate(
    expression: BinaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): boolean | undefined {
    assert(expression.operator === this.operator);
    const leftValue = evaluateExpression(
      expression.left,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    assert(leftValue === undefined || typeof leftValue === 'boolean');
    if (strategy === EvaluationStrategy.LAZY) {
      if (leftValue === undefined) {
        return undefined;
      }
      if (this.canSkipRightOperandEvaluation(leftValue)) {
        return this.getShortCircuitValue();
      }
    }

    const rightValue = evaluateExpression(
      expression.right,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (leftValue === undefined || rightValue === undefined) {
      return undefined;
    }
    assert(typeof rightValue === 'boolean');

    return this.doEvaluate(leftValue, rightValue);
  }

  polarsEvaluate(
    expression: BinaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): PolarsInternal | undefined {
    assert(expression.operator === this.operator);
    const left = polarsEvaluateExpression(
      expression.left,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );

    if (strategy === EvaluationStrategy.LAZY && left === undefined) {
      return undefined;
    }

    const rightValue = polarsEvaluateExpression(
      expression.right,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );
    if (left === undefined || rightValue === undefined) {
      return undefined;
    }

    return this.polarsDoEvaluate(
      left,
      rightValue,
      expression,
      validationContext,
    );
  }
}

export abstract class DefaultTernaryOperatorEvaluator<
  FirstValue extends InternalValueRepresentation,
  SecondValue extends InternalValueRepresentation,
  ThirdValue extends InternalValueRepresentation,
  ReturnValue extends InternalValueRepresentation,
> implements OperatorEvaluator<TernaryExpression>
{
  constructor(
    public readonly operator: TernaryExpressionOperator,
    private readonly firstValueTypeguard: InternalValueRepresentationTypeguard<FirstValue>,
    private readonly secondValueTypeguard: InternalValueRepresentationTypeguard<SecondValue>,
    private readonly thirdValueTypeguard: InternalValueRepresentationTypeguard<ThirdValue>,
  ) {}

  protected abstract doEvaluate(
    firstValue: FirstValue,
    secondValue: SecondValue,
    thirdValue: ThirdValue,
    expression: TernaryExpression,
    context: ValidationContext | undefined,
  ): ReturnValue | undefined;

  evaluate(
    expression: TernaryExpression,
    evaluationContext: EvaluationContext,
    wrapperFactories: WrapperFactoryProvider,
    strategy: EvaluationStrategy,
    validationContext: ValidationContext | undefined,
  ): ReturnValue | undefined {
    // The following linting exception can be removed when a second ternary operator is added
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    assert(expression.operator === this.operator);

    const firstValue = evaluateExpression(
      expression.first,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );

    if (strategy === EvaluationStrategy.LAZY && firstValue === undefined) {
      return undefined;
    }

    const secondValue = evaluateExpression(
      expression.second,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );

    const thirdValue = evaluateExpression(
      expression.third,
      evaluationContext,
      wrapperFactories,
      validationContext,
      strategy,
    );

    if (
      firstValue === undefined ||
      secondValue === undefined ||
      thirdValue === undefined
    ) {
      return undefined;
    }

    assert(this.firstValueTypeguard(firstValue));
    assert(this.secondValueTypeguard(secondValue));
    assert(this.thirdValueTypeguard(thirdValue));

    return this.doEvaluate(
      firstValue,
      secondValue,
      thirdValue,
      expression,
      validationContext,
    );
  }
}
