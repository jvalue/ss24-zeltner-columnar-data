// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { strict as assert } from 'assert';

import {
  BlockDefinition,
  ConstraintDefinition,
  EvaluationContext,
  InternalValueRepresentation,
  PipelineDefinition,
  PropertyAssignment,
  TransformDefinition,
  Valuetype,
  evaluatePropertyValue,
  getOrFailBockMetaInf,
  getOrFailConstraintMetaInf,
  isBlockDefinition,
  isExpressionConstraintDefinition,
  isPipelineDefinition,
  isPropertyBody,
  isTransformDefinition,
  isTypedConstraintDefinition,
} from '@jvalue/jayvee-language-server';
import { assertUnreachable, isReference } from 'langium';

import {
  DebugGranularity,
  DebugTargets,
} from './debugging/debug-configuration';
import { Logger } from './logger';

export type StackNode =
  | BlockDefinition
  | ConstraintDefinition
  | TransformDefinition;

export class ExecutionContext {
  private readonly stack: StackNode[] = [];

  constructor(
    public readonly pipeline: PipelineDefinition,
    public readonly logger: Logger,
    public readonly runOptions: {
      isDebugMode: boolean;
      debugGranularity: DebugGranularity;
      debugTargets: DebugTargets;
    },
    public readonly evaluationContext: EvaluationContext,
  ) {
    logger.setLoggingContext(pipeline.name);
  }

  public enterNode(node: StackNode) {
    this.stack.push(node);

    this.updateLoggingContext();
  }

  public exitNode(node: StackNode) {
    const poppedNode = this.stack.pop();
    assert(poppedNode === node);

    this.updateLoggingContext();
  }

  /**
   * @returns the latest stack node. Returns the pipeline if the stack is empty.
   */
  public getCurrentNode(): StackNode | PipelineDefinition {
    const currentNode = this.stack[this.stack.length - 1];
    if (currentNode === undefined) {
      return this.pipeline;
    }

    return currentNode;
  }

  private updateLoggingContext() {
    this.logger.setLoggingDepth(this.stack.length);
    this.logger.setLoggingContext(this.getCurrentNode().name);
  }

  public getPropertyValue<I extends InternalValueRepresentation>(
    propertyName: string,
    valuetype: Valuetype<I>,
  ): I {
    const property = this.getProperty(propertyName);

    if (property === undefined) {
      return this.getDefaultPropertyValue(propertyName, valuetype);
    }

    const propertyValue = evaluatePropertyValue(
      property,
      this.evaluationContext,
      valuetype,
    );
    assert(propertyValue !== undefined);
    return propertyValue;
  }

  public getProperty(propertyName: string): PropertyAssignment | undefined {
    const currentNode = this.getCurrentNode();
    if (
      isPipelineDefinition(currentNode) ||
      isExpressionConstraintDefinition(currentNode)
    ) {
      return undefined;
    }

    const body = currentNode.body;
    if (!isPropertyBody(body)) {
      return undefined;
    }

    return body.properties.find((property) => property.name === propertyName);
  }

  public getOrFailProperty(propertyName: string): PropertyAssignment {
    const property = this.getProperty(propertyName);
    assert(property !== undefined);

    return property;
  }

  private getDefaultPropertyValue<I extends InternalValueRepresentation>(
    propertyName: string,
    valuetype: Valuetype<I>,
  ): I {
    const metaInf = this.getMetaInformationOfCurrentNode();
    const propertySpec = metaInf.getPropertySpecification(propertyName);
    assert(propertySpec !== undefined);

    const defaultValue = propertySpec.defaultValue;
    assert(defaultValue !== undefined);
    assert(valuetype.isInternalValueRepresentation(defaultValue));

    return defaultValue;
  }

  private getMetaInformationOfCurrentNode() {
    const currentNode = this.getCurrentNode();
    assert(!isPipelineDefinition(currentNode));
    assert(!isExpressionConstraintDefinition(currentNode));
    assert(!isTransformDefinition(currentNode));

    if (isTypedConstraintDefinition(currentNode)) {
      return getOrFailConstraintMetaInf(currentNode.type);
    } else if (isBlockDefinition(currentNode)) {
      assert(isReference(currentNode.type));
      return getOrFailBockMetaInf(currentNode.type);
    }
    assertUnreachable(currentNode);
  }
}
