// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { strict as assert } from 'assert';

import {
  type AstNode,
  type Reference,
  assertUnreachable,
  isReference,
} from 'langium';

import {
  type OperatorEvaluatorRegistry,
  type PolarsInternalValueRepresentation,
  type TsInternalValueRepresentation,
} from '../expressions';
import {
  type BlockTypePipeline,
  type BuiltinConstrainttypeDefinition,
  type CellRangeLiteral,
  type CompositeBlockTypeDefinition,
  type PipeDefinition,
  type PipelineDefinition,
  type ReferenceableBlockTypeDefinition,
  type ValueTypeReference,
  type ValuetypeDefinition,
  isBuiltinConstrainttypeDefinition,
  isReferenceableBlockTypeDefinition,
  isValueTypeReference,
  isValuetypeDefinition,
} from '../generated/ast';

import { type AstNodeWrapper } from './ast-node-wrapper';
import { CellRangeWrapper } from './cell-range-wrapper';
import { PipeWrapper } from './pipe-wrapper';
import { PipelineWrapper } from './pipeline-wrapper';
import { BlockTypeWrapper } from './typed-object/block-type-wrapper';
import { ConstraintTypeWrapper } from './typed-object/constrainttype-wrapper';
import { type PrimitiveValueType, type ValueType } from './value-type';
import { AtomicValueType } from './value-type/atomic-value-type';
import { CollectionValueType } from './value-type/primitive/collection/collection-value-type';
import {
  type PolarsValueTypeProvider,
  type TsValueTypeProvider,
  type ValueTypeProvider,
} from './value-type/primitive/primitive-value-type-provider';

abstract class AstNodeWrapperFactory<
  N extends AstNode,
  W extends AstNodeWrapper<N>,
> {
  abstract canWrap(toBeWrapped: N | Reference<N>): boolean;
  abstract doWrap(toBeWrapped: N | Reference<N>): W;

  wrap(toBeWrapped: N | Reference<N>): W {
    assert(
      this.canWrap(toBeWrapped),
      `AstNode ${this.getName(toBeWrapped)} cannot be wrapped`,
    );
    return this.doWrap(toBeWrapped);
  }

  private getName(toBeWrapped: N | Reference<N>): string {
    const node = isReference(toBeWrapped) ? toBeWrapped.ref : toBeWrapped;
    if (node === undefined) {
      return '<unresolved reference>';
    }
    if ('name' in node && typeof node.name === 'string') {
      return node.name;
    }
    return '<unnamed ast node>';
  }
}

export abstract class WrapperFactoryProvider {
  readonly BlockType: BlockTypeWrapperFactory;
  readonly ConstraintType: ConstraintTypeWrapperFactory;
  readonly Pipeline: PipelineWrapperFactory;
  readonly Pipe: PipeWrapperFactory;
  readonly CellRange: CellRangeWrapperFactory;
  readonly TypedObject: TypedObjectWrapperFactory;
  abstract readonly ValueType: ValueTypeWrapperFactory;

  constructor(
    protected readonly operatorEvaluatorRegistry: OperatorEvaluatorRegistry,
    protected readonly primitiveValueTypeContainer: ValueTypeProvider,
  ) {
    this.CellRange = new CellRangeWrapperFactory();
    this.BlockType = new BlockTypeWrapperFactory(
      this.operatorEvaluatorRegistry,
      primitiveValueTypeContainer,
      this,
    );
    this.ConstraintType = new ConstraintTypeWrapperFactory(
      this.operatorEvaluatorRegistry,
      primitiveValueTypeContainer,
      this,
    );
    this.Pipe = new PipeWrapperFactory();
    this.Pipeline = new PipelineWrapperFactory(this.Pipe);
    this.TypedObject = new TypedObjectWrapperFactory(
      this.BlockType,
      this.ConstraintType,
    );
  }
}

export class PolarsWrapperFactoryProvider extends WrapperFactoryProvider {
  override readonly ValueType: PolarsValueTypeWrapperFactory;

  constructor(
    operatorEvaluatorRegistry: OperatorEvaluatorRegistry,
    protected override readonly primitiveValueTypeContainer: PolarsValueTypeProvider,
  ) {
    super(operatorEvaluatorRegistry, primitiveValueTypeContainer);
    this.ValueType = new PolarsValueTypeWrapperFactory(
      this,
      primitiveValueTypeContainer,
    );
  }
}

export class TsWrapperFactoryProvider extends WrapperFactoryProvider {
  override readonly ValueType: TsValueTypeWrapperFactory;

  constructor(
    operatorEvaluatorRegistry: OperatorEvaluatorRegistry,
    protected override readonly primitiveValueTypeContainer: TsValueTypeProvider,
  ) {
    super(operatorEvaluatorRegistry, primitiveValueTypeContainer);
    this.ValueType = new TsValueTypeWrapperFactory(
      this,
      primitiveValueTypeContainer,
    );
  }
}

class CellRangeWrapperFactory extends AstNodeWrapperFactory<
  CellRangeLiteral,
  CellRangeWrapper
> {
  canWrap(toBeWrapped: CellRangeLiteral): boolean {
    return CellRangeWrapper.canBeWrapped(toBeWrapped);
  }
  doWrap(toBeWrapped: CellRangeLiteral): CellRangeWrapper {
    return new CellRangeWrapper(toBeWrapped);
  }
}

class BlockTypeWrapperFactory extends AstNodeWrapperFactory<
  ReferenceableBlockTypeDefinition,
  BlockTypeWrapper
> {
  constructor(
    private readonly operatorEvaluatorRegistry: OperatorEvaluatorRegistry,
    private readonly valueTypeProvider: ValueTypeProvider,
    private readonly wrapperFactories: WrapperFactoryProvider,
  ) {
    super();
  }

  canWrap(
    toBeWrapped:
      | ReferenceableBlockTypeDefinition
      | Reference<ReferenceableBlockTypeDefinition>,
  ): boolean {
    return BlockTypeWrapper.canBeWrapped(toBeWrapped);
  }
  doWrap(
    toBeWrapped:
      | ReferenceableBlockTypeDefinition
      | Reference<ReferenceableBlockTypeDefinition>,
  ): BlockTypeWrapper {
    return new BlockTypeWrapper(
      toBeWrapped,
      this.operatorEvaluatorRegistry,
      this.valueTypeProvider,
      this.wrapperFactories,
    );
  }
}

class ConstraintTypeWrapperFactory extends AstNodeWrapperFactory<
  BuiltinConstrainttypeDefinition,
  ConstraintTypeWrapper
> {
  constructor(
    private readonly operatorEvaluatorRegistry: OperatorEvaluatorRegistry,
    private readonly valueTypeProvider: ValueTypeProvider,
    private readonly wrapperFactories: WrapperFactoryProvider,
  ) {
    super();
  }

  canWrap(
    toBeWrapped:
      | BuiltinConstrainttypeDefinition
      | Reference<BuiltinConstrainttypeDefinition>,
  ): boolean {
    return ConstraintTypeWrapper.canBeWrapped(toBeWrapped);
  }
  doWrap(
    toBeWrapped:
      | BuiltinConstrainttypeDefinition
      | Reference<BuiltinConstrainttypeDefinition>,
  ): ConstraintTypeWrapper {
    return new ConstraintTypeWrapper(
      toBeWrapped,
      this.operatorEvaluatorRegistry,
      this.valueTypeProvider,
      this.wrapperFactories,
    );
  }
}

class PipelineWrapperFactory extends AstNodeWrapperFactory<
  PipelineDefinition | CompositeBlockTypeDefinition,
  PipelineWrapper<PipelineDefinition | CompositeBlockTypeDefinition>
> {
  constructor(private pipeWrapperFactory: IPipeWrapperFactory) {
    super();
  }

  canWrap(
    toBeWrapped: PipelineDefinition | CompositeBlockTypeDefinition,
  ): boolean {
    return PipelineWrapper.canBeWrapped(toBeWrapped, this.pipeWrapperFactory);
  }

  override wrap<T extends PipelineDefinition | CompositeBlockTypeDefinition>( // override to adjust typing
    toBeWrapped: T | Reference<T>,
  ): PipelineWrapper<T> {
    return super.wrap(toBeWrapped) as PipelineWrapper<T>; // implementation forwards to doWrap, so typing will be correct
  }

  doWrap<T extends PipelineDefinition | CompositeBlockTypeDefinition>(
    toBeWrapped: T,
  ): PipelineWrapper<T> {
    return new PipelineWrapper(toBeWrapped, this.pipeWrapperFactory);
  }
}

export interface IPipeWrapperFactory {
  canWrap(
    toBeWrapped: PipeDefinition | BlockTypePipeline,
    chainIndex: number,
  ): boolean;

  wrap<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
    chainIndex: number,
  ): PipeWrapper<T>;

  doWrap<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
    chainIndex: number,
  ): PipeWrapper<T>;

  wrapAll<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
  ): PipeWrapper<T>[];
}

class PipeWrapperFactory implements IPipeWrapperFactory {
  // does not extend AstNodeWrapperFactory as requires argument chainIndex for wrapping

  canWrap(
    toBeWrapped: PipeDefinition | BlockTypePipeline,
    chainIndex: number,
  ): boolean {
    return PipeWrapper.canBeWrapped(toBeWrapped, chainIndex);
  }

  wrap<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
    chainIndex: number,
  ): PipeWrapper<T> {
    assert(this.canWrap(toBeWrapped, chainIndex), `Pipe cannot be wrapped`);
    return this.doWrap(toBeWrapped, chainIndex);
  }

  doWrap<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
    chainIndex: number,
  ): PipeWrapper<T> {
    return new PipeWrapper(toBeWrapped, chainIndex);
  }

  wrapAll<T extends PipeDefinition | BlockTypePipeline>(
    toBeWrapped: T,
  ): PipeWrapper<T>[] {
    const result: PipeWrapper<T>[] = [];
    for (
      let chainIndex = 0;
      chainIndex < toBeWrapped.blocks.length - 1;
      ++chainIndex
    ) {
      if (!this.canWrap(toBeWrapped, chainIndex)) {
        continue;
      }
      const pipeWrapper = this.wrap(toBeWrapped, chainIndex);
      result.push(pipeWrapper);
    }
    return result;
  }
}

class TypedObjectWrapperFactory {
  // does not extend AstNodeWrapperFactory as behavior differs, e.g., no thrown error, allowing undefined as parameter
  constructor(
    private readonly blockTypeWrapperFactory: BlockTypeWrapperFactory,
    private readonly constraintTypeWrapperFactory: ConstraintTypeWrapperFactory,
  ) {}

  /**
   * Creates a wrapper for the typed object.
   * Returns undefined if wrapping is not possible (does not throw an error).
   */
  wrap(
    toBeWrapped:
      | Reference<ReferenceableBlockTypeDefinition>
      | Reference<BuiltinConstrainttypeDefinition>
      | BuiltinConstrainttypeDefinition
      | ReferenceableBlockTypeDefinition
      | undefined,
  ): BlockTypeWrapper | ConstraintTypeWrapper | undefined {
    const type = isReference(toBeWrapped) ? toBeWrapped.ref : toBeWrapped;
    if (type === undefined) {
      return undefined;
    }

    if (isReferenceableBlockTypeDefinition(type)) {
      if (!this.blockTypeWrapperFactory.canWrap(type)) {
        return undefined;
      }
      return this.blockTypeWrapperFactory.wrap(type);
    } else if (isBuiltinConstrainttypeDefinition(type)) {
      if (!this.constraintTypeWrapperFactory.canWrap(type)) {
        return undefined;
      }
      return this.constraintTypeWrapperFactory.wrap(type);
    }
    assertUnreachable(type);
  }
}

abstract class ValueTypeWrapperFactory {
  constructor(
    protected readonly wrapperFactories: WrapperFactoryProvider,
    protected readonly primitiveValueTypeProvider: ValueTypeProvider,
  ) {}

  abstract wrap(
    identifier: ValuetypeDefinition | ValueTypeReference | undefined,
  ): ValueType | undefined;

  abstract wrapCollection(
    collectionRef: ValueTypeReference,
  ): CollectionValueType;

  abstract wrapPrimitive(
    builtinValuetype: ValuetypeDefinition,
  ): PrimitiveValueType | undefined;
}

class PolarsValueTypeWrapperFactory extends ValueTypeWrapperFactory {
  constructor(
    wrapperFactories: WrapperFactoryProvider,
    protected override readonly primitiveValueTypeProvider: PolarsValueTypeProvider,
  ) {
    super(wrapperFactories, primitiveValueTypeProvider);
  }

  override wrap(
    identifier: ValuetypeDefinition | ValueTypeReference | undefined,
  ): ValueType<PolarsInternalValueRepresentation> | undefined {
    if (identifier === undefined) {
      return undefined;
    } else if (isValueTypeReference(identifier)) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      const valueTypeDefinition = identifier?.reference?.ref;
      if (valueTypeDefinition?.name === 'Collection') {
        return this.wrapCollection(identifier);
      }
      return this.wrap(valueTypeDefinition);
    } else if (isValuetypeDefinition(identifier)) {
      if (identifier.name === 'Collection') {
        // We don't have an object representing a generic collection
        return;
      }
      if (identifier.isBuiltin) {
        return this.wrapPrimitive(identifier);
      }
      return new AtomicValueType<PolarsInternalValueRepresentation>(
        identifier,
        this.primitiveValueTypeProvider,
        this.wrapperFactories,
      );
    }
    assertUnreachable(identifier);
  }

  override wrapCollection(
    collectionRef: ValueTypeReference,
  ): CollectionValueType<PolarsInternalValueRepresentation> {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const collectionDefinition = collectionRef?.reference?.ref;
    assert(collectionDefinition?.name === 'Collection');
    const collectionGenerics = collectionRef.genericRefs;
    if (collectionGenerics.length !== 1) {
      throw new Error(
        "Valuetype Collection needs exactly one generic parameter to define its elements' type",
      );
    }
    const generic = collectionGenerics[0];
    assert(generic !== undefined);
    const elementValuetype = this.wrap(generic.ref);
    if (elementValuetype === undefined) {
      throw new Error(
        "Could not create value type for the elements' type of value type Collection",
      );
    }
    return new CollectionValueType(elementValuetype);
  }

  override wrapPrimitive(
    builtinValuetype: ValuetypeDefinition,
  ): PrimitiveValueType<PolarsInternalValueRepresentation> | undefined {
    assert(builtinValuetype.isBuiltin);
    const name = builtinValuetype.name;
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (name === undefined) {
      return undefined;
    }

    const matchingPrimitives =
      this.primitiveValueTypeProvider.Primitives.getAll().filter(
        (valueType: ValueType<PolarsInternalValueRepresentation>) =>
          valueType.getName() === name,
      );
    if (matchingPrimitives.length === 0) {
      throw new Error(
        `Found no PrimitiveValuetype for builtin value type "${name}"`,
      );
    }
    if (matchingPrimitives.length > 1) {
      throw new Error(
        `Found multiple ambiguous PrimitiveValuetype for builtin value type "${name}"`,
      );
    }
    return matchingPrimitives[0];
  }
}

class TsValueTypeWrapperFactory extends ValueTypeWrapperFactory {
  constructor(
    wrapperFactories: WrapperFactoryProvider,
    protected override readonly primitiveValueTypeProvider: TsValueTypeProvider,
  ) {
    super(wrapperFactories, primitiveValueTypeProvider);
  }

  override wrap(
    identifier: ValuetypeDefinition | ValueTypeReference | undefined,
  ): ValueType<TsInternalValueRepresentation> | undefined {
    if (identifier === undefined) {
      return undefined;
    } else if (isValueTypeReference(identifier)) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      const valueTypeDefinition = identifier?.reference?.ref;
      if (valueTypeDefinition?.name === 'Collection') {
        return this.wrapCollection(identifier);
      }
      return this.wrap(valueTypeDefinition);
    } else if (isValuetypeDefinition(identifier)) {
      if (identifier.name === 'Collection') {
        // We don't have an object representing a generic collection
        return;
      }
      if (identifier.isBuiltin) {
        return this.wrapPrimitive(identifier);
      }
      return new AtomicValueType<TsInternalValueRepresentation>(
        identifier,
        this.primitiveValueTypeProvider,
        this.wrapperFactories,
      );
    }
    assertUnreachable(identifier);
  }

  override wrapCollection(
    collectionRef: ValueTypeReference,
  ): CollectionValueType<TsInternalValueRepresentation> {
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    const collectionDefinition = collectionRef?.reference?.ref;
    assert(collectionDefinition?.name === 'Collection');
    const collectionGenerics = collectionRef.genericRefs;
    if (collectionGenerics.length !== 1) {
      throw new Error(
        "Valuetype Collection needs exactly one generic parameter to define its elements' type",
      );
    }
    const generic = collectionGenerics[0];
    assert(generic !== undefined);
    const elementValuetype = this.wrap(generic.ref);
    if (elementValuetype === undefined) {
      throw new Error(
        "Could not create value type for the elements' type of value type Collection",
      );
    }
    return new CollectionValueType(elementValuetype);
  }

  override wrapPrimitive(
    builtinValuetype: ValuetypeDefinition,
  ): PrimitiveValueType<TsInternalValueRepresentation> | undefined {
    assert(builtinValuetype.isBuiltin);
    const name = builtinValuetype.name;
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (name === undefined) {
      return undefined;
    }

    const matchingPrimitives =
      this.primitiveValueTypeProvider.Primitives.getAll().filter(
        (valueType: ValueType<TsInternalValueRepresentation>) =>
          valueType.getName() === name,
      );
    if (matchingPrimitives.length === 0) {
      throw new Error(
        `Found no PrimitiveValuetype for builtin value type "${name}"`,
      );
    }
    if (matchingPrimitives.length > 1) {
      throw new Error(
        `Found multiple ambiguous PrimitiveValuetype for builtin value type "${name}"`,
      );
    }
    return matchingPrimitives[0];
  }
}
