// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  type BlockDefinition,
  isCompositeBlockTypeDefinition,
} from '@jvalue/jayvee-language-server';

import { type BlockExecutor } from './blocks';
import { type BlockExecutorClass } from './blocks/block-executor-class';
import {
  createCompositeBlockExecutor,
  getInputType,
  getOutputType,
} from './blocks/composite-block-executor';
import { type Logger } from './logging';

export abstract class JayveeExecExtension {
  abstract getBlockExecutors(): BlockExecutorClass[];

  getExecutorForBlockType(
    blockTypeName: string,
    usePolars: boolean,
    logger: Logger,
  ): BlockExecutorClass | undefined {
    if (blockTypeName === 'TableInterpreter') {
      blockTypeName = usePolars
        ? 'PolarsTableInterpreter'
        : 'TsTableInterpreter';
    }
    logger.logDebug(`Trying to find executor for ${blockTypeName}`);

    return this.getBlockExecutors().find(
      (x: BlockExecutorClass) => x.type === blockTypeName,
    );
  }

  createBlockExecutor(
    block: BlockDefinition,
    usePolars: boolean,
    logger: Logger,
  ): BlockExecutor {
    const blockType = block.type.ref;
    assert(blockType !== undefined);

    let blockExecutor = this.getExecutorForBlockType(
      blockType.name,
      usePolars,
      logger,
    );

    if (
      blockExecutor === undefined &&
      isCompositeBlockTypeDefinition(block.type.ref)
    ) {
      blockExecutor = createCompositeBlockExecutor(
        getInputType(block.type.ref),
        getOutputType(block.type.ref),
        block,
      );
    }

    assert(
      blockExecutor !== undefined,
      `No executor was registered for block type ${blockType.name}`,
    );

    return new blockExecutor();
  }
}
