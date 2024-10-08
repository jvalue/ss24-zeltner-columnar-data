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

export abstract class JayveeExecExtension {
  abstract getBlockExecutors(): BlockExecutorClass[];

  getExecutorForBlockType(
    blockTypeName: string,
    usePolars: boolean,
    useRusqlite: boolean,
  ): BlockExecutorClass | undefined {
    if (blockTypeName === 'TableInterpreter') {
      blockTypeName = usePolars
        ? 'PolarsTableInterpreter'
        : 'TsTableInterpreter';
    }
    if (blockTypeName === 'TableTransformer') {
      blockTypeName = usePolars
        ? 'PolarsTableTransformer'
        : 'TsTableTransformer';
    }
    if (blockTypeName === 'SQLiteLoader') {
      if (useRusqlite) {
        blockTypeName = 'RustSQLiteLoader';
      } else if (usePolars) {
        blockTypeName = 'PolarsSQLiteLoader';
      } else {
        blockTypeName = 'TsSQLiteLoader';
      }
    }
    return this.getBlockExecutors().find(
      (x: BlockExecutorClass) => x.type === blockTypeName,
    );
  }

  createBlockExecutor(
    block: BlockDefinition,
    usePolars: boolean,
    useRusqlite: boolean,
  ): BlockExecutor {
    const blockType = block.type.ref;
    assert(blockType !== undefined);

    let blockExecutor = this.getExecutorForBlockType(
      blockType.name,
      usePolars,
      useRusqlite,
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
