// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import * as R from '@jvalue/jayvee-execution';
import {
  AbstractBlockExecutor,
  type BlockExecutorClass,
  PolarsTable,
  implementsStatic,
} from '@jvalue/jayvee-execution';
import { IOType } from '@jvalue/jayvee-language-server';
import pl from 'nodejs-polars';

import { FileToTableInterpreterExecutor } from './file-to-table-interpreter-executor';

@implementsStatic<BlockExecutorClass>()
export class LocalFileToTableExtractorExecutor extends AbstractBlockExecutor<
  IOType.NONE,
  IOType.TABLE
> {
  public static readonly type = 'LocalFileToTableExtractor';

  constructor() {
    super(IOType.NONE, IOType.TABLE);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  override async doExecute(
    _input: R.None,
    context: R.ExecutionContext,
  ): Promise<R.Result<R.Table>> {
    const filePath = context.getPropertyValue(
      'filePath',
      context.valueTypeProvider.Primitives.Text,
    );

    if (filePath.includes('..')) {
      return R.err({
        message: 'File path cannot include "..". Path traversal is restricted.',
        diagnostic: { node: context.getCurrentNode(), property: 'filePath' },
      });
    }

    context.logger.logDebug(`Validating row(s) according to the column types`);

    const options = FileToTableInterpreterExecutor.csvOptions(context);
    const df = pl.readCSV(filePath, options);
    const table = new PolarsTable(df, context.valueTypeProvider);
    context.logger.logDebug(
      `Validation completed, the resulting table has ${table.nRows} row(s) and ${table.nColumns} column(s)`,
    );
    return R.ok(table);
  }
}
