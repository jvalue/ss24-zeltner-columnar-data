// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import * as R from '@jvalue/jayvee-execution';
import {
  AbstractBlockExecutor,
  type BlockExecutorClass,
  type ExecutionContext,
  PolarsTable,
  implementsStatic,
} from '@jvalue/jayvee-execution';
import {
  IOType,
  type ValueType,
  type ValuetypeAssignment,
} from '@jvalue/jayvee-language-server';
import pl, {
  type DataType as PlDType,
  type ReadCsvOptions,
} from 'nodejs-polars';

import {
  TableInterpeterExecutor,
  toPolarsDataTypeWithLogs,
} from './table-interpreter-executor';

export interface ColumnDefinitionEntry {
  sheetColumnIndex: number;
  columnName: string;
  valueType: ValueType;
  astNode: ValuetypeAssignment;
}

@implementsStatic<BlockExecutorClass>()
export class FileToTableInterpreterExecutor extends AbstractBlockExecutor<
  IOType.FILE,
  IOType.TABLE
> {
  public static readonly type = 'FileToTableInterpreter';

  constructor() {
    super(IOType.FILE, IOType.TABLE);
  }

  public static colsAndSchema(context: ExecutionContext): {
    columnNames: string[];
    schema: Record<string, PlDType>;
  } {
    const vtasss = context.getPropertyValue(
      'columns',
      context.valueTypeProvider.createCollectionValueTypeOf(
        context.valueTypeProvider.Primitives.ValuetypeAssignment,
      ),
    );
    const colDefs =
      TableInterpeterExecutor.deriveColumnDefinitionEntriesWithoutHeader(
        vtasss,
        context,
      );

    const schema: Record<string, PlDType> = {};
    const columnNames = colDefs.map((colDef) => {
      schema[colDef.columnName] = toPolarsDataTypeWithLogs(
        colDef.valueType,
        context.logger,
      );
      return colDef.columnName;
    });
    return {
      columnNames: columnNames,
      schema: schema,
    };
  }
  public static csvOptions(context: ExecutionContext): Partial<ReadCsvOptions> {
    const header = context.getPropertyValue(
      'header',
      context.valueTypeProvider.Primitives.Boolean,
    );
    const encoding = context.getPropertyValue(
      'encoding',
      context.valueTypeProvider.Primitives.Text,
    );
    if (encoding !== 'utf-8') {
      context.logger.logErr(
        `Encoding ${encoding} not supported. The data might be read incorrectly`,
      );
    }
    const enc = encoding !== 'utf-8' ? 'utf8' : 'utf8-lossy';

    const delimiter = context.getPropertyValue(
      'delimiter',
      context.valueTypeProvider.Primitives.Text,
    );
    const enclosing = context.getPropertyValue(
      'enclosing',
      context.valueTypeProvider.Primitives.Text,
    );
    const { columnNames, schema } = this.colsAndSchema(context);
    return {
      columns: columnNames,
      hasHeader: header,
      encoding: enc,
      sep: delimiter,
      quoteChar: enclosing,
      schema: schema,
    };
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  override async doExecute(
    file: R.BinaryFile,
    context: R.ExecutionContext,
  ): Promise<R.Result<R.Table>> {
    context.logger.logDebug(`Validating row(s) according to the column types`);

    const resultingTable =
      FileToTableInterpreterExecutor.constructAndValidateTable(
        Buffer.from(file.content),
        FileToTableInterpreterExecutor.csvOptions(context),
        context,
      );
    context.logger.logDebug(
      `Validation completed, the resulting table has ${resultingTable.nRows} row(s) and ${resultingTable.nColumns} column(s)`,
    );
    return R.ok(resultingTable);
  }

  public static constructAndValidateTable(
    content: Buffer,
    options: Partial<ReadCsvOptions>,
    context: ExecutionContext,
  ): PolarsTable {
    context.logger.logDebug(JSON.stringify(options.schema));
    const df = pl.readCSV(content, options);
    return new PolarsTable(df, context.valueTypeProvider);
  }
}
