// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import { strict as assert } from 'assert';

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
  isPrimitiveValueType,
} from '@jvalue/jayvee-language-server';
import pl, {
  type DataType as PlDType,
  type ReadCsvOptions,
} from 'nodejs-polars';

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

  // TODO: Decide if this is the right place for this function
  private toPlDType(vt: ValueType, logger: R.Logger): PlDType | undefined {
    if (isPrimitiveValueType(vt)) {
      const dt = vt.asPolarsDType();
      if (dt === undefined) {
        logger.logErr(`${vt.getName()} to polars is not yet implemented`);
        return undefined;
      }
      return dt;
    }
    logger.logDebug(
      `${vt.getName()} isnt primitive and thus not convertible to polars`,
    );
    return undefined;
  }

  protected colsAndSchema(context: ExecutionContext): {
    columnNames: string[];
    schema: Record<string, PlDType>;
  } {
    const vtasss = context.getPropertyValue(
      'columns',
      context.valueTypeProvider.createCollectionValueTypeOf(
        context.valueTypeProvider.Primitives.ValuetypeAssignment,
      ),
    );
    const colDefs = this.deriveColumnDefinitionEntriesWithoutHeader(
      vtasss,
      context,
    );

    const schema: Record<string, PlDType> = {};
    const columnNames = colDefs.map((colDef) => {
      if (!isPrimitiveValueType(colDef.valueType)) {
        throw new Error(
          `${colDef.valueType.getName()} is not supported in tables`,
        );
      }
      const dt = colDef.valueType.asPolarsDType();
      if (dt === undefined) {
        throw new Error(
          `${colDef.valueType.getName()} is not supported in tables`,
        );
      }
      schema[colDef.columnName] = dt;
      return colDef.columnName;
    });
    return {
      columnNames: columnNames,
      schema: schema,
    };
  }
  protected csvOptions(context: ExecutionContext): Partial<ReadCsvOptions> {
    const header = context.getPropertyValue(
      'header',
      context.valueTypeProvider.Primitives.Boolean,
    );
    const encoding = context.getPropertyValue(
      'encoding',
      context.valueTypeProvider.Primitives.Text,
    );
    if (encoding !== 'utf-8') {
      throw new Error(`encoding ${encoding} Only utf8 is supported`);
    }

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
      encoding: 'utf8',
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

    const resultingTable = this.constructAndValidateTable(
      Buffer.from(file.content),
      this.csvOptions(context),
      context,
    );
    context.logger.logDebug(
      `Validation completed, the resulting table has ${resultingTable.getNumberOfRows()} row(s) and ${resultingTable.getNumberOfColumns()} column(s)`,
    );
    return R.ok(resultingTable);
  }

  protected constructAndValidateTable(
    content: Buffer,
    options: Partial<ReadCsvOptions>,
    context: ExecutionContext,
  ): PolarsTable {
    context.logger.logDebug(JSON.stringify(options.schema));
    const df = pl.readCSV(content, options);
    return new PolarsTable(df);
  }

  protected deriveColumnDefinitionEntriesWithoutHeader(
    columnDefinitions: ValuetypeAssignment[],
    context: ExecutionContext,
  ): ColumnDefinitionEntry[] {
    return columnDefinitions.map<ColumnDefinitionEntry>(
      (columnDefinition, columnDefinitionIndex) => {
        const columnValuetype = context.wrapperFactories.ValueType.wrap(
          columnDefinition.type,
        );
        assert(columnValuetype !== undefined);
        return {
          sheetColumnIndex: columnDefinitionIndex,
          columnName: columnDefinition.name,
          valueType: columnValuetype,
          astNode: columnDefinition,
        };
      },
    );
  }

  protected deriveColumnDefinitionEntriesFromHeader(
    columnDefinitions: ValuetypeAssignment[],
    headerRow: string[],
    context: ExecutionContext,
  ): ColumnDefinitionEntry[] {
    context.logger.logDebug(`Matching header with provided column names`);

    const columnEntries: ColumnDefinitionEntry[] = [];
    for (const columnDefinition of columnDefinitions) {
      const indexOfMatchingHeader = headerRow.findIndex(
        (headerColumnName) => headerColumnName === columnDefinition.name,
      );
      if (indexOfMatchingHeader === -1) {
        context.logger.logDebug(
          `Omitting column "${columnDefinition.name}" as the name was not found in the header`,
        );
        continue;
      }
      const columnValuetype = context.wrapperFactories.ValueType.wrap(
        columnDefinition.type,
      );
      assert(columnValuetype !== undefined);

      columnEntries.push({
        sheetColumnIndex: indexOfMatchingHeader,
        columnName: columnDefinition.name,
        valueType: columnValuetype,
        astNode: columnDefinition,
      });
    }

    return columnEntries;
  }
}
