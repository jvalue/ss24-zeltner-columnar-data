// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import * as R from '@jvalue/jayvee-execution';
import {
  AbstractBlockExecutor,
  type BlockExecutorClass,
  type ExecutionContext,
  PolarsTable,
  type Sheet,
  type Table,
  type TableRowMap,
  TsTable,
  implementsStatic,
  isValidValueRepresentation,
  parseValueToInternalRepresentation,
} from '@jvalue/jayvee-execution';
import {
  CellIndex,
  IOType,
  type InternalValueRepresentation,
  type ValueType,
  type ValuetypeAssignment,
  rowIndexToString,
} from '@jvalue/jayvee-language-server';
import { type DataType as PlDType, pl } from 'nodejs-polars';

export interface ColumnDefinitionEntry {
  sheetColumnIndex: number;
  columnName: string;
  valueType: ValueType;
  astNode: ValuetypeAssignment;
}

export abstract class TableInterpeter extends AbstractBlockExecutor<
  IOType.SHEET,
  IOType.TABLE
> {
  constructor() {
    super(IOType.SHEET, IOType.TABLE);
  }

  public static deriveColumnDefinitionEntriesWithoutHeader(
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

  public static deriveColumnDefinitionEntriesFromHeader(
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

  // eslint-disable-next-line @typescript-eslint/require-await
  override async doExecute(
    inputSheet: Sheet,
    context: R.ExecutionContext,
  ): Promise<R.Result<R.Table>> {
    const header = context.getPropertyValue(
      'header',
      context.valueTypeProvider.Primitives.Boolean,
    );
    const columnDefinitions = context.getPropertyValue(
      'columns',
      context.valueTypeProvider.createCollectionValueTypeOf(
        context.valueTypeProvider.Primitives.ValuetypeAssignment,
      ),
    );

    let columnEntries: ColumnDefinitionEntry[];

    if (header) {
      if (inputSheet.getNumberOfRows() < 1) {
        return R.err({
          message: 'The input sheet is empty and thus has no header',
          diagnostic: {
            node: context.getOrFailProperty('header'),
          },
        });
      }

      const headerRow = inputSheet.getHeaderRow();

      columnEntries = TableInterpeter.deriveColumnDefinitionEntriesFromHeader(
        columnDefinitions,
        headerRow,
        context,
      );
    } else {
      if (inputSheet.getNumberOfColumns() < columnDefinitions.length) {
        return R.err({
          message: `There are ${
            columnDefinitions.length
          } column definitions but the input sheet only has ${inputSheet.getNumberOfColumns()} columns`,
          diagnostic: {
            node: context.getOrFailProperty('columns'),
          },
        });
      }

      columnEntries =
        TableInterpeter.deriveColumnDefinitionEntriesWithoutHeader(
          columnDefinitions,
          context,
        );
    }

    const numberOfTableRows = header
      ? inputSheet.getNumberOfRows() - 1
      : inputSheet.getNumberOfRows();
    context.logger.logDebug(
      `Validating ${numberOfTableRows} row(s) according to the column types`,
    );

    const resultingTable = this.constructAndValidateTable(
      inputSheet,
      header,
      columnEntries,
      context,
    );
    context.logger.logDebug(
      `Validation completed, the resulting table has ${resultingTable.nRows} row(s) and ${resultingTable.nColumns} column(s)`,
    );
    return R.ok(resultingTable);
  }

  protected abstract constructAndValidateTable(
    sheet: Sheet,
    header: boolean,
    columnEntries: ColumnDefinitionEntry[],
    context: ExecutionContext,
  ): Table;

  protected parseAndValidateValue(
    value: string,
    valueType: ValueType,
    context: ExecutionContext,
  ): InternalValueRepresentation | undefined {
    const parsedValue = parseValueToInternalRepresentation(value, valueType);
    if (parsedValue === undefined) {
      return undefined;
    }

    if (!isValidValueRepresentation(parsedValue, valueType, context)) {
      return undefined;
    }
    return parsedValue;
  }
}

export function toPolarsDataTypeWithLogs(
  vt: ValueType,
  logger: R.Logger,
): PlDType {
  const dt = vt.toPolarsDataType();
  const defalt = pl.Utf8;
  if (dt === undefined) {
    logger.logErr(
      `${vt.getName()} is neither primitive nor atomic and thus not yet supported. Attempting to use ${defalt.toString()}`,
    );
  }
  return dt ?? defalt;
}

@implementsStatic<BlockExecutorClass>()
export class PolarsTableInterpreterExecutor extends TableInterpeter {
  public static readonly type = 'PolarsTableInterpreter';

  protected override constructAndValidateTable(
    sheet: Sheet,
    header: boolean,
    columnEntries: ColumnDefinitionEntry[],
    context: ExecutionContext,
  ): PolarsTable {
    const rows = header ? sheet.getData().slice(1) : sheet.getData();
    const series = columnEntries.map((cEntry) =>
      this.constructSeries(rows, cEntry, context),
    );
    const df = pl.DataFrame(series);
    return new PolarsTable(df, context.valueTypeProvider);
  }

  private constructSeries(
    rows: readonly (readonly string[])[],
    columnEntry: ColumnDefinitionEntry,
    context: ExecutionContext,
  ): pl.Series {
    const dtype = toPolarsDataTypeWithLogs(
      columnEntry.valueType,
      context.logger,
    );
    const cData = rows.map((row) => {
      const cell = row[columnEntry.sheetColumnIndex];
      if (cell === undefined) {
        throw new Error('columnEntries had more elements than the sheet data');
      }
      const vt = dtype.equals(pl.String)
        ? context.valueTypeProvider.Primitives.Text
        : columnEntry.valueType;
      return this.parseAndValidateValue(cell, vt, context);
    });
    return pl.Series(columnEntry.columnName, cData, dtype);
  }
}

@implementsStatic<BlockExecutorClass>()
export class TsTableInterpreterExecutor extends TableInterpeter {
  public static readonly type = 'TsTableInterpreter';

  protected override constructAndValidateTable(
    sheet: Sheet,
    header: boolean,
    columnEntries: ColumnDefinitionEntry[],
    context: ExecutionContext,
  ): Table {
    const table = new TsTable();

    // add columns
    columnEntries.forEach((columnEntry) => {
      table.addColumn(
        columnEntry.columnName,
        new R.TsTableColumn(columnEntry.columnName, columnEntry.valueType),
      );
    });

    // add rows
    sheet.iterateRows((sheetRow, sheetRowIndex) => {
      if (header && sheetRowIndex === 0) {
        return;
      }

      const tableRow = this.constructAndValidateTableRow(
        sheetRow,
        sheetRowIndex,
        columnEntries,
        context,
      );
      if (tableRow === undefined) {
        context.logger.logDebug(
          `Omitting row ${rowIndexToString(sheetRowIndex)}`,
        );
        return;
      }
      table.addRow(tableRow);
    });
    return table;
  }

  private constructAndValidateTableRow(
    sheetRow: string[],
    sheetRowIndex: number,
    columnEntries: ColumnDefinitionEntry[],
    context: ExecutionContext,
  ): TableRowMap | undefined {
    let invalidRow = false;
    const tableRow: TableRowMap = {};
    columnEntries.forEach((columnEntry) => {
      const sheetColumnIndex = columnEntry.sheetColumnIndex;
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const value = sheetRow[sheetColumnIndex]!;
      const valueType = columnEntry.valueType;

      const parsedValue = this.parseAndValidateValue(value, valueType, context);
      if (parsedValue === undefined) {
        const currentCellIndex = new CellIndex(sheetColumnIndex, sheetRowIndex);
        context.logger.logDebug(
          `Invalid value at cell ${currentCellIndex.toString()}: "${value}" does not match the type ${columnEntry.valueType.getName()}`,
        );
        invalidRow = true;
        return;
      }

      tableRow[columnEntry.columnName] = parsedValue;
    });
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (invalidRow) {
      return undefined;
    }

    assert(Object.keys(tableRow).length === columnEntries.length);
    return tableRow;
  }
}
