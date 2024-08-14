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
  type PortDetails,
  TsTransformExecutor,
  implementsStatic,
} from '@jvalue/jayvee-execution';
import { IOType } from '@jvalue/jayvee-language-server';
import { pl } from 'nodejs-polars';

export abstract class TableTransformerExecutor extends AbstractBlockExecutor<
  IOType.TABLE,
  IOType.TABLE
> {
  constructor() {
    super(IOType.TABLE, IOType.TABLE);
  }

  protected logColumnOverwriteStatus(
    inputTable: R.Table,
    outputColumnName: string,
    context: R.ExecutionContext,
    transformOutputDetails: PortDetails,
  ) {
    const outputColumn = inputTable.getColumn(outputColumnName);
    if (outputColumn !== undefined) {
      context.logger.logInfo(
        `Column "${outputColumnName}" will be overwritten`,
      );

      // log if output column type changes
      if (!outputColumn.valueType.equals(transformOutputDetails.valueType)) {
        context.logger.logInfo(
          `Column "${outputColumnName}" will change its type from ${outputColumn.valueType.getName()} to ${transformOutputDetails.valueType.getName()}`,
        );
      }
    }
  }

  protected checkInputColumnsExist(
    inputColumnNames: string[],
    inputTable: R.Table,
    context: R.ExecutionContext,
  ): R.Result<undefined> {
    // check input columns exist
    let i = 0;
    for (const inputColumnName of inputColumnNames) {
      const inputColumn = inputTable.getColumn(inputColumnName);
      if (inputColumn === undefined) {
        return R.err({
          message: `The specified input column "${inputColumnName}" does not exist in the given table`,
          diagnostic: {
            node: context.getOrFailProperty('inputColumns').value,
            property: 'values',
            index: i,
          },
        });
      }
      ++i;
    }
    return R.ok(undefined);
  }
}

@implementsStatic<BlockExecutorClass>()
export class PolarsTableTransformerExecutor extends TableTransformerExecutor {
  public static readonly type = 'PolarsTableTransformer';

  // INFO: This function
  // checks whether the input column ValueTypes match with the transform input ValueTypes
  // returns a map from input names to column names
  //
  private checkInputColumnsMatchTransformInputTypes(
    inputColumnNames: string[],
    inputTable: R.PolarsTable,
    transformInputDetailsList: PortDetails[],
    context: R.ExecutionContext,
  ): R.Result<Map<string, pl.Expr>> {
    const variableToColumnMap = new Map<string, pl.Expr>();
    for (let i = 0; i < inputColumnNames.length; ++i) {
      const inputColumnName = inputColumnNames[i];
      assert(inputColumnName !== undefined);
      const inputColumn = inputTable.getColumn(inputColumnName);
      assert(inputColumn !== undefined);

      const matchingInputDetails = transformInputDetailsList[i];
      assert(matchingInputDetails !== undefined);

      if (
        !inputColumn.valueType.isConvertibleTo(matchingInputDetails.valueType)
      ) {
        return R.err({
          message: `Type ${inputColumn.valueType.getName()} of column "${inputColumnName}" is not convertible to type ${matchingInputDetails.valueType.getName()}`,
          diagnostic: {
            node: context.getOrFailProperty('uses'),
          },
        });
      }
      const variableName = matchingInputDetails.port.name;
      variableToColumnMap.set(variableName, pl.col(inputColumn.name));
    }
    return R.ok(variableToColumnMap);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  override async doExecute(
    inputTable: R.PolarsTable,
    context: R.ExecutionContext,
  ): Promise<R.Result<R.PolarsTable>> {
    const inputColumnNames = context.getPropertyValue(
      'inputColumns',
      context.valueTypeProvider.createCollectionValueTypeOf(
        context.valueTypeProvider.Primitives.Text,
      ),
    );
    const outputColumnName = context.getPropertyValue(
      'outputColumn',
      context.valueTypeProvider.Primitives.Text,
    );
    const usedTransform = context.getPropertyValue(
      'uses',
      context.valueTypeProvider.Primitives.Transform,
    );

    const checkInputColumnsExistResult = this.checkInputColumnsExist(
      inputColumnNames,
      inputTable,
      context,
    );
    if (R.isErr(checkInputColumnsExistResult)) {
      return checkInputColumnsExistResult;
    }

    const executor = new R.PolarsTransformExecutor(usedTransform, context);

    const transformInputDetailsList = executor.getInputDetails();
    const checkInputColumnsMatchTransformInputTypesResult =
      this.checkInputColumnsMatchTransformInputTypes(
        inputColumnNames,
        inputTable,
        transformInputDetailsList,
        context,
      );
    if (R.isErr(checkInputColumnsMatchTransformInputTypesResult)) {
      return checkInputColumnsMatchTransformInputTypesResult;
    }

    const varToCols = R.okData(checkInputColumnsMatchTransformInputTypesResult);

    const expr = executor.executeTransform(varToCols, context);

    this.logColumnOverwriteStatus(
      inputTable,
      outputColumnName,
      context,
      executor.getOutputDetails(),
    );

    if (expr === undefined) {
      return R.err({
        message: 'Skipping transform: Could not evaluate transform expression',
        diagnostic: {
          node: context.getCurrentNode(),
        },
      });
    }

    const newTable = inputTable.withColumn(expr.alias(outputColumnName));
    return R.ok(newTable);
  }
}

@implementsStatic<BlockExecutorClass>()
export class TsTableTransformerExecutor extends TableTransformerExecutor {
  public static readonly type = 'TsTableTransformer';

  private checkInputColumnsMatchTransformInputTypes(
    inputColumnNames: string[],
    inputTable: R.TsTable,
    transformInputDetailsList: PortDetails[],
    context: R.ExecutionContext,
  ): R.Result<Map<string, R.TsTableColumn>> {
    const variableToColumnMap = new Map<string, R.TsTableColumn>();
    for (let i = 0; i < inputColumnNames.length; ++i) {
      const inputColumnName = inputColumnNames[i];
      assert(inputColumnName !== undefined);
      const inputColumn = inputTable.getColumn(inputColumnName);
      assert(inputColumn !== undefined);

      const matchingInputDetails = transformInputDetailsList[i];
      assert(matchingInputDetails !== undefined);

      if (
        !inputColumn.valueType.isConvertibleTo(matchingInputDetails.valueType)
      ) {
        return R.err({
          message: `Type ${inputColumn.valueType.getName()} of column "${inputColumnName}" is not convertible to type ${matchingInputDetails.valueType.getName()}`,
          diagnostic: {
            node: context.getOrFailProperty('uses'),
          },
        });
      }
      const variableName = matchingInputDetails.port.name;
      variableToColumnMap.set(variableName, inputColumn);
    }
    return R.ok(variableToColumnMap);
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  override async doExecute(
    inputTable: R.TsTable,
    context: ExecutionContext,
  ): Promise<R.Result<R.TsTable>> {
    const inputColumnNames = context.getPropertyValue(
      'inputColumns',
      context.valueTypeProvider.createCollectionValueTypeOf(
        context.valueTypeProvider.Primitives.Text,
      ),
    );
    const outputColumnName = context.getPropertyValue(
      'outputColumn',
      context.valueTypeProvider.Primitives.Text,
    );
    const usedTransform = context.getPropertyValue(
      'uses',
      context.valueTypeProvider.Primitives.Transform,
    );

    const checkInputColumnsExistResult = this.checkInputColumnsExist(
      inputColumnNames,
      inputTable,
      context,
    );
    if (R.isErr(checkInputColumnsExistResult)) {
      return checkInputColumnsExistResult;
    }

    const executor = new TsTransformExecutor(usedTransform, context);
    const transformInputDetailsList = executor.getInputDetails();
    const transformOutputDetails = executor.getOutputDetails();

    const checkInputColumnsMatchTransformInputTypesResult =
      this.checkInputColumnsMatchTransformInputTypes(
        inputColumnNames,
        inputTable,
        transformInputDetailsList,
        context,
      );
    if (R.isErr(checkInputColumnsMatchTransformInputTypesResult)) {
      return checkInputColumnsMatchTransformInputTypesResult;
    }
    const variableToColumnMap = R.okData(
      checkInputColumnsMatchTransformInputTypesResult,
    );

    this.logColumnOverwriteStatus(
      inputTable,
      outputColumnName,
      context,
      transformOutputDetails,
    );

    const transformResult = executor.executeTransform(
      {
        columns: variableToColumnMap,
        numberOfRows: inputTable.nRows,
      },
      context,
    );

    if (transformResult === undefined) {
      return R.ok(inputTable);
    }

    const outputTable = this.createOutputTable(
      inputTable,
      transformResult,
      outputColumnName,
    );

    return R.ok(outputTable);
  }

  private createOutputTable(
    inputTable: R.TsTable,
    transformResult: {
      resultingColumn: R.TsTableColumn;
      rowsToDelete: number[];
    },
    outputColumnName: string,
  ) {
    const outputTable = inputTable.clone();
    outputTable.dropRows(transformResult.rowsToDelete);
    outputTable.addColumn(outputColumnName, transformResult.resultingColumn);
    return outputTable;
  }
}
