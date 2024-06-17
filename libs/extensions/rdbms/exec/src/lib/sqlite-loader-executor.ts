// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

import * as R from '@jvalue/jayvee-execution';
import {
  AbstractBlockExecutor,
  type BlockExecutorClass,
  type ExecutionContext,
  NONE,
  type None,
  Table,
  implementsStatic,
} from '@jvalue/jayvee-execution';
import { IOType } from '@jvalue/jayvee-language-server';
import sqlite3 from 'sqlite3';

export abstract class SQLiteLoaderExecutor<
  T extends R.Table,
> extends AbstractBlockExecutor<IOType.TABLE, IOType.NONE> {
  constructor() {
    super(IOType.TABLE, IOType.NONE);
  }

  override async doExecute(
    table: T,
    context: ExecutionContext,
  ): Promise<R.Result<None>> {
    const file = context.getPropertyValue(
      'file',
      context.valueTypeProvider.Primitives.Text,
    );
    const tableName = context.getPropertyValue(
      'table',
      context.valueTypeProvider.Primitives.Text,
    );
    const dropTable = context.getPropertyValue(
      'dropTable',
      context.valueTypeProvider.Primitives.Boolean,
    );
    return await this.executeLoad(table, file, tableName, dropTable, context);
  }

  protected abstract executeLoad(
    table: T,
    file: string,
    tableName: string,
    dropTable: boolean,
    context: ExecutionContext,
  ): Promise<R.Result<None>>;

  protected async runQuery(
    db: sqlite3.Database,
    query: string,
  ): Promise<sqlite3.RunResult> {
    return new Promise((resolve, reject) => {
      db.run(query, (result: sqlite3.RunResult, error: Error | null) =>
        error ? reject(error) : resolve(result),
      );
    });
  }
}

@implementsStatic<BlockExecutorClass>()
export class PolarsSQLiteLoaderExecutor extends SQLiteLoaderExecutor<R.PolarsTable> {
  public static readonly type = 'PolarsSQLiteLoader';

  protected override async executeLoad(
    table: R.PolarsTable,
    file: string,
    tableName: string,
    dropTable: boolean,
    context: R.ExecutionContext,
  ): Promise<R.Result<R.None>> {
    let db: sqlite3.Database | undefined;
    try {
      context.logger.logDebug(`Opening database file ${file}`);
      db = new sqlite3.Database(file);

      if (dropTable) {
        context.logger.logDebug(
          `Dropping previous table "${tableName}" if it exists`,
        );
        await this.runQuery(db, Table.generateDropTableStatement(tableName));
      }

      context.logger.logDebug(`Creating table "${tableName}"`);
      await this.runQuery(db, table.generateCreateTableStatement(tableName));
      context.logger.logDebug(
        `Inserting ${table.nRows} row(s) into table "${tableName}"`,
      );
      await this.runQuery(db, table.generateInsertValuesStatement(tableName));

      context.logger.logDebug(
        `The data was successfully loaded into the database`,
      );
      return R.ok(NONE);
    } catch (err: unknown) {
      return R.err({
        message: `Could not write to sqlite database: ${
          err instanceof Error ? err.message : JSON.stringify(err)
        }`,
        diagnostic: { node: context.getCurrentNode(), property: 'name' },
      });
    } finally {
      db?.close();
    }
  }
}

@implementsStatic<BlockExecutorClass>()
export class TsSQLiteLoaderExecutor extends SQLiteLoaderExecutor<R.TsTable> {
  public static readonly type = 'TsSQLiteLoader';

  protected override async executeLoad(
    table: R.TsTable,
    file: string,
    tableName: string,
    dropTable: boolean,
    context: ExecutionContext,
  ): Promise<R.Result<None>> {
    let db: sqlite3.Database | undefined;

    try {
      context.logger.logDebug(`Opening database file ${file}`);
      db = new sqlite3.Database(file);

      if (dropTable) {
        context.logger.logDebug(
          `Dropping previous table "${tableName}" if it exists`,
        );
        await this.runQuery(db, Table.generateDropTableStatement(tableName));
      }

      context.logger.logDebug(`Creating table "${tableName}"`);
      await this.runQuery(db, table.generateCreateTableStatement(tableName));
      context.logger.logDebug(
        `Inserting ${table.nRows} row(s) into table "${tableName}"`,
      );
      await this.runQuery(db, table.generateInsertValuesStatement(tableName));

      context.logger.logDebug(
        `The data was successfully loaded into the database`,
      );
      return R.ok(NONE);
    } catch (err: unknown) {
      return R.err({
        message: `Could not write to sqlite database: ${
          err instanceof Error ? err.message : JSON.stringify(err)
        }`,
        diagnostic: { node: context.getCurrentNode(), property: 'name' },
      });
    } finally {
      db?.close();
    }
  }
}
