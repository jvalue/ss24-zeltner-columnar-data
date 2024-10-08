// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';
import { type Writable } from 'node:stream';

import {
  INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  IOType,
  type InternalValueRepresentation,
  type ValueType,
  type ValueTypeProvider,
} from '@jvalue/jayvee-language-server';
import { zipWith } from 'fp-ts/lib/Array.js';
import { type WriteIPCOptions, type pl } from 'nodejs-polars';

import {
  SQLColumnTypeVisitor,
  SQLValueRepresentationVisitor,
} from '../value-types/visitors/';

import {
  type IOTypeImplementation,
  type IoTypeVisitor,
} from './io-type-implementation';
import {
  PolarsTableColumn,
  type TableColumn,
  type TsTableColumn,
} from './table-column';

export * from './table-column';

export type TableRow = Record<string, InternalValueRepresentation>;

/**
 * Invariant: the shape of the table is always a rectangle.
 * This means all columns must have the same size.
 */
export abstract class Table implements IOTypeImplementation<IOType.TABLE> {
  public readonly ioType = IOType.TABLE;

  abstract withColumn(column: TableColumn): Table;
  abstract get nRows(): number;
  abstract get nColumns(): number;
  abstract get columns(): ReadonlyArray<TableColumn>;
  abstract getColumn(name: string): TableColumn | undefined;
  abstract clone(): Table;
  abstract acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;

  abstract isPolars(): this is PolarsTable;
  abstract isTypescript(): this is TsTable;

  static generateDropTableStatement(tableName: string): string {
    return `DROP TABLE IF EXISTS "${tableName}";`;
  }

  abstract generateInsertValuesStatement(tableName: string): string;
  abstract generateCreateTableStatement(tableName: string): string;
}

export class PolarsTable extends Table {
  public constructor(
    private df: pl.DataFrame,
    private valueTypeProvider: ValueTypeProvider,
  ) {
    super();
  }

  getTypes(): ValueType[] {
    return this.df.dtypes.map((dt) =>
      this.valueTypeProvider.fromPolarsDType(dt),
    );
  }

  override generateInsertValuesStatement(tableName: string): string {
    const valueRepresentationVisitor = new SQLValueRepresentationVisitor();

    const formattedValues = this.df
      .rows()
      .map((row) => {
        const rowValues = zipWith(row, this.getTypes(), (e, t) => {
          if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(e)) {
            return t.acceptVisitor(valueRepresentationVisitor)(e);
          }
          return 'NULL';
        });
        return `(${rowValues.join(',')})`;
      })
      .join(', ');

    const formattedColumns = this.df.columns.map((c) => `"${c}"`).join(',');
    const stmnt = `INSERT INTO "${tableName}" (${formattedColumns}) VALUES ${formattedValues}`;
    return stmnt;
  }

  override generateCreateTableStatement(tableName: string): string {
    const columnTypeVisitor = new SQLColumnTypeVisitor();

    const columnStatements = this.columns.map((column) => {
      return `"${column.name}" ${column.valueType.acceptVisitor(
        columnTypeVisitor,
      )}`;
    });

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnStatements.join(
      ',',
    )});`;
  }

  override withColumn(column: PolarsTableColumn): PolarsTable;
  withColumn(expr: pl.Expr): PolarsTable;
  withColumn(column: PolarsTableColumn | pl.Expr): PolarsTable {
    const tmp = 'series' in column ? column.series : column;
    const ndf = this.df.withColumn(tmp);

    return new PolarsTable(ndf, this.valueTypeProvider);
  }

  override get nRows(): number {
    return this.df.height;
  }

  override get nColumns(): number {
    return this.df.width;
  }

  override get columns(): readonly PolarsTableColumn[] {
    const seriess = this.df.getColumns();
    return seriess.map((s) => {
      const valueType = this.valueTypeProvider.fromPolarsDType(s.dtype);
      return new PolarsTableColumn(s, valueType);
    });
  }

  override getColumn(name: string): PolarsTableColumn | undefined {
    try {
      const s = this.df.getColumn(name);
      const valueType = this.valueTypeProvider.fromPolarsDType(s.dtype);
      return new PolarsTableColumn(s, valueType);
    } catch {
      return undefined;
    }
  }

  override clone(): PolarsTable {
    return new PolarsTable(this.df.clone(), this.valueTypeProvider);
  }

  override acceptVisitor<R>(visitor: IoTypeVisitor<R>): R {
    return visitor.visitPolarsTable(this);
  }

  override isPolars(): this is PolarsTable {
    return true;
  }

  override isTypescript(): this is TsTable {
    return false;
  }

  override toString(): string {
    return this.df.toString();
  }

  writeIpc(options?: WriteIPCOptions): Buffer {
    return this.df.writeIPC(options);
  }
  writeIpcTo(destination: string | Writable, options?: WriteIPCOptions): void {
    this.df.writeIPC(destination, options);
  }
}

export class TsTable extends Table {
  public constructor(
    private numberOfRows = 0,
    private _columns = new Map<string, TsTableColumn>(),
  ) {
    super();
  }

  override withColumn(column: TsTableColumn): TsTable {
    assert(column.length === this.numberOfRows);
    const newTable = this.clone();
    newTable.addColumn(column.name, column);
    return newTable;
  }

  addColumn(name: string, column: TsTableColumn): void {
    assert(column.length === this.numberOfRows);
    column.name = name;
    this._columns.set(name, column);
  }

  /**
   * Tries to add a new row to this table.
   * NOTE: This method will only add the row if the table has at least one column!
   * @param row data of this row for each column
   */
  addRow(row: TableRow): void {
    const rowLength = Object.keys(row).length;
    assert(
      rowLength === this._columns.size,
      `Added row has the wrong dimension (expected: ${this.nColumns}, actual: ${rowLength})`,
    );
    if (rowLength === 0) {
      return;
    }
    assert(
      Object.keys(row).every((x) => this.hasColumn(x)),
      'Added row does not fit the columns in the table',
    );

    Object.entries(row).forEach(([columnName, value]) => {
      const column = this._columns.get(columnName);
      assert(column !== undefined);

      assert(column.valueType.isInternalValueRepresentation(value));
      column.push(value);
    });

    this.numberOfRows++;
  }

  dropRow(rowIdx: number): void {
    assert(rowIdx < this.numberOfRows);

    this.columns.forEach((column) => {
      column.drop(rowIdx);
    });

    this.numberOfRows--;
  }

  dropRows(rowIds: number[]): void {
    rowIds
      .sort((a, b) => b - a) // delete descending to avoid messing up row indices
      .forEach((rowId) => {
        this.dropRow(rowId);
      });
  }

  override get nRows(): number {
    return this.numberOfRows;
  }

  override get nColumns(): number {
    return this._columns.size;
  }

  hasColumn(name: string): boolean {
    return this._columns.has(name);
  }

  override get columns(): readonly TsTableColumn[] {
    return [...this._columns.values()];
  }

  override getColumn(name: string): TsTableColumn | undefined {
    return this._columns.get(name);
  }

  getRow(rowId: number): InternalValueRepresentation[] {
    const numberOfRows = this.nRows;
    if (rowId >= numberOfRows) {
      throw new Error(
        `Trying to access table row ${rowId} (of ${numberOfRows} rows)`,
      );
    }

    return [...this.columns.values()].map((col) => {
      const cell = col.at(rowId);
      if (cell === undefined) {
        throw new Error(`Unexpected undefined for cell in row ${rowId}`);
      }
      return cell;
    });
  }

  override generateInsertValuesStatement(tableName: string): string {
    const valueRepresentationVisitor = new SQLValueRepresentationVisitor();

    const columns = this.columns;
    const formattedRowValues: string[] = [];
    for (let rowIndex = 0; rowIndex < this.nRows; ++rowIndex) {
      const rowValues: string[] = [];
      for (const column of columns) {
        const entry = column.at(rowIndex);

        const formattedValue =
          entry === undefined
            ? 'NULL'
            : column.valueType.acceptVisitor(valueRepresentationVisitor)(entry);

        rowValues.push(formattedValue);
      }
      formattedRowValues.push(`(${rowValues.join(',')})`);
    }

    const formattedColumns = columns.map((c) => `"${c.name}"`).join(',');

    return `INSERT INTO "${tableName}" (${formattedColumns}) VALUES ${formattedRowValues.join(
      ', ',
    )}`;
  }

  override generateCreateTableStatement(tableName: string): string {
    const columnTypeVisitor = new SQLColumnTypeVisitor();

    const columns = [...this.columns];
    const columnStatements = columns.map((column) => {
      return `"${column.name}" ${column.valueType.acceptVisitor(
        columnTypeVisitor,
      )}`;
    });

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnStatements.join(
      ',',
    )});`;
  }

  override clone(): TsTable {
    const newColumns: Map<string, TsTableColumn> = new Map();
    this._columns.forEach((column, name) => {
      newColumns.set(name, column.clone());
    });

    return new TsTable(this.numberOfRows, newColumns);
  }

  override acceptVisitor<R>(visitor: IoTypeVisitor<R>): R {
    return visitor.visitTsTable(this);
  }

  override isPolars(): this is PolarsTable {
    return false;
  }

  override isTypescript(): this is TsTable {
    return true;
  }
}
