// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  IOType,
  type InternalValueRepresentation,
  type PolarsInternal,
  type ValueType,
  type ValueTypeProvider,
} from '@jvalue/jayvee-language-server';
import { zipWith } from 'fp-ts/lib/Array.js';
import { type pl } from 'nodejs-polars';

import { type ExecutionContext } from '../../execution-context';
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

export type TableRow = (InternalValueRepresentation | undefined)[];
export type TableRowMap = Record<
  string,
  InternalValueRepresentation | undefined
>;

export const TABLEROW_TYPEGUARD = (value: unknown): value is TableRow =>
  Array.isArray(value) &&
  value.every(
    (e) => e === undefined || INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(e),
  );

/**
 * Invariant: the shape of the table is always a rectangle.
 * This means all columns must have the same size.
 */
export abstract class Table implements IOTypeImplementation<IOType.TABLE> {
  public readonly ioType = IOType.TABLE;

  abstract withColumn(column: TableColumn): Table;
  abstract getNumberOfRows(): number;
  abstract getNumberOfColumns(): number;
  abstract hasColumn(name: string): boolean;
  abstract getColumns(): ReadonlyArray<TableColumn>;
  abstract getColumn(name: string): TableColumn | undefined;
  abstract getRow(id: number): TableRow;
  abstract clone(): Table;
  abstract acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;

  abstract isPolars(): this is PolarsTable;
  abstract isTypescript(): this is TsTable;

  static generateDropTableStatement(tableName: string): string {
    return `DROP TABLE IF EXISTS "${tableName}";`;
  }

  abstract generateInsertValuesStatement(
    tableName: string,
    context: ExecutionContext,
  ): string;

  abstract generateCreateTableStatement(
    tableName: string,
    context: ExecutionContext,
  ): string;
}

export class PolarsTable extends Table {
  public constructor(
    private df: pl.DataFrame,
    private valueTypeProvider: ValueTypeProvider,
  ) {
    super();
  }

  getTypes(vts: ValueTypeProvider): ValueType[] {
    return this.df.dtypes.map((dt) => vts.fromPolarsDType(dt));
  }

  override generateInsertValuesStatement(
    tableName: string,
    context: ExecutionContext,
  ): string {
    const valueRepresentationVisitor = new SQLValueRepresentationVisitor();

    const formattedValues = this.df
      .rows()
      .map((row) => {
        const rowValues = zipWith(
          row,
          this.getTypes(context.valueTypeProvider),
          (e, t) => {
            if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(e)) {
              return t.acceptVisitor(valueRepresentationVisitor)(e);
            }
            return 'NULL';
          },
        );
        return `(${rowValues.join(',')})`;
      })
      .join(', ');

    const formattedColumns = this.df.columns.map((c) => `"${c}"`).join(',');
    const stmnt = `INSERT INTO "${tableName}" (${formattedColumns}) VALUES ${formattedValues}`;
    return stmnt;
  }

  override generateCreateTableStatement(tableName: string): string {
    const columnTypeVisitor = new SQLColumnTypeVisitor();

    const columnStatements = this.getColumns().map((column) => {
      return `"${column.name}" ${column.valueType.acceptVisitor(
        columnTypeVisitor,
      )}`;
    });

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnStatements.join(
      ',',
    )});`;
  }

  override withColumn(column: PolarsTableColumn): PolarsTable {
    const ndf = this.df.withColumn(column.series);

    return new PolarsTable(ndf, this.valueTypeProvider);
  }

  withColumnFromInternal(expr: PolarsInternal): PolarsTable {
    const ndf = this.df.withColumn(expr);
    return new PolarsTable(ndf, this.valueTypeProvider);
  }

  override getNumberOfRows(): number {
    return this.df.height;
  }

  override getNumberOfColumns(): number {
    return this.df.width;
  }

  override hasColumn(name: string): boolean {
    try {
      this.df.getColumn(name);
      return true;
    } catch {
      return false;
    }
  }

  override getColumns(): readonly PolarsTableColumn[] {
    const seriess = this.df.getColumns();
    return seriess.map((s) => {
      return new PolarsTableColumn(s, this.valueTypeProvider);
    });
  }

  override getColumn(name: string): PolarsTableColumn | undefined {
    try {
      const s = this.df.getColumn(name);
      return new PolarsTableColumn(s, this.valueTypeProvider);
    } catch {
      return undefined;
    }
  }

  override getRow(id: number): TableRow {
    const row = this.df.row(id);
    if (TABLEROW_TYPEGUARD(row)) {
      return row;
    }
    throw new Error(`row: Expected InternalRepresentation not ${typeof row} `);
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
}

export class TsTable extends Table {
  public constructor(
    private numberOfRows = 0,
    private columns = new Map<string, TsTableColumn>(),
  ) {
    super();
  }

  override withColumn(column: TsTableColumn): TsTable {
    assert(column.length === this.numberOfRows);
    const nt = this.clone();
    nt.columns.set(column.name, column);
    return nt;
  }

  /**
   * Tries to add a new row to this table.
   * NOTE: This method will only add the row if the table has at least one column!
   * @param row data of this row for each column
   */
  addRow(row: TableRowMap): void {
    const rowLength = Object.keys(row).length;
    assert(
      rowLength === this.columns.size,
      `Added row has the wrong dimension (expected: ${this.columns.size}, actual: ${rowLength})`,
    );
    if (rowLength === 0) {
      return;
    }
    assert(
      Object.keys(row).every((x) => this.hasColumn(x)),
      'Added row does not fit the columns in the table',
    );

    Object.entries(row).forEach(([columnName, value]) => {
      const column = this.columns.get(columnName);
      assert(column !== undefined);

      assert(column.valueType.isInternalValueRepresentation(value));
      column.push(value);
    });

    this.numberOfRows++;
  }

  addColumn(name: string, column: TsTableColumn): void {
    assert(column.length === this.numberOfRows);
    column.name = name;
    this.columns.set(name, column);
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

  override getNumberOfRows(): number {
    return this.numberOfRows;
  }

  override getNumberOfColumns(): number {
    return this.columns.size;
  }

  override hasColumn(name: string): boolean {
    return this.columns.has(name);
  }

  override getColumns(): readonly TsTableColumn[] {
    return [...this.columns.values()];
  }

  override getColumn(name: string): TsTableColumn | undefined {
    return this.columns.get(name);
  }

  getRow(rowId: number): InternalValueRepresentation[] {
    const numberOfRows = this.getNumberOfRows();
    if (rowId >= numberOfRows) {
      throw new Error(
        `Trying to access table row ${rowId} (of ${numberOfRows} rows)`,
      );
    }

    return [...this.columns.values()].map((col) => {
      const cell = col.nth(rowId);
      if (cell === undefined) {
        throw new Error(`Unexpected undefined for cell in row ${rowId}`);
      }
      return cell;
    });
  }

  override generateInsertValuesStatement(tableName: string): string {
    const valueRepresentationVisitor = new SQLValueRepresentationVisitor();

    const columns = this.getColumns();
    const formattedRowValues: string[] = [];
    for (let rowIndex = 0; rowIndex < this.getNumberOfRows(); ++rowIndex) {
      const rowValues: string[] = [];
      for (const column of columns) {
        const entry = column.nth(rowIndex);

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

    const columns = [...this.getColumns()];
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
    this.columns.forEach((column, name) => {
      const clonedName: unknown = JSON.parse(JSON.stringify(name));
      assert(typeof clonedName === 'string');
      newColumns.set(clonedName, column.clone());
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
