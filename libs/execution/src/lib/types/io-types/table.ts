// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  IOType,
  type InternalValueRepresentation,
  type PolarsAtomicInternalValueRepresentation,
  type TsInternalValueRepresentation,
  type ValueType,
} from '@jvalue/jayvee-language-server';
import { DataType, pl } from 'nodejs-polars';

import {
  SQLColumnTypeVisitor,
  SQLValueRepresentationVisitor,
} from '../value-types/visitors/';

import {
  type IOTypeImplementation,
  type IoTypeVisitor,
} from './io-type-implementation';
import { type } from 'os';

export enum TableImpl {
  Polars,
  Typescript,
}

export interface TableColumn {
  getValueType(): ValueType;
  getName(): string;
  as_array(): readonly InternalValueRepresentation[];
  nth(n: number): InternalValueRepresentation | undefined;
  getImpl(): TableImpl;
}

export abstract class AbstractTableColumn implements TableColumn {
  abstract getImpl(): TableImpl;
  abstract getValueType(): ValueType<InternalValueRepresentation>;
  abstract getName(): string;
  abstract as_array(): readonly InternalValueRepresentation[];
  abstract nth(n: number): InternalValueRepresentation | undefined;
}

export class PolarsTableColumn extends AbstractTableColumn {
  constructor(public series: pl.Series) {
    super();
  }

  override getValueType(): ValueType<PolarsAtomicInternalValueRepresentation> {
    throw new Error('getValueType() not implemented.');
  }
  override getName(): string {
    return this.series.name;
  }

  override as_array(): readonly PolarsAtomicInternalValueRepresentation[] {
    throw new Error('as_arry() not implemented.');
  }

  override nth(n: number): PolarsAtomicInternalValueRepresentation | undefined {
    const e = this.series.getIndex(n) as unknown;
    if (e instanceof DataType) {
      return e;
    }
    return undefined;
  }

  override getImpl(): TableImpl {
    return TableImpl.Polars;
  }
}

export class TsTableColumn<
  T extends TsInternalValueRepresentation = TsInternalValueRepresentation,
> extends AbstractTableColumn {
  constructor(
    public name: string,
    public values: T[],
    public valueType: ValueType<T>,
  ) {
    super();
  }

  override getValueType(): ValueType<T> {
    return this.valueType;
  }

  override getName(): string {
    return this.name;
  }

  override as_array(): readonly T[] {
    return this.values;
  }

  override nth(n: number): T | undefined {
    return this.values.at(n);
  }

  override getImpl(): TableImpl {
    return TableImpl.Typescript;
  }
}

export type TsTableRow = Record<string, TsInternalValueRepresentation>;

/**
 * Invariant: the shape of the table is always a rectangle.
 * This means all columns must have the same size.
 */

// export interface Table extends IOTypeImplementation<IOType.TABLE> {
//   withColumn(column: TableColumn): Table;
//   filter<F>(cond: F): Table;
//   getNumberOfRows(): number;
//   getNumberOfColumns(): number;
//   hasColumn(name: string): boolean;
//   getColumns(): ReadonlyArray<TableColumn>;
//   getColumn(name: string): TableColumn | undefined;
//   getRow(id: number): InternalValueRepresentation[];
//   clone(): Table;
//   acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;
//   getImpl(): TableImpl;
// }

export abstract class Table implements IOTypeImplementation<IOType.TABLE> {
  public readonly ioType = IOType.TABLE;

  abstract withColumn(column: AbstractTableColumn): Table;
  abstract filter<F>(cond: F): Table;
  abstract getNumberOfRows(): number;
  abstract getNumberOfColumns(): number;
  abstract hasColumn(name: string): boolean;
  abstract getColumns(): ReadonlyArray<AbstractTableColumn>;
  abstract getColumn(name: string): AbstractTableColumn | undefined;
  abstract getRow(id: number): InternalValueRepresentation[];
  abstract clone(): Table;
  abstract acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;
  abstract getImpl(): TableImpl;

  static newEmpty(impl: TableImpl): Table {
    switch (impl) {
      case TableImpl.Polars:
        return new PolarsTable(pl.DataFrame({}));
      case TableImpl.Typescript:
        return new TsTable(0);
    }
  }

  static generateDropTableStatement(tableName: string): string {
    return `DROP TABLE IF EXISTS "${tableName}";`;
  }

  generateInsertValuesStatement(tableName: string): string {
    const valueRepresentationVisitor = new SQLValueRepresentationVisitor();

    const columnNames = [
      ...this.getColumns().map((c) => {
        return c.getName();
      }),
    ];
    const formattedRowValues: string[] = [];
    for (let rowIndex = 0; rowIndex < this.getNumberOfRows(); ++rowIndex) {
      const rowValues: string[] = [];
      for (const columnName of columnNames) {
        const column = this.getColumn(columnName);
        const entry = column?.nth(rowIndex);
        assert(entry !== undefined);
        const formattedValue = column
          ?.getValueType()
          .acceptVisitor(valueRepresentationVisitor)(entry);
        assert(formattedValue !== undefined);
        rowValues.push(formattedValue);
      }
      formattedRowValues.push(`(${rowValues.join(',')})`);
    }

    const formattedColumns = columnNames.map((c) => `"${c}"`).join(',');

    return `INSERT INTO "${tableName}" (${formattedColumns}) VALUES ${formattedRowValues.join(
      ', ',
    )}`;
  }

  generateCreateTableStatement(tableName: string): string {
    const columnTypeVisitor = new SQLColumnTypeVisitor();

    const columns = [...this.getColumns()];
    const columnStatements = columns.map((column) => {
      return `"${column.getName()}" ${column
        .getValueType()
        .acceptVisitor(columnTypeVisitor)}`;
    });

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnStatements.join(
      ',',
    )});`;
  }
}

export class PolarsTable extends Table {
  df: pl.DataFrame = pl.DataFrame();

  public constructor(df: pl.DataFrame) {
    super();
    this.df = df;
  }

  override withColumn(column: PolarsTableColumn): PolarsTable {
    const ndf = this.df.withColumn(column.series);
    return new PolarsTable(ndf);
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

  override filter<F>(cond: F): PolarsTable {
    throw new Error('filter() not implemented.');
  }

  override getColumns(): readonly PolarsTableColumn[] {
    const seriess = this.df.getColumns();
    return seriess.map((s) => {
      return new PolarsTableColumn(s);
    });
  }

  override getColumn(name: string): PolarsTableColumn | undefined {
    try {
      const s = this.df.getColumn(name);
      return new PolarsTableColumn(s);
    } catch {
      return undefined;
    }
  }

  override getRow(id: number): PolarsAtomicInternalValueRepresentation[] {
    return this.df.row(id).map((cell) => {
      if (cell instanceof DataType) {
        return cell;
      }
      throw new Error('Expected a pola-rs datatype');
    });
  }

  override clone(): PolarsTable {
    return new PolarsTable(this.df.clone());
  }

  override acceptVisitor<R>(visitor: IoTypeVisitor<R>): R {
    return visitor.visitPolarsTable(this);
  }

  override getImpl(): TableImpl {
    return TableImpl.Polars;
  }
}

export class TsTable extends Table {
  private numberOfRows = 0;

  private columns = new Map<string, TsTableColumn>();

  public constructor(numberOfRows = 0) {
    super();
    this.numberOfRows = numberOfRows;
  }

  override withColumn(column: TsTableColumn): TsTable {
    assert(column.values.length === this.numberOfRows);
    const nt = this.clone();
    nt.columns.set(column.name, column);
    return nt;
  }

  override filter<F>(cond: F): TsTable {
    throw new Error('filter() not implemented.');
  }

  /**
   * Tries to add a new row to this table.
   * NOTE: This method will only add the row if the table has at least one column!
   * @param row data of this row for each column
   */
  addRow(row: TsTableRow): void {
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
      column.values.push(value);
    });

    this.numberOfRows++;
  }

  addColumn(name: string, column: TsTableColumn): void {
    assert(column.values.length === this.numberOfRows);
    this.columns.set(name, column);
  }

  dropRow(rowId: number): void {
    assert(rowId < this.numberOfRows);

    this.columns.forEach((column) => {
      column.values.splice(rowId, 1);
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
      const cell = col.as_array().at(rowId);
      if (cell === undefined) {
        throw new Error(`Unexpected undefined for cell in row ${rowId}`);
      }
      return cell;
    });
  }

  override clone(): TsTable {
    const cloned = new TsTable(this.numberOfRows);
    cloned.columns = structuredClone(this.columns);
    return cloned;
  }

  override acceptVisitor<R>(visitor: IoTypeVisitor<R>): R {
    return visitor.visitTsTable(this);
  }

  override getImpl(): TableImpl {
    return TableImpl.Typescript;
  }
}
