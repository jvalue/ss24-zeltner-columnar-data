// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD,
  INTERNAL_VALUE_REPRESENTATION_TYPEGUARD,
  IOType,
  type InternalValueRepresentation,
  type ValueType,
  type ValueTypeProvider,
} from '@jvalue/jayvee-language-server';
import { zipWith } from 'fp-ts/lib/Array.js';
import { pl } from 'nodejs-polars';

import { type ExecutionContext } from '../../execution-context';
import {
  SQLColumnTypeVisitor,
  SQLValueRepresentationVisitor,
} from '../value-types/visitors/';

import {
  type IOTypeImplementation,
  type IoTypeVisitor,
} from './io-type-implementation';

export abstract class TableColumn {
  abstract getValueType(provider: ValueTypeProvider): ValueType;
  abstract getName(): string;
  abstract nth(n: number): InternalValueRepresentation | undefined | null;
  abstract clone(): TableColumn;

  abstract isPolars(): this is PolarsTableColumn;
  abstract isTypescript(): this is TsTableColumn;
}

export class PolarsTableColumn extends TableColumn {
  constructor(private series: pl.Series) {
    super();
  }

  override getValueType(provider: ValueTypeProvider): ValueType {
    return provider.fromPolarsDType(this.series.dtype);
  }

  override getName(): string {
    return this.series.name;
  }

  override nth(n: number): InternalValueRepresentation | undefined | null {
    const nth = this.series.getIndex(n) as unknown;
    if (INTERNAL_VALUE_REPRESENTATION_TYPEGUARD(nth)) {
      return nth;
    }
    if (nth == null) {
      return null;
    }
    throw new Error(
      `Expected InternalRepresentation not ${JSON.stringify(
        nth,
      )} (${typeof nth})`,
    );
  }

  override clone(): PolarsTableColumn {
    return new PolarsTableColumn(this.series.clone());
  }

  override isPolars(): this is PolarsTableColumn {
    return true;
  }

  override isTypescript(): this is TsTableColumn {
    return false;
  }

  getSeries(): Readonly<pl.Series> {
    return this.series;
  }
}

export class TsTableColumn<
  T extends InternalValueRepresentation = InternalValueRepresentation,
> extends TableColumn {
  constructor(
    public name: string,
    public valueType: ValueType<T>,
    public values: T[] = [],
  ) {
    super();
  }

  override getValueType(): ValueType<T> {
    return this.valueType;
  }

  override getName(): string {
    return this.name;
  }

  override nth(n: number): T | undefined {
    return this.values.at(n);
  }

  override clone(): TsTableColumn {
    // HACK: This feels wrong, but I didn't find any other solution
    const clonedName: unknown = JSON.parse(JSON.stringify(this.name));
    assert(typeof clonedName === 'string');
    const clonedValues: unknown = JSON.parse(JSON.stringify(this.values));
    assert(
      INTERNAL_ARRAY_REPRESENTATION_TYPEGUARD(clonedValues) &&
        clonedValues.every((e) =>
          this.valueType.isInternalValueRepresentation(e),
        ),
    );
    // INFO: `this.valueType` must not be cloned, because the typesystem depends
    // on it being the same object
    return new TsTableColumn(clonedName, this.valueType, clonedValues);
  }

  override isPolars(): this is PolarsTableColumn {
    return false;
  }

  override isTypescript(): this is TsTableColumn<T> {
    return true;
  }

  push(x: T) {
    if (this.valueType.isInternalValueRepresentation(x)) {
      this.values.push(x);
    }
  }
}

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
  public constructor(public df: pl.DataFrame = pl.DataFrame()) {
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

  override generateCreateTableStatement(
    tableName: string,
    context: ExecutionContext,
  ): string {
    const columnTypeVisitor = new SQLColumnTypeVisitor();

    const columnStatements = this.getColumns().map((column) => {
      return `"${column.getName()}" ${column
        .getValueType(context.valueTypeProvider)
        .acceptVisitor(columnTypeVisitor)}`;
    });

    return `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnStatements.join(
      ',',
    )});`;
  }

  override withColumn(column: PolarsTableColumn): PolarsTable {
    const ndf = this.df.withColumn(column.getSeries());

    return new PolarsTable(ndf);
  }

  withColumnByExpr(expr: pl.Expr): PolarsTable {
    const ndf = this.df.withColumn(expr);
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

  override getRow(id: number): TableRow {
    const row = this.df.row(id);
    if (TABLEROW_TYPEGUARD(row)) {
      return row;
    }
    throw new Error(`row: Expected InternalRepresentation not ${typeof row} `);
  }

  override clone(): PolarsTable {
    return new PolarsTable(this.df.clone());
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
}

export class TsTable extends Table {
  public constructor(
    private numberOfRows = 0,
    private columns = new Map<string, TsTableColumn>(),
  ) {
    super();
  }

  override withColumn(column: TsTableColumn): TsTable {
    assert(column.values.length === this.numberOfRows);
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
      column.values.push(value);
    });

    this.numberOfRows++;
  }

  addColumn(name: string, column: TsTableColumn): void {
    assert(column.values.length === this.numberOfRows);
    column.name = name;
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
            : column.getValueType().acceptVisitor(valueRepresentationVisitor)(
                entry,
              );

        rowValues.push(formattedValue);
      }
      formattedRowValues.push(`(${rowValues.join(',')})`);
    }

    const formattedColumns = columns.map((c) => `"${c.getName()}"`).join(',');

    return `INSERT INTO "${tableName}" (${formattedColumns}) VALUES ${formattedRowValues.join(
      ', ',
    )}`;
  }

  override generateCreateTableStatement(tableName: string): string {
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
