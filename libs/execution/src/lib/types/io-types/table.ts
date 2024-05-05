// SPDX-FileCopyrightText: 2023 Friedrich-Alexander-Universitat Erlangen-Nurnberg
//
// SPDX-License-Identifier: AGPL-3.0-only

// eslint-disable-next-line unicorn/prefer-node-protocol
import { strict as assert } from 'assert';

import {
  IOType,
  type InternalValueRepresentation,
  type PolarsAtomicInternalValueRepresentation,
  PolarsInternalValueRepresentation,
  type TsInternalValueRepresentation,
  type ValueType,
} from '@jvalue/jayvee-language-server';
import { zip, zipWith } from 'fp-ts/lib/ReadonlyNonEmptyArray';
import { DataType, pl } from 'nodejs-polars';

import {
  type IOTypeImplementation,
  type IoTypeVisitor,
} from './io-type-implementation';

export interface TableColumn {
  getValueType(): ValueType;
  getName(): string;
  as_array(): readonly InternalValueRepresentation[];
  nth(n: number): InternalValueRepresentation | undefined;
}

export abstract class AbstractTableColumn implements TableColumn {
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
    throw new Error('Method not implemented.');
  }
  override getName(): string {
    return this.series.name;
  }

  override as_array(): readonly PolarsAtomicInternalValueRepresentation[] {
    throw new Error('Method not implemented.');
  }

  override nth(n: number): PolarsAtomicInternalValueRepresentation | undefined {
    const e = this.series.getIndex(n) as unknown;
    if (e instanceof DataType) {
      return e;
    }
    return undefined;
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
}

/**
 * Invariant: the shape of the table is always a rectangle.
 * This means all columns must have the same size.
 */

export interface Table extends IOTypeImplementation<IOType.TABLE> {
  withColumn(column: TableColumn): Table;
  filter<F>(cond: F): Table;
  getNumberOfRows(): number;
  getNumberOfColumns(): number;
  hasColumn(name: string): boolean;
  getColumns(): ReadonlyArray<TableColumn>;
  getColumn(name: string): TableColumn | undefined;
  getRow(id: number): InternalValueRepresentation[];
  clone(): Table;
  acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;
}

export abstract class AbstractTable implements Table {
  public readonly ioType = IOType.TABLE;

  abstract withColumn(column: AbstractTableColumn): AbstractTable;
  abstract filter<F>(cond: F): AbstractTable;
  abstract getNumberOfRows(): number;
  abstract getNumberOfColumns(): number;
  abstract hasColumn(name: string): boolean;
  abstract getColumns(): ReadonlyArray<AbstractTableColumn>;
  abstract getColumn(name: string): AbstractTableColumn | undefined;
  abstract getRow(id: number): InternalValueRepresentation[];
  abstract clone(): AbstractTable;
  abstract acceptVisitor<R>(visitor: IoTypeVisitor<R>): R;
}

export class PolarsTable extends AbstractTable {
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

  override filter<F>(_cond: F): Table {
    throw new Error('Method not implemented.');
  }

  override getColumns(): readonly PolarsTableColumn[] {
    const seriess = this.df.getColumns();
    return seriess.map((s, _i, _ss) => {
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
}

export class TsTable extends AbstractTable {
  private numberOfRows = 0;

  private columns = new Map<string, TsTableColumn>();

  public constructor(numberOfRows = 0) {
    super();
    this.numberOfRows = numberOfRows;
  }

  override withColumn<T extends TsInternalValueRepresentation>(
    column: TsTableColumn<T>,
  ): TsTable {
    assert(column.values.length === this.numberOfRows);
    const nt = this.clone();
    nt.columns.set(column.name, column);
    return nt;
  }

  override filter<F>(cond: F): TsTable {
    throw new Error('Metho not implemented.');
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
}
