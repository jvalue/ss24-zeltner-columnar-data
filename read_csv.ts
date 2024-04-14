import process from "node:process";

import pl, { DataFrame } from "nodejs-polars";
import { Database } from "sqlite3";

function getColsString(
  df: pl.DataFrame,
  f: (col: pl.Series) => string,
): string {
  let col_names: string[] = [];
  for (const col of df.getColumns()) {
    col_names.push(f(col));
  }
  return col_names.join(", ");
}

function createTable(db: Database, df: DataFrame, name: string) {
  const nameAndDType = (col: pl.Series) => {
    return `${col.name}`;
  };
  db.run(
    `CREATE TABLE IF NOT EXISTS cars (${getColsString(df, nameAndDType)})`,
  );
}

function insertValues(db: Database, df: DataFrame, table_name: string) {
  const name = (col: pl.Series) => {
    return `${col.name}`;
  };
  let values = [];
  for (const row of df.rows()) {
    let r = [];
    for (const cell of row) {
      r.push(`${cell}`);
    }
    values.push(`(${row.join(", ")})`);
  }
  const query = `INSERT INTO ${table_name} (${getColsString(df, name)}) VALUES ${values.join(", ")}`;
  console.log(query);
  db.run(query);
}

function writeSQLite(df: pl.DataFrame, table: string) {
  const db = new Database("./db.sqlite");
  createTable(db, df, table);
  insertValues(db, df, table);
}

let path = process.argv[2];
const df = pl.readCSV(path);
writeSQLite(df, "cars");
