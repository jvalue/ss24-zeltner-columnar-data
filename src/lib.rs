#![deny(clippy::all)]

use std::{cmp::Ordering, fs};

use napi::Status;
use napi_derive::napi;
use polars::prelude::*;
use rayon::prelude::*;
use rusqlite::Connection;

#[napi]
pub fn load_sqlite(
  ipc_path: String,
  table_name: String,
  sqlite_path: String,
  drop_table: bool,
) -> Result<(), napi::Error> {
  println!("Opening ArrowIPC file {ipc_path}");
  let file = fs::File::open(ipc_path).map_err(|e| napi::Error::from(e))?;
  println!("Reading dataframe");

  let df = IpcReader::new(file)
    .set_rechunk(true)
    .finish()
    .map_err(|e| {
      napi::Error::<Status>::from_reason(format!(
        "The ArrowIPC file did not contain a valid pola.rs dataframe: {e}"
      ))
    })?;

  println!("Opening database file {sqlite_path}");
  let conn = rusqlite::Connection::open(sqlite_path).map_err(|e| {
    napi::Error::<Status>::from_reason(format!(
      "Could not create a connection to the sqlite database: {e}"
    ))
  })?;

  if drop_table {
    println!("Dropping previous table {table_name} if it exists");
    conn
      .execute(&format!("DROP TABLE {table_name}"), [])
      .map_err(|e| {
        napi::Error::<Status>::from_reason(format!("Could not drop table {table_name}: {e}"))
      })?;
  }

  println!("Creating table {table_name}");
  let create_table = generate_create_table(&df, &table_name);
  conn.execute(&create_table, []).map_err(|e| {
    napi::Error::<Status>::from_reason(format!("Could not create table {table_name}: {e}"))
  })?;

  insert_table(&conn, &df, &table_name)
}

fn generate_create_table(df: &DataFrame, table_name: &str) -> String {
  let sch = df.schema();
  let sch = sch
    .into_iter()
    .map(|(c, t)| format!("[{c}] {}", dtype_sql(&t)))
    .collect::<Vec<_>>()
    .join(", ");
  format!("CREATE TABLE IF NOT EXISTS {table_name} ({sch});")
}

fn insert_table(conn: &Connection, df: &DataFrame, table_name: &str) -> Result<(), napi::Error> {
  println!("Inserting {} row(s) into table {table_name}", df.height());
  let mut inserted = 0;
  let rows = generate_values(df);
  let iter = rows.chunks(10000);
  let cols = generate_columns(df);
  for chunk in iter {
    let sql = format!(
      "INSERT INTO {table_name} ({}) VALUES {};",
      cols,
      chunk.join(",\n")
    );
    inserted += conn.execute(&sql, []).map_err(|e| {
      napi::Error::<Status>::from_reason(format!("Statement failed with error {e}:\n{sql}"))
    })?;
  }
  match inserted.cmp(&df.height()) {
    Ordering::Less => Err(napi::Error::<Status>::from_reason(format!(
      "{} row(s) were not inserted into {table_name}",
      df.height()
    ))),
    Ordering::Equal => {
      println!("The data was successfully loaded into the database");
      Ok(())
    }
    Ordering::Greater => unreachable!("Cannot insert more rows than there are in the dataframe"),
  }
}

fn generate_columns(df: &DataFrame) -> String {
  df.get_column_names()
    .into_iter()
    .map(|col_name| format!("[{col_name}]"))
    .collect::<Vec<_>>()
    .join(", ")
}

fn generate_values(df: &DataFrame) -> Vec<String> {
  let columns = df
    .iter()
    .collect::<Vec<_>>()
    .into_par_iter()
    .map(|ser| {
      ser
        .iter()
        .collect::<Vec<_>>()
        .into_par_iter()
        .map(|cell| cell_sql(cell))
        .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>();
  let rows = transpose(columns);
  let formatted_values = rows
    .into_par_iter()
    .map(|row| format!("({})", row.join(", ")))
    .collect::<Vec<_>>();
  formatted_values
}

// FIXME: https://stackoverflow.com/a/64499219
fn transpose<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
  assert!(!v.is_empty());
  let len = v[0].len();
  let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
  (0..len)
    .map(|_| {
      iters
        .iter_mut()
        .map(|n| n.next().unwrap())
        .collect::<Vec<T>>()
    })
    .collect()
}

fn dtype_sql(dt: &polars::datatypes::DataType) -> &'static str {
  match dt {
    DataType::Boolean => "BOOL",
    DataType::UInt8
    | DataType::UInt16
    | DataType::UInt32
    | DataType::UInt64
    | DataType::Int8
    | DataType::Int16
    | DataType::Int32
    | DataType::Int64 => "INT",
    DataType::Float32 | DataType::Float64 => "FLOAT",
    DataType::String => "VARCHAR",
    DataType::Binary
    | DataType::BinaryOffset
    | DataType::Date
    | DataType::Datetime(_, _)
    | DataType::Duration(_)
    | DataType::Time
    | DataType::List(_)
    | DataType::Null
    | DataType::Unknown(_) => todo!(),
  }
}

fn cell_sql(val: AnyValue) -> String {
  match val {
    AnyValue::Null => "NULL".to_string(),
    AnyValue::Boolean(b) => if b { "TRUE" } else { "FALSE" }.to_string(),
    // NOTE: This is necessary to escape single quotes inside the string
    // Sadly, it's really bad for performance
    AnyValue::String(s) => format!("'{}'", s.replace('\'', "\'\'")),
    AnyValue::UInt8(i) => i.to_string(),
    AnyValue::UInt16(i) => i.to_string(),
    AnyValue::UInt32(i) => i.to_string(),
    AnyValue::UInt64(i) => i.to_string(),
    AnyValue::Int8(i) => i.to_string(),
    AnyValue::Int16(i) => i.to_string(),
    AnyValue::Int32(i) => i.to_string(),
    AnyValue::Int64(i) => i.to_string(),
    AnyValue::Float32(i) => i.to_string(),
    AnyValue::Float64(i) => i.to_string(),
    AnyValue::Date(_) => todo!(),
    AnyValue::Datetime(_, _, _) => todo!(),
    AnyValue::Duration(_, _) => todo!(),
    AnyValue::Time(_) => todo!(),
    AnyValue::List(_) => todo!(),
    // NOTE: This is necessary to escape single quotes inside the string
    // Sadly, it's really bad for performance
    AnyValue::StringOwned(s) => format!("'{}'", s.replace('\'', "\'\'")),
    AnyValue::Binary(_) => todo!(),
    AnyValue::BinaryOwned(_) => todo!(),
  }
}
