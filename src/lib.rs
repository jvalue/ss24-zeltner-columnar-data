#![deny(clippy::all)]

use std::fs;

use napi_derive::napi;
use polars::prelude::*;
use rayon::prelude::*;

#[napi]
pub fn load_sqlite(ipc_path: String, table_name: String, sqlite_path: String, replace: bool) {
  let file = fs::File::open(ipc_path).expect("path wasn't valid");
  let df = IpcReader::new(file)
    .set_rechunk(true)
    .finish()
    .expect("Should be valid df");
  let st = generate_insert_table(&df, &table_name);
  println!("{st:?}");
  let ins = generate_create_table(&df, &table_name, replace);
  let conn = rusqlite::Connection::open(sqlite_path).expect("connection failed");
  conn.execute(&format!("DROP TABLE {table_name}"), []);
  conn.execute(&ins, []).unwrap();
  conn.execute(&st, []).unwrap();
  println!("{ins}\n{st}");
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

fn generate_create_table(df: &DataFrame, table_name: &str, replace: bool) -> String {
  let sch = df.schema();
  let sch = sch
    .into_iter()
    .par_bridge()
    .map(|(c, t)| format!("{c} {}", dtype_sql(&t)))
    .collect::<Vec<_>>()
    .join(",\n");
  format!("CREATE TABLE {table_name} (\n{sch}\n);")
}

fn generate_insert_table(df: &DataFrame, table_name: &str) -> String {
  format!(
    "INSERT INTO {table_name} ({})\nVALUES\n{};",
    generate_columns(df),
    generate_values(df)
  )
}

fn generate_columns(df: &DataFrame) -> String {
  df.get_column_names().join(", ")
}

fn generate_values(df: &DataFrame) -> String {
  let columns = df
    .iter()
    // .par_bridge()
    .map(|ser| {
      ser
        .iter()
        // .par_bridge()
        .map(|cell| cell_sql(cell))
        .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>();
  let rows = transpose(columns);
  let formatted_values = rows
    .into_par_iter()
    .map(|row| format!("({})", row.join(", ")))
    .collect::<Vec<_>>()
    .join(",\n");
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

fn cell_sql(val: AnyValue) -> String {
  match val {
    AnyValue::Null => "NULL".to_string(),
    AnyValue::Boolean(b) => if b { "TRUE" } else { "FALSE" }.to_string(),
    AnyValue::String(s) => format!("'{s}'"),
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
    AnyValue::StringOwned(s) => format!("'{s}'"),
    AnyValue::Binary(_) => todo!(),
    AnyValue::BinaryOwned(_) => todo!(),
  }
}
