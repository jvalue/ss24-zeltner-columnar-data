#![deny(clippy::all)]
#![feature(iterator_try_collect)]

use arrow::{error::ArrowError, ipc::reader::FileReader, record_batch::RecordBatch};
use connector_arrow::{
  api::{Append, Connector, SchemaEdit},
  sqlite::SQLiteConnection,
};
use napi::Status;
use napi_derive::napi;
use std::fs;

fn ipc_reader(path: &str) -> Result<FileReader<fs::File>, napi::Error> {
  println!("Opening ArrowIPC file {path}");
  let file = fs::File::open(path).map_err(|e| napi::Error::from(e))?;
  FileReader::try_new(file, None).map_err(|e| {
    napi::Error::<Status>::from_reason(format!("{path} is not a valid arrow ipc file: {e}"))
  })
}

fn db_connection(sqlite_path: &str) -> Result<SQLiteConnection, napi::Error> {
  println!("Opening database file {sqlite_path}");
  rusqlite::Connection::open(sqlite_path)
    .map(|conn| SQLiteConnection::new(conn))
    .map_err(|e| {
      napi::Error::<Status>::from_reason(format!(
        "Could not create a connection to the sqlite database: {e}"
      ))
    })
}

fn append<'conn, A, I>(mut appender: A, batches: I) -> Result<usize, napi::Error>
where
  A: Append<'conn>,
  I: Iterator<Item = Result<RecordBatch, ArrowError>>,
{
  let mut inserted_rows = 0;
  println!("Inserting rows");
  for batch in batches {
    match batch {
      Ok(batch) => {
        let tmp = batch.num_rows();
        appender.append(batch).map_err(|e| {
          napi::Error::<Status>::from_reason(format!("Could not append batch: {e}"))
        })?;
        inserted_rows += tmp;
      }
      Err(e) => {
        return Err(napi::Error::<Status>::from_reason(format!(
          "Could not read batch: {e}"
        )))
      }
    }
  }
  appender
    .finish()
    .map_err(|e| napi::Error::<Status>::from_reason(format!("Could not finish appending: {e}")))?;

  Ok(inserted_rows)
}

#[napi]
pub fn load_sqlite(
  ipc_path: String,
  table_name: String,
  sqlite_path: String,
  drop_table: bool,
) -> Result<(), napi::Error> {
  let reader = ipc_reader(&ipc_path)?;
  let mut conn = db_connection(&sqlite_path)?;

  if drop_table {
    println!("Dropping previous table {table_name} if it exists");
    let res = conn.table_drop(&table_name);
    if let Err(e) = res {
      eprintln!("Did not drop table {table_name}: {e}")
    }
  }

  println!("Creating table {table_name}");
  conn
    .table_create(&table_name, reader.schema())
    .map_err(|e| {
      napi::Error::<Status>::from_reason(format!("Could not create table {table_name}: {e}"))
    })?;

  let appender = conn.append(&table_name).map_err(|e| {
    napi::Error::<Status>::from_reason(format!("Could begin to insert values: {e}"))
  })?;
  let inserted_rows = append(appender, reader)?;
  println!("Inserted {inserted_rows} rows");
  Ok(())
}
