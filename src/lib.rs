#![deny(clippy::all)]

use std::fs;

use adbc_core::Connection;
use napi_derive::napi;
use polars::prelude::*;
use rayon::prelude::*;

#[napi]
pub fn generate_insert_table(path: String) -> String {
  let file = fs::File::open(path).expect("path wasn't valid");
  let df = IpcReader::new(file)
    .set_rechunk(true)
    .finish()
    .expect("Should be valid df");
  generate_insert_table_internal(df)
}

fn generate_insert_table_internal(df: DataFrame) -> String {
  let x = df
    .iter()
    .par_bridge()
    .map(|ser| ser.iter().par_bridge().map(|cell| cell_sql(cell)));
  todo!()
}

// FIXME: https://stackoverflow.com/a/64499219
fn transpose2<Outer, Inner, T>(v: Outer) -> Vec<Vec<T>>
where
  Outer: ParallelIterator<Item = Inner>,
  Inner: ParallelIterator<Item = T>,
{
  let len = v[0].len();
  let mut iters: Vec<_> = v.collect();
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
    AnyValue::String(s) => s.to_string(),
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
    AnyValue::StringOwned(s) => s.to_string(),
    AnyValue::Binary(_) => todo!(),
    AnyValue::BinaryOwned(_) => todo!(),
  }
}
