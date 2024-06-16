#!/usr/bin/env bash

ts_name="$1"
npm run "example:$ts_name"

db_name="$2"
sql_ts=$ts_name.sql
sqlite3 "$db_name" .dump > "$sql_ts"


polars_name="$ts_name-polars"
npm run "example:$polars_name"

sql_polars=$polars_name.sql
sqlite3 "$db_name" .dump > "$sql_polars"

diff -s "$sql_ts" "$sql_polars"
