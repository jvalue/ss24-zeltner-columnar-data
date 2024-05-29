import pl, { DataFrame, DataType, Float64, Int64, col } from "nodejs-polars";
import { TupleType } from "typescript";

function mfold(
	df: DataFrame,
	names: string[],
	operation: (s1: pl.Series, s2: pl.Series) => pl.Series,
): DataFrame {
	const ns = df.select(pl.col(names)).fold(operation);
	return df.withColumn(ns);
}

function test1() {
	const df = pl.DataFrame({
		nrs: [1, 2, 3, undefined, 5],
		names: ["foo", "ham", "spam", "egg", undefined],
		groups: ["A", "A", "B", "C", "B"],
	});

	console.log(df.select(pl.col(["nrs", "groups"])));

	const ndf = mfold(df, ["nrs", "groups"], (s1, s2) => {
		return s1.add(s2);
	});

	console.log(ndf);
}

function createTable(
	rows: string[][],
	cols: string[],
	schema: Record<string, DataType>,
): pl.DataFrame {
	const plcols = cols.map((cName, cIdx) => {
		const cData = rows.map((row) => row[cIdx]);
		return pl.Series(cName, cData, schema[cName]);
	});
	return pl.DataFrame(plcols);
}

function test2() {
	const data = [
		[1, 2, 3, undefined, 5],
		["foo", "ham", "spam", "egg", undefined],
		["A", "A", "B", "C", "B"],
	];
	const df = pl.DataFrame(data, {
		columns: ["nrs", "food", "c"],
		schema: {
			nrs: pl.Int32,
			food: pl.String,
			c: pl.String,
		},
	});
	// console.log(df.toString());

	const ndf = createTable(
		[
			["1", "foo", "A"],
			["2", "ham", "A"],
		],
		["nrs", "food", "c"],
		{
			nrs: pl.Int64,
			food: pl.String,
			c: pl.Utf8,
		},
	);
	console.log(ndf);
}

function test3() {
	const dt = pl.List(pl.Int64);
	let ret = "unset";
	switch (dt) {
		case pl.String:
			ret = "String";
			break;
		case pl.Int64:
			ret = "int";
			break;
		case pl.List(pl.Int64):
			ret = dt.variant;
			break;
		default:
			ret = "default";
			break;
	}
}

function test4() {
	const df = pl.DataFrame({
		nrs: [1, 2, 3, undefined, 5],
		onrs: [1, 2, 3, undefined, 5],
		names: ["foo", "ham", "spam", "egg", undefined],
		groups: ["A", "A", "B", "C", "B"],
	});
	const ndf = df.select(pl.col("nrs").minus(1).alias("mi"));
	console.log(ndf);
}

function test5() {
	const df = pl.DataFrame({});
	let series = pl.repeat(5, 3).cast(pl.UInt8);
	console.log(series);
	const expr = df.withColumn(pl.col(series));
	console.log(expr);
}

function test6() {
	const ser: pl.Series = pl.Series([]);
	console.log(ser instanceof pl.Series);
	const expr: pl.Expr = pl.col("somename");
	console.log(expr instanceof pl.Series);
}

function test7() {
	function foo(): pl.Expr | pl.Series {
		return pl.col("someCol");
	}

	function typeguard_expr(x: pl.Expr | pl.Series): x is pl.Expr {
		return !("name" in x);
	}

	const x: pl.Expr | pl.Series = foo();
	if ("name" in x) {
		console.log(x.name);
	}
}

function test8() {
	const df = pl.DataFrame({
		nrs: [1, 2, 3, undefined, 5],
		names: ["foo", "ham", "spam", "egg", undefined],
		groups: ["A", "A", "B", "C", "B"],
	});

	const lit = pl.lit(5)

	console.log(df.select(pl.col("nrs").mul(lit)));
}

function main() {
	test8();
}

main();
