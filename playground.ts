import pl, { DataFrame } from "nodejs-polars";

function mfold(
  df: DataFrame,
  names: string[],
  operation: (s1: pl.Series, s2: pl.Series) => pl.Series,
): DataFrame {
  const ns = df.select(pl.col(names)).fold(operation);
  return df.withColumn(ns);
}

function main() {
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

main();
