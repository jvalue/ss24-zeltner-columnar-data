import pl from "nodejs-polars";

const fooSeries = pl.Series("foo", [1, 2, 3]);
const sum = fooSeries.sum();
console.log(sum);
