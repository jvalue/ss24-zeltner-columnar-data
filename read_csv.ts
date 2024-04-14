import process from "node:process";

import pl from "nodejs-polars";

let path = process.argv[2];
const csv = pl.readCSV(path);
console.log(csv);
