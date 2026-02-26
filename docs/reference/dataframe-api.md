# DataFrame API Reference

## Construction

- `DataFrame.fromRows(rows)`
- `DataFrame.fromColumns(columns)`

## Core Transformations

- `select`, `drop`, `rename`, `cast`
- `filter`, `sortBy`, `groupBy`, `join`
- `withColumn`, `derive`, `apply`

## Reshape and Set Ops

- `pivot`, `melt`, `transpose`, `explode`
- `concat`, `union`, `intersection`, `difference`

## New Convenience Methods

- `assign(otherDf)`: merge columns from another DataFrame; collisions overwrite.
- `relocate(columns, { before | after })`: reorder columns around an anchor.
- `reify()`: materialize a copied DataFrame for value isolation.
- `lookup(other, on, values?)`: key-based enrichment with one output row per left row.
- `spread(column, options?)`: expand arrays/objects into columns.
- `unroll(columnOrColumns, options?)`: expand arrays into rows.
- `impute(values, { expand })`: fill nulls and optionally generate missing key combinations.

## I/O Methods

- `fromCSV`, `toCSV`
- `fromJSON`, `toJSON`, `toNDJSON`
- `fromExcel`, `toExcel`
- `fromParquet`, `toParquet`
- `fromArrow`, `toArrow`
- `toSQL`
